use std::collections::{HashMap, HashSet};
use std::fmt;
use std::ops::{BitAnd, BitOr, Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use bytes::BufMut;
use datafusion::arrow::datatypes::DataType;
use datafusion::scalar::ScalarValue;
use models::predicate::domain::{utf8_from, ColumnDomains, Domain, Range};
use models::tag::{self, TagFromParts};
use models::{utils, SeriesId, SeriesKey, Tag, TagKey, TagValue};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::RwLock;
use trace::{debug, error, info};

use super::binlog::{AddSeries, DeleteSeries, IndexBinlog, IndexBinlogBlock, UpdateSeriesKey};
use super::cache::ForwardIndexCache;
use super::{IndexEngine, IndexError, IndexResult};
use crate::file_system::file::IFile;
use crate::file_system::file_manager;
use crate::index::binlog::{BinlogReader, BinlogWriter};
use crate::index::ts_index::fmt::Debug;
use crate::{byte_utils, file_utils, UpdateSetValue};

const SERIES_ID_PREFIX: &str = "_id_";
const SERIES_KEY_PREFIX: &str = "_key_";
const DELETED_SERIES_KEY_PREFIX: &str = "_deleted_key_";
const AUTO_INCR_ID_KEY: &str = "_auto_incr_id";

/// Used to maintain forward and inverted indexes
///
/// # Example
///
/// The following is an index relationship diagram
///
/// In the following example, there are two records whose series keys are SeriesKey1 and SeriesKey2
///
/// SeriesKey1: Table1 T1=1a,T2=2a,T3=3a
///
/// SeriesKey2: Table1 T1=1b,T2=2b,T3=3a
///
///
/// ```text
/// SeriesKey1
/// ┌────────┐
/// │Table1  │◄───────────────────────── ┌──────────┐
/// ├──┬──┬──┤                           │SeriesId-1│
/// │T1│T2│T3│ ────────────────────────► └──────────┘
/// │1a│2a│3a│                            ▲  ▲  ▲
/// └┬─┴┬─┴─┬┘                            │  │  │
///  │  │   │                             │  │  │
///  │  │   └─────────────────────────────┘  │  │
///  │  │                                    │  │
///  │  └────────────────────────────────────┘  │
///  │                                          │
///  └──────────────────────────────────────────┘
///
///     
/// ┌────────┐
/// │Table1  │◄───────────────────────── ┌──────────┐
/// ├──┬──┬──┤                           │SeriesId-2│
/// │T1│T2│T3│ ────────────────────────► └──────────┘
/// │1b│2b│3a│                            ▲  ▲  ▲
/// └┬─┴┬─┴─┬┘                            │  │  │
///  │  │   │                             │  │  │
///  │  │   └─────────────────────────────┘  │  │
///  │  │                                    │  │
///  │  └────────────────────────────────────┘  │
///  │                                          │
///  └──────────────────────────────────────────┘
///
/// point1 with SeriesKey1(Table1 T1=1a,T2=2a,T3=3a), the generated indexes are
///     _key_Table1_T1_1a_T2_2a_T3_3a -> SeriesId-1
///     Table1.T1=1a -> SeriesId-1                    ──┐  
///     Table1.T2=2a -> SeriesId-1                      │---- Inverted index, used to filter data based on tag value
///     Table1.T3=3a -> SeriesId-1                    ──┘
///     _id_SeriesId-1 -> SeriesKey1                  ------- Used to construct the result
///
/// point2 with SeriesKey1(Table1 T1=1b,T2=2b,T3=3a), the generated indexes are
///     _key_Table1_T1_1b_T2_2b_T3_3a -> SeriesId-2
///     Table1.T1=1b -> SeriesId-2
///     Table1.T2=2b -> SeriesId-2
///     Table1.T3=3a -> SeriesId-2
///     _id_SeriesId-2 -> SeriesKey2
/// ```
pub struct TSIndex {

    // 存储路径信息
    path: PathBuf,
    // 下一个数据的id
    incr_id: AtomicU32,
    // 当前已经写入的数量
    write_count: AtomicU32,

    // 通过该对象进行binlog的读写  binlog记录的是有关series的变化
    binlog: Arc<RwLock<IndexBinlog>>,
    // 利用基数树 存储索引信息  并且存储的是倒排索引 (由第三方库实现)
    storage: Arc<RwLock<IndexEngine>>,
    // 正向缓存 直接存储seriesId 与 series table/tag 信息
    forward_cache: ForwardIndexCache,
    // 通知监听binlog的对象
    binlog_change_sender: UnboundedSender<()>,
}

// TsIndex 维护了需要的各种索引
impl TSIndex {
    pub async fn new(path: impl AsRef<Path>) -> IndexResult<Arc<Self>> {
        let path = path.as_ref();

        let binlog = IndexBinlog::new(path).await?;
        let storage = IndexEngine::new(path)?;

        // 可能有残留数据
        let incr_id = match storage.get(AUTO_INCR_ID_KEY.as_bytes())? {
            Some(data) => byte_utils::decode_be_u32(&data),
            None => 0,
        };

        let (binlog_change_sender, binlog_change_reciver) = unbounded_channel();

        let mut ts_index = Self {
            binlog: Arc::new(RwLock::new(binlog)),
            storage: Arc::new(RwLock::new(storage)),
            incr_id: AtomicU32::new(incr_id),
            write_count: AtomicU32::new(0),
            path: path.into(),
            forward_cache: ForwardIndexCache::new(1_000_000),
            binlog_change_sender,
        };

        // 在启动时 先通过binlog文件重建索引
        ts_index.recover().await?;
        let ts_index = Arc::new(ts_index);

        // 运行后台任务监听binlog的变化 每次针对series的操作会写入到binlog中 然后在后台构建索引
        run_index_job(ts_index.clone(), binlog_change_reciver);
        info!(
            "Recovered index dir '{}', incr_id start at: {incr_id}",
            path.display()
        );

        Ok(ts_index)
    }

    // 尝试恢复数据
    async fn recover(&mut self) -> IndexResult<()> {
        let path = self.path.clone();
        // 遍历该目录下所有文件
        let files = file_manager::list_file_names(&path);
        for filename in files.iter() {

            // 读取binlog文件
            if let Ok(file_id) = file_utils::get_index_binlog_file_id(filename) {
                let file_path = path.join(filename);
                info!("Recovering index binlog: '{}'", file_path.display());
                let tmp_file = BinlogWriter::open(file_id, &file_path).await?;
                let mut reader_file = BinlogReader::new(file_id, tmp_file.file.into()).await?;
                self.recover_from_file(&mut reader_file).await?;
            }
        }

        Ok(())
    }

    // 写入binlog数据
    async fn write_binlog(&self, blocks: &[IndexBinlogBlock]) -> IndexResult<()> {

        // 通过 binlog对象 与底层文件交互
        self.binlog.write().await.write_blocks(blocks).await?;
        // 发送消息通知后台任务
        self.binlog_change_sender
            .send(())
            .map_err(|e| IndexError::IndexStroage {
                msg: format!("Send binlog change failed, err: {}", e),
            })?;

        Ok(())
    }

    /// Only add deleted series key to sid mapping
    async fn add_deleted_series(&self, id: SeriesId, series_key: &SeriesKey) -> IndexResult<()> {
        let key_buf = encode_deleted_series_key(series_key.table(), series_key.tags());
        self.storage.write().await.set(&key_buf, &id.to_be_bytes())
    }

    async fn remove_deleted_series(&self, series_key: &SeriesKey) -> IndexResult<()> {
        let key_buf = encode_deleted_series_key(series_key.table(), series_key.tags());
        self.storage.write().await.delete(&key_buf)
    }

    // 这里只是构建series的索引信息  因为本对象只是一个索引
    async fn add_series(&self, id: SeriesId, series_key: &SeriesKey) -> IndexResult<()> {
        let mut storage_w = self.storage.write().await;

        // 对series数据进行编码 并写入到buf  这个就是倒排索引  通过table，tags反查 series
        let key_buf = encode_series_key(series_key.table(), series_key.tags());
        storage_w.set(&key_buf, &id.to_be_bytes())?;
        // 这里是正向查询
        storage_w.set(&encode_series_id_key(id), &series_key.encode())?;

        for tag in series_key.tags() {
            // 按照标签来构建倒排索引  因为标签是很容易重复的 所以这里用modify 将不同的id追加到倒排索引结构
            let key = encode_inverted_index_key(series_key.table(), &tag.key, &tag.value);
            storage_w.modify(&key, id, true)?;
        }
        // 空的标签也算一种key
        if series_key.tags().is_empty() {
            let key = encode_inverted_index_key(series_key.table(), &[], &[]);
            storage_w.modify(&key, id, true)?;
        }

        Ok(())
    }

    // 从binlog文件中恢复数据
    async fn recover_from_file(&mut self, reader_file: &mut BinlogReader) -> IndexResult<()> {
        let mut max_id = self.incr_id.load(Ordering::Relaxed);
        while let Some(block) = reader_file.next_block().await? {
            match block {

                // 读取数据块
                IndexBinlogBlock::Add(blocks) => {
                    for block in blocks {
                        // 使用series的数据构建索引
                        self.add_series(block.series_id(), block.data()).await?;

                        // 更新当前id
                        if max_id < block.series_id() {
                            max_id = block.series_id()
                        }
                    }
                }
                // 删除索引
                IndexBinlogBlock::Delete(block) => {
                    // delete series
                    self.del_series_id_from_engine(block.series_id()).await?;
                }

                // series数据发生了变化  用它更新倒排索引
                IndexBinlogBlock::Update(block) => {
                    let series_id = block.series_id();
                    let new_series_key = block.new_series();
                    let old_series_key = block.old_series();
                    let recovering = block.recovering();

                    trace::debug!(
                        "Recover update series: {:?}, series_id: {:?}, old_series_key: {:?}",
                        new_series_key,
                        series_id,
                        old_series_key,
                    );

                    // 代表这个block块是在什么时候添加的
                    self.del_series_id_from_engine(series_id).await?;
                    if recovering {
                        self.remove_deleted_series(old_series_key).await?;
                    } else {
                        // The modified key can be found when restarting and recover wal.
                        // deleted的数据 前缀是不一样的
                        self.add_deleted_series(series_id, old_series_key).await?;
                    }
                    // 为新的table/tag构建正向/倒排索引
                    self.add_series(series_id, new_series_key).await?;
                }
            }
        }

        // 此时已经完成了从binlog重建缓存的工作了 更新最大记录的id
        self.incr_id.store(max_id, Ordering::Relaxed);

        let id_bytes = self.incr_id.load(Ordering::Relaxed).to_be_bytes();
        self.storage
            .write()
            .await
            .set(AUTO_INCR_ID_KEY.as_bytes(), &id_bytes)?;

        // 对索引数据进行刷盘
        self.storage.write().await.flush()?;
        reader_file.advance_read_offset(0).await?;

        Ok(())
    }

    // 获取被标记成删除的 series
    pub async fn get_deleted_series_id(&self, series_key: &SeriesKey) -> IndexResult<Option<u32>> {
        let key_buf = encode_deleted_series_key(series_key.table(), series_key.tags());
        if let Some(val) = self.storage.read().await.get(&key_buf)? {
            let id = byte_utils::decode_be_u32(&val);
            return Ok(Some(id));
        }

        Ok(None)
    }

    pub async fn get_series_id(&self, series_key: &SeriesKey) -> IndexResult<Option<u32>> {
        // 通过hash加速查询
        if let Some(id) = self.forward_cache.get_series_id_by_key(series_key) {
            return Ok(Some(id));
        }

        // 未找到的情况下 利用倒排索引查询
        let key_buf = encode_series_key(series_key.table(), series_key.tags());
        if let Some(val) = self.storage.read().await.get(&key_buf)? {
            let id = byte_utils::decode_be_u32(&val);
            // 并加入到正向查询缓存 加速下次查询
            self.forward_cache.add(id, series_key.clone());

            return Ok(Some(id));
        }

        Ok(None)
    }

    pub async fn get_series_key(&self, sid: SeriesId) -> IndexResult<Option<SeriesKey>> {
        // 先通过正向缓存 使用id 查询key信息
        if let Some(key) = self.forward_cache.get_series_key_by_id(sid) {
            return Ok(Some(key));
        }

        let series_key = self.storage.read().await.get(&encode_series_id_key(sid))?;
        if let Some(res) = series_key {
            let key = SeriesKey::decode(&res)
                .map_err(|e| IndexError::DecodeSeriesKey { msg: e.to_string() })?;

            // 加入到缓存中 加速下次查询
            self.forward_cache.add(sid, key.clone());

            return Ok(Some(key));
        }

        Ok(None)
    }

    // 仅当series数据不存在时 触发添加
    pub async fn add_series_if_not_exists(
        &self,
        series_keys: Vec<SeriesKey>,
    ) -> IndexResult<Vec<u32>> {
        let mut ids = Vec::with_capacity(series_keys.len());
        let mut blocks_data = Vec::new();
        for series_key in series_keys.into_iter() {
            let key_buf = encode_series_key(series_key.table(), series_key.tags());
            {
                let mut storage_w = self.storage.write().await;

                // 先查看 table/tag 能否检索到数据  存在的情况下 加入到ids
                if let Some(val) = storage_w.get(&key_buf)? {
                    ids.push(byte_utils::decode_be_u32(&val));
                    continue;
                }

                // 还不存在该series数据
                let id = self.incr_id.fetch_add(1, Ordering::Relaxed) + 1;
                // 先写缓存 后写 binlog
                storage_w.set(&key_buf, &id.to_be_bytes())?;
                let block = AddSeries::new(utils::now_timestamp_nanos(), id, series_key.clone());
                ids.push(id);
                blocks_data.push(block);

                // write cache  顺便加入到正向缓存
                self.forward_cache.add(id, series_key);
            }
        }

        // 写入binlog
        self.write_binlog(&[IndexBinlogBlock::Add(blocks_data)])
            .await?;

        Ok(ids)
    }

    // 检查是否需要刷盘
    async fn check_to_flush(&self, force: bool) -> IndexResult<()> {
        let count = self.write_count.fetch_add(1, Ordering::Relaxed);
        if !force && count < 20000 {
            return Ok(());
        }

        let mut storage_w = self.storage.write().await;
        let id_bytes = self.incr_id.load(Ordering::Relaxed).to_be_bytes();
        // 记录最新id 并手动刷盘
        storage_w.set(AUTO_INCR_ID_KEY.as_bytes(), &id_bytes)?;
        storage_w.flush()?;

        let current_id;
        {
            let mut binlog_w = self.binlog.write().await;
            binlog_w.advance_write_offset(0).await?;
            current_id = binlog_w.current_write_file_id();
        }

        let log_dir = self.path.clone();
        let files = file_manager::list_file_names(&log_dir);

        // 因为索引数据已经写入磁盘了 旧的binlog就不需要了
        for filename in files.iter() {

            // 其余binlog文件要删除
            if let Ok(file_id) = file_utils::get_index_binlog_file_id(filename) {
                if current_id != file_id {
                    let _ = std::fs::remove_file(log_dir.join(filename));
                }
            }
        }

        self.write_count.store(0, Ordering::Relaxed);

        Ok(())
    }

    // 将删除series的操作作用到缓存上
    pub async fn del_series_info(&self, sid: u32) -> IndexResult<()> {
        // first write binlog
        let block = IndexBinlogBlock::Delete(DeleteSeries::new(sid));

        // 写入binlog 同时发送消息
        self.write_binlog(&[block]).await?;
        // TODO @Subsegment only delete mapping: key -> sid
        // then delete forward index and inverted index
        self.del_series_id_from_engine(sid).await?;

        let _ = self.check_to_flush(false).await;

        Ok(())
    }

    // 删除某个series相关的索引
    async fn del_series_id_from_engine(&self, sid: u32) -> IndexResult<()> {
        let mut storage_w = self.storage.write().await;

        // 先尝试从正向索引读取 seriesKey的数据
        let series_key = match self.forward_cache.get_series_key_by_id(sid) {
            Some(k) => Some(k),

            // 在倒排索引中也存储了一份正向数据
            None => match storage_w.get(&encode_series_id_key(sid))? {
                Some(res) => {
                    let key = SeriesKey::decode(&res)
                        .map_err(|e| IndexError::DecodeSeriesKey { msg: e.to_string() })?;
                    Some(key)
                }
                None => None,
            },
        };

        // 删除正向数据
        let _ = storage_w.delete(&encode_series_id_key(sid));
        if let Some(series_key) = series_key {
            // 删除缓存中数据
            self.forward_cache.del(sid, series_key.hash());
            // 删除倒排数据
            let key_buf = encode_series_key(series_key.table(), series_key.tags());
            let _ = storage_w.delete(&key_buf);
            for tag in series_key.tags() {
                let key = encode_inverted_index_key(series_key.table(), &tag.key, &tag.value);
                storage_w.modify(&key, sid, false)?;
            }
        }

        Ok(())
    }

    // 根据领域来获取series
    pub async fn get_series_ids_by_domains(
        &self,
        tab: &str,  // 这个应该是table的意思
        tag_domains: &ColumnDomains<String>,
    ) -> IndexResult<Vec<u32>> {

        // 代表匹配所有记录 不传标签
        if tag_domains.is_all() {
            // Match all records
            debug!("pushed tags filter is All.");
            return self.get_series_id_list(tab, &[]).await;
        }

        // domain为空 返回空值
        if tag_domains.is_none() {
            // Does not match any record, return null
            debug!("pushed tags filter is None.");
            return Ok(vec![]);
        }

        // safe: tag_domains is not empty
        // 除开特殊情况后 读取domain的信息
        let domains = unsafe { tag_domains.domains_unsafe() };

        debug!("Index get sids: pushed tag_domains: {:?}", domains);
        let mut series_ids = vec![];
        // 每个domain的结果分开存储
        for (k, v) in domains.iter() {
            let rb = self.get_series_ids_by_domain(tab, k, v).await?;
            // 每个domain相关的分开存储
            series_ids.push(rb);
        }

        debug!(
            "Index get sids: filter scan result series_ids: {:?}",
            series_ids
        );

        // 将所有id叠在一起
        let result = series_ids
            .into_iter()
            .reduce(|p, c| p.bitand(c))
            .unwrap_or_default()
            .into_iter()
            .collect::<Vec<_>>();

        Ok(result)
    }

    // 通过标签反查所有 seriesId
    pub async fn get_series_id_list(&self, tab: &str, tags: &[Tag]) -> IndexResult<Vec<u32>> {
        let res = self.get_series_id_bitmap(tab, tags).await?.iter().collect();
        Ok(res)
    }

    // 通过标签 利用倒排索引反查所有系列id
    pub async fn get_series_id_bitmap(
        &self,
        tab: &str,
        tags: &[Tag],
    ) -> IndexResult<roaring::RoaringBitmap> {

        // 使用位图记录id
        let mut bitmap = roaring::RoaringBitmap::new();
        let storage_r = self.storage.read().await;

        // 代表查找所有标签  使用前缀查找
        if tags.is_empty() {
            let prefix = format!("{}.", tab);
            let it = storage_r.prefix(prefix.as_bytes())?;
            for val in it {
                let val = val.map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;
                let rb = storage_r.load_rb(&val.1)?;

                bitmap = bitmap.bitor(rb);
            }
        } else {
            // 通过table/tab 生成key
            let key = encode_inverted_index_key(tab, &tags[0].key, &tags[0].value);
            if let Some(rb) = storage_r.get_rb(&key)? {
                bitmap = rb;
            }

            for tag in &tags[1..] {
                let key = encode_inverted_index_key(tab, &tag.key, &tag.value);
                if let Some(rb) = storage_r.get_rb(&key)? {
                    // 追加到位图上
                    bitmap = bitmap.bitand(rb);
                } else {
                    return Ok(roaring::RoaringBitmap::new());
                }
            }
        }

        Ok(bitmap)
    }

    // 查询该标签key 相关的所有数据 (也就是没有指定标签value)
    pub async fn get_series_ids_by_tag_key(
        &self,
        tab: &str,
        tag_key: &TagKey,
    ) -> IndexResult<Vec<SeriesId>> {
        // TODO inverted index cache
        let lower_bound = encode_inverted_min_index_key(tab, tag_key);
        let upper_bound = encode_inverted_max_index_key(tab, tag_key);

        let mut bitmap = roaring::RoaringBitmap::new();
        // Search the sid list corresponding to qualified tags in the range
        let storage_r = self.storage.read().await;
        let iter = storage_r.range(lower_bound..upper_bound);
        for item in iter {
            let item = item?;
            let rb = storage_r.load_rb(&item.1)?;
            bitmap = bitmap.bitor(rb);
        }

        let series_ids = bitmap.into_iter().collect::<Vec<_>>();

        Ok(series_ids)
    }

    // 查询某个domain相关的seriesId
    pub async fn get_series_ids_by_domain(
        &self,
        tab: &str,
        tag_key: &str,
        v: &Domain,
    ) -> IndexResult<roaring::RoaringBitmap> {
        let mut bitmap = roaring::RoaringBitmap::new();
        match v {

            // 代表标签key 关联的一个范围
            Domain::Range(range_set) => {
                let storage_r = self.storage.read().await;

                // 多个范围
                for (_, range) in range_set.low_indexed_ranges().into_iter() {
                    // 生成查询条件
                    let key_range = filter_range_to_index_key_range(tab, tag_key, range);
                    let (is_equal, equal_key) = is_equal_value(&key_range);

                    // bound范围内只有一个值 简单查询
                    if is_equal {
                        if let Some(rb) = storage_r.get_rb(&equal_key)? {
                            bitmap = bitmap.bitor(rb);
                        };

                        continue;
                    }

                    // Search the sid list corresponding to qualified tags in the range
                    // 指定范围产生一个迭代器 遍历id
                    let iter = storage_r.range(key_range);
                    for item in iter {
                        let item = item?;
                        let rb = storage_r.load_rb(&item.1)?;
                        bitmap = bitmap.bitor(rb);
                    }
                }
            }
            Domain::Equtable(val) => {
                let storage_r = self.storage.read().await;

                // 代表查询包含在内的
                if val.is_white_list() {
                    // Contains the given value
                    for entry in val.entries().into_iter() {
                        let index_key = tag_value_to_index_key(tab, tag_key, entry.value());
                        if let Some(rb) = storage_r.get_rb(&index_key)? {
                            bitmap = bitmap.bitor(rb);
                        };
                    }
                } else {
                    // Does not contain a given value, that is, a value other than a given value
                    // TODO will not deal with this situation for the time being
                    // 代表忽略包含在内的  目前未实现
                    bitmap = self.get_series_id_bitmap(tab, &[]).await?;
                }
            }
            Domain::None => {
                // Normally, it will not go here unless no judgment is made at the ColumnDomains level
                // If you go here, you will directly return an empty series, because the tag condition in the map is' and '
                return Ok(roaring::RoaringBitmap::new());
            }
            Domain::All => {
                // Normally, it will not go here unless no judgment is made at the ColumnDomains level
                // The current tag is not filtered, all series are obtained, and the next tag is processed
                bitmap = self.get_series_id_bitmap(tab, &[]).await?;
            }
        };

        Ok(bitmap)
    }

    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }

    // 刷新索引数据到文件
    pub async fn flush(&self) -> IndexResult<()> {
        self.check_to_flush(true).await?;

        Ok(())
    }

    /// 记录更新series key的binlog，并且删除缓存中的sid -> series key的映射
    pub async fn update_series_key(
        &self,
        old_series_keys: Vec<SeriesKey>,
        new_series_keys: Vec<SeriesKey>,
        sids: Vec<SeriesId>,
        recovering: bool,
    ) -> IndexResult<()> {
        let waiting_delete_series = sids
            .iter()
            .zip(old_series_keys.iter())
            .map(|(sid, key)| (*sid, key.hash()))
            .collect::<Vec<_>>();

        // write binlog
        let blocks = old_series_keys
            .into_iter()
            .zip(new_series_keys.into_iter().zip(sids.into_iter()))
            .map(|(old_series, (new_series, sid))| {
                IndexBinlogBlock::Update(UpdateSeriesKey::new(
                    old_series, new_series, sid, recovering,
                ))
            })
            .collect::<Vec<_>>();

        // 将改动写入binlog
        self.write_binlog(&blocks).await?;

        // Only delete old index, not add new index
        for (sid, key_hash) in waiting_delete_series {
            self.forward_cache.del(sid, key_hash);
        }

        Ok(())
    }

    /// 获取所有匹配的旧的series key及其更新后的series key，以及对应的series id
    /// (old_series_keys, new_series_keys, sids)
    pub async fn prepare_update_tags_value(
        &self,
        new_tags: &[UpdateSetValue<TagKey, TagValue>],  // 一组要使用的新标签
        matched_series: &[SeriesKey],  // 作用在这些series上
    ) -> IndexResult<(Vec<SeriesKey>, Vec<SeriesKey>, Vec<SeriesId>)> {
        // Find all matching series ids
        let mut ids = vec![];

        // 获取就的标签
        let mut old_keys = vec![];
        for key in matched_series {
            if let Some(sid) = self.get_series_id(key).await? {
                ids.push(sid);
                old_keys.push(key.clone());
            }
        }

        // 存储所有合并后的新标签
        let mut new_keys = vec![];
        let mut new_keys_set = HashSet::new();
        for key in &old_keys {
            // modify tag value
            // 收集旧的标签
            let mut old_tags = key
                .tags
                .iter()
                .map(|Tag { key, value }| (key, Some(value.as_ref())))
                .collect::<HashMap<_, _>>();

            // 这是组合后的新标签
            for UpdateSetValue { key, value } in new_tags {
                old_tags.insert(key, value.as_deref());
            }
            // 移除值为 Null 的 tag
            let mut tags = old_tags
                .iter()
                .flat_map(|(key, value)| value.map(|val| Tag::new(key.to_vec(), val.to_vec())))
                .collect::<Vec<_>>();

            tag::sort_tags(&mut tags);

            // 产生新标签
            let new_key = SeriesKey {
                tags,
                table: key.table.clone(),
            };

            // check conflict
            // 已经存在认为是异常情况
            if self.get_series_id(&new_key).await?.is_some() {
                trace::warn!("Series already exists: {:?}", new_key);
                return Err(IndexError::SeriesAlreadyExists {
                    key: key.to_string(),
                });
            }
            // TODO 去重
            new_keys.push(new_key);
        }

        new_keys_set.extend(new_keys.clone());

        // 检查新生成的series key是否有重复key    代表出现了重复的标签
        if new_keys_set.len() != new_keys.len() {
            return Err(IndexError::SeriesAlreadyExists {
                key: "new series keys".to_string(),
            });
        }

        Ok((old_keys, new_keys, ids))
    }

    // 重命名标签
    pub async fn rename_tag(
        &self,
        table: &str,
        old_tag_name: &TagKey,
        new_tag_name: &TagKey,
        dry_run: bool,  // 代表只是进行检查
    ) -> IndexResult<()> {
        // Find all matching series ids
        // 查询旧标签关联的一组系列
        let ids = self.get_series_ids_by_tag_key(table, old_tag_name).await?;

        if ids.is_empty() {
            return Ok(());
        }

        // 存储新生成的key
        let mut new_keys = vec![];
        let mut waiting_delete_series = vec![];

        // 遍历每个系列id
        for sid in &ids {

            // 查看此时的标签 (key 就是由table和tab合成的)
            if let Some(mut key) = self.get_series_key(*sid).await? {
                let old_key_hash = key.hash();
                // 旧标签相关的这些系列会受到影响
                waiting_delete_series.push((*sid, old_key_hash));

                // modify tag key  修改标签名
                for tag in key.tags.iter_mut() {
                    if &tag.key == old_tag_name {
                        tag.key = new_tag_name.clone();
                    }
                }

                // check conflict
                if self.get_series_id(&key).await?.is_some() {
                    return Err(IndexError::SeriesAlreadyExists {
                        key: key.to_string(),
                    });
                }

                new_keys.push(key);
            }
        }

        // If only do conflict checking, wal and cache will not be written.
        if dry_run {
            return Ok(());
        }

        // write binlog
        // 生成更新操作
        let mut blocks = vec![];
        for (sid, key) in ids.into_iter().zip(new_keys.into_iter()) {
            if let Some(old_key) = self.get_series_key(sid).await? {
                let block =
                    IndexBinlogBlock::Update(UpdateSeriesKey::new(old_key, key, sid, false));
                blocks.push(block);
            } else {
                return Err(IndexError::SeriesNotExists);
            }
        }
        self.write_binlog(&blocks).await?;

        // Only delete old index, not add new index
        for (sid, key_hash) in waiting_delete_series {
            self.forward_cache.del(sid, key_hash);
        }

        Ok(())
    }
}

impl Debug for TSIndex {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Ok(())
    }
}

pub fn is_equal_value(range: &impl RangeBounds<Vec<u8>>) -> (bool, Vec<u8>) {
    if let std::ops::Bound::Included(start) = range.start_bound() {
        if let std::ops::Bound::Included(end) = range.end_bound() {
            if start == end {
                return (true, start.clone());
            }
        }
    }

    (false, vec![])
}

// 根据范围查询
pub fn filter_range_to_index_key_range(
    tab: &str,
    tag_key: &str,
    range: &Range,
) -> impl RangeBounds<Vec<u8>> {
    let start_bound = range.start_bound();
    let end_bound = range.end_bound();

    // Convert ScalarValue value to inverted index key
    // 将范围中的每个值都变成一个key
    let generate_index_key = |v: &ScalarValue| tag_value_to_index_key(tab, tag_key, v);

    // Convert the tag value in Bound to the inverted index key
    let translate_bound = |bound: Bound<&ScalarValue>, is_lower: bool| match bound {
        Bound::Unbounded => {
            let buf = if is_lower {
                encode_inverted_min_index_key(tab, tag_key.as_bytes())
            } else {
                encode_inverted_max_index_key(tab, tag_key.as_bytes())
            };
            Bound::Included(buf)
        }
        Bound::Included(v) => Bound::Included(generate_index_key(v)),
        Bound::Excluded(v) => Bound::Excluded(generate_index_key(v)),
    };

    (
        translate_bound(start_bound, true),
        translate_bound(end_bound, false),
    )
}

pub fn tag_value_to_index_key(tab: &str, tag_key: &str, v: &ScalarValue) -> Vec<u8> {
    // Tag can only be of string type
    assert_eq!(DataType::Utf8, v.get_datatype());

    // Convert a string to an inverted index key
    let generate_index_key = |tag_val| {
        let tag = Tag::from_parts(tag_key, tag_val);
        encode_inverted_index_key(tab, &tag.key, &tag.value)
    };

    unsafe { utf8_from(v).map(generate_index_key).unwrap_unchecked() }
}

pub fn encode_series_id_key(id: u32) -> Vec<u8> {
    let len = SERIES_ID_PREFIX.len() + 4;
    let mut buf = Vec::with_capacity(len);
    buf.extend_from_slice(SERIES_ID_PREFIX.as_bytes());
    buf.extend_from_slice(&id.to_be_bytes());

    buf
}

pub fn encode_inverted_max_index_key(tab: &str, tag_key: &[u8]) -> Vec<u8> {
    tab.as_bytes()
        .iter()
        .chain(".".as_bytes())
        .chain(tag_key)
        .chain(">".as_bytes())
        .cloned()
        .collect()
}

pub fn encode_inverted_min_index_key(tab: &str, tag_key: &[u8]) -> Vec<u8> {
    tab.as_bytes()
        .iter()
        .chain(".".as_bytes())
        .chain(tag_key)
        .chain("<".as_bytes())
        .cloned()
        .collect()
}

// tab.tag=val
pub fn encode_inverted_index_key(tab: &str, tag_key: &[u8], tag_val: &[u8]) -> Vec<u8> {
    tab.as_bytes()
        .iter()
        .chain(".".as_bytes())
        .chain(tag_key)
        .chain("=".as_bytes())
        .chain(tag_val)
        .cloned()
        .collect()
}

pub fn encode_deleted_series_key(tab: &str, tags: &[Tag]) -> Vec<u8> {
    encode_series_key_with_prefix(DELETED_SERIES_KEY_PREFIX, tab, tags)
}

pub fn encode_series_key(tab: &str, tags: &[Tag]) -> Vec<u8> {
    encode_series_key_with_prefix(SERIES_KEY_PREFIX, tab, tags)
}

fn encode_series_key_with_prefix(prefix: &str, tab: &str, tags: &[Tag]) -> Vec<u8> {
    let mut len = prefix.len() + 2 + tab.len();
    for tag in tags.iter() {
        len += 2 + tag.key.len();
        len += 2 + tag.value.len();
    }

    let mut buf = Vec::with_capacity(len);
    buf.extend_from_slice(prefix.as_bytes());
    buf.extend_from_slice(&(tab.len() as u16).to_be_bytes());
    buf.extend_from_slice(tab.as_bytes());
    for tag in tags.iter() {
        buf.extend_from_slice(&(tag.key.len() as u16).to_be_bytes());
        buf.extend_from_slice(&tag.key);

        buf.extend_from_slice(&(tag.value.len() as u16).to_be_bytes());
        buf.extend_from_slice(&tag.value);
    }

    buf
}

pub fn decode_series_id_list(data: &[u8]) -> IndexResult<Vec<u32>> {
    if data.len() % 4 != 0 {
        return Err(IndexError::DecodeSeriesIDList);
    }

    let count = data.len() / 4;
    let mut list: Vec<u32> = Vec::with_capacity(count);
    for i in 0..count {
        let id = byte_utils::decode_be_u32(&data[i * 4..]);
        list.push(id);
    }

    Ok(list)
}

pub fn encode_series_id_list(list: &[u32]) -> Vec<u8> {
    let mut data: Vec<u8> = Vec::with_capacity(list.len() * 4);
    for i in list {
        data.put_u32(*i);
    }

    data
}

// 运行一个后台任务
pub fn run_index_job(ts_index: Arc<TSIndex>, mut binlog_change_reciver: UnboundedReceiver<()>) {
    tokio::spawn(async move {
        let path = ts_index.path.clone();

        // 记录每个binlog文件此时处理到的偏移量
        let mut handle_file = HashMap::new();

        // 接收binlog日志 并进行处理
        while (binlog_change_reciver.recv().await).is_some() {
            let files = file_manager::list_file_names(&path);

            // 遍历所有文件
            for filename in files.iter() {
                if let Ok(file_id) = file_utils::get_index_binlog_file_id(filename) {
                    let file_path = path.join(filename);
                    let file = match file_manager::open_file(&file_path).await {
                        Ok(f) => f,
                        Err(e) => {
                            error!(
                                "Open index binlog file '{}' failed, err: {}",
                                file_path.display(),
                                e
                            );
                            continue;
                        }
                    };

                    // 已经处理过了
                    if file.len() <= *handle_file.get(&file_id).unwrap_or(&0) {
                        continue;
                    }

                    let mut reader_file = match BinlogReader::new(file_id, file.into()).await {
                        Ok(r) => r,
                        Err(e) => {
                            error!(
                                "Open index binlog file '{}' failed, err: {}",
                                file_path.display(),
                                e
                            );
                            continue;
                        }
                    };

                    // 定位到上次处理的位置
                    if let Some(pos) = handle_file.get(&file_id) {
                        let res = reader_file.seek(*pos);
                        if let Err(e) = res {
                            error!(
                                "Seek index binlog file '{}' failed, err: {}",
                                file_path.display(),
                                e
                            );
                        }
                    }

                    // 不断的读取binlog数据块
                    while let Ok(Some(block)) = reader_file.next_block().await {
                        if reader_file.pos() <= *handle_file.get(&file_id).unwrap_or(&0) {
                            continue;
                        }

                        match block {

                            // 根据不同类型 走不同处理逻辑
                            IndexBinlogBlock::Add(blocks) => {
                                for block in blocks {
                                    let _ = ts_index
                                        .add_series(block.series_id(), block.data())
                                        .await
                                        .map_err(|err| {
                                            error!("Add series failed, err: {}", err);
                                        });
                                }

                                let _ = ts_index.check_to_flush(false).await;
                                handle_file.insert(file_id, reader_file.pos());
                            }
                            IndexBinlogBlock::Delete(block) => {
                                // delete series
                                let _ = ts_index.del_series_id_from_engine(block.series_id()).await;
                                let _ = ts_index.check_to_flush(false).await;
                                handle_file.insert(file_id, reader_file.pos());
                            }
                            IndexBinlogBlock::Update(block) => {
                                let series_id = block.series_id();
                                let new_series_key = block.new_series();
                                let old_series_key = block.old_series();
                                let recovering = block.recovering();

                                trace::debug!(
                                    "update series: {:?}, series_id: {:?}, old_series_key: {}",
                                    new_series_key,
                                    series_id,
                                    old_series_key,
                                );

                                let _ = ts_index
                                    .del_series_id_from_engine(series_id)
                                    .await
                                    .map_err(|err| {
                                        error!("Delete series failed for Update, err: {}", err);
                                    });

                                // 代表此时正处于 启动 tskv 并恢复数据的阶段
                                if recovering {
                                    // 清理标记为删除状态的series key
                                    let _ = ts_index
                                        .remove_deleted_series(old_series_key)
                                        .await
                                        .map_err(|err| {
                                            error!(
                                                "Add deleted series failed for Update, err: {}",
                                                err
                                            );
                                        });
                                } else {
                                    // The modified key can be found when restarting and recover wal.
                                    let _ = ts_index
                                        .add_deleted_series(series_id, old_series_key)
                                        .await
                                        .map_err(|err| {
                                            error!(
                                                "Add deleted series failed for Update, err: {}",
                                                err
                                            );
                                        });
                                }

                                let _ = ts_index
                                    .add_series(series_id, new_series_key)
                                    .await
                                    .map_err(|err| {
                                        error!("Add series failed for Update, err: {}", err);
                                    });

                                let _ = ts_index.check_to_flush(false).await;
                                handle_file.insert(file_id, reader_file.pos());
                            }
                        }
                    }
                }
            }
        }
    });
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use models::schema::ExternalTableSchema;
    use models::{SeriesId, SeriesKey, Tag};

    use super::TSIndex;
    use crate::UpdateSetValue;

    /// ( sid, database, table, [(tag_key, tag_value)] )
    type SeriesKeyDesc<'a> = (SeriesId, &'a str, &'a str, Vec<(&'a str, &'a str)>);

    fn build_series_keys(series_keys_desc: &[SeriesKeyDesc<'_>]) -> Vec<SeriesKey> {
        let mut series_keys = Vec::with_capacity(series_keys_desc.len());
        for (_, _, table, tags) in series_keys_desc {
            series_keys.push(SeriesKey {
                tags: tags
                    .iter()
                    .map(|(k, v)| Tag::new(k.as_bytes().to_vec(), v.as_bytes().to_vec()))
                    .collect(),
                table: table.to_string(),
            });
        }
        series_keys
    }

    #[tokio::test]
    async fn test_index() {
        let dir = "/tmp/test/ts_index/1";
        let _ = std::fs::remove_dir_all(dir);
        let database = "db_test";
        let mut max_sid = 0;
        {
            // Generic tests.
            #[rustfmt::skip]
            let series_keys_desc: Vec<SeriesKeyDesc> = vec![
                (0, database, "table_test", vec![("loc", "bj"), ("host", "h1")]),
                (0, database, "table_test", vec![("loc", "bj"), ("host", "h2")]),
                (0, database, "table_test", vec![("loc", "bj"), ("host", "h3")]),
                (0, database, "ma", vec![("ta", "a1"), ("tb", "b1")]),
                (0, database, "ma", vec![("ta", "a1"), ("tb", "b1")]),
                (0, database, "table_test", vec![("loc", "nj"), ("host", "h1")]),
                (0, database, "table_test", vec![("loc", "nj"), ("host", "h2")]),
                (0, database, "table_test", vec![("loc", "nj"), ("host", "h3")]),
            ];
            let series_keys = build_series_keys(&series_keys_desc);
            // Test build_series_key()
            assert_eq!(series_keys_desc.len(), series_keys.len());
            for ((_, _, table, tags), series_key) in series_keys_desc.iter().zip(series_keys.iter())
            {
                assert_eq!(*table, &series_key.table);
                assert_eq!(tags.len(), series_key.tags.len());
                for ((k, v), tag) in tags.iter().zip(series_key.tags.iter()) {
                    assert_eq!(k.as_bytes(), tag.key.as_slice());
                    assert_eq!(v.as_bytes(), tag.value.as_slice());
                }
            }

            let ts_index = TSIndex::new(dir).await.unwrap();
            // Insert series into index.
            let mut series_keys_sids = Vec::with_capacity(series_keys_desc.len());
            for (i, series_key) in series_keys.iter().enumerate() {
                let sid = ts_index
                    .add_series_if_not_exists(vec![series_key.clone()])
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(100)).await;
                let last_key = ts_index.get_series_key(sid[0]).await.unwrap().unwrap();
                assert_eq!(series_key.to_string(), last_key.to_string());

                max_sid = max_sid.max(sid[0]);
                println!("test_index#1: series {i} - '{series_key}' - id: {:?}", sid);
                series_keys_sids.push(sid[0]);
            }

            // Test get series from index
            for (i, series_key) in series_keys.iter().enumerate() {
                let sid = ts_index.get_series_id(series_key).await.unwrap().unwrap();
                assert_eq!(series_keys_sids[i], sid);

                let list = ts_index
                    .get_series_id_list(&series_key.table, &series_key.tags)
                    .await
                    .unwrap();
                assert_eq!(vec![sid], list);
            }

            // Test query series list
            let query_t = "table_test";
            let query_k = b"host".to_vec();
            let query_v = b"h2".to_vec();
            let mut list_1 = ts_index
                .get_series_id_list(query_t, &[Tag::new(query_k.clone(), query_v.clone())])
                .await
                .unwrap();
            list_1.sort();
            let mut list_2 = Vec::with_capacity(list_1.len());
            for (i, series_key) in series_keys.iter().enumerate() {
                if series_key.table == query_t
                    && series_key
                        .tags
                        .iter()
                        .any(|t| t.key == query_k && t.value == query_v)
                {
                    list_2.push(series_keys_sids[i]);
                }
            }
            list_2.sort();
            assert_eq!(list_1, list_2);

            // Test delete series
            ts_index.del_series_info(series_keys_sids[1]).await.unwrap();
            assert_eq!(ts_index.get_series_id(&series_keys[1]).await.unwrap(), None);
            let list = ts_index.get_series_id_list(query_t, &[]).await.unwrap();
            assert_eq!(list.len(), 5);
        }

        {
            // Test re-open, query and insert.
            let ts_index = TSIndex::new(dir).await.unwrap();
            let list = ts_index
                .get_series_id_list("table_test", &[])
                .await
                .unwrap();
            assert_eq!(list.len(), 5);

            #[rustfmt::skip]
            let series_keys_desc: Vec<SeriesKeyDesc> = vec![
                (0, database, "table_test", vec![("loc", "dj"), ("host", "h1")]),
                (0, database, "table_test", vec![("loc", "dj"), ("host", "h2")]),
                (0, database, "ma", vec![("ta", "a2"), ("tb", "b2")]),
                (0, database, "ma", vec![("ta", "a2"), ("tb", "b2")]),
                (0, database, "table_test", vec![("loc", "xj"), ("host", "h1")]),
                (0, database, "table_test", vec![("loc", "xj"), ("host", "h2")]),
            ];
            let series_keys = build_series_keys(&series_keys_desc);

            // Test insert after re-open.
            let prev_max_sid = max_sid;
            for (i, series_key) in series_keys.iter().enumerate() {
                let sid = ts_index
                    .add_series_if_not_exists(vec![series_key.clone()])
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(100)).await;
                let last_key = ts_index.get_series_key(sid[0]).await.unwrap().unwrap();
                assert_eq!(series_key.to_string(), last_key.to_string());

                assert!(sid[0] > prev_max_sid);
                max_sid = max_sid.max(sid[0]);
                println!("test_index#2: series {i} - '{series_key}' - id: {:?}", sid);
            }
        }

        // Test re-open, do not insert and then re-open.
        let ts_index = TSIndex::new(dir).await.unwrap();
        drop(ts_index);
        let ts_index = TSIndex::new(dir).await.unwrap();
        #[rustfmt::skip]
        let series_keys_desc: Vec<SeriesKeyDesc> = vec![
            (0, database, "table_test", vec![("loc", "dbj"), ("host", "h1")]),
            (0, database, "table_test", vec![("loc", "dnj"), ("host", "h2")]),
            (0, database, "table_test", vec![("loc", "xbj"), ("host", "h1")]),
            (0, database, "table_test", vec![("loc", "xnj"), ("host", "h2")]),
        ];
        let series_keys = build_series_keys(&series_keys_desc);
        let prev_max_sid = max_sid;
        for (i, series_key) in series_keys.iter().enumerate() {
            let sid = ts_index
                .add_series_if_not_exists(vec![series_key.clone()])
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
            let last_key = ts_index.get_series_key(sid[0]).await.unwrap().unwrap();
            assert_eq!(series_key.to_string(), last_key.to_string());

            assert!(sid[0] > prev_max_sid);
            println!("test_index#3: series {i} - '{series_key}' - id: {:?}", sid);
        }
    }

    #[test]
    fn test_serde() {
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("address", DataType::Utf8, false),
            Field::new("priority", DataType::UInt8, false),
        ]);

        let schema = ExternalTableSchema {
            tenant: "cnosdb".to_string(),
            db: "hello".to_string(),
            name: "world".to_string(),
            file_compression_type: "test".to_string(),
            file_type: "1".to_string(),
            location: "2".to_string(),
            target_partitions: 3,
            table_partition_cols: vec![("4".to_string(), DataType::UInt8)],
            has_header: true,
            delimiter: 5,
            schema,
        };

        let ans_inter = serde_json::to_string(&schema).unwrap();
        let ans = serde_json::from_str::<ExternalTableSchema>(&ans_inter).unwrap();

        assert_eq!(ans, schema);
    }

    #[tokio::test]
    async fn test_rename_tag() {
        let table_name = "table";
        let dir = "/tmp/test/cnosdb/ts_index/rename_tag";
        let _ = std::fs::remove_dir_all(dir);

        let ts_index = TSIndex::new(dir).await.unwrap();

        let tags1 = vec![
            Tag::new("station".as_bytes().to_vec(), "a0".as_bytes().to_vec()),
            Tag::new("region".as_bytes().to_vec(), "SX".as_bytes().to_vec()),
        ];
        let tags2 = vec![
            Tag::new("station".as_bytes().to_vec(), "a1".as_bytes().to_vec()),
            Tag::new("region".as_bytes().to_vec(), "SN".as_bytes().to_vec()),
        ];
        let tags3 = vec![Tag::new(
            "region".as_bytes().to_vec(),
            "HB".as_bytes().to_vec(),
        )];
        let tags4 = vec![Tag::new(
            "station".as_bytes().to_vec(),
            "a1".as_bytes().to_vec(),
        )];

        let series_keys = [tags1, tags2, tags3, tags4]
            .into_iter()
            .map(|tags| SeriesKey {
                tags,
                table: table_name.to_string(),
            })
            .collect::<Vec<_>>();

        // 添加series
        let sids = ts_index
            .add_series_if_not_exists(series_keys.clone())
            .await
            .unwrap();

        assert_eq!(sids.len(), 4);

        // 校验写入成功
        for (sid, expected_series) in sids.iter().zip(series_keys.iter()) {
            let series = ts_index.get_series_key(*sid).await.unwrap().unwrap();
            assert_eq!(expected_series, &series)
        }

        // Wait for binlog to be consumed
        tokio::time::sleep(Duration::from_secs(1)).await;

        // 修改tag
        let old_tag_name = "station";
        let new_tag_name = "new_station";

        let expected_sids = ts_index
            .get_series_ids_by_tag_key(table_name, &old_tag_name.as_bytes().to_vec())
            .await
            .unwrap();

        ts_index
            .rename_tag(
                table_name,
                &old_tag_name.as_bytes().to_vec(),
                &new_tag_name.as_bytes().to_vec(),
                false,
            )
            .await
            .unwrap();

        // Wait for binlog to be consumed
        tokio::time::sleep(Duration::from_secs(1)).await;

        // 校验修改成功
        for (sid, expected_series) in sids.iter().zip(series_keys.iter()) {
            let series = ts_index.get_series_key(*sid).await.unwrap().unwrap();
            for (expected_tag, tag) in expected_series.tags.iter().zip(series.tags.iter()) {
                if expected_tag.key == old_tag_name.as_bytes().to_vec() {
                    assert_eq!(new_tag_name.as_bytes().to_vec(), tag.key);
                } else {
                    assert_eq!(expected_tag.key, tag.key);
                }
            }
        }

        let actual_sids = ts_index
            .get_series_ids_by_tag_key(table_name, &new_tag_name.as_bytes().to_vec())
            .await
            .unwrap();
        assert_eq!(expected_sids, actual_sids);
    }

    #[tokio::test]
    async fn test_update_tags_value() {
        let table_name = "table";
        let dir = "/tmp/test/cnosdb/ts_index/update_tags_value";
        let _ = std::fs::remove_dir_all(dir);

        let ts_index = TSIndex::new(dir).await.unwrap();

        let tags1 = vec![
            Tag::new("station".as_bytes().to_vec(), "a0".as_bytes().to_vec()),
            Tag::new("region".as_bytes().to_vec(), "SX".as_bytes().to_vec()),
        ];
        let waiting_update_tags = vec![
            Tag::new("station".as_bytes().to_vec(), "a1".as_bytes().to_vec()),
            Tag::new("region".as_bytes().to_vec(), "SN".as_bytes().to_vec()),
        ];
        let tags3 = vec![Tag::new(
            "region".as_bytes().to_vec(),
            "HB".as_bytes().to_vec(),
        )];
        let tags4 = vec![Tag::new(
            "station".as_bytes().to_vec(),
            "a1".as_bytes().to_vec(),
        )];

        let series_keys = [tags1, waiting_update_tags.clone(), tags3, tags4]
            .into_iter()
            .map(|tags| SeriesKey {
                tags,
                table: table_name.to_string(),
            })
            .collect::<Vec<_>>();

        // 添加series
        let sids = ts_index
            .add_series_if_not_exists(series_keys.clone())
            .await
            .unwrap();

        assert_eq!(sids.len(), 4);

        // 校验写入成功
        for (sid, expected_series) in sids.iter().zip(series_keys.iter()) {
            let series = ts_index.get_series_key(*sid).await.unwrap().unwrap();
            assert_eq!(expected_series, &series)
        }

        let matched_series = SeriesKey {
            tags: waiting_update_tags,
            table: table_name.to_string(),
        };
        let (old_series_keys, new_series_keys, matched_sids) = ts_index
            .prepare_update_tags_value(
                &[UpdateSetValue {
                    key: "station".as_bytes().to_vec(),
                    value: Some("a2".as_bytes().to_vec()),
                }],
                &[matched_series.clone()],
            )
            .await
            .unwrap();
        ts_index
            .update_series_key(old_series_keys, new_series_keys, matched_sids, false)
            .await
            .unwrap();

        // Wait for binlog to be consumed
        tokio::time::sleep(Duration::from_secs(1)).await;

        // 校验修改成功
        for (sid, expected_series) in sids.iter().zip(series_keys.iter()) {
            let series = ts_index.get_series_key(*sid).await.unwrap().unwrap();
            if series != matched_series {
                continue;
            }

            for (expected_tag, tag) in expected_series.tags.iter().zip(series.tags.iter()) {
                if expected_tag.key == "station".as_bytes().to_vec() {
                    assert_eq!("a2".as_bytes().to_vec(), tag.value);
                } else {
                    assert_eq!(expected_tag.key, tag.key);
                }
            }
        }
    }
}
