use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::fs::{remove_file, rename};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cache::ShardedAsyncCache;
use memory_pool::MemoryPoolRef;
use meta::model::MetaRef;
use metrics::metric_register::MetricsRegister;
use models::Timestamp;
use parking_lot::RwLock as SyncRwLock;
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot::Sender as OneShotSender;
use tokio::sync::RwLock;
use utils::BloomFilter;

use crate::compaction::{CompactTask, FlushReq};
use crate::context::GlobalContext;
use crate::error::{Error, Result};
use crate::file_system::file_manager::try_exists;
use crate::kv_option::{Options, StorageOptions, DELTA_PATH, TSM_PATH};
use crate::memcache::MemCache;
use crate::record_file::{Reader, RecordDataType, RecordDataVersion, Writer};
use crate::tseries_family::{ColumnFile, LevelInfo, Version};
use crate::tsm::TsmReader;
use crate::version_set::VersionSet;
use crate::{byte_utils, file_utils, ColumnFileId, LevelId, TseriesFamilyId};

const MAX_BATCH_SIZE: usize = 64;

// 描述一个数据文件
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct CompactMeta {
    pub file_id: ColumnFileId,
    pub file_size: u64,
    pub tsf_id: TseriesFamilyId,
    pub level: LevelId,
    pub min_ts: Timestamp,
    pub max_ts: Timestamp,
    pub high_seq: u64,
    pub low_seq: u64,
    pub is_delta: bool,
}

impl Default for CompactMeta {
    fn default() -> Self {
        Self {
            file_id: 0,
            file_size: 0,
            tsf_id: 0,
            level: 0,
            min_ts: Timestamp::MAX,
            max_ts: Timestamp::MIN,
            high_seq: u64::MIN,
            low_seq: u64::MIN,
            is_delta: false,
        }
    }
}

/// There are serial fields set with default value:
/// - tsf_id
/// - high_seq
/// - low_seq
impl From<&ColumnFile> for CompactMeta {
    fn from(file: &ColumnFile) -> Self {
        Self {
            file_id: file.file_id(),
            file_size: file.size(),
            level: file.level(),
            min_ts: file.time_range().min_ts,
            max_ts: file.time_range().max_ts,
            is_delta: file.is_delta(),
            ..Default::default()
        }
    }
}

impl CompactMeta {

    // 返回文件路径
    pub fn file_path(
        &self,
        storage_opt: &StorageOptions,
        database: &str,
        ts_family_id: TseriesFamilyId,
    ) -> PathBuf {
        if self.is_delta {
            let base_dir = storage_opt.delta_dir(database, ts_family_id);
            file_utils::make_delta_file(base_dir, self.file_id)
        } else {
            let base_dir = storage_opt.tsm_dir(database, ts_family_id);
            file_utils::make_tsm_file(base_dir, self.file_id)
        }
    }

    // 修改文件名
    pub async fn rename_file(
        &mut self,
        storage_opt: &StorageOptions,
        database: &str,
        ts_family_id: TseriesFamilyId,
        file_id: ColumnFileId,  // 基于新id 生成新path
    ) -> Result<PathBuf> {
        let old_name = if self.is_delta {
            let base_dir = storage_opt
                .move_dir(database, ts_family_id)
                .join(DELTA_PATH);
            file_utils::make_delta_file(base_dir, self.file_id)
        } else {
            let base_dir = storage_opt.move_dir(database, ts_family_id).join(TSM_PATH);
            file_utils::make_tsm_file(base_dir, self.file_id)
        };

        let new_name = if self.is_delta {
            let base_dir = storage_opt.delta_dir(database, ts_family_id);
            file_utils::make_delta_file(base_dir, file_id)
        } else {
            let base_dir = storage_opt.tsm_dir(database, ts_family_id);
            file_utils::make_tsm_file(base_dir, file_id)
        };
        trace::info!("rename file from {:?} to {:?}", &old_name, &new_name);
        file_utils::rename(old_name, &new_name).await?;
        self.file_id = file_id;
        Ok(new_name)
    }
}

pub struct CompactMetaBuilder {
    pub ts_family_id: TseriesFamilyId,
}

impl CompactMetaBuilder {
    pub fn new(ts_family_id: TseriesFamilyId) -> Self {
        Self { ts_family_id }
    }

    pub fn build(
        &self,
        file_id: u64,
        file_size: u64,
        level: LevelId,
        min_ts: Timestamp,
        max_ts: Timestamp,
    ) -> CompactMeta {
        CompactMeta {
            file_id,
            file_size,
            tsf_id: self.ts_family_id,
            level,
            min_ts,
            max_ts,
            is_delta: level == 0,
            ..Default::default()
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct VersionEdit {
    pub has_seq_no: bool,
    pub seq_no: u64,  // 此时出现的最大时间戳
    pub has_file_id: bool,
    pub file_id: u64,  // 当往本对象中不断添加文件时 对应最大的文件id
    pub max_level_ts: Timestamp,
    pub add_files: Vec<CompactMeta>,  // 相比之前version的变化 每个 CompactMeta中包含level和文件id
    pub del_files: Vec<CompactMeta>,

    // 代表是一个添加/移除 vnode操作
    pub del_tsf: bool,
    pub add_tsf: bool,
    pub tsf_id: TseriesFamilyId,
    pub tsf_name: String,
}

impl Default for VersionEdit {
    fn default() -> Self {
        Self {
            has_seq_no: false,
            seq_no: 0,
            has_file_id: false,
            file_id: 0,
            max_level_ts: i64::MIN,
            add_files: vec![],
            del_files: vec![],
            del_tsf: false,
            add_tsf: false,
            tsf_id: 0,
            tsf_name: String::from(""),
        }
    }
}

impl VersionEdit {
    pub fn new(vnode_id: TseriesFamilyId) -> Self {
        Self {
            tsf_id: vnode_id,
            ..Default::default()
        }
    }

    pub fn new_add_vnode(vnode_id: u32, owner: String, seq_no: u64) -> Self {
        Self {
            tsf_id: vnode_id,
            tsf_name: owner,
            add_tsf: true,
            has_seq_no: true,
            seq_no,
            ..Default::default()
        }
    }

    pub fn new_del_vnode(vnode_id: u32) -> Self {
        Self {
            tsf_id: vnode_id,
            del_tsf: true,
            ..Default::default()
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| Error::RecordFileEncode { source: (e) })
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        bincode::deserialize(buf).map_err(|e| Error::RecordFileDecode { source: (e) })
    }

    pub fn encode_vec(data: &[Self]) -> Result<Vec<u8>> {
        let mut buf: Vec<u8> = Vec::with_capacity(data.len() * 32);

        // 将多个edit编码后设置到buf中
        for ve in data {
            let ve_buf =
                bincode::serialize(ve).map_err(|e| Error::RecordFileEncode { source: (e) })?;
            let pos = buf.len();
            buf.resize(pos + 4 + ve_buf.len(), 0_u8);
            buf[pos..pos + 4].copy_from_slice((ve_buf.len() as u32).to_be_bytes().as_slice());
            buf[pos + 4..].copy_from_slice(&ve_buf);
        }

        Ok(buf)
    }

    pub fn decode_vec(buf: &[u8]) -> Result<Vec<Self>> {
        let mut list: Vec<Self> = Vec::with_capacity(buf.len() / 32 + 1);
        let mut pos = 0_usize;
        while pos < buf.len() {
            if buf.len() - pos < 4 {
                break;
            }
            let len = byte_utils::decode_be_u32(&buf[pos..pos + 4]);
            pos += 4;
            if buf.len() - pos < len as usize {
                break;
            }
            let ve = Self::decode(&buf[pos..pos + len as usize])?;
            pos += len as usize;
            list.push(ve);
        }

        Ok(list)
    }

    // 某个合并元数据 以及这些数据相关的最大时间戳
    pub fn add_file(&mut self, compact_meta: CompactMeta, max_level_ts: i64) {
        if compact_meta.high_seq != 0 {
            // ComapctMeta.seq_no only makes sense when flush.
            // In other processes, we set high_seq 0 .
            self.has_seq_no = true;
            self.seq_no = compact_meta.high_seq;
        }
        self.has_file_id = true;
        self.file_id = self.file_id.max(compact_meta.file_id);
        self.max_level_ts = max_level_ts;
        self.tsf_id = compact_meta.tsf_id;
        self.add_files.push(compact_meta);
    }

    // 标记这些文件已经被删除了  一般是在compact后 之前的旧文件就可以删除了
    pub fn del_file(&mut self, level: LevelId, file_id: u64, is_delta: bool) {
        self.del_files.push(CompactMeta {
            file_id,
            level,
            is_delta,
            ..Default::default()
        });
    }
}

impl Display for VersionEdit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "seq_no: {}, file_id: {}, add_files: {}, del_files: {}, del_tsf: {}, add_tsf: {}, tsf_id: {}, tsf_name: {}, has_seq_no: {}, has_file_id: {}, max_level_ts: {}",
               self.seq_no, self.file_id, self.add_files.len(), self.del_files.len(), self.del_tsf, self.add_tsf, self.tsf_id, self.tsf_name, self.has_seq_no, self.has_file_id, self.max_level_ts)
    }
}

// 一个描述信息 相当于是一个清单对象 一开始通过加载清单对象可以知道有哪些version 以及他们下面各level相关的数据文件
pub struct Summary {
    meta: MetaRef,  // 通过该对象与元数据服务交互
    file_no: u64,
    version_set: Arc<RwLock<VersionSet>>,  // 维护多个数据库的版本数据
    ctx: Arc<GlobalContext>,
    writer: Writer,   // 该对象与底层的某个 record文件连接 (record文件可以写入summary格式数据)
    opt: Arc<Options>,
    runtime: Arc<Runtime>,   // tokio的异步运行时
    metrics_register: Arc<MetricsRegister>,
}

impl Summary {
    // create a new summary file    创建一个新的summary对象   总结对象
    pub async fn new(
        opt: Arc<Options>,
        runtime: Arc<Runtime>,
        meta: MetaRef,
        memory_pool: MemoryPoolRef,
        metrics_register: Arc<MetricsRegister>,
    ) -> Result<Self> {
        let db = VersionEdit::default();
        // 专门有目录存储总结文件信息
        let path = file_utils::make_summary_file(opt.storage.summary_dir(), 0);

        // record文件 可以用于存储4种类型的数据 这里创建用于存储summary信息的文件
        let mut w = Writer::open(path, RecordDataType::Summary).await.unwrap();
        let buf = db.encode()?;

        // 现在写入的还是空数据
        let _ = w
            .write_record(
                RecordDataVersion::V1.into(),
                RecordDataType::Summary.into(),
                &[&buf],
            )
            .await?;
        w.sync().await?;

        Ok(Self {
            meta: meta.clone(),
            file_no: 0,
            version_set: Arc::new(RwLock::new(VersionSet::empty(
                meta.clone(),
                opt.clone(),
                runtime.clone(),
                memory_pool,
                metrics_register.clone(),
            ))),
            ctx: Arc::new(GlobalContext::default()),
            writer: w,
            opt,
            runtime,
            metrics_register,
        })
    }

    /// Recover from summary file
    ///
    /// If `load_file_filter` is `true`, field_filter will be loaded from file,
    /// otherwise default `BloomFilter::default()`
    /// 从已经存在的summary文件恢复数据
    #[allow(clippy::too_many_arguments)]
    pub async fn recover(
        meta: MetaRef,
        opt: Arc<Options>,
        runtime: Arc<Runtime>,
        memory_pool: MemoryPoolRef,
        flush_task_sender: Sender<FlushReq>,
        compact_task_sender: Sender<CompactTask>,
        load_field_filter: bool,
        metrics_register: Arc<MetricsRegister>,
    ) -> Result<Self> {
        let summary_path = opt.storage.summary_dir();

        // 找到之前的summary文件
        let path = file_utils::make_summary_file(&summary_path, 0);

        // 文件已存在 就会续写
        let writer = Writer::open(path, RecordDataType::Summary).await.unwrap();
        let ctx = Arc::new(GlobalContext::default());
        let rd = Box::new(
            Reader::open(&file_utils::make_summary_file(&summary_path, 0))
                .await
                .unwrap(),
        );
        // 通过读取文件 加载summary
        let vs = Self::recover_version(
            meta.clone(),
            rd,
            &ctx,
            opt.clone(),
            runtime.clone(),
            memory_pool,
            flush_task_sender,
            compact_task_sender,
            load_field_filter,
            metrics_register.clone(),
        )
        .await?;

        Ok(Self {
            meta: meta.clone(),
            file_no: 0,
            version_set: Arc::new(RwLock::new(vs)),
            ctx,
            writer,
            opt,
            runtime,
            metrics_register,
        })
    }

    /// Recover from summary file
    ///
    /// If `load_file_filter` is `true`, field_filter will be loaded from file,
    /// otherwise default `BloomFilter::default()`
    #[allow(clippy::too_many_arguments)]
    pub async fn recover_version(
        meta: MetaRef,
        mut reader: Box<Reader>,  // 通过reader对象读取数据
        ctx: &GlobalContext,
        opt: Arc<Options>,
        runtime: Arc<Runtime>,
        memory_pool: MemoryPoolRef,
        flush_task_sender: Sender<FlushReq>,
        compact_task_sender: Sender<CompactTask>,
        load_field_filter: bool,  // 是否需要读取布隆过滤器
        metrics_register: Arc<MetricsRegister>,
    ) -> Result<VersionSet> {
        // 记录针对某个列族的某次version变化
        let mut tsf_edits_map: HashMap<TseriesFamilyId, Vec<VersionEdit>> = HashMap::new();
        let mut database_map: HashMap<String, Arc<String>> = HashMap::new();
        // 代表该列族关联的是哪个db
        let mut tsf_database_map: HashMap<TseriesFamilyId, Arc<String>> = HashMap::new();

        loop {
            // 按照既定的格式 读取一条record
            let res = reader.read_record().await;
            match res {
                Ok(result) => {
                    // 还原成 versionEdit
                    let ed = VersionEdit::decode(&result.data)?;

                    if ed.add_tsf {
                        // 看来针对 versionSet的操作都被记录下来了 可以一步步还原
                        let db_ref = database_map
                            .entry(ed.tsf_name.clone())
                            .or_insert_with(|| Arc::new(ed.tsf_name.clone()));
                        tsf_database_map.insert(ed.tsf_id, db_ref.clone());
                        if ed.has_file_id {
                            tsf_edits_map.insert(ed.tsf_id, vec![ed]);
                        } else {
                            tsf_edits_map.insert(ed.tsf_id, vec![]);
                        }
                    } else if ed.del_tsf {
                        tsf_edits_map.remove(&ed.tsf_id);
                        tsf_database_map.remove(&ed.tsf_id);
                        // 代表追加
                    } else if let Some(data) = tsf_edits_map.get_mut(&ed.tsf_id) {
                        data.push(ed);
                    }
                }
                Err(Error::Eof) => break,
                Err(Error::RecordFileHashCheckFailed { .. }) => continue,
                Err(e) => {
                    return Err(e);
                }
            }
        }

        let mut versions = HashMap::new();
        let mut max_seq_no_all = 0_u64;
        let mut has_file_id = false;
        let mut max_file_id_all = 0_u64;

        // 遍历所有列族  相关的edit
        for (tsf_id, edits) in tsf_edits_map {

            // 获取列族相关的db名
            let database = tsf_database_map.remove(&tsf_id).unwrap();

            let mut files: HashMap<u64, CompactMeta> = HashMap::new();
            let mut max_seq_no = 0;
            let mut max_level_ts = i64::MIN;

            // 遍历每次变化
            for e in edits {
                // 如果本次edit 有序列号 和 file_id  则更新max
                if e.has_seq_no {
                    max_seq_no = std::cmp::max(max_seq_no, e.seq_no);
                    max_seq_no_all = std::cmp::max(max_seq_no_all, e.seq_no);
                }
                if e.has_file_id {
                    has_file_id = true;
                    max_file_id_all = std::cmp::max(max_file_id_all, e.file_id);
                }

                // 更新 max_level_ts
                max_level_ts = std::cmp::max(max_level_ts, e.max_level_ts);
                for m in e.del_files {
                    files.remove(&m.file_id);
                }
                for m in e.add_files {
                    files.insert(m.file_id, m);
                }
            }

            // Recover levels_info according to `CompactMeta`s;
            let tsm_reader_cache = Arc::new(ShardedAsyncCache::create_lru_sharded_cache(
                opt.storage.max_cached_readers,
            ));
            let weak_tsm_reader_cache = Arc::downgrade(&tsm_reader_cache);

            // 产生level_info 数组
            let mut levels = LevelInfo::init_levels(database.clone(), tsf_id, opt.storage.clone());

            // 遍历元数据 找到那些数据文件
            for meta in files.into_values() {
                let field_filter = if load_field_filter {
                    // 打开这些数据文件reader对象 获取他们的布隆过滤器
                    let tsm_path = meta.file_path(opt.storage.as_ref(), &database, tsf_id);
                    let tsm_reader = TsmReader::open(tsm_path).await?;
                    tsm_reader.bloom_filter()
                } else {
                    Arc::new(BloomFilter::default())
                };

                // 每个文件填充到level中
                levels[meta.level as usize].push_compact_meta(
                    &meta,
                    field_filter,
                    weak_tsm_reader_cache.clone(),
                );
            }

            // 当某个列族相关的所有数据都已经读取完毕后 可以初始化version了
            let ver = Version::new(
                tsf_id,
                database,
                opt.storage.clone(),
                max_seq_no,
                levels,
                max_level_ts,
                tsm_reader_cache,
            );
            // 所有version 组成了VersionSet
            versions.insert(tsf_id, Arc::new(ver));
        }

        // 代表确实出现了文件  在上下文中记录下个文件的id
        if has_file_id {
            ctx.set_file_id(max_file_id_all + 1);
        }

        // 根据相关信息 产生VersionSet
        let vs = VersionSet::new(
            meta,
            opt,
            runtime,
            memory_pool,
            versions,
            flush_task_sender,
            compact_task_sender,
            metrics_register,
        )
        .await?;
        Ok(vs)
    }

    /// Applies version edit to summary file, and generates new version for TseriesFamily.
    /// 作用一组 VersionEdit
    pub async fn apply_version_edit(
        &mut self,
        version_edits: Vec<VersionEdit>,
        file_metas: HashMap<ColumnFileId, Arc<BloomFilter>>,
        mem_caches: HashMap<TseriesFamilyId, Vec<Arc<SyncRwLock<MemCache>>>>,   // 代表此时要维护的缓存
    ) -> Result<()> {
        self.write_summary(version_edits, file_metas, mem_caches)
            .await?;
        // 判断是否要生成快照
        self.roll_summary_file().await?;
        Ok(())
    }

    /// Write VersionEdits into summary file, generate and then apply new Versions for TseriesFamilies.
    /// 写入一组 summary数据
    async fn write_summary(
        &mut self,
        version_edits: Vec<VersionEdit>,
        mut file_metas: HashMap<ColumnFileId, Arc<BloomFilter>>,
        mem_caches: HashMap<TseriesFamilyId, Vec<Arc<SyncRwLock<MemCache>>>>,  // 新版本要维护的缓存
    ) -> Result<()> {
        // Write VersionEdits into summary file and join VersionEdits by Database/TseriesFamilyId.
        let mut tsf_version_edits: HashMap<TseriesFamilyId, Vec<VersionEdit>> = HashMap::new();
        let mut tsf_min_seq: HashMap<TseriesFamilyId, u64> = HashMap::new();
        let mut del_tsf: HashSet<TseriesFamilyId> = HashSet::new();

        // 遍历本次相关的所有edit
        for edit in version_edits.into_iter() {
            let buf = edit.encode()?;

            // 将edit挨个记录到 record文件中
            let _ = self
                .writer
                .write_record(
                    RecordDataVersion::V1.into(),
                    RecordDataType::Summary.into(),
                    &[&buf],
                )
                .await?;
            self.writer.sync().await?;

            // 同时设置到内存   (按照列族分组)
            tsf_version_edits
                .entry(edit.tsf_id)
                .or_default()
                .push(edit.clone());
            if edit.has_seq_no {
                tsf_min_seq.insert(edit.tsf_id, edit.seq_no);
            }
            if edit.del_tsf {
                del_tsf.insert(edit.tsf_id);
            }
        }

        // For each TsereiesFamily - VersionEdits，generate a new Version and then apply it.
        let version_set = self.version_set.read().await;

        // 现在将这些edit作用到内存数据上
        for (tsf_id, edits) in tsf_version_edits {
            let min_seq = tsf_min_seq.get(&tsf_id);

            // 如果处理 del_tsf为true的edit 在之前已经从version_set中删除了该列族的数据了  返回None 也就不会更新version了
            if let Some(tsf) = version_set.get_tsfamily_by_tf_id(tsf_id).await {
                trace::info!("Applying new version for ts_family {}.", tsf_id);

                // 将edit作用到version上 产生新的version
                let new_version = tsf.read().await.version().copy_apply_version_edits(
                    edits,
                    &mut file_metas,
                    min_seq.copied(),
                );

                // 更新当前列族的version 和缓存数据
                let flushed_mem_cahces = mem_caches.get(&tsf_id);
                tsf.write()
                    .await
                    .new_version(new_version, flushed_mem_cahces);
                trace::info!("Applied new version for ts_family {}.", tsf_id);
            }
        }
        drop(version_set);

        Ok(())
    }

    // 检查当前summary文件是否过长 并产生快照文件
    async fn roll_summary_file(&mut self) -> Result<()> {
        if self.writer.file_size() >= self.opt.storage.max_summary_size {
            // 为当前VersionSet中 各个version生成快照
            let (edits, file_metas) = {
                let vs = self.version_set.read().await;
                vs.snapshot().await
            };

            // 将数据写入临时文件 并打算替换本文件
            let new_path = &file_utils::make_summary_file_tmp(self.opt.storage.summary_dir());
            let old_path = &self.writer.path().clone();
            if try_exists(new_path) {
                match remove_file(new_path) {
                    Ok(_) => (),
                    Err(e) => {
                        trace::error!("Failed to remove file '{}': {:?}", new_path.display(), e);
                    }
                };
            }
            self.writer = Writer::open(new_path, RecordDataType::Summary)
                .await
                .unwrap();
            self.write_summary(edits, file_metas, HashMap::new())
                .await?;
            match rename(new_path, old_path) {
                Ok(_) => (),
                Err(e) => {
                    trace::error!(
                        "Failed to remove old file '{}' and create new file '{}': {:?}",
                        old_path.display(),
                        new_path.display(),
                        e
                    );
                }
            };
            self.writer = Writer::open(old_path, RecordDataType::Summary)
                .await
                .unwrap();
        }
        Ok(())
    }

    pub fn version_set(&self) -> Arc<RwLock<VersionSet>> {
        self.version_set.clone()
    }

    pub fn global_context(&self) -> Arc<GlobalContext> {
        self.ctx.clone()
    }
}

pub async fn print_summary_statistics(path: impl AsRef<Path>) {
    let mut reader = Reader::open(&path).await.unwrap();
    println!("============================================================");
    let mut i = 0_usize;
    loop {
        match reader.read_record().await {
            Ok(record) => {
                let ve = VersionEdit::decode(&record.data).unwrap();
                println!("VersionEdit #{}, vnode_id: {}", i, ve.tsf_id);
                println!("------------------------------------------------------------");
                i += 1;
                if ve.add_tsf {
                    println!("  Add ts_family: {}", ve.tsf_id);
                    println!("------------------------------------------------------------");
                }
                if ve.del_tsf {
                    println!("  Delete ts_family: {}", ve.tsf_id);
                    println!("------------------------------------------------------------");
                }
                if ve.has_seq_no {
                    println!("  Presist sequence: {}", ve.seq_no);
                    println!("------------------------------------------------------------");
                }
                if ve.has_file_id {
                    if ve.add_files.is_empty() && ve.del_files.is_empty() {
                        println!("  Add file: None. Delete file: None.");
                    }
                    if !ve.add_files.is_empty() {
                        let mut buffer = String::new();
                        ve.add_files.iter().for_each(|f| {
                            buffer.push_str(
                                format!("{} (level: {}, {} B), ", f.file_id, f.level, f.file_size)
                                    .as_str(),
                            )
                        });
                        if !buffer.is_empty() {
                            buffer.truncate(buffer.len() - 2);
                        }
                        println!("  Add file:[ {} ]", buffer);
                    }
                    if !ve.del_files.is_empty() {
                        let mut buffer = String::new();
                        ve.del_files.iter().for_each(|f| {
                            buffer
                                .push_str(format!("{} (level: {}), ", f.file_id, f.level).as_str())
                        });
                        if !buffer.is_empty() {
                            buffer.truncate(buffer.len() - 2);
                        }
                        println!("  Delete file:[ {} ]", buffer);
                    }
                }
            }
            Err(Error::Eof) => break,
            Err(Error::RecordFileHashCheckFailed { .. }) => continue,
            Err(err) => panic!("Errors when read summary file: {}", err),
        }
        println!("============================================================");
    }
}

pub struct SummaryProcessor {
    summary: Box<Summary>,
    cbs: Vec<OneShotSender<Result<()>>>,  // 用于回复结果
    edits: Vec<VersionEdit>,
    file_metas: HashMap<ColumnFileId, Arc<BloomFilter>>,
    mem_caches: HashMap<TseriesFamilyId, Vec<Arc<SyncRwLock<MemCache>>>>,
}

impl SummaryProcessor {
    pub fn new(summary: Box<Summary>) -> Self {
        Self {
            summary,
            cbs: Vec::with_capacity(32),
            edits: Vec::with_capacity(32),
            file_metas: HashMap::new(),
            mem_caches: HashMap::new(),
        }
    }

    // 把task的信息 都添加到processor中
    pub fn batch(&mut self, task: SummaryTask) {
        if let Some(file_metas) = task.file_metas {
            self.file_metas.extend(file_metas);
        }
        if let Some(mem_caches) = task.mem_caches {
            self.mem_caches.extend(mem_caches);
        }

        let mut req = task.request;
        self.edits.append(&mut req.version_edits);
        self.cbs.push(req.call_back);
    }

    // 将本对象所有edit作用到summary上
    pub async fn apply(&mut self) {
        let edits = std::mem::replace(&mut self.edits, Vec::with_capacity(32));
        let file_metas = std::mem::take(&mut self.file_metas);
        let mem_caches = std::mem::take(&mut self.mem_caches);
        match self
            .summary
            .apply_version_edit(edits, file_metas, mem_caches)
            .await
        {
            // 作用完所有更新后  通过send回复结果
            Ok(()) => {
                for cb in self.cbs.drain(..) {
                    let _ = cb.send(Ok(()));
                }
            }
            Err(e) => {
                trace::error!("Failed to apply VersionEdit to summary: {e}");
                for cb in self.cbs.drain(..) {
                    let _ = cb.send(Err(Error::ErrApplyEdit));
                }
            }
        }
    }

    pub fn summary(&self) -> &Summary {
        &self.summary
    }
}

// 总结任务
#[derive(Debug)]
pub struct SummaryTask {
    pub request: WriteSummaryRequest,   // 内部包含了一组 edit 需要将这些作用到summary上
    pub file_metas: Option<HashMap<ColumnFileId, Arc<BloomFilter>>>,
    pub mem_caches: Option<HashMap<TseriesFamilyId, Vec<Arc<SyncRwLock<MemCache>>>>>,
}

impl SummaryTask {
    pub fn new(
        version_edits: Vec<VersionEdit>,
        file_metas: Option<HashMap<ColumnFileId, Arc<BloomFilter>>>,
        mem_caches: Option<HashMap<TseriesFamilyId, Vec<Arc<SyncRwLock<MemCache>>>>>,
        call_back: OneShotSender<Result<()>>,
    ) -> Self {
        Self {
            request: WriteSummaryRequest {
                version_edits,
                call_back,
            },
            file_metas,
            mem_caches,
        }
    }
}

// 代表需要将这些edit作用到summary上
#[derive(Debug)]
pub struct WriteSummaryRequest {
    pub version_edits: Vec<VersionEdit>,
    pub call_back: OneShotSender<Result<()>>,
}

// 调度器用于发送summary任务
#[derive(Clone)]
pub struct SummaryScheduler {
    sender: Sender<SummaryTask>,
}

impl SummaryScheduler {
    pub fn new(sender: Sender<SummaryTask>) -> Self {
        Self { sender }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::fs;
    use std::sync::Arc;

    use memory_pool::GreedyMemoryPool;
    use meta::model::meta_admin::AdminMeta;
    use meta::model::MetaRef;
    use metrics::metric_register::MetricsRegister;
    use models::schema::{make_owner, DatabaseSchema, TenantOptions};
    use tokio::runtime::Runtime;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Sender;
    use utils::BloomFilter;

    use crate::compaction::{CompactTask, FlushReq};
    use crate::context::GlobalContext;
    use crate::file_system::file_manager;
    use crate::kv_option::Options;
    use crate::kvcore::{COMPACT_REQ_CHANNEL_CAP, SUMMARY_REQ_CHANNEL_CAP};
    use crate::summary::{CompactMeta, Summary, SummaryTask, VersionEdit};

    #[test]
    fn test_version_edit() {
        let mut ve = VersionEdit::default();

        let add_file_100 = CompactMeta {
            file_id: 100,
            ..Default::default()
        };
        ve.add_file(add_file_100, 100_000_000);

        let del_file_101 = CompactMeta {
            file_id: 101,
            ..Default::default()
        };
        ve.del_files.push(del_file_101);

        let ve_buf = ve.encode().unwrap();
        let ve2 = VersionEdit::decode(&ve_buf).unwrap();
        assert_eq!(ve2, ve);

        let ves = vec![ve, ve2];
        let ves_buf = VersionEdit::encode_vec(&ves).unwrap();
        let ves_2 = VersionEdit::decode_vec(&ves_buf).unwrap();
        assert_eq!(ves, ves_2);
    }

    #[test]
    fn test_summary() {
        let mut config = config::get_config_for_test();

        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(4)
                .build()
                .unwrap(),
        );

        // NOTICE: Make sure these channel senders are dropped before test_summary() finished.
        let (summary_task_sender, mut summary_task_receiver) =
            mpsc::channel::<SummaryTask>(SUMMARY_REQ_CHANNEL_CAP);
        let summary_job_mock = runtime.spawn(async move {
            println!("Mock summary job started (test_summary).");
            while let Some(t) = summary_task_receiver.recv().await {
                // Do nothing
                let _ = t.request.call_back.send(Ok(()));
            }
            println!("Mock summary job finished (test_summary).");
        });
        let (flush_task_sender, mut flush_task_receiver) =
            mpsc::channel::<FlushReq>(config.storage.flush_req_channel_cap);
        let flush_job_mock = runtime.spawn(async move {
            println!("Mock flush job started (test_summary).");
            while flush_task_receiver.recv().await.is_some() {
                // Do nothing
            }
            println!("Mock flush job finished (test_summary).");
        });
        let (compact_task_sender, mut compact_task_receiver) =
            mpsc::channel::<CompactTask>(COMPACT_REQ_CHANNEL_CAP);
        let compact_job_mock = runtime.spawn(async move {
            println!("Mock compact job started (test_summary).");
            while compact_task_receiver.recv().await.is_some() {
                // Do nothing
            }
            println!("Mock compact job finished (test_summary).");
        });

        let runtime_ref = runtime.clone();
        runtime.block_on(async move {
            println!("Running test: test_summary_recover");
            let base_dir = "/tmp/test/summary/test_summary_recover".to_string();
            let _ = fs::remove_dir_all(&base_dir);
            fs::create_dir_all(&base_dir).unwrap();
            config.storage.path = base_dir.clone();
            test_summary_recover(
                config.clone(),
                runtime_ref.clone(),
                flush_task_sender.clone(),
                compact_task_sender.clone(),
            )
            .await;

            println!("Running test: test_tsf_num_recover");
            let base_dir = "/tmp/test/summary/test_tsf_num_recover".to_string();
            let _ = fs::remove_dir_all(&base_dir);
            fs::create_dir_all(&base_dir).unwrap();
            config.storage.path = base_dir.clone();
            test_tsf_num_recover(
                config.clone(),
                runtime_ref.clone(),
                flush_task_sender.clone(),
                compact_task_sender.clone(),
            )
            .await;

            println!("Running test: test_recover_summary_with_roll_0");
            let base_dir = "/tmp/test/summary/test_recover_summary_with_roll_0".to_string();
            let _ = fs::remove_dir_all(&base_dir);
            fs::create_dir_all(&base_dir).unwrap();
            config.storage.path = base_dir.clone();
            test_recover_summary_with_roll_0(
                config.clone(),
                runtime_ref.clone(),
                summary_task_sender.clone(),
                flush_task_sender.clone(),
                compact_task_sender.clone(),
            )
            .await;

            println!("Running test: test_recover_summary_with_roll_1");
            let base_dir = "/tmp/test/summary/test_recover_summary_with_roll_1".to_string();
            let _ = fs::remove_dir_all(&base_dir);
            config.storage.path = base_dir.clone();
            test_recover_summary_with_roll_1(
                config.clone(),
                runtime_ref.clone(),
                summary_task_sender,
                flush_task_sender,
                compact_task_sender,
            )
            .await;

            let _ = tokio::join!(summary_job_mock, flush_job_mock, compact_job_mock);
        });
    }

    async fn test_summary_recover(
        config: config::Config,
        runtime: Arc<Runtime>,
        flush_task_sender: Sender<FlushReq>,
        compact_task_sender: Sender<CompactTask>,
    ) {
        let opt = Arc::new(Options::from(&config));
        let meta_manager = AdminMeta::new(config.clone()).await;

        meta_manager.add_data_node().await.unwrap();

        let _ = meta_manager
            .create_tenant("cnosdb".to_string(), TenantOptions::default())
            .await;
        let summary_dir = opt.storage.summary_dir();
        if !file_manager::try_exists(&summary_dir) {
            std::fs::create_dir_all(&summary_dir).unwrap();
        }
        let memory_pool = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024));
        let mut summary = Summary::new(
            opt.clone(),
            runtime.clone(),
            meta_manager.clone(),
            memory_pool.clone(),
            Arc::new(MetricsRegister::default()),
        )
        .await
        .unwrap();
        let edit = VersionEdit::new_add_vnode(100, "cnosdb.hello".to_string(), 0);
        summary
            .apply_version_edit(vec![edit], HashMap::new(), HashMap::new())
            .await
            .unwrap();
        let _summary = Summary::recover(
            meta_manager,
            opt.clone(),
            runtime.clone(),
            memory_pool,
            flush_task_sender,
            compact_task_sender,
            false,
            Arc::new(MetricsRegister::default()),
        )
        .await
        .unwrap();
    }

    async fn test_tsf_num_recover(
        config: config::Config,
        runtime: Arc<Runtime>,
        flush_task_sender: Sender<FlushReq>,
        compact_task_sender: Sender<CompactTask>,
    ) {
        let opt = Arc::new(Options::from(&config));
        let meta_manager: MetaRef = AdminMeta::new(config.clone()).await;

        meta_manager.add_data_node().await.unwrap();

        let _ = meta_manager
            .create_tenant("cnosdb".to_string(), TenantOptions::default())
            .await;
        let summary_dir = opt.storage.summary_dir();
        if !file_manager::try_exists(&summary_dir) {
            std::fs::create_dir_all(&summary_dir).unwrap();
        }
        let memory_pool = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024));
        let mut summary = Summary::new(
            opt.clone(),
            runtime.clone(),
            meta_manager.clone(),
            memory_pool.clone(),
            Arc::new(MetricsRegister::default()),
        )
        .await
        .unwrap();

        let edit = VersionEdit::new_add_vnode(100, "cnosdb.hello".to_string(), 0);
        summary
            .apply_version_edit(vec![edit], HashMap::new(), HashMap::new())
            .await
            .unwrap();
        let mut summary = Summary::recover(
            meta_manager.clone(),
            opt.clone(),
            runtime.clone(),
            memory_pool.clone(),
            flush_task_sender.clone(),
            compact_task_sender.clone(),
            false,
            Arc::new(MetricsRegister::default()),
        )
        .await
        .unwrap();
        assert_eq!(summary.version_set.read().await.tsf_num().await, 1);
        let edit = VersionEdit::new_del_vnode(100);
        summary
            .apply_version_edit(vec![edit], HashMap::new(), HashMap::new())
            .await
            .unwrap();
        let summary = Summary::recover(
            meta_manager,
            opt.clone(),
            runtime,
            memory_pool.clone(),
            flush_task_sender,
            compact_task_sender,
            false,
            Arc::new(MetricsRegister::default()),
        )
        .await
        .unwrap();
        assert_eq!(summary.version_set.read().await.tsf_num().await, 0);
    }

    // tips : we can use a small max_summary_size
    #[allow(clippy::too_many_arguments)]
    async fn test_recover_summary_with_roll_0(
        config: config::Config,
        runtime: Arc<Runtime>,
        summary_task_sender: Sender<SummaryTask>,
        flush_task_sender: Sender<FlushReq>,
        compact_task_sender: Sender<CompactTask>,
    ) {
        let opt = Arc::new(Options::from(&config));
        let meta_manager: MetaRef = AdminMeta::new(config.clone()).await;

        meta_manager.add_data_node().await.unwrap();
        let _ = meta_manager
            .create_tenant("cnosdb".to_string(), TenantOptions::default())
            .await;
        let database = "test".to_string();
        let summary_dir = opt.storage.summary_dir();
        if !file_manager::try_exists(&summary_dir) {
            std::fs::create_dir_all(&summary_dir).unwrap();
        }
        let memory_pool = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024));
        let global = Arc::new(GlobalContext::new());
        let mut summary = Summary::new(
            opt.clone(),
            runtime.clone(),
            meta_manager.clone(),
            memory_pool.clone(),
            Arc::new(MetricsRegister::default()),
        )
        .await
        .unwrap();

        let mut edits = vec![];
        let db = summary
            .version_set
            .write()
            .await
            .create_db(DatabaseSchema::new("cnosdb", &database))
            .await
            .unwrap();
        for i in 0..40 {
            db.write()
                .await
                .add_tsfamily(
                    i,
                    None,
                    summary_task_sender.clone(),
                    flush_task_sender.clone(),
                    compact_task_sender.clone(),
                    global.clone(),
                )
                .await
                .expect("add tsfamily successfully");
            let edit = VersionEdit::new_add_vnode(i, make_owner("cnosdb", &database), 0);
            edits.push(edit.clone());
        }

        for _ in 0..100 {
            for i in 0..20 {
                db.write()
                    .await
                    .del_tsfamily(i, summary_task_sender.clone())
                    .await;
                edits.push(VersionEdit::new_del_vnode(i));
            }
        }
        summary
            .apply_version_edit(edits, HashMap::new(), HashMap::new())
            .await
            .unwrap();

        let summary = Summary::recover(
            meta_manager.clone(),
            opt.clone(),
            runtime,
            memory_pool.clone(),
            flush_task_sender.clone(),
            compact_task_sender,
            false,
            Arc::new(MetricsRegister::default()),
        )
        .await
        .unwrap();

        assert_eq!(summary.version_set.read().await.tsf_num().await, 20);
    }

    #[allow(clippy::too_many_arguments)]
    async fn test_recover_summary_with_roll_1(
        config: config::Config,
        runtime: Arc<Runtime>,
        summary_task_sender: Sender<SummaryTask>,
        flush_task_sender: Sender<FlushReq>,
        compact_task_sender: Sender<CompactTask>,
    ) {
        let memory_pool = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024));

        let opt = Arc::new(Options::from(&config));
        let meta_manager: MetaRef = AdminMeta::new(config.clone()).await;

        meta_manager.add_data_node().await.unwrap();

        let _ = meta_manager
            .create_tenant("cnosdb".to_string(), TenantOptions::default())
            .await;
        let database = "test".to_string();
        let summary_dir = opt.storage.summary_dir();
        if !file_manager::try_exists(&summary_dir) {
            std::fs::create_dir_all(&summary_dir).unwrap();
        }
        let mut summary = Summary::new(
            opt.clone(),
            runtime.clone(),
            meta_manager.clone(),
            memory_pool.clone(),
            Arc::new(MetricsRegister::default()),
        )
        .await
        .unwrap();

        let db = summary
            .version_set
            .write()
            .await
            .create_db(DatabaseSchema::new("cnosdb", &database))
            .await
            .unwrap();
        let cxt = Arc::new(GlobalContext::new());
        db.write()
            .await
            .add_tsfamily(
                10,
                None,
                summary_task_sender.clone(),
                flush_task_sender.clone(),
                compact_task_sender.clone(),
                cxt.clone(),
            )
            .await
            .expect("add tsfamily failed");

        let mut edits = vec![];
        let edit = VersionEdit::new_add_vnode(10, "cnosdb.hello".to_string(), 0);
        edits.push(edit);

        for _ in 0..100 {
            db.write()
                .await
                .del_tsfamily(0, summary_task_sender.clone())
                .await;
            let edit = VersionEdit::new_del_vnode(0);
            edits.push(edit);
        }

        {
            let vs = summary.version_set.write().await;
            let tsf = vs.get_tsfamily_by_tf_id(10).await.unwrap();
            let mut version = tsf.read().await.version().copy_apply_version_edits(
                edits.clone(),
                &mut HashMap::new(),
                None,
            );
            let tsm_reader_cache = Arc::downgrade(version.tsm_reader_cache());

            let mut edit = VersionEdit::new(10);
            let meta = CompactMeta {
                file_id: 15,
                is_delta: false,
                file_size: 100,
                level: 1,
                min_ts: 1,
                max_ts: 1,
                tsf_id: 10,
                high_seq: 1,
                ..Default::default()
            };
            version.levels_info_mut()[1].push_compact_meta(
                &meta,
                Arc::new(BloomFilter::default()),
                tsm_reader_cache,
            );
            tsf.write().await.new_version(version, None);
            edit.add_file(meta, 1);
            edits.push(edit);
        }

        summary
            .apply_version_edit(edits, HashMap::new(), HashMap::new())
            .await
            .unwrap();

        let summary = Summary::recover(
            meta_manager,
            opt,
            runtime,
            memory_pool.clone(),
            flush_task_sender,
            compact_task_sender,
            false,
            Arc::new(MetricsRegister::default()),
        )
        .await
        .unwrap();

        let vs = summary.version_set.read().await;
        let tsf = vs.get_tsfamily_by_tf_id(10).await.unwrap();
        assert_eq!(tsf.read().await.version().last_seq(), 1);
        assert_eq!(tsf.read().await.version().levels_info()[1].tsf_id, 10);
        assert!(!tsf.read().await.version().levels_info()[1].files[0].is_delta());
        assert_eq!(
            tsf.read().await.version().levels_info()[1].files[0].file_id(),
            15
        );
        assert_eq!(
            tsf.read().await.version().levels_info()[1].files[0].size(),
            100
        );
        assert_eq!(summary.ctx.file_id(), 16);
    }
}
