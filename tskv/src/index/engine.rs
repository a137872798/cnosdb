use std::fs;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};

use radixdb;
use radixdb::store;
use radixdb::store::BlobStore;
use trace::debug;

use super::{IndexError, IndexResult};

// 索引引擎
#[derive(Debug)]
pub struct IndexEngine {
    // 关联一个目录
    dir: PathBuf,
    // 基数树中key相当于是数据页的编号 通过加载value可以得到那一页的数据
    db: radixdb::RadixTree<store::PagedFileStore>,
    // 推测应该是代表该文件内的数据 按页分配
    store: store::PagedFileStore,
}

impl IndexEngine {
    pub fn new(path: impl AsRef<Path>) -> IndexResult<Self> {
        let path = path.as_ref();
        // 创建目录
        let _ = fs::create_dir_all(path);
        debug!("Creating index engine : {:?}", &path);

        // 创建index.db 文件
        let db_path = path.join("index.db");
        let file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(db_path)
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;

        // store/db 都是在该文件上做修改
        let store = store::PagedFileStore::new(file, 1024 * 1024)
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;
        let db = radixdb::RadixTree::try_load(store.clone(), store.last_id())
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;

        Ok(Self {
            db,
            store,
            dir: path.into(),
        })
    }

    // 将某个数据页插入到基数树中 便于检索
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> IndexResult<()> {
        self.db
            .try_insert(key, value)
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;

        Ok(())
    }

    // 还原该页的数据
    pub fn get(&self, key: &[u8]) -> IndexResult<Option<Vec<u8>>> {
        let val = self
            .db
            .try_get(key)
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;

        match val {
            Some(v) => {
                let data = self.load(&v)?;

                Ok(Some(data))
            }
            None => Ok(None),
        }
    }

    // 从文件中还原出数据
    pub fn load(&self, val: &radixdb::node::Value<store::PagedFileStore>) -> IndexResult<Vec<u8>> {
        let blob = val
            .load(&self.store)
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;

        Ok(blob.to_vec())
    }

    // 将数据还原成一个位图 代表这个数据页内存储的是一个位图?
    pub fn get_rb(&self, key: &[u8]) -> IndexResult<Option<roaring::RoaringBitmap>> {
        if let Some(data) = self.get(key)? {
            let rb = roaring::RoaringBitmap::deserialize_from(&*data)
                .map_err(|e| IndexError::RoaringBitmap { source: e })?;

            Ok(Some(rb))
        } else {
            Ok(None)
        }
    }

    // 直接传入数据页 并还原出位图
    pub fn load_rb(
        &self,
        val: &radixdb::node::Value<store::PagedFileStore>,
    ) -> IndexResult<roaring::RoaringBitmap> {
        let data = self.load(val)?;

        let rb = roaring::RoaringBitmap::deserialize_from(&*data)
            .map_err(|e| IndexError::RoaringBitmap { source: e })?;

        Ok(rb)
    }

    // 构建倒排索引
    pub fn build_revert_index(&self, key: &[u8], id: u32, add: bool) -> IndexResult<Vec<u8>> {

        // 根据key 还原出位图  这个位图本身就是倒排索引?
        let mut rb = match self.get(key)? {
            Some(val) => roaring::RoaringBitmap::deserialize_from(&*val)
                .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?,

            None => roaring::RoaringBitmap::new(),
        };

        if add {
            rb.insert(id);
        } else {
            rb.remove(id);
        }

        let mut bytes = vec![];
        // 重新序列化
        rb.serialize_into(&mut bytes)
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;

        Ok(bytes)
    }

    pub fn modify(&mut self, key: &[u8], id: u32, add: bool) -> IndexResult<()> {
        let mut rb = match self.get(key)? {
            Some(val) => roaring::RoaringBitmap::deserialize_from(&*val)
                .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?,

            None => roaring::RoaringBitmap::new(),
        };

        if add {
            rb.insert(id);
        } else {
            rb.remove(id);
        }

        let mut bytes = vec![];
        rb.serialize_into(&mut bytes)
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;

        // 覆盖之前的数据页
        self.set(key, &bytes)?;

        Ok(())
    }

    // 删除某个倒排索引
    pub fn delete(&mut self, key: &[u8]) -> IndexResult<()> {
        self.db
            .try_remove(key)
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;

        Ok(())
    }

    // 将2棵基数树的数据进行合并
    pub fn combine(&mut self, tree: radixdb::RadixTree) -> IndexResult<()> {
        // 依赖第三方的api
        self.db
            .try_outer_combine_with(&tree, radixdb::node::DetachConverter, |a, b| {
                a.set(Some(b.downcast()));
                Ok(())
            })
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;

        Ok(())
    }

    pub fn exist(&self, key: &[u8]) -> IndexResult<bool> {
        let result = self
            .db
            .try_contains_key(key)
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;

        Ok(result)
    }

    // 产生迭代器 遍历基数树的数据页
    pub fn range(&self, range: impl RangeBounds<Vec<u8>>) -> RangeKeyValIter {
        RangeKeyValIter::new_iterator(
            copy_bound(range.start_bound()),
            copy_bound(range.end_bound()),
            self.db.try_iter(),
        )
    }

    // 通过前缀匹配
    pub fn prefix<'a>(
        &'a self,
        key: &'a [u8],
    ) -> IndexResult<radixdb::node::KeyValueIter<store::PagedFileStore>> {
        self.db
            .try_scan_prefix(key)
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })
    }

    // 数据刷盘
    pub fn flush(&mut self) -> IndexResult<()> {
        let _id = self
            .db
            .try_reattach()
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;

        self.store
            .sync()
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;

        Ok(())
    }
}

// copy数据 不丢失所有权
fn copy_bound(bound: std::ops::Bound<&Vec<u8>>) -> std::ops::Bound<Vec<u8>> {
    match bound {
        std::ops::Bound::Included(val) => std::ops::Bound::Included(val.to_vec()),
        std::ops::Bound::Excluded(val) => std::ops::Bound::Excluded(val.to_vec()),
        std::ops::Bound::Unbounded => std::ops::Bound::Unbounded,
    }
}
pub struct RangeKeyValIter {
    start: std::ops::Bound<Vec<u8>>,
    end: std::ops::Bound<Vec<u8>>,

    iter: radixdb::node::KeyValueIter<store::PagedFileStore>,
}

impl RangeKeyValIter {
    pub fn new_iterator(
        start: std::ops::Bound<Vec<u8>>,
        end: std::ops::Bound<Vec<u8>>,
        iter: radixdb::node::KeyValueIter<store::PagedFileStore>,
    ) -> Self {
        Self { iter, start, end }
    }
}

// 迭代能力主要还是通过内部的迭代器提供的
impl Iterator for RangeKeyValIter {
    type Item = IndexResult<(
        radixdb::node::IterKey,
        radixdb::node::Value<store::PagedFileStore>,
    )>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.iter.next() {
                None => {
                    return None;
                }

                // 迭代得到一个数据页
                Some(item) => match item {
                    Err(e) => {
                        return Some(Err(IndexError::IndexStroage { msg: e.to_string() }));
                    }

                    Ok(item) => match &self.start {
                        std::ops::Bound::Included(start) => match &self.end {
                            std::ops::Bound::Included(end) => {
                                if item.0.as_ref() >= start.as_slice()
                                    && item.0.as_ref() <= end.as_slice()
                                {
                                    return Some(Ok(item));
                                } else if item.0.as_ref() > end.as_slice() {
                                    return None;
                                }
                            }
                            std::ops::Bound::Excluded(end) => {
                                if item.0.as_ref() >= start.as_slice()
                                    && item.0.as_ref() < end.as_slice()
                                {
                                    return Some(Ok(item));
                                } else if item.0.as_ref() >= end.as_slice() {
                                    return None;
                                }
                            }
                            std::ops::Bound::Unbounded => {
                                if item.0.as_ref() >= start.as_slice() {
                                    return Some(Ok(item));
                                }
                            }
                        },

                        std::ops::Bound::Excluded(start) => match &self.end {
                            std::ops::Bound::Included(end) => {
                                if item.0.as_ref() > start.as_slice()
                                    && item.0.as_ref() <= end.as_slice()
                                {
                                    return Some(Ok(item));
                                } else if item.0.as_ref() > end.as_slice() {
                                    return None;
                                }
                            }
                            std::ops::Bound::Excluded(end) => {
                                if item.0.as_ref() > start.as_slice()
                                    && item.0.as_ref() < end.as_slice()
                                {
                                    return Some(Ok(item));
                                } else if item.0.as_ref() >= end.as_slice() {
                                    return None;
                                }
                            }
                            std::ops::Bound::Unbounded => {
                                if item.0.as_ref() > start.as_slice() {
                                    return Some(Ok(item));
                                }
                            }
                        },

                        std::ops::Bound::Unbounded => match &self.end {
                            std::ops::Bound::Included(end) => {
                                if item.0.as_ref() <= end.as_slice() {
                                    return Some(Ok(item));
                                } else if item.0.as_ref() > end.as_slice() {
                                    return None;
                                }
                            }
                            std::ops::Bound::Excluded(end) => {
                                if item.0.as_ref() < end.as_slice() {
                                    return Some(Ok(item));
                                } else if item.0.as_ref() >= end.as_slice() {
                                    return None;
                                }
                            }
                            std::ops::Bound::Unbounded => {
                                return Some(Ok(item));
                            }
                        },
                    },
                },
            }
        }
    }
}

mod test {
    use std::sync::atomic::AtomicU64;
    use std::sync::{self, Arc};

    use models::utils::now_timestamp_nanos;
    use tokio::time::{self, Duration};

    use super::IndexEngine;

    #[tokio::test]
    async fn test_engine() {
        let mut engine = IndexEngine::new("/tmp/test/1").unwrap();
        // engine.set(b"key1", b"v11111").unwrap();
        // engine.set(b"key2", b"v22222").unwrap();
        // engine.set(b"key3", b"v33333").unwrap();
        // engine.set(b"key4", b"v44444").unwrap();
        // engine.set(b"key5", b"v55555").unwrap();

        engine.set(b"key3", b"v333334").unwrap();
        engine.flush().unwrap();

        println!("=== {:?}", engine.get(b"key"));
        println!("=== {:?}", engine.get(b"key1"));
        println!("=== {:?}", engine.get(b"key2"));
        println!("=== {:?}", engine.get(b"key3"));
        println!("=== {:?}", engine.delete(b"key3"));
        println!("=== {:?}", engine.get(b"key3"));
    }

    async fn test_engine_write_perf() {
        let mut engine = IndexEngine::new("/tmp/test/2").unwrap();

        let mut begin = now_timestamp_nanos() / 1000000;
        for i in 1..10001 {
            let key = format!("key012345678901234567890123456789_{}", i);
            let val = format!("val012345678901234567890123456789_{}", i);
            engine.set(key.as_bytes(), val.as_bytes()).unwrap();
            if i % 100000 == 0 {
                engine.flush().unwrap();

                let end = now_timestamp_nanos() / 1000000;
                println!("{}  : time {}", i, end - begin);
                begin = end;
            }
        }
    }

    async fn test_engine_read_perf() {
        let engine = IndexEngine::new("/tmp/test/3").unwrap();
        let engine = Arc::new(engine);

        let atomic = Arc::new(AtomicU64::new(0));

        for _ in 0..8 {
            //tokio::spawn(random_read(engine.clone(), atomic.clone()));
            let parm = (engine.clone(), atomic.clone());
            std::thread::spawn(|| random_read(parm.0, parm.1));
        }

        time::sleep(Duration::from_secs(3)).await;
    }

    fn engine_iter(engine: Arc<IndexEngine>) {
        let it = engine.prefix("key".as_bytes()).unwrap();
        for item in it {
            let item = item.unwrap();
            let key = std::str::from_utf8(item.0.as_ref()).unwrap();
            let val = engine.load(&item.1).unwrap();
            let val = std::str::from_utf8(&val).unwrap();

            println!("{}: {}", key, val)
        }
    }

    fn random_read(engine: Arc<IndexEngine>, count: Arc<AtomicU64>) {
        for _i in 1..10000000 {
            let random: i32 = rand::Rng::gen_range(&mut rand::thread_rng(), 1..=10000000);

            let key = format!("key012345678901234567890123456789_{}", random);

            engine.get(key.as_bytes()).unwrap();

            let total = count.fetch_add(1, sync::atomic::Ordering::SeqCst);
            if total % 100000 == 0 {
                println!(
                    "read total: {}; time: {}",
                    total,
                    now_timestamp_nanos() / 1000000
                );
            }
        }
    }
}
