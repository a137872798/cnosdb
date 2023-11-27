use std::collections::{BTreeMap, HashMap};
use std::fmt::Write;
use std::ops::RangeBounds;
use std::sync::Arc;

use replication::errors::{ReplicationError, ReplicationResult};
use replication::{EntryStorage, RaftNodeId, RaftNodeInfo, TypeConfig};
use tokio::sync::Mutex;

use crate::byte_utils::decode_be_u64;
use crate::file_system::file_manager;
use crate::wal::reader::{Block, WalReader};
use crate::wal::{writer, VnodeWal};
use crate::{file_utils, Error, Result};

// https://datafuselabs.github.io/openraft/getting-started.html

// openraft::declare_raft_types!(
//     pub VnodeRaftConfig:
//         D            = reader::Block,
//         R            = u64,
//         NodeId       = u64,
//         Node         = openraft::BasicNode,
//         Entry        = openraft::Entry<VnodeRaftConfig>,
//         SnapshotData = std::io::Cursor<Vec<u8>>,
//         AsyncRuntime = openraft::TokioRuntime,
// );

// 对应raft协议的一条日志
pub type RaftEntry = openraft::Entry<TypeConfig>;

// 描述当前raft group的所有成言
pub type RaftLogMembership = openraft::Membership<RaftNodeId, RaftNodeInfo>;

// 在writer中使用的task
pub type RaftRequestForWalWrite = writer::Task;

// 将数据解析成 openraft::Entry
pub fn new_raft_entry(buf: &[u8]) -> Result<RaftEntry> {
    bincode::deserialize(buf).map_err(|e| Error::Decode { source: e })
}

pub struct RaftEntryStorage {
    inner: Arc<Mutex<RaftEntryStorageInner>>,
}


// 表示用于存储raft日志
impl RaftEntryStorage {

    // 每个关联一个vnode
    pub fn new(wal: VnodeWal) -> Self {
        Self {
            inner: Arc::new(Mutex::new(RaftEntryStorageInner {
                wal,
                seq_wal_pos_index: BTreeMap::new(),
                wal_ref_count_index: HashMap::new(),
            })),
        }
    }

    /// Read WAL files to recover
    pub async fn recover(&self) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.recover().await
    }
}

#[async_trait::async_trait]
impl EntryStorage for RaftEntryStorage {
    async fn entry(&self, seq_no: u64) -> ReplicationResult<Option<RaftEntry>> {
        let mut inner = self.inner.lock().await;

        // 通过读取序列号 找到wal文件id
        let (wal_id, pos) = match inner.seq_wal_pos_index.get(&seq_no) {
            Some((wal_id, pos)) => (*wal_id, *pos),
            None => return Ok(None),
        };

        // 读取目标文件 该偏移量的数据
        inner.read(wal_id, pos).await
    }

    async fn del_before(&self, seq_no: u64) -> ReplicationResult<()> {
        let mut inner = self.inner.lock().await;
        inner.mark_delete_before(seq_no);
        // 扫描可以删除的文件
        let wal_ids_can_delete = inner.get_empty_old_wal_ids();
        inner.wal.delete_wal_files(&wal_ids_can_delete).await;
        Ok(())
    }

    async fn del_after(&self, seq_no: u64) -> ReplicationResult<()> {
        let mut inner = self.inner.lock().await;
        // 标记偏移量之后的数据可以删除
        inner.mark_delete_after(seq_no);
        Ok(())
    }

    // 追加一组日志数据
    async fn append(&self, entries: &[RaftEntry]) -> ReplicationResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let first_seq_no = entries[0].log_id.index;
        let mut inner = self.inner.lock().await;
        // 先标记 这样如果序列号冲突应该就会覆盖
        inner.mark_delete_after(first_seq_no);
        for ent in entries {
            let seq = ent.log_id.index;
            let wal_id = inner.wal.current_wal_id();
            let pos = inner.wal.current_wal_size();
            inner
                .wal
                .write_raft_entry(ent)
                .await
                .map_err(|e| ReplicationError::RaftInternalErr { msg: e.to_string() })?;
            inner.wal.sync().await.unwrap();
            // 记录日志序号与文件偏移量的关系
            inner.mark_write_wal(seq, wal_id, pos);
        }
        Ok(())
    }

    async fn last_entry(&self) -> ReplicationResult<Option<RaftEntry>> {
        let mut inner = self.inner.lock().await;

        // 读取最后一个日志数据
        if let Some(wal_id_pos) = inner.seq_wal_pos_index.last_entry() {
            let (wal_id, pos) = wal_id_pos.get().to_owned();
            inner.read(wal_id, pos).await
        } else {
            Ok(None)
        }
    }

    // 返回范围内的所有日志
    async fn entries(
        &self,
        begin_seq_no: u64,
        end_seq_no: u64,
    ) -> ReplicationResult<Vec<RaftEntry>> {
        let mut inner = self.inner.lock().await;

        let min_seq: u64;
        let max_seq: u64;
        if let Some(wal_id_pos) = inner.seq_wal_pos_index.first_entry() {
            let seq = *wal_id_pos.key();
            min_seq = seq.max(begin_seq_no);
        } else {
            min_seq = u64::MAX;
        }
        if let Some(wal_id_pos) = inner.seq_wal_pos_index.last_entry() {
            let seq = *wal_id_pos.key();
            max_seq = seq.min(end_seq_no - 1);
        } else {
            max_seq = 0;
        }

        if min_seq > max_seq {
            return Ok(Vec::new());
        }

        // 读取范围数据
        inner.read_range(min_seq..=max_seq).await
    }
}

struct RaftEntryStorageInner {
    /// 具备对一个vnode的数据进行读写的能力 (一个wal目录下所有数据文件)
    wal: VnodeWal,
    /// Maps seq to (WAL id, position).     记录每个日志序列号 对应在wal文件的record偏移量
    seq_wal_pos_index: BTreeMap<u64, (u64, u64)>,
    /// Maps WAL id to it's record count.   标记数据文件的引用次数 避免被提前删除
    wal_ref_count_index: HashMap<u64, u64>,
}

impl RaftEntryStorageInner {
    async fn read(&mut self, wal_id: u64, pos: u64) -> ReplicationResult<Option<RaftEntry>> {
        match self
            .wal
            .read(wal_id, pos)
            .await
            .map_err(|e| ReplicationError::RaftInternalErr { msg: e.to_string() })?
        {
            Some(d) => {
                let entry = match d {
                    Block::RaftLog(e) => Some(e),
                    _ => None,
                };
                Ok(entry)
            }
            None => Ok(None),
        }
    }

    async fn read_range(
        &mut self,
        range: impl RangeBounds<u64>,
    ) -> ReplicationResult<Vec<RaftEntry>> {
        let mut entries = Vec::new();
        // 找到所有日志数据
        for (_seq, (wal_id, pos)) in self.seq_wal_pos_index.range(range) {
            if let Some(Block::RaftLog(e)) = self
                .wal
                .read(*wal_id, *pos)
                .await
                .map_err(|e| ReplicationError::RaftInternalErr { msg: e.to_string() })?
            {
                entries.push(e);
            }
        }

        Ok(entries)
    }

    fn mark_write_wal(&mut self, seq_no: u64, wal_id: u64, pos: u64) {
        // 记录对应关系
        self.seq_wal_pos_index.insert(seq_no, (wal_id, pos));

        // 顺便记录引用次数
        let ref_count = self.wal_ref_count_index.entry(wal_id).or_default();
        *ref_count += 1;
    }

    // 这里只是删除seq_wal_pos_index中的记录 不过能否查到日志记录 就依赖该索引
    fn mark_delete_before(&mut self, seq_no: u64) {
        self.seq_wal_pos_index.retain(|&seq, (wal_id, _)| {
            if seq >= seq_no {
                let ref_count = self.wal_ref_count_index.entry(*wal_id).or_default();
                if *ref_count > 0 {
                    *ref_count -= 1;
                }
                return true;
            }
            false
        });
    }

    fn mark_delete_after(&mut self, seq_no: u64) {
        self.seq_wal_pos_index.retain(|&seq, (wal_id, _)| {
            if seq < seq_no {
                let ref_count = self.wal_ref_count_index.entry(*wal_id).or_default();
                if *ref_count > 0 {
                    *ref_count -= 1;
                }
                return true;
            }
            false
        });
    }

    /// Get id list of old WALs that don't needed.
    /// 找到不再被引用的数据文件
    fn get_empty_old_wal_ids(&self) -> Vec<u64> {
        let current_wal_id = self.wal.current_wal_id();
        let mut wal_ids = Vec::new();
        for (wal_id, ref_count) in self.wal_ref_count_index.iter() {
            if *wal_id == current_wal_id {
                continue;
            }
            if *ref_count == 0 {
                wal_ids.push(*wal_id);
            }
        }
        wal_ids
    }

    /// Read WAL files to recover `Self::seq_wal_pos_index`.
    /// 通过重新加载wal目录下的数据文件 恢复索引结果
    pub async fn recover(&mut self) -> Result<()> {
        let wal_files = file_manager::list_file_names(self.wal.wal_dir());

        // 遍历所有数据文件
        for file_name in wal_files {
            // If file name cannot be parsed to wal id, skip that file.
            let wal_id = match file_utils::get_wal_file_id(&file_name) {
                Ok(id) => id,
                Err(_) => continue,
            };
            let path = self.wal.wal_dir().join(&file_name);
            if !file_manager::try_exists(&path) {
                continue;
            }
            let reader = WalReader::open(&path).await?;
            let mut reader = reader.take_record_reader();
            loop {
                // 不断的读取数据
                let (pos, seq) = match reader.read_record().await {
                    Ok(r) => {
                        if r.data.len() < 9 {
                            continue;
                        }
                        let seq = decode_be_u64(&r.data[1..9]);
                        (r.pos, seq)
                    }
                    Err(Error::Eof) => {
                        break;
                    }
                    Err(Error::RecordFileHashCheckFailed { .. }) => continue,
                    Err(e) => {
                        trace::error!("Error reading wal: {:?}", e);
                        return Err(Error::WalTruncated);
                    }
                };
                self.mark_write_wal(seq, wal_id, pos);
            }
        }

        Ok(())
    }

    fn format_seq_wal_pos_index(&self) -> String {
        let mut buf = String::new();
        if self.seq_wal_pos_index.is_empty() {
            return buf;
        }
        let mut i = 0;
        for (seq, (wal_id, pos)) in self.seq_wal_pos_index.iter() {
            buf.write_fmt(format_args!("[{seq}, ({wal_id}, {pos})]"))
                .unwrap();
            i += 1;
            if i < self.seq_wal_pos_index.len() {
                buf.write_str(", ").unwrap();
            }
        }

        buf
    }
}

#[cfg(test)]
mod test {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::AtomicUsize;
    use std::sync::{atomic, Arc};

    use models::schema::make_owner;
    use replication::apply_store::HeedApplyStorage;
    use replication::node_store::NodeStorage;
    use replication::state_store::StateStorage;
    use replication::{ApplyStorageRef, EntryStorageRef, RaftNodeInfo};

    use crate::wal::raft_store::RaftEntryStorage;
    use crate::wal::VnodeWal;
    use crate::Result;

    pub async fn get_vnode_wal(dir: impl AsRef<Path>) -> Result<VnodeWal> {
        let dir = dir.as_ref();
        let owner = make_owner("cnosdb", "test_db");
        let owner = Arc::new(owner);
        let wal_option = crate::kv_option::WalOptions {
            enabled: true,
            path: dir.to_path_buf(),
            wal_req_channel_cap: 1024,
            max_file_size: 1024 * 1024,
            flush_trigger_total_file_size: 128,
            sync: false,
            sync_interval: std::time::Duration::from_secs(3600),
        };

        VnodeWal::new(Arc::new(wal_option), owner, 1234).await
    }

    pub async fn get_node_store(dir: impl AsRef<Path>) -> Arc<NodeStorage> {
        trace::debug!("----------------------------------------");
        let dir = dir.as_ref();
        let wal = get_vnode_wal(dir).await.unwrap();
        let entry = RaftEntryStorage::new(wal);
        let entry: EntryStorageRef = Arc::new(entry);

        let state = StateStorage::open(dir.join("state")).unwrap();
        let engine = HeedApplyStorage::open(dir.join("engine")).unwrap();

        let state = Arc::new(state);
        let engine: ApplyStorageRef = Arc::new(engine);

        let info = RaftNodeInfo {
            group_id: 2222,
            address: "127.0.0.1:12345".to_string(),
        };

        let storage = NodeStorage::open(1000, info, state, engine, entry).unwrap();

        Arc::new(storage)
    }

    #[test]
    fn test_wal_raft_storage_with_openraft_cases() {
        let dir = PathBuf::from("/tmp/test/wal/raft/1");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        trace::init_default_global_tracing(
            &dir,
            "test_wal_raft_storage_with_openraft_cases",
            "debug",
        );

        let case_id = AtomicUsize::new(0);
        if let Err(e) = openraft::testing::Suite::test_all(|| {
            let id = case_id.fetch_add(1, atomic::Ordering::Relaxed);
            get_node_store(dir.join(id.to_string()))
        }) {
            trace::error!("{e}");
            panic!("{e:?}");
        }
    }

    #[tokio::test]
    #[ignore = "deprecated"]
    async fn test_wal_raft_storage() {
        let dir = PathBuf::from("/tmp/test/wal/raft/2");
        // let _ = std::fs::remove_dir_all(&dir);
        // std::fs::create_dir_all(&dir).unwrap();

        trace::init_default_global_tracing(&dir, "test_wal_raft_storage", "debug");

        let wal = get_vnode_wal(&dir).await.unwrap();
        let storage = RaftEntryStorage::new(wal);
        storage.recover().await.unwrap();
        let inner = storage.inner.lock().await;
        println!("recover finished: {}", inner.format_seq_wal_pos_index());
    }
}
