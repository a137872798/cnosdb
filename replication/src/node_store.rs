use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::async_trait::async_trait;
use openraft::storage::{LogState, Snapshot};
use openraft::{
    Entry, EntryPayload, LogId, RaftLogReader, RaftSnapshotBuilder, RaftStorage, RaftTypeConfig,
    SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use trace::info;
use tracing::debug;

use crate::errors::ReplicationResult;
use crate::state_store::StateStorage;
use crate::{
    ApplyContext, ApplyStorageRef, EntryStorageRef, RaftNodeId, RaftNodeInfo, Response, TypeConfig,
};

// 代表被序列化后的快照数据
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct SerializableSnapshot {

    // 记录最后一条日志
    pub last_applied_log: Option<LogId<RaftNodeId>>,
    // 最后集群中识别到的节点
    pub last_membership: StoredMembership<RaftNodeId, RaftNodeInfo>,

    /// Application data.
    pub data: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<RaftNodeId, RaftNodeInfo>,

    /// The data of the Serializable-Snapshot.
    pub data: Vec<u8>,
}

// #[derive(Clone)]
// 描述raft节点有关存储的东西
pub struct NodeStorage {
    id: RaftNodeId,
    info: RaftNodeInfo,

    // 存储一些raft协议需要用到的状态
    state: Arc<StateStorage>,
    // 这里定义节点处理请求的逻辑
    engine: ApplyStorageRef,
    // 存储raft日志
    raft_logs: EntryStorageRef,
}

impl NodeStorage {

    // 将openRaft需要的各个组件组合起来就成了 NodeStorage
    pub fn open(
        id: RaftNodeId,
        info: RaftNodeInfo,
        state: Arc<StateStorage>,
        engine: ApplyStorageRef,
        raft_logs: EntryStorageRef,
    ) -> ReplicationResult<Self> {
        Ok(Self {
            id,
            info,
            state,
            engine,
            raft_logs,
        })
    }

    fn group_id(&self) -> u32 {
        self.info.group_id
    }

    pub async fn destory(&self) -> ReplicationResult<()> {
        // stateStore维护所有raft组的数据 销毁以group为单位
        self.state.del_group(self.group_id())?;
        // 终止应用层服务
        self.engine.destory().await?;

        Ok(())
    }

    // 创建快照
    async fn create_snapshot(&self) -> ReplicationResult<SerializableSnapshot> {
        let data = self.engine.snapshot().await?;
        let snapshot = SerializableSnapshot {
            data,
            last_applied_log: self.state.get_last_applied_log(self.group_id())?,
            last_membership: self.state.get_last_membership(self.group_id())?,
        };

        Ok(snapshot)
    }

    // 从快照中进行恢复
    async fn apply_snapshot(&self, sm: SerializableSnapshot) -> ReplicationResult<()> {
        let log_id = sm.last_applied_log.unwrap_or_default();
        self.state.set_last_applied_log(self.group_id(), log_id)?;
        self.state
            .set_last_membership(self.group_id(), sm.last_membership)?;

        self.engine.restore(&sm.data).await?;

        Ok(())
    }
}

type StorageResult<T> = Result<T, StorageError<RaftNodeId>>;

// openRaft开放的api 当需要读取raft日志时调用
#[async_trait]
impl RaftLogReader<TypeConfig> for Arc<NodeStorage> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &mut self,
        range: RB,
    ) -> StorageResult<Vec<Entry<TypeConfig>>> {
        debug!("Storage callback try_get_log_entries: [{:?})", range);

        let start = match range.start_bound() {
            std::ops::Bound::Included(x) => *x,
            std::ops::Bound::Excluded(x) => *x + 1,
            std::ops::Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            std::ops::Bound::Included(x) => *x + 1,
            std::ops::Bound::Excluded(x) => *x,
            std::ops::Bound::Unbounded => u64::MAX,
        };

        let entries = self
            .raft_logs
            .entries(start, end)
            .await
            .map_err(|e| StorageError::IO {
                source: StorageIOError::read_logs(&e),
            })?;

        Ok(entries)
    }
}

// 描述如何构建快照
#[async_trait]
impl RaftSnapshotBuilder<TypeConfig> for Arc<NodeStorage> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<RaftNodeId>> {
        debug!("Storage callback build_snapshot");

        // 创建快照
        let snapshot = self
            .create_snapshot()
            .await
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let snap_data =
            bincode::serialize(&snapshot).map_err(|e| StorageIOError::read_state_machine(&e))?;

        // 增加快照偏移量
        let snapshot_idx = self
            .state
            .incr_snapshot_index(self.group_id(), 1)
            .map_err(|e| StorageIOError::write_state_machine(&e))?;

        let snapshot_id = if let Some(last) = snapshot.last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let meta = SnapshotMeta {
            snapshot_id,
            last_log_id: snapshot.last_applied_log,
            last_membership: snapshot.last_membership,
        };

        let snapshot_stored = StoredSnapshot {
            meta: meta.clone(),
            data: snap_data.clone(),
        };

        // 快照数据通过 StateStore存储
        self.state
            .set_snapshot(self.group_id(), snapshot_stored)
            .map_err(|e| StorageIOError::write_state_machine(&e))?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(snap_data)),
        })
    }
}

#[async_trait]
impl RaftStorage<TypeConfig> for Arc<NodeStorage> {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    // 获取日志状态
    async fn get_log_state(&mut self) -> StorageResult<LogState<TypeConfig>> {
        debug!("Storage callback get_log_state");

        // 最后一条日志
        let last = self
            .raft_logs
            .last_entry()
            .await
            .map_err(|e| StorageIOError::read_logs(&e))?
            .map(|ent| ent.log_id);

        // 上次清理到的日志
        let last_purged_log_id = self
            .state
            .get_last_purged(self.group_id())
            .map_err(|e| StorageIOError::read(&e))?;

        let last_log_id = match last {
            None => last_purged_log_id,
            Some(x) => Some(x),
        };
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    // 存储vote信息
    #[tracing::instrument(level = "trace", skip(self))]
    async fn save_vote(&mut self, vote: &Vote<RaftNodeId>) -> Result<(), StorageError<RaftNodeId>> {
        debug!("Storage callback save_vote vote: {:?}", vote);

        self.state
            .set_vote(self.group_id(), vote)
            .map_err(|e| StorageError::IO {
                source: StorageIOError::write_vote(&e),
            })?;

        Ok(())
    }

    // 读取vote信息
    async fn read_vote(&mut self) -> Result<Option<Vote<RaftNodeId>>, StorageError<RaftNodeId>> {
        debug!("Storage callback read_vote");

        let vote = self
            .state
            .get_vote(self.group_id())
            .map_err(|e| StorageError::IO {
                source: StorageIOError::read_vote(&e),
            })?;

        Ok(vote)
    }

    // 在raft集群中 收到的请求将作为日志保存
    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn append_to_log<I>(&mut self, entries: I) -> StorageResult<()>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let entries: Vec<Entry<TypeConfig>> = entries.into_iter().collect();
        if entries.is_empty() {
            return Ok(());
        }

        let begin = entries.first().map_or(0, |ent| ent.log_id.index);
        let end = entries.last().map_or(0, |ent| ent.log_id.index);
        debug!("Storage callback append_to_log entires:[{}~{}]", begin, end);

        self.raft_logs
            .append(&entries)
            .await
            .map_err(|e| StorageIOError::write_logs(&e))?;

        Ok(())
    }

    // 当新节点被推举为leader后 之前的节点可能会有一些脏数据 需要清理
    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_conflict_logs_since(&mut self, log_id: LogId<RaftNodeId>) -> StorageResult<()> {
        debug!(
            "Storage callback delete_conflict_logs_since log_id: {:?}",
            log_id
        );

        self.raft_logs
            .del_after(log_id.index)
            .await
            .map_err(|e| StorageIOError::write_logs(&e))?;

        Ok(())
    }

    // 清理指定偏移量前的日志
    #[tracing::instrument(level = "debug", skip(self))]
    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<RaftNodeId>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        debug!("Storage callback purge_logs_upto log_id: {:?}", log_id);

        self.state
            .set_last_purged(self.group_id(), log_id)
            .map_err(|e| StorageIOError::write(&e))?;

        self.raft_logs
            .del_before(log_id.index + 1)
            .await
            .map_err(|e| StorageIOError::write_logs(&e))?;

        Ok(())
    }

    // 获取最后的状态
    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<RaftNodeId>>,
            StoredMembership<RaftNodeId, RaftNodeInfo>,
        ),
        StorageError<RaftNodeId>,
    > {
        debug!("Storage callback last_applied_state");

        let log_id = self
            .state
            .get_last_applied_log(self.group_id())
            .map_err(|e| StorageIOError::read_state_machine(&e))?;

        let member_ship = self
            .state
            .get_last_membership(self.group_id())
            .map_err(|e| StorageIOError::read_state_machine(&e))?;

        Ok((log_id, member_ship))
    }

    // 接受请求 并交给状态机处理  (从协议层到应用层)
    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<Response>, StorageError<RaftNodeId>> {
        let mut res = Vec::with_capacity(entries.len());

        for entry in entries {
            debug!(
                "Storage callback apply_to_state_machine log_id: {:?}",
                entry.log_id
            );

            // 每个请求都会留档为一条日志
            self.state
                .set_last_applied_log(self.group_id(), entry.log_id)
                .map_err(|e| StorageIOError::write(&e))?;

            match entry.payload {
                EntryPayload::Blank => {
                    res.push(vec![]);
                }

                EntryPayload::Normal(ref req) => {
                    let ctx = ApplyContext {
                        index: entry.log_id.index,
                        raft_id: self.id,
                    };
                    // 交给应用层处理
                    let rsp = self
                        .engine
                        .apply(&ctx, req)
                        .await
                        .map_err(|e| StorageIOError::write(&e))?;

                    res.push(rsp);
                }

                // 代表收到的是一条修改成员关系的请求
                EntryPayload::Membership(ref mem) => {
                    self.state
                        .set_last_membership(
                            self.group_id(),
                            StoredMembership::new(Some(entry.log_id), mem.clone()),
                        )
                        .map_err(|e| StorageIOError::write(&e))?;

                    res.push(vec![]);
                }
            };
        }

        Ok(res)
    }

    // 产生一个空的快照对象 用于接收leader发送过来的快照
    #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<<TypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<RaftNodeId>> {
        debug!("Storage callback begin_receiving_snapshot");

        Ok(Box::new(Cursor::new(Vec::new())))
    }

    // 基于快照恢复本地数据
    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<RaftNodeId, RaftNodeInfo>,
        snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        debug!(
            "Storage callback install_snapshot size: {}",
            snapshot.get_ref().len()
        );

        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        // Update the state machine.

        let updated_snapshot: SerializableSnapshot = bincode::deserialize(&new_snapshot.data)
            .map_err(|e| StorageIOError::read_snapshot(Some(new_snapshot.meta.signature()), &e))?;

        self.apply_snapshot(updated_snapshot)
            .await
            .map_err(|e| StorageIOError::write(&e))?;

        self.state
            .set_snapshot(self.group_id(), new_snapshot)
            .map_err(|e| StorageIOError::write(&e))?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<RaftNodeId>> {
        debug!("Storage callback get_current_snapshot");

        match self
            .state
            .get_snapshot(self.group_id())
            .map_err(|e| StorageIOError::read_state_machine(&e))?
        {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta,
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => Ok(None),
        }
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        debug!("Storage callback get_log_reader");

        self.clone()
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        debug!("Storage callback get_snapshot_builder");

        self.clone()
    }
}

mod test {
    use std::sync::Arc;

    use super::NodeStorage;
    use crate::apply_store::HeedApplyStorage;
    use crate::entry_store::HeedEntryStorage;
    use crate::state_store::StateStorage;
    use crate::{ApplyStorageRef, EntryStorageRef, RaftNodeInfo};

    #[test]
    pub fn test_node_store() {
        let path = "/tmp/cnosdb/test_raft_store".to_string();
        std::fs::remove_dir_all(path.clone());
        std::fs::create_dir_all(path.clone()).unwrap();

        openraft::testing::Suite::test_all(get_node_store).unwrap();
        std::fs::remove_dir_all(path);
    }

    pub async fn get_node_store() -> Arc<NodeStorage> {
        let path = tempfile::tempdir_in("/tmp/cnosdb/test_raft_store").unwrap();

        let state = StateStorage::open(path.path().join("state")).unwrap();
        let entry = HeedEntryStorage::open(path.path().join("entry")).unwrap();
        let engine = HeedApplyStorage::open(path.path().join("engine")).unwrap();

        let state = Arc::new(state);
        let entry: EntryStorageRef = Arc::new(entry);
        let engine: ApplyStorageRef = Arc::new(engine);

        let info = RaftNodeInfo {
            group_id: 2222,
            address: "127.0.0.1:1234".to_string(),
        };

        let storage = NodeStorage::open(1000, info, state, engine, entry).unwrap();

        Arc::new(storage)
    }
}
