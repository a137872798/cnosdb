use std::fs;
use std::path::Path;

use heed::types::*;
use heed::{Database, Env};
use openraft::{LogId, StoredMembership, Vote};
use serde::{Deserialize, Serialize};

use crate::errors::ReplicationResult;
use crate::node_store::StoredSnapshot;
use crate::{RaftNodeId, RaftNodeInfo};

pub struct Key {}

// 基于id 生成各种key
impl Key {
    fn node_summary(id: u32) -> String {
        format!("node_summary_{}", id)
    }

    fn applied_log(id: u32) -> String {
        format!("applied_log_{}", id)
    }

    fn membership(id: u32) -> String {
        format!("membership_{}", id)
    }

    fn purged_log_id(id: u32) -> String {
        format!("purged_log_id_{}", id)
    }

    fn snapshot_index(id: u32) -> String {
        format!("snapshot_index_{}", id)
    }

    fn vote_key(id: u32) -> String {
        format!("vote_{}", id)
    }

    fn snapshot_key(id: u32) -> String {
        format!("snapshot_{}", id)
    }

    fn already_init_key(id: u32) -> String {
        format!("already_init_{}", id)
    }
}


// raft节点的描述信息
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct RaftNodeSummary {
    // 租户信息
    pub tenant: String,
    // 数据库信息
    pub db_name: String,
    // raft组id
    pub group_id: u32,
    // 节点id
    pub raft_id: u64,
}

// 也是用了heed框架
pub struct StateStorage {
    env: Env,
    db: Database<Str, OwnedSlice<u8>>,
}

// 该对象维护了raft协议需要的一些状态信息
impl StateStorage {

    pub fn open(path: impl AsRef<Path>) -> ReplicationResult<Self> {
        fs::create_dir_all(&path)?;

        let env = heed::EnvOpenOptions::new()
            .map_size(1024 * 1024 * 1024)
            .max_dbs(16)
            .open(path)?;

        let db: Database<Str, OwnedSlice<u8>> = env.create_database(Some("stat"))?;
        let storage = Self { env, db };

        Ok(storage)
    }

    // 获取事务读对象
    fn reader_txn(&self) -> ReplicationResult<heed::RoTxn> {
        let reader = self.env.read_txn()?;

        Ok(reader)
    }

    // 获取事务读写对象
    fn writer_txn(&self) -> ReplicationResult<heed::RwTxn> {
        let writer = self.env.write_txn()?;
        Ok(writer)
    }


    // 根据key 查询数据
    fn get<T>(&self, reader: &heed::RoTxn, key: &str) -> ReplicationResult<Option<T>>
    where
        for<'a> T: Deserialize<'a>,
    {
        if let Some(data) = self.db.get(reader, key)? {
            let val = serde_json::from_slice(&data)?;
            Ok(Some(val))
        } else {
            Ok(None)
        }
    }

    // 将val转换成json后 存储到db中
    fn set<T>(&self, writer: &mut heed::RwTxn, key: &str, val: &T) -> ReplicationResult<()>
    where
        for<'a> T: Serialize,
    {
        let data = serde_json::to_vec(val)?;

        self.db.put(writer, key, &data)?;

        Ok(())
    }

    fn del(&self, writer: &mut heed::RwTxn, key: &str) -> ReplicationResult<()> {
        self.db.delete(writer, key)?;

        Ok(())
    }

    // 代表group准备就绪
    pub fn is_already_init(&self, group_id: u32) -> ReplicationResult<bool> {
        let reader = self.env.read_txn()?;
        if self
            .db
            .get(&reader, &Key::already_init_key(group_id))?
            .is_some()
        {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn set_init_flag(&self, group_id: u32) -> ReplicationResult<()> {
        let mut writer = self.writer_txn()?;
        self.db
            .put(&mut writer, &Key::already_init_key(group_id), b"true")?;
        writer.commit()?;

        Ok(())
    }

    // 获取raft组的所有成员
    pub fn get_last_membership(
        &self,
        group_id: u32,
    ) -> ReplicationResult<StoredMembership<RaftNodeId, RaftNodeInfo>> {
        let reader = self.reader_txn()?;
        let mem_ship: StoredMembership<RaftNodeId, RaftNodeInfo> = self
            .get(&reader, &Key::membership(group_id))?
            .unwrap_or_default();

        Ok(mem_ship)
    }

    // 更新成员信息
    pub fn set_last_membership(
        &self,
        group_id: u32,
        membership: StoredMembership<RaftNodeId, RaftNodeInfo>,
    ) -> ReplicationResult<()> {
        let mut writer = self.writer_txn()?;
        self.set(&mut writer, &Key::membership(group_id), &membership)?;
        writer.commit()?;

        Ok(())
    }

    // 查看最新的日志id
    pub fn get_last_applied_log(
        &self,
        group_id: u32,
    ) -> ReplicationResult<Option<LogId<RaftNodeId>>> {
        let reader = self.reader_txn()?;
        let log_id: Option<LogId<RaftNodeId>> = self.get(&reader, &Key::applied_log(group_id))?;

        Ok(log_id)
    }

    // 设置最新的日志id
    pub fn set_last_applied_log(
        &self,
        group_id: u32,
        log_id: LogId<RaftNodeId>,
    ) -> ReplicationResult<()> {
        let mut writer = self.writer_txn()?;
        self.set(&mut writer, &Key::applied_log(group_id), &log_id)?;
        writer.commit()?;

        Ok(())
    }

    // 获取最近一次清除对应的日志id
    pub fn get_last_purged(&self, group_id: u32) -> ReplicationResult<Option<LogId<u64>>> {
        let reader = self.reader_txn()?;
        let log_id: Option<LogId<RaftNodeId>> = self.get(&reader, &Key::purged_log_id(group_id))?;

        Ok(log_id)
    }

    // 设置清除后保留的日志id
    pub fn set_last_purged(&self, group_id: u32, log_id: LogId<u64>) -> ReplicationResult<()> {
        let mut writer = self.writer_txn()?;
        self.set(&mut writer, &Key::purged_log_id(group_id), &log_id)?;
        writer.commit()?;

        Ok(())
    }

    // 获取快照对应的偏移量
    pub fn get_snapshot_index(&self, group_id: u32) -> ReplicationResult<u64> {
        let reader = self.reader_txn()?;
        let index: u64 = self
            .get(&reader, &Key::snapshot_index(group_id))?
            .unwrap_or(0);

        Ok(index)
    }

    // 设置快照偏移量
    pub fn set_snapshot_index(&self, group_id: u32, index: u64) -> ReplicationResult<()> {
        let mut writer = self.writer_txn()?;
        self.set(&mut writer, &Key::snapshot_index(group_id), &index)?;
        writer.commit()?;

        Ok(())
    }

    // 增加快照偏移量
    pub fn incr_snapshot_index(&self, group_id: u32, add: u64) -> ReplicationResult<u64> {
        let mut writer = self.writer_txn()?;
        let index: u64 = self
            .get(&writer, &Key::snapshot_index(group_id))?
            .unwrap_or(0)
            + add;

        self.set(&mut writer, &Key::snapshot_index(group_id), &index)?;
        writer.commit()?;

        Ok(index)
    }

    // 获取最新的投票信息
    pub fn get_vote(&self, group_id: u32) -> ReplicationResult<Option<Vote<RaftNodeId>>> {
        let reader = self.reader_txn()?;
        let vote_val: Option<Vote<RaftNodeId>> = self.get(&reader, &Key::vote_key(group_id))?;

        Ok(vote_val)
    }

    // 进行选票
    pub fn set_vote(&self, group_id: u32, vote: &Vote<RaftNodeId>) -> ReplicationResult<()> {
        let mut writer = self.writer_txn()?;
        self.set(&mut writer, &Key::vote_key(group_id), vote)?;
        writer.commit()?;

        Ok(())
    }

    pub fn get_snapshot(&self, group_id: u32) -> ReplicationResult<Option<StoredSnapshot>> {
        let reader = self.reader_txn()?;
        let snapshot: Option<StoredSnapshot> = self.get(&reader, &Key::snapshot_key(group_id))?;

        Ok(snapshot)
    }

    pub fn set_snapshot(&self, group_id: u32, snap: StoredSnapshot) -> ReplicationResult<()> {
        let mut writer = self.writer_txn()?;
        self.set(&mut writer, &Key::snapshot_key(group_id), &snap)?;
        writer.commit()?;

        Ok(())
    }

    pub fn get_node_summary(&self, group_id: u32) -> ReplicationResult<Option<RaftNodeSummary>> {
        let reader = self.reader_txn()?;
        let summary: Option<RaftNodeSummary> = self.get(&reader, &Key::node_summary(group_id))?;

        Ok(summary)
    }

    pub fn set_node_summary(
        &self,
        group_id: u32,
        summary: &RaftNodeSummary,
    ) -> ReplicationResult<()> {
        let mut writer = self.writer_txn()?;
        self.set(&mut writer, &Key::node_summary(group_id), summary)?;
        writer.commit()?;

        Ok(())
    }

    // summary信息 一开始就存储在db中
    pub fn all_nodes_summary(&self) -> ReplicationResult<Vec<RaftNodeSummary>> {
        let mut nodes_summary = vec![];
        let reader = self.reader_txn()?;
        let iter = self.db.prefix_iter(&reader, "node_summary_")?;
        for pair in iter {
            let (_, data) = pair?;
            let summary: RaftNodeSummary = serde_json::from_slice(&data)?;
            nodes_summary.push(summary);
        }

        Ok(nodes_summary)
    }

    // 删除某个group相关的所有数据
    pub fn del_group(&self, group_id: u32) -> ReplicationResult<()> {
        let mut writer = self.writer_txn()?;
        self.del(&mut writer, &Key::applied_log(group_id))?;
        self.del(&mut writer, &Key::membership(group_id))?;
        self.del(&mut writer, &Key::purged_log_id(group_id))?;
        self.del(&mut writer, &Key::snapshot_index(group_id))?;
        self.del(&mut writer, &Key::vote_key(group_id))?;
        self.del(&mut writer, &Key::snapshot_key(group_id))?;
        self.del(&mut writer, &Key::already_init_key(group_id))?;
        self.del(&mut writer, &Key::node_summary(group_id))?;
        writer.commit()?;

        Ok(())
    }

    pub fn debug(&self) {
        let reader = self.reader_txn().unwrap();
        let iter = self.db.iter(&reader).unwrap();
        for pair in iter {
            let (key, val) = pair.unwrap();
            println!("{}: {}", key, String::from_utf8_lossy(&val));
        }
    }
}

#[cfg(test)]
mod test {
    use std::fs;
    use std::path::Path;

    use heed::types::*;
    use heed::Database;

    #[test]
    #[ignore]
    fn test_heed() {
        let path = "/tmp/cnosdb/test_heed";
        fs::create_dir_all(Path::new(&path)).unwrap();
        let env = heed::EnvOpenOptions::new()
            .map_size(1024 * 1024 * 1024)
            .max_dbs(128)
            .open(path)
            .unwrap();

        let tdb: Database<OwnedType<u64>, Str> = env.create_database(Some("test")).unwrap();
        let mut writer = env.write_txn().unwrap();
        tdb.put(&mut writer, &100, "v100").unwrap();
        tdb.put(&mut writer, &101, "v101").unwrap();
        tdb.put(&mut writer, &102, "v102").unwrap();
        tdb.put(&mut writer, &103, "v103").unwrap();
        tdb.put(&mut writer, &104, "v104").unwrap();
        writer.commit().unwrap();

        let mut writer = env.write_txn().unwrap();
        tdb.delete_range(&mut writer, &(..80)).unwrap();
        writer.commit().unwrap();

        let reader = env.read_txn().unwrap();
        let iter = tdb.range(&reader, &(101..103)).unwrap();
        for pair in iter {
            let (index, data) = pair.unwrap();
            println!("--- {}, {}", index, data);
        }

        fs::remove_dir_all(path).unwrap();
    }
}
