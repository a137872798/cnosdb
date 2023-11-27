use std::fs;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use heed::byteorder::BigEndian;
use heed::types::*;
use heed::{Database, Env};
use openraft::Entry;

use crate::errors::ReplicationResult;
use crate::{EntryStorage, TypeConfig};

// --------------------------------------------------------------------------- //
type BEU64 = U64<BigEndian>;

// 简单看作一个存储
pub struct HeedEntryStorage {
    // 这个是配置信息
    env: Env,
    // 这个是存储容器
    db: Database<OwnedType<BEU64>, OwnedSlice<u8>>,
}


// 注意条目存储
impl HeedEntryStorage {
    // 也是根据指定的目录初始化  存储注意条目  (在元数据服务器上被创建)
    pub fn open(path: impl AsRef<Path>) -> ReplicationResult<Self> {
        fs::create_dir_all(&path)?;

        let env = heed::EnvOpenOptions::new()
            .map_size(1024 * 1024 * 1024)
            .max_dbs(128)
            .open(path)?;

        // 第三方库 先简单看作一个存储
        let db: Database<OwnedType<BEU64>, OwnedSlice<u8>> =
            env.create_database(Some("entries"))?;
        let storage = Self { env, db };

        Ok(storage)
    }

    // 清空db中的数据
    fn clear(&self) -> ReplicationResult<()> {
        // writer 应该是为了避免数据被删除到一半
        let mut writer = self.env.write_txn()?;
        self.db.clear(&mut writer)?;
        writer.commit()?;

        Ok(())
    }
}

// 借助heed框架 存储raft日志
#[async_trait]
impl EntryStorage for HeedEntryStorage {

    // 往storage中追加一个entry
    async fn append(&self, ents: &[Entry<TypeConfig>]) -> ReplicationResult<()> {
        if ents.is_empty() {
            return Ok(());
        }

        // Remove all entries overwritten by `ents`.
        // index 就是每条记录的编号
        let begin = ents[0].log_id.index;
        // 每当要插入一个新的entry时 之后的数据都需要删除 因为是脏数据 (回忆raft算法)
        self.del_after(begin).await?;

        // 事务写入
        let mut writer = self.env.write_txn()?;
        for entry in ents {
            let index = entry.log_id.index;

            let data = bincode::serialize(entry)?;
            self.db.put(&mut writer, &BEU64::new(index), &data)?;
        }
        writer.commit()?;

        Ok(())
    }

    async fn last_entry(&self) -> ReplicationResult<Option<Entry<TypeConfig>>> {
        let reader = self.env.read_txn()?;
        if let Some((_, data)) = self.db.last(&reader)? {
            let entry = bincode::deserialize::<Entry<TypeConfig>>(&data)?;
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }

    async fn entry(&self, index: u64) -> ReplicationResult<Option<Entry<TypeConfig>>> {
        let reader = self.env.read_txn()?;
        if let Some(data) = self.db.get(&reader, &BEU64::new(index))? {
            let entry = bincode::deserialize::<Entry<TypeConfig>>(&data)?;
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }

    // 返回范围内数据
    async fn entries(&self, low: u64, high: u64) -> ReplicationResult<Vec<Entry<TypeConfig>>> {
        let mut ents = vec![];

        let reader = self.env.read_txn()?;
        let range = BEU64::new(low)..BEU64::new(high);
        let iter = self.db.range(&reader, &range)?;
        for pair in iter {
            let (_, data) = pair?;
            ents.push(bincode::deserialize::<Entry<TypeConfig>>(&data)?);
        }

        Ok(ents)
    }

    // 删除index之后的数据
    async fn del_after(&self, index: u64) -> ReplicationResult<()> {
        let mut writer = self.env.write_txn()?;
        let range = BEU64::new(index)..;
        self.db.delete_range(&mut writer, &range)?;
        writer.commit()?;

        Ok(())
    }

    async fn del_before(&self, index: u64) -> ReplicationResult<()> {
        let mut writer = self.env.write_txn()?;
        let range = ..BEU64::new(index);
        self.db.delete_range(&mut writer, &range)?;
        writer.commit()?;

        Ok(())
    }
}

mod test {
    use heed::byteorder::BigEndian;
    use heed::types::*;
    use heed::Database;

    #[test]
    #[ignore]
    fn test_heed_range() {
        type BEU64 = U64<BigEndian>;

        let path = "/tmp/cnosdb/8201-entry";
        let env = heed::EnvOpenOptions::new()
            .map_size(1024 * 1024 * 1024)
            .max_dbs(128)
            .open(path)
            .unwrap();

        let db: Database<OwnedType<BEU64>, OwnedSlice<u8>> =
            env.create_database(Some("entries")).unwrap();

        let mut wtxn = env.write_txn().unwrap();
        let range = ..BEU64::new(4);
        db.delete_range(&mut wtxn, &range).unwrap();
        wtxn.commit().unwrap();

        let reader = env.read_txn().unwrap();
        let range = BEU64::new(0)..BEU64::new(1000000);
        let iter = db.range(&reader, &range).unwrap();
        for pair in iter {
            let (index, _) = pair.unwrap();
            println!("--- {}", index);
        }
    }
}
