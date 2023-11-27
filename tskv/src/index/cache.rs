use std::sync::Arc;

use cache::{ShardedSyncCache, SyncCache};
use models::{SeriesId, SeriesKey};

// 将一个系列和它的一组标签组合在一起
#[derive(Debug)]
pub struct SeriesKeyInfo {
    pub key: SeriesKey,
    pub hash: u64,
    pub id: SeriesId,
}

// 正向缓存
#[derive(Debug)]
pub struct ForwardIndexCache {
    // 可以先简单看作是一个kv缓存  通过SeriesId检索
    id_map: ShardedSyncCache<SeriesId, Arc<SeriesKeyInfo>>,
    // 通过hash检索
    hash_map: ShardedSyncCache<u64, Arc<SeriesKeyInfo>>,
}

impl ForwardIndexCache {
    pub fn new(size: usize) -> Self {
        Self {
            id_map: ShardedSyncCache::create_lru_sharded_cache(size),
            hash_map: ShardedSyncCache::create_lru_sharded_cache(size),
        }
    }

    // 加入缓存
    fn add_series_key_info(&self, info: SeriesKeyInfo) {
        let id = info.id;
        let hash = info.hash;
        let info_ref = Arc::new(info);

        self.id_map.insert(id, info_ref.clone());
        self.hash_map.insert(hash, info_ref);
    }

    pub fn add(&self, id: SeriesId, key: SeriesKey) {
        let info = SeriesKeyInfo {
            hash: key.hash(),
            key,
            id,
        };

        self.add_series_key_info(info);
    }

    // 删除缓存
    pub fn del(&self, id: SeriesId, hash: u64) {
        self.id_map.remove(&id);
        self.hash_map.remove(&hash);
    }

    // 从缓存获取数据
    pub fn get_series_id_by_key(&self, key: &SeriesKey) -> Option<SeriesId> {
        let hash = key.hash();

        if let Some(info) = self.hash_map.get(&hash) {
            if info.key.eq(key) {
                return Some(info.id);
            }
        }

        None
    }

    // id不会发生hash碰撞
    pub fn get_series_key_by_id(&self, id: SeriesId) -> Option<SeriesKey> {
        self.id_map.get(&id).map(|info| info.key.clone())
    }
}
