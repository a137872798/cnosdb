pub mod check;
mod compact;
mod flush;
mod iterator;
pub mod job;
mod picker;

use std::sync::Arc;

pub use compact::*;
pub use flush::*;
use parking_lot::RwLock;
pub use picker::*;

use crate::kv_option::StorageOptions;
use crate::memcache::MemCache;
use crate::tseries_family::{ColumnFile, Version};
use crate::{LevelId, TseriesFamilyId};

// 数据压缩任务   包含vnodeId 以及是否为冷热节点
pub enum CompactTask {
    Vnode(TseriesFamilyId),
    ColdVnode(TseriesFamilyId),
}

// 压缩数据的请求
pub struct CompactReq {
    // 本次针对哪个系列
    pub ts_family_id: TseriesFamilyId,
    // 针对的数据库
    pub database: Arc<String>,
    // 一些选项信息
    storage_opt: Arc<StorageOptions>,

    // 每个file 对应一个列数据文件  代表本次要合并的数据文件
    files: Vec<Arc<ColumnFile>>,
    version: Arc<Version>,
    pub out_level: LevelId, // 该值为合并目标level+1 如果目标level为4  该值也是4 (到达上限了)
}

// 代表一个刷盘请求
pub struct FlushReq {
    pub ts_family_id: TseriesFamilyId,
    // 应该说还未刷盘的数据 实际上就是在内存中的缓存?
    pub mems: Vec<Arc<RwLock<MemCache>>>,
    // 是否强制刷盘
    pub force_flush: bool,
    // 代表要刷盘的序列号范围
    pub low_seq_no: u64,
    pub high_seq_no: u64,
}

impl std::fmt::Display for FlushReq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FlushReq on vnode: {}, low_seq_no: {}, high_seq_no: {} caches_num: {}, force_flush: {}",
            self.ts_family_id,
            self.low_seq_no,
            self.high_seq_no,
            self.mems.len(),
            self.force_flush,
        )
    }
}
