use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::fmt::{Display, Formatter};
use std::pin::Pin;
use std::sync::Arc;

use models::predicate::domain::TimeRange;
use models::{FieldId, Timestamp};
use snafu::ResultExt;
use trace::{error, info, trace};
use utils::BloomFilter;

use super::iterator::BufferedIterator;
use crate::compaction::CompactReq;
use crate::context::GlobalContext;
use crate::error::{self, Result};
use crate::summary::{CompactMeta, VersionEdit};
use crate::tseries_family::TseriesFamily;
use crate::tsm::{
    self, BlockMeta, BlockMetaIterator, DataBlock, EncodedDataBlock, IndexIterator, IndexMeta,
    TsmReader, TsmWriter,
};
use crate::{ColumnFileId, Error, LevelId, TseriesFamilyId};

/// Temporary compacting data block meta
/// 在数据合并过程中用到的临时的元数据
#[derive(Clone)]
pub(crate) struct CompactingBlockMeta {
    reader_idx: usize,   // 对应第几个数据文件
    reader: Arc<TsmReader>,  // 当前正在读取的数据文件
    meta: BlockMeta,  // 这个对象是配合reader使用的  便于快速读取DataBlock
}

impl PartialEq for CompactingBlockMeta {
    fn eq(&self, other: &Self) -> bool {
        self.reader.file_id() == other.reader.file_id() && self.meta == other.meta
    }
}

impl Eq for CompactingBlockMeta {}

impl PartialOrd for CompactingBlockMeta {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CompactingBlockMeta {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.meta.cmp(&other.meta)
    }
}

impl Display for CompactingBlockMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {{ len: {}, min_ts: {}, max_ts: {} }}",
            self.meta.field_type(),
            self.meta.count(),
            self.meta.min_ts(),
            self.meta.max_ts(),
        )
    }
}

// 在数据合并过程中 会用到的block元数据
impl CompactingBlockMeta {
    pub fn new(tsm_reader_idx: usize, tsm_reader: Arc<TsmReader>, block_meta: BlockMeta) -> Self {
        Self {
            reader_idx: tsm_reader_idx,
            reader: tsm_reader,
            meta: block_meta,
        }
    }

    // 获得这块数据的时间范围
    pub fn time_range(&self) -> TimeRange {
        self.meta.time_range()
    }

    // 检查2个元数据之间是否有交集
    pub fn overlaps(&self, other: &Self) -> bool {
        self.meta.min_ts() <= other.meta.max_ts() && self.meta.max_ts() >= other.meta.min_ts()
    }

    pub fn overlaps_time_range(&self, time_range: &TimeRange) -> bool {
        self.meta.min_ts() <= time_range.max_ts && self.meta.max_ts() >= time_range.min_ts
    }

    // 可以根据元数据信息读取DataBlock
    pub async fn get_data_block(&self) -> Result<DataBlock> {
        self.reader
            .get_data_block(&self.meta)
            .await
            .context(error::ReadTsmSnafu)
    }

    pub async fn get_raw_data(&self, dst: &mut Vec<u8>) -> Result<usize> {
        self.reader
            .get_raw_data(&self.meta, dst)
            .await
            .context(error::ReadTsmSnafu)
    }

    // 表示该数据文件是否有相关的坟墓数据
    pub fn has_tombstone(&self) -> bool {
        self.reader.has_tombstone()
    }
}

// 每个BlockMeta 对应一个DataBlock 在合并过程中会读取到多个数据文件 每个数据块对应一个CompactingBlockMeta
// CompactingBlockMetaGroup 就对应多个数据块
#[derive(Clone)]
pub(crate) struct CompactingBlockMetaGroup {
    field_id: FieldId,
    blk_metas: Vec<CompactingBlockMeta>,
    time_range: TimeRange,
}

impl CompactingBlockMetaGroup {

    // 一开始只有一个block的数据块
    pub fn new(field_id: FieldId, blk_meta: CompactingBlockMeta) -> Self {
        let time_range = blk_meta.time_range();
        Self {
            field_id,
            blk_metas: vec![blk_meta],
            time_range,
        }
    }

    pub fn overlaps(&self, other: &Self) -> bool {
        self.time_range.overlaps(&other.time_range)
    }

    pub fn append(&mut self, other: &mut CompactingBlockMetaGroup) {
        self.blk_metas.append(&mut other.blk_metas);
        self.time_range.merge(&other.time_range);
    }

    // 将内部多个 blk_metas 描述的block数据进行合并
    pub async fn merge(
        mut self,
        previous_block: Option<CompactingBlock>,  // 这个block与本次的group一定是没有时间交集的
        max_block_size: usize,
    ) -> Result<Vec<CompactingBlock>> {
        // 无block数据 不需要合并
        if self.blk_metas.is_empty() {
            return Ok(vec![]);
        }
        // 先按照文件顺序排序
        self.blk_metas
            .sort_by(|a, b| a.reader_idx.cmp(&b.reader_idx).reverse());

        let merged_block;

        // compact过程中 要顺便去掉墓碑数据  如果没有墓碑数据 相当于不用处理
        if self.blk_metas.len() == 1 && !self.blk_metas[0].has_tombstone() {
            // Only one compacting block and has no tombstone, write as raw block.
            trace!("only one compacting block, write as raw block");
            let meta_0 = &self.blk_metas[0].meta;
            let mut buf_0 = Vec::with_capacity(meta_0.size() as usize);

            // 直接读取原始数据 读取过来的也是编码过的
            let data_len_0 = self.blk_metas[0].get_raw_data(&mut buf_0).await?;
            buf_0.truncate(data_len_0);

            if meta_0.size() >= max_block_size as u64 {
                // Raw data block is full, so do not merge with the previous, directly return.
                // 当前block已经超过了 maxSize 不需要跟previousBlock合并了
                let mut merged_blks = Vec::new();
                if let Some(blk) = previous_block {
                    merged_blks.push(blk);
                }
                merged_blks.push(CompactingBlock::raw(
                    self.blk_metas[0].reader_idx,
                    meta_0.clone(),
                    buf_0,
                ));

                return Ok(merged_blks);

                // 此时本次block数据没有超过 maxBlock 与上个进行合并
            } else if let Some(compacting_block) = previous_block {
                // Raw block is not full, so decode and merge with compacting_block.
                // 要先解码 然后进行数据合并
                let decoded_raw_block = tsm::decode_data_block(
                    &buf_0,
                    meta_0.field_type(),
                    meta_0.val_off() - meta_0.offset(),
                )
                .context(error::ReadTsmSnafu)?;

                // previousBlock 与当前block无时间交集 直接将数据合并在一起即可 难点在于有交集的数据合并
                let mut data_block = compacting_block.decode()?;
                data_block.extend(decoded_raw_block);

                merged_block = data_block;
            } else {
                // Raw block is not full, but nothing to merge with, directly return.
                // 虽然当前block未满 但是还没有previousBlock 所以直接返回即可
                return Ok(vec![CompactingBlock::raw(
                    self.blk_metas[0].reader_idx,
                    meta_0.clone(),
                    buf_0,
                )]);
            }
        } else {
            // One block with tombstone or multi compacting blocks, decode and merge these data block.
            // 要开始数据合并了  还会顺便去掉墓碑标记的范围数据
            trace!(
                "there are {} compacting blocks, need to decode and merge",
                self.blk_metas.len()
            );
            let head = &mut self.blk_metas[0];

            // 读取dataBlock数据 会自动完成decode 以及去除掉坟墓数据
            let mut head_block = head.get_data_block().await?;


            // 将previous_block与本block合并 因为这2个block 已经确保没有时间交集了 所以可以直接追加
            if let Some(compacting_block) = previous_block {
                let mut data_block = compacting_block.decode()?;
                data_block.extend(head_block);
                head_block = data_block;
            }

            // 剩下的数据块 进行合并
            for blk_meta in self.blk_metas[1..].iter_mut() {
                // Merge decoded data block.
                let blk_block = blk_meta.get_data_block().await?;
                // 将block 一条条合并 就是通过比较时间戳
                head_block = head_block.merge(blk_block);
            }
            merged_block = head_block;
        }

        // 将数据块 按照size进行拆解
        self.chunk_merged_block(merged_block, max_block_size)
    }

    // 将数据块 按照size拆分
    fn chunk_merged_block(
        &self,
        data_block: DataBlock,
        max_block_size: usize,
    ) -> Result<Vec<CompactingBlock>> {
        let mut merged_blks = Vec::new();

        // 不需要处理 产生一个表示已经解码过的block
        if max_block_size == 0 || data_block.len() < max_block_size {
            // Data block elements less than max_block_size, do not encode it.
            // Try to merge with the next CompactingBlockMetaGroup.
            merged_blks.push(CompactingBlock::decoded(0, self.field_id, data_block));
        } else {
            let len = data_block.len();
            let mut start = 0;
            let mut end = len.min(max_block_size);

            // 每次读取一个block的大小
            while start + end < len {
                // Encode decoded data blocks into chunks.
                let encoded_blk =
                    EncodedDataBlock::encode(&data_block, start, end).map_err(|e| {
                        Error::WriteTsm {
                            source: tsm::WriteTsmError::Encode { source: e },
                        }
                    })?;
                merged_blks.push(CompactingBlock::encoded(0, self.field_id, encoded_blk));

                start += end;
                end = len.min(start + max_block_size);
            }

            // 剩下的包装成一个block
            if start < len {
                // Encode the remaining decoded data blocks.
                let encoded_blk =
                    EncodedDataBlock::encode(&data_block, start, len).map_err(|e| {
                        Error::WriteTsm {
                            source: tsm::WriteTsmError::Encode { source: e },
                        }
                    })?;
                merged_blks.push(CompactingBlock::encoded(0, self.field_id, encoded_blk));
            }
        }

        Ok(merged_blks)
    }

    pub fn is_empty(&self) -> bool {
        self.blk_metas.is_empty()
    }

    pub fn len(&self) -> usize {
        self.blk_metas.len()
    }
}

/// Temporary compacting data block.
/// - priority: When merging two (timestamp, value) pair with the same
/// timestamp from two data blocks, pair from data block with lower
/// priority will be discarded.
/// 描述合并中的数据块的状态
pub(crate) enum CompactingBlock {

    // 代表解码后的数据
    Decoded {
        priority: usize,
        field_id: FieldId,
        data_block: DataBlock,
    },
    // 编码后的数据
    Encoded {
        priority: usize,
        field_id: FieldId,
        data_block: EncodedDataBlock,
    },
    // 字节流数据
    Raw {
        priority: usize,
        meta: BlockMeta,
        raw: Vec<u8>,
    },
}

impl Display for CompactingBlock {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CompactingBlock::Decoded {
                priority,
                field_id,
                data_block,
            } => {
                write!(f, "p: {priority}, f: {field_id}, block: {data_block}")
            }
            CompactingBlock::Encoded {
                priority,
                field_id,
                data_block,
            } => {
                write!(f, "p: {priority}, f: {field_id}, block: {data_block}")
            }
            CompactingBlock::Raw { priority, meta, .. } => {
                write!(
                    f,
                    "p: {priority}, f: {}, block: {}: {{ len: {}, min_ts: {}, max_ts: {} }}",
                    meta.field_id(),
                    meta.field_type(),
                    meta.count(),
                    meta.min_ts(),
                    meta.max_ts()
                )
            }
        }
    }
}

impl CompactingBlock {
    pub fn decoded(priority: usize, field_id: FieldId, data_block: DataBlock) -> CompactingBlock {
        Self::Decoded {
            priority,
            field_id,
            data_block,
        }
    }

    pub fn encoded(
        priority: usize,
        field_id: FieldId,
        data_block: EncodedDataBlock,
    ) -> CompactingBlock {
        Self::Encoded {
            priority,
            field_id,
            data_block,
        }
    }

    pub fn raw(priority: usize, meta: BlockMeta, raw: Vec<u8>) -> CompactingBlock {
        CompactingBlock::Raw {
            priority,
            meta,
            raw,
        }
    }

    pub fn decode(self) -> Result<DataBlock> {
        match self {
            CompactingBlock::Decoded { data_block, .. } => Ok(data_block),
            CompactingBlock::Encoded { data_block, .. } => {
                data_block.decode().context(error::DecodeSnafu)
            }
            CompactingBlock::Raw { raw, meta, .. } => {
                tsm::decode_data_block(&raw, meta.field_type(), meta.val_off() - meta.offset())
                    .context(error::ReadTsmSnafu)
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            CompactingBlock::Decoded { data_block, .. } => data_block.len(),
            CompactingBlock::Encoded { data_block, .. } => data_block.count as usize,
            CompactingBlock::Raw { meta, .. } => meta.count() as usize,
        }
    }
}

// 代表合并中的数据文件
struct CompactingFile {
    // 文件序号
    i: usize,
    // 该对象用于读取底层文件
    tsm_reader: Arc<TsmReader>,
    // 该迭代器可以缓存最近读取的值
    index_iter: BufferedIterator<IndexIterator>,
    // 当前正在读取的field
    field_id: Option<FieldId>,
}

impl CompactingFile {
    fn new(i: usize, tsm_reader: Arc<TsmReader>) -> Self {
        // 该对象是用于遍历 IndexMeta的
        let mut index_iter = BufferedIterator::new(tsm_reader.index_iterator());
        // 通过元数据可以知道这是哪一列
        let first_field_id = index_iter.peek().map(|i| i.field_id());
        Self {
            i,
            tsm_reader,
            index_iter,
            field_id: first_field_id,
        }
    }

    // 弹出最上面的indexMeta  同时更新field_id
    fn next(&mut self) -> Option<&IndexMeta> {
        let idx_meta = self.index_iter.next();
        idx_meta.map(|i| self.field_id.replace(i.field_id()));
        idx_meta
    }

    fn peek(&mut self) -> Option<&IndexMeta> {
        self.index_iter.peek()
    }
}

impl Eq for CompactingFile {}

impl PartialEq for CompactingFile {
    fn eq(&self, other: &Self) -> bool {
        self.tsm_reader.file_id() == other.tsm_reader.file_id() && self.field_id == other.field_id
    }
}

impl Ord for CompactingFile {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.field_id.cmp(&other.field_id).reverse()
    }
}

impl PartialOrd for CompactingFile {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// 通过该迭代器 可以将不同reader中的数据合并
pub(crate) struct CompactIterator {
    tsm_readers: Vec<Arc<TsmReader>>,

    /// 这些reader在一开始就进入了堆中
    compacting_files: BinaryHeap<Pin<Box<CompactingFile>>>,
    /// Maximum values in generated CompactingBlock
    max_data_block_size: usize,
    // /// The time range of data to be merged of level-0 data blocks.
    // /// The level-0 data that out of the thime range will write back to level-0.
    // level_time_range: TimeRange,
    /// Decode a data block even though it doesn't need to merge with others,
    /// return CompactingBlock::DataBlock rather than CompactingBlock::Raw .
    decode_non_overlap_blocks: bool,

    tmp_tsm_blk_meta_iters: Vec<BlockMetaIterator>,
    /// Index to mark `Peekable<BlockMetaIterator>` in witch `TsmReader`,
    /// tmp_tsm_blks[i] is in self.tsm_readers[ tmp_tsm_blk_tsm_reader_idx[i] ]
    tmp_tsm_blk_tsm_reader_idx: Vec<usize>,
    /// When a TSM file at index i is ended, finished_idxes[i] is set to true.
    /// 代表读完了几个数据文件
    finished_readers: Vec<bool>,
    /// How many finished_idxes is set to true.
    finished_reader_cnt: usize,

    /// 该迭代器当前正在扫描的field
    curr_fid: Option<FieldId>,

    merging_blk_meta_groups: VecDeque<CompactingBlockMetaGroup>,
}

/// To reduce construction code
impl Default for CompactIterator {
    fn default() -> Self {
        Self {
            tsm_readers: Default::default(),
            compacting_files: Default::default(),
            max_data_block_size: 0,
            decode_non_overlap_blocks: false,
            tmp_tsm_blk_meta_iters: Default::default(),
            tmp_tsm_blk_tsm_reader_idx: Default::default(),
            finished_readers: Default::default(),
            finished_reader_cnt: Default::default(),
            curr_fid: Default::default(),
            merging_blk_meta_groups: Default::default(),
        }
    }
}

impl CompactIterator {
    pub(crate) fn new(
        tsm_readers: Vec<Arc<TsmReader>>,   // 多个reader的数据 经过迭代器处理后自动合并
        max_data_block_size: usize,
        decode_non_overlap_blocks: bool,
    ) -> Self {

        // 这个堆 对比的是field的id  也就是将所有reader 按列进行读取和合并
        let compacting_files: BinaryHeap<Pin<Box<CompactingFile>>> = tsm_readers
            .iter()
            .enumerate()
            .map(|(i, r)| Box::pin(CompactingFile::new(i, r.clone())))
            .collect();
        let compacting_files_cnt = compacting_files.len();

        Self {
            tsm_readers,
            compacting_files,
            max_data_block_size,
            decode_non_overlap_blocks,
            finished_readers: vec![false; compacting_files_cnt],
            ..Default::default()
        }
    }

    /// Update tmp_tsm_blks and tmp_tsm_blk_tsm_reader_idx for field id in next iteration.
    /// 一次性加载所有文件有关某field的数据到tmp迭代器中
    fn next_field_id(&mut self) {
        self.curr_fid = None;

        // 因为每个文件的schema应该是一样的 所以只要看第一个就可以了
        if let Some(f) = self.compacting_files.peek() {
            if self.curr_fid.is_none() {
                trace!(
                    "selected new field {:?} from file {} as current field id",
                    f.field_id,
                    f.tsm_reader.file_id()
                );
                self.curr_fid = f.field_id
            }
        } else {
            // 这个就是没文件了
            // TODO finished
            trace!("no file to select, mark finished");
            self.finished_reader_cnt += 1;
        }

        // 现在的目标 就是将所有文件同一列的迭代器拉取出来 一起处理
        while let Some(mut f) = self.compacting_files.pop() {
            // 得到此时正在读取的列
            let loop_field_id = f.field_id;
            let loop_file_i = f.i;

            // 在loop中 将每个文件同一列的数据读取出来
            if self.curr_fid == loop_field_id {
                // 获取当前indexMeta
                if let Some(idx_meta) = f.peek() {
                    // 读取里面的blockMeta 并设置进迭代器
                    self.tmp_tsm_blk_meta_iters.push(idx_meta.block_iterator());
                    // 获取当前参与本轮field compact的文件
                    self.tmp_tsm_blk_tsm_reader_idx.push(loop_file_i);
                    trace!("merging idx_meta: field_id: {}, field_type: {:?}, block_count: {}, time_range: {:?}",
                        idx_meta.field_id(),
                        idx_meta.field_type(),
                        idx_meta.block_count(),
                        idx_meta.time_range()
                    );
                    // 把indexMeta 弹出后 重新进入堆  因为此时fieldId发生变化 就不会停留在heap的上面了
                    f.next();
                    self.compacting_files.push(f);
                } else {
                    // This tsm-file has been finished
                    // 代表某个文件被读取完了
                    trace!("file {} is finished.", loop_file_i);
                    self.finished_readers[loop_file_i] = true;
                    self.finished_reader_cnt += 1;
                }
            } else {
                // 代表所有文件都轮了一遍 该field的数据都已经加载出来了 结束循环
                self.compacting_files.push(f);
                break;
            }
        }
    }

    /// Collect merging `DataBlock`s.
    /// 根据tmp迭代器中的数据 产生group数据
    async fn fetch_merging_block_meta_groups(&mut self) -> bool {

        // 此时没有待处理的数据 直接返回
        if self.tmp_tsm_blk_meta_iters.is_empty() {
            return false;
        }
        let field_id = match self.curr_fid {
            Some(fid) => fid,
            None => return false,
        };

        // 每个CompactingBlockMeta 对应一个block迭代器
        let mut blk_metas: Vec<CompactingBlockMeta> =
            Vec::with_capacity(self.tmp_tsm_blk_meta_iters.len());
        // Get all block_meta, and check if it's tsm file has a related tombstone file.

        // 遍历
        for (i, blk_iter) in self.tmp_tsm_blk_meta_iters.iter_mut().enumerate() {

            // 遍历每个block元数据
            for blk_meta in blk_iter.by_ref() {

                // 找到对应数据文件的reader
                let tsm_reader_idx = self.tmp_tsm_blk_tsm_reader_idx[i];
                let tsm_reader_ptr = self.tsm_readers[tsm_reader_idx].clone();

                // 将相关信息包起来
                blk_metas.push(CompactingBlockMeta::new(
                    tsm_reader_idx,
                    tsm_reader_ptr,
                    blk_meta,
                ));
            }
        }
        // Sort by field_id, min_ts and max_ts.
        blk_metas.sort();

        // 这里已经将多个数据文件某field的block数据放到同一级了
        let mut blk_meta_groups: Vec<CompactingBlockMetaGroup> =
            Vec::with_capacity(blk_metas.len());

        // 将他们包装成group 一开始每个group都只有一个block
        for blk_meta in blk_metas {
            blk_meta_groups.push(CompactingBlockMetaGroup::new(field_id, blk_meta));
        }
        // Compact blk_meta_groups.
        let mut i = 0;
        loop {
            let mut head_idx = i;
            // Find the first non-empty as head.
            // 跳过空的 找到第一个有效的group
            for (off, bmg) in blk_meta_groups[i..].iter().enumerate() {
                if !bmg.is_empty() {
                    head_idx += off;
                    break;
                }
            }
            if head_idx >= blk_meta_groups.len() - 1 {
                // There no other blk_meta_group to merge with the last one.
                break;
            }

            // 先选择一个header 然后从剩下的元素中找到可以合并的
            let mut head = blk_meta_groups[head_idx].clone();
            i = head_idx + 1;
            for bmg in blk_meta_groups[i..].iter_mut() {
                // 代表已经被合并过了
                if bmg.is_empty() {
                    continue;
                }

                // 有时间交集的 就可以进行合并
                if head.overlaps(bmg) {
                    // append后  bmg.len 会被设置成0  也就变空了
                    head.append(bmg);
                }
            }
            // 更新head
            blk_meta_groups[head_idx] = head;
        }

        // 上面的操作将block 按照时间交集分成了几个块   有交集的块就可以进行合并
        let blk_meta_groups: VecDeque<CompactingBlockMetaGroup> = blk_meta_groups
            .into_iter()
            .filter(|l| !l.is_empty())
            .collect();
        trace!(
            "selected merging meta groups: {}",
            blk_meta_groups
                .iter()
                .map(|g| format!(
                    "[{}]",
                    g.blk_metas
                        .iter()
                        .map(|b| format!("{}", b))
                        .collect::<Vec<String>>()
                        .join(", ")
                ))
                .collect::<Vec<String>>()
                .join(", ")
        );

        self.merging_blk_meta_groups = blk_meta_groups;

        true
    }
}

impl CompactIterator {

    // 从这些参与合并的数据文件中 返回下一个group  参与合并的都是同一个level的数据文件
    pub(crate) async fn next(&mut self) -> Option<CompactingBlockMetaGroup> {
        // 先尝试从merging_blk_meta_groups中获取 因为单field可能生成不止一组
        if let Some(g) = self.merging_blk_meta_groups.pop_front() {
            return Some(g);
        }

        // For each tsm-file, get next index reader for current iteration field id
        // changs
        // 一次性加载所有文件有关某field的数据到tmp迭代器中
        self.next_field_id();

        trace!(
            "selected {} blocks meta iterators",
            self.tmp_tsm_blk_meta_iters.len()
        );

        // 代表所有数据都已经读完了
        if self.tmp_tsm_blk_meta_iters.is_empty() {
            trace!("iteration field_id {:?} is finished", self.curr_fid);
            self.curr_fid = None;
            return None;
        }

        // Get all of block_metas of this field id, and merge these blocks
        // 产生CompactingBlockMetaGroup 并填充到merging_blk_meta_groups中
        self.fetch_merging_block_meta_groups().await;

        if let Some(g) = self.merging_blk_meta_groups.pop_front() {
            return Some(g);
        }
        None
    }
}

/// Returns if r1 (min_ts, max_ts) overlaps r2 (min_ts, max_ts)
fn overlaps_tuples(r1: (i64, i64), r2: (i64, i64)) -> bool {
    r1.0 <= r2.1 && r1.1 >= r2.0
}

// 该方法是外部访问的入口
pub async fn run_compaction_job(
    request: CompactReq,   // req中已经包含本次要合并的数据文件了
    kernel: Arc<GlobalContext>,
) -> Result<Option<(VersionEdit, HashMap<ColumnFileId, Arc<BloomFilter>>)>> {
    info!(
        "Compaction: Running compaction job on ts_family: {} and files: [ {} ]",
        request.ts_family_id,
        request
            .files
            .iter()
            .map(|f| {
                format!(
                    "{{ Level-{}, file_id: {}, time_range: {}-{} }}",
                    f.level(),
                    f.file_id(),
                    f.time_range().min_ts,
                    f.time_range().max_ts
                )
            })
            .collect::<Vec<String>>()
            .join(", ")
    );

    if request.files.is_empty() {
        // Nothing to compact
        return Ok(None);
    }

    // Buffers all tsm-files and it's indexes for this compaction
    let tsf_id = request.ts_family_id;
    let mut tsm_readers = Vec::new();

    // 为本次涉及到的所有数据文件 生成reader
    for col_file in request.files.iter() {
        let tsm_reader = request.version.get_tsm_reader(col_file.file_path()).await?;
        tsm_readers.push(tsm_reader);
    }

    let max_block_size = TseriesFamily::MAX_DATA_BLOCK_SIZE as usize;

    // CompactIterator 每次迭代会产生一个group  在这个group中是各个数据文件针对某个field 有时间交集的部分
    let mut iter = CompactIterator::new(tsm_readers, max_block_size, false);

    // 生成临时文件
    let tsm_dir = request.storage_opt.tsm_dir(&request.database, tsf_id);
    let mut tsm_writer = tsm::new_tsm_writer(&tsm_dir, kernel.file_id_next(), false, 0).await?;
    info!(
        "Compaction: File: {} been created (level: {}).",
        tsm_writer.sequence(),
        request.out_level
    );

    let mut version_edit = VersionEdit::new(tsf_id);
    let mut file_metas: HashMap<ColumnFileId, Arc<BloomFilter>> = HashMap::new();

    // 代表之前已经完成合并过的block
    let mut previous_merged_block: Option<CompactingBlock> = None;
    let mut fid = iter.curr_fid;

    // 迭代处理group
    while let Some(blk_meta_group) = iter.next().await {
        trace!("===============================");

        // next() 的调用会推动fid的变化  发现fid不一致时 将上一批数据写完
        if fid.is_some() && fid != iter.curr_fid {
            // Iteration of next field id, write previous merged block.
            if let Some(blk) = previous_merged_block.take() {
                // Write the small previous merged block.
                if write_tsm(
                    &mut tsm_writer,
                    blk,
                    &mut file_metas,
                    &mut version_edit,
                    &request,
                )
                .await?
                {
                    tsm_writer =
                        tsm::new_tsm_writer(&tsm_dir, kernel.file_id_next(), false, 0).await?;
                    info!(
                        "Compaction: File: {} been created (level: {}).",
                        tsm_writer.sequence(),
                        request.out_level
                    );
                }
            }
        }

        fid = iter.curr_fid;
        // 将group内的block 和 previousBlock 合并成一个block
        let mut compacting_blks = blk_meta_group
            .merge(previous_merged_block.take(), max_block_size)
            .await?;

        // 只有当该block比较小的时候 才考虑作为previous与下个合并
        if compacting_blks.len() == 1 && compacting_blks[0].len() < max_block_size {
            // The only one data block too small, try to extend the next compacting blocks.
            previous_merged_block = Some(compacting_blks.remove(0));
            continue;
        }

        let last_blk_idx = compacting_blks.len() - 1;
        for (i, blk) in compacting_blks.into_iter().enumerate() {

            // 只有当该block比较小的时候 才考虑作为previous与下个合并
            if i == last_blk_idx && blk.len() < max_block_size {
                // The last data block too small, try to extend to
                // the next compacting blocks (current field id).
                previous_merged_block = Some(blk);
                // block后会进入下次循环
                break;
            }

            // 将block数据写入数据文件   该方法默认返回false
            if write_tsm(
                &mut tsm_writer,
                blk,
                &mut file_metas,
                &mut version_edit,
                &request,
            )
            .await?
            {

                tsm_writer = tsm::new_tsm_writer(&tsm_dir, kernel.file_id_next(), false, 0).await?;
                info!(
                    "Compaction: File: {} been created (level: {}).",
                    tsm_writer.sequence(),
                    request.out_level
                );
            }
        }
    }

    // 此时已经完成了数据的合并和写入  只需要一个数据文件了 该数据文件的排布还是按列存储 每个列 分为多个block
    if let Some(blk) = previous_merged_block {
        let _max_file_size_exceed = write_tsm(
            &mut tsm_writer,
            blk,
            &mut file_metas,
            &mut version_edit,
            &request,
        )
        .await?;
    }

    // 此时已经完成了所有数据的合并

    // 刷盘 并将变化记录到edit中
    if !tsm_writer.finished() {
        finish_write_tsm(
            &mut tsm_writer,
            &mut file_metas,
            &mut version_edit,
            &request,
            request.version.max_level_ts(),
        )
        .await?;
    }

    // 这些文件不在需要了  在edit中记录为del
    for file in request.files {
        version_edit.del_file(file.level(), file.file_id(), file.is_delta());
    }

    info!(
        "Compaction: Compact finished, version edits: {:?}",
        version_edit
    );
    Ok(Some((version_edit, file_metas)))
}

// 将合并过的数据块写入到 tsm数据文件中
async fn write_tsm(
    tsm_writer: &mut TsmWriter,
    blk: CompactingBlock,
    file_metas: &mut HashMap<ColumnFileId, Arc<BloomFilter>>,
    version_edit: &mut VersionEdit,
    request: &CompactReq,
) -> Result<bool> {

    // 在writer中 都开放了相应的api
    let write_ret = match blk {
        CompactingBlock::Decoded {
            field_id: fid,
            data_block: b,
            ..
        } => tsm_writer.write_block(fid, &b).await,
        CompactingBlock::Encoded {
            field_id,
            data_block,
            ..
        } => tsm_writer.write_encoded_block(field_id, &data_block).await,
        CompactingBlock::Raw { meta, raw, .. } => tsm_writer.write_raw(&meta, &raw).await,
    };
    if let Err(e) = write_ret {
        match e {
            tsm::WriteTsmError::WriteIO { source } => {
                // TODO try re-run compaction on other time.
                error!("Failed compaction: IO error when write tsm: {:?}", source);
                return Err(Error::IO { source });
            }
            tsm::WriteTsmError::Encode { source } => {
                // TODO try re-run compaction on other time.
                error!(
                    "Failed compaction: encoding error when write tsm: {:?}",
                    source
                );
                return Err(Error::Encode { source });
            }
            tsm::WriteTsmError::MaxFileSizeExceed { .. } => {
                finish_write_tsm(
                    tsm_writer,
                    file_metas,
                    version_edit,
                    request,
                    request.version.max_level_ts(),
                )
                .await?;
                return Ok(true);
            }
            tsm::WriteTsmError::Finished { path } => {
                error!(
                    "Trying to write by a finished tsm writer: {}",
                    path.display()
                );
            }
        }
    }

    Ok(false)
}

// 触发刷盘逻辑
async fn finish_write_tsm(
    tsm_writer: &mut TsmWriter,
    file_metas: &mut HashMap<ColumnFileId, Arc<BloomFilter>>,
    version_edit: &mut VersionEdit,
    request: &CompactReq,
    max_level_ts: Timestamp,
) -> Result<()> {

    // 写入索引数据
    tsm_writer
        .write_index()
        .await
        .context(error::WriteTsmSnafu)?;
    // 索引数据刷盘
    tsm_writer.finish().await.context(error::WriteTsmSnafu)?;

    // 此时只增加了这一个数据文件
    file_metas.insert(
        tsm_writer.sequence(),
        Arc::new(tsm_writer.bloom_filter_cloned()),
    );
    info!(
        "Compaction: File: {} write finished (level: {}, {} B).",
        tsm_writer.sequence(),
        request.out_level,
        tsm_writer.size()
    );

    // 将描述元数据保存到edit中
    let cm = new_compact_meta(tsm_writer, request.ts_family_id, request.out_level);
    version_edit.add_file(cm, max_level_ts);

    Ok(())
}

fn new_compact_meta(
    tsm_writer: &TsmWriter,
    tsf_id: TseriesFamilyId,
    level: LevelId,
) -> CompactMeta {
    CompactMeta {
        file_id: tsm_writer.sequence(),
        file_size: tsm_writer.size(),
        tsf_id,
        level,
        min_ts: tsm_writer.min_ts(),
        max_ts: tsm_writer.max_ts(),
        high_seq: 0,
        low_seq: 0,
        is_delta: false,
    }
}

#[cfg(test)]
pub mod test {
    use core::panic;
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use cache::ShardedAsyncCache;
    use minivec::MiniVec;
    use models::predicate::domain::TimeRange;
    use models::{FieldId, PhysicalDType as ValueType, Timestamp};

    use crate::compaction::{run_compaction_job, CompactReq};
    use crate::context::GlobalContext;
    use crate::file_system::file_manager;
    use crate::kv_option::Options;
    use crate::summary::VersionEdit;
    use crate::tseries_family::{ColumnFile, LevelInfo, Version};
    use crate::tsm::codec::DataBlockEncoding;
    use crate::tsm::{self, DataBlock, TsmReader, TsmTombstone};
    use crate::{file_utils, ColumnFileId};

    pub(crate) async fn write_data_blocks_to_column_file(
        dir: impl AsRef<Path>,
        data: Vec<HashMap<FieldId, Vec<DataBlock>>>,
    ) -> (u64, Vec<Arc<ColumnFile>>) {
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }
        let mut cfs = Vec::new();
        let mut file_seq = 0;
        for (i, d) in data.iter().enumerate() {
            file_seq = i as u64 + 1;
            let mut writer = tsm::new_tsm_writer(&dir, file_seq, false, 0).await.unwrap();
            for (fid, data_blks) in d.iter() {
                for blk in data_blks.iter() {
                    writer.write_block(*fid, blk).await.unwrap();
                }
            }
            writer.write_index().await.unwrap();
            writer.finish().await.unwrap();
            let mut cf = ColumnFile::new(
                file_seq,
                2,
                TimeRange::new(writer.min_ts(), writer.max_ts()),
                writer.size(),
                false,
                writer.path(),
            );
            cf.set_field_id_filter(Arc::new(writer.bloom_filter_cloned()));
            cfs.push(Arc::new(cf));
        }
        (file_seq + 1, cfs)
    }

    async fn read_data_blocks_from_column_file(
        path: impl AsRef<Path>,
    ) -> HashMap<FieldId, Vec<DataBlock>> {
        let tsm_reader = TsmReader::open(path).await.unwrap();
        let mut data: HashMap<FieldId, Vec<DataBlock>> = HashMap::new();
        for idx in tsm_reader.index_iterator() {
            let field_id = idx.field_id();
            for blk_meta in idx.block_iterator() {
                let blk = tsm_reader.get_data_block(&blk_meta).await.unwrap();
                data.entry(field_id).or_default().push(blk);
            }
        }
        data
    }

    fn get_result_file_path(dir: impl AsRef<Path>, version_edit: VersionEdit) -> PathBuf {
        if version_edit.has_file_id && !version_edit.add_files.is_empty() {
            let file_id = version_edit.add_files.first().unwrap().file_id;
            return file_utils::make_tsm_file(dir, file_id);
        }

        panic!("VersionEdit doesn't contain any add_files.");
    }

    /// Compare DataBlocks in path with the expected_Data using assert_eq.
    async fn check_column_file(
        dir: impl AsRef<Path>,
        version_edit: VersionEdit,
        expected_data: HashMap<FieldId, Vec<DataBlock>>,
    ) {
        let path = get_result_file_path(dir, version_edit);
        let data = read_data_blocks_from_column_file(path).await;
        let mut data_field_ids = data.keys().copied().collect::<Vec<_>>();
        data_field_ids.sort_unstable();
        let mut expected_data_field_ids = expected_data.keys().copied().collect::<Vec<_>>();
        expected_data_field_ids.sort_unstable();
        assert_eq!(data_field_ids, expected_data_field_ids);

        for (k, v) in expected_data.iter() {
            let data_blks = data.get(k).unwrap();
            if v.len() != data_blks.len() {
                let v_str = format_data_blocks(v.as_slice());
                let data_blks_str = format_data_blocks(data_blks.as_slice());
                panic!("fid={k}, v.len != data_blks.len: v={v_str}, data_blks={data_blks_str}")
            }
            assert_eq!(v.len(), data_blks.len());
            for (v_idx, v_blk) in v.iter().enumerate() {
                assert_eq!(data_blks.get(v_idx).unwrap(), v_blk);
            }
        }
    }

    pub(crate) fn create_options(base_dir: String) -> Arc<Options> {
        let mut config = config::get_config_for_test();
        config.storage.path = base_dir;
        let opt = Options::from(&config);
        Arc::new(opt)
    }

    fn prepare_compact_req_and_kernel(
        database: Arc<String>,
        opt: Arc<Options>,
        next_file_id: u64,
        files: Vec<Arc<ColumnFile>>,
    ) -> (CompactReq, Arc<GlobalContext>) {
        let version = Arc::new(Version::new(
            1,
            database.clone(),
            opt.storage.clone(),
            1,
            LevelInfo::init_levels(database.clone(), 0, opt.storage.clone()),
            1000,
            Arc::new(ShardedAsyncCache::create_lru_sharded_cache(1)),
        ));
        let compact_req = CompactReq {
            ts_family_id: 1,
            database,
            storage_opt: opt.storage.clone(),
            files,
            version,
            out_level: 2,
        };
        let kernel = Arc::new(GlobalContext::new());
        kernel.set_file_id(next_file_id);

        (compact_req, kernel)
    }

    fn format_data_blocks(data_blocks: &[DataBlock]) -> String {
        format!(
            "[{}]",
            data_blocks
                .iter()
                .map(|b| format!("{}", b))
                .collect::<Vec<String>>()
                .join(", ")
        )
    }

    #[tokio::test]
    async fn test_compaction_fast() {
        #[rustfmt::skip]
        let data = vec![
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3], enc: DataBlockEncoding::default() }]),
                (2, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3], enc: DataBlockEncoding::default() }]),
                (3, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3], enc: DataBlockEncoding::default() }]),
            ]),
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6], enc: DataBlockEncoding::default() }]),
                (2, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6], enc: DataBlockEncoding::default() }]),
                (3, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6], enc: DataBlockEncoding::default() }]),
            ]),
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9], enc: DataBlockEncoding::default() }]),
                (2, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9], enc: DataBlockEncoding::default() }]),
                (3, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9], enc: DataBlockEncoding::default() }]),
            ]),
        ];
        #[rustfmt::skip]
        let expected_data = HashMap::from([
            (1, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() }]),
            (2, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() }]),
            (3, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() }]),
        ]);

        let dir = "/tmp/test/compaction";
        let database = Arc::new("dba".to_string());
        let opt = create_options(dir.to_string());
        let dir = opt.storage.tsm_dir(&database, 1);

        let (next_file_id, files) = write_data_blocks_to_column_file(&dir, data).await;
        let (compact_req, kernel) =
            prepare_compact_req_and_kernel(database, opt, next_file_id, files);
        let (version_edit, _) = run_compaction_job(compact_req, kernel)
            .await
            .unwrap()
            .unwrap();
        check_column_file(dir, version_edit, expected_data).await;
    }

    #[tokio::test]
    async fn test_compaction_1() {
        #[rustfmt::skip]
        let data = vec![
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6], enc: DataBlockEncoding::default() }]),
                (2, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6], enc: DataBlockEncoding::default() }]),
                (3, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6], enc: DataBlockEncoding::default() }]),
            ]),
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3], enc: DataBlockEncoding::default() }]),
                (2, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3], enc: DataBlockEncoding::default() }]),
                (3, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3], enc: DataBlockEncoding::default() }]),
            ]),
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9], enc: DataBlockEncoding::default() }]),
                (2, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9], enc: DataBlockEncoding::default() }]),
                (3, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9], enc: DataBlockEncoding::default() }]),
            ]),
        ];
        #[rustfmt::skip]
        let expected_data = HashMap::from([
            (1, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() }]),
            (2, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() }]),
            (3, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() }]),
        ]);

        let dir = "/tmp/test/compaction/1";
        let database = Arc::new("dba".to_string());
        let opt = create_options(dir.to_string());
        let dir = opt.storage.tsm_dir(&database, 1);

        let (next_file_id, files) = write_data_blocks_to_column_file(&dir, data).await;
        let (compact_req, kernel) =
            prepare_compact_req_and_kernel(database, opt, next_file_id, files);
        let (version_edit, _) = run_compaction_job(compact_req, kernel)
            .await
            .unwrap()
            .unwrap();
        check_column_file(dir, version_edit, expected_data).await;
    }

    #[tokio::test]
    async fn test_compaction_2() {
        #[rustfmt::skip]
        let data = vec![
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4], val: vec![1, 2, 3, 5], enc: DataBlockEncoding::default() }]),
                (3, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4], val: vec![1, 2, 3, 5], enc: DataBlockEncoding::default() }]),
                (4, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3], enc: DataBlockEncoding::default() }]),
            ]),
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6], enc: DataBlockEncoding::default() }]),
                (2, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6], enc: DataBlockEncoding::default() }]),
                (3, vec![DataBlock::I64 { ts: vec![4, 5, 6, 7], val: vec![4, 5, 6, 8], enc: DataBlockEncoding::default() }]),
            ]),
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9], enc: DataBlockEncoding::default() }]),
                (2, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9], enc: DataBlockEncoding::default() }]),
                (3, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9], enc: DataBlockEncoding::default() }]),
            ]),
        ];
        #[rustfmt::skip]
        let expected_data = HashMap::from([
            (1, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() }]),
            (2, vec![DataBlock::I64 { ts: vec![4, 5, 6, 7, 8, 9], val: vec![4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() }]),
            (3, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() }]),
            (4, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3], enc: DataBlockEncoding::default() }]),
        ]);

        let dir = "/tmp/test/compaction/2";
        let database = Arc::new("dba".to_string());
        let opt = create_options(dir.to_string());
        let dir = opt.storage.tsm_dir(&database, 1);

        let (next_file_id, files) = write_data_blocks_to_column_file(&dir, data).await;
        let (compact_req, kernel) =
            prepare_compact_req_and_kernel(database, opt, next_file_id, files);
        let (version_edit, _) = run_compaction_job(compact_req, kernel)
            .await
            .unwrap()
            .unwrap();
        check_column_file(dir, version_edit, expected_data).await;
    }

    /// Returns a generated `DataBlock` with default value and specified size, `DataBlock::ts`
    /// is all the time-ranges in data_descriptors.
    ///
    /// The default value is different for each ValueType:
    /// - Unsigned: 1
    /// - Integer: 1
    /// - String: "1"
    /// - Float: 1.0
    /// - Boolean: true
    /// - Unknown: will create a panic
    fn generate_data_block(value_type: ValueType, data_descriptors: Vec<(i64, i64)>) -> DataBlock {
        match value_type {
            ValueType::Unsigned => {
                let mut ts_vec: Vec<Timestamp> = Vec::with_capacity(1000);
                let mut val_vec: Vec<u64> = Vec::with_capacity(1000);
                for (min_ts, max_ts) in data_descriptors {
                    for ts in min_ts..max_ts + 1 {
                        ts_vec.push(ts);
                        val_vec.push(1_u64);
                    }
                }
                DataBlock::U64 {
                    ts: ts_vec,
                    val: val_vec,
                    enc: DataBlockEncoding::default(),
                }
            }
            ValueType::Integer => {
                let mut ts_vec: Vec<Timestamp> = Vec::with_capacity(1000);
                let mut val_vec: Vec<i64> = Vec::with_capacity(1000);
                for (min_ts, max_ts) in data_descriptors {
                    for ts in min_ts..max_ts + 1 {
                        ts_vec.push(ts);
                        val_vec.push(1_i64);
                    }
                }
                DataBlock::I64 {
                    ts: ts_vec,
                    val: val_vec,
                    enc: DataBlockEncoding::default(),
                }
            }
            ValueType::String => {
                let word = MiniVec::from(&b"1"[..]);
                let mut ts_vec: Vec<Timestamp> = Vec::with_capacity(10000);
                let mut val_vec: Vec<MiniVec<u8>> = Vec::with_capacity(10000);
                for (min_ts, max_ts) in data_descriptors {
                    for ts in min_ts..max_ts + 1 {
                        ts_vec.push(ts);
                        val_vec.push(word.clone());
                    }
                }
                DataBlock::Str {
                    ts: ts_vec,
                    val: val_vec,
                    enc: DataBlockEncoding::default(),
                }
            }
            ValueType::Float => {
                let mut ts_vec: Vec<Timestamp> = Vec::with_capacity(10000);
                let mut val_vec: Vec<f64> = Vec::with_capacity(10000);
                for (min_ts, max_ts) in data_descriptors {
                    for ts in min_ts..max_ts + 1 {
                        ts_vec.push(ts);
                        val_vec.push(1.0);
                    }
                }
                DataBlock::F64 {
                    ts: ts_vec,
                    val: val_vec,
                    enc: DataBlockEncoding::default(),
                }
            }
            ValueType::Boolean => {
                let mut ts_vec: Vec<Timestamp> = Vec::with_capacity(10000);
                let mut val_vec: Vec<bool> = Vec::with_capacity(10000);
                for (min_ts, max_ts) in data_descriptors {
                    for ts in min_ts..max_ts + 1 {
                        ts_vec.push(ts);
                        val_vec.push(true);
                    }
                }
                DataBlock::Bool {
                    ts: ts_vec,
                    val: val_vec,
                    enc: DataBlockEncoding::default(),
                }
            }
            ValueType::Unknown => {
                panic!("value type is Unknown")
            }
        }
    }

    #[tokio::test]
    async fn test_compaction_3() {
        #[rustfmt::skip]
        let data_desc = [
            // [( tsm_sequence, vec![ (ValueType, FieldId, Timestamp_Begin, Timestamp_end) ] )]
            (1_u64, vec![
                // 1, 1~2500
                (ValueType::Unsigned, 1_u64, 1_i64, 1000_i64),
                (ValueType::Unsigned, 1, 1001, 2000),
                (ValueType::Unsigned, 1, 2001, 2500),
                // 2, 1~1500
                (ValueType::Integer, 2, 1, 1000),
                (ValueType::Integer, 2, 1001, 1500),
                // 3, 1~1500
                (ValueType::Boolean, 3, 1, 1000),
                (ValueType::Boolean, 3, 1001, 1500),
            ]),
            (2, vec![
                // 1, 2001~4500
                (ValueType::Unsigned, 1, 2001, 3000),
                (ValueType::Unsigned, 1, 3001, 4000),
                (ValueType::Unsigned, 1, 4001, 4500),
                // 2, 1001~3000
                (ValueType::Integer, 2, 1001, 2000),
                (ValueType::Integer, 2, 2001, 3000),
                // 3, 1001~2500
                (ValueType::Boolean, 3, 1001, 2000),
                (ValueType::Boolean, 3, 2001, 2500),
                // 4, 1~1500
                (ValueType::Float, 4, 1, 1000),
                (ValueType::Float, 4, 1001, 1500),
            ]),
            (3, vec![
                // 1, 4001~6500
                (ValueType::Unsigned, 1, 4001, 5000),
                (ValueType::Unsigned, 1, 5001, 6000),
                (ValueType::Unsigned, 1, 6001, 6500),
                // 2, 3001~5000
                (ValueType::Integer, 2, 3001, 4000),
                (ValueType::Integer, 2, 4001, 5000),
                // 3, 2001~3500
                (ValueType::Boolean, 3, 2001, 3000),
                (ValueType::Boolean, 3, 3001, 3500),
                // 4. 1001~2500
                (ValueType::Float, 4, 1001, 2000),
                (ValueType::Float, 4, 2001, 2500),
            ]),
        ];
        #[rustfmt::skip]
        let expected_data: HashMap<FieldId, Vec<DataBlock>> = HashMap::from(
            [
                // 1, 1~6500
                (1, vec![
                    generate_data_block(ValueType::Unsigned, vec![(1, 1000)]),
                    generate_data_block(ValueType::Unsigned, vec![(1001, 2000)]),
                    generate_data_block(ValueType::Unsigned, vec![(2001, 3000)]),
                    generate_data_block(ValueType::Unsigned, vec![(3001, 4000)]),
                    generate_data_block(ValueType::Unsigned, vec![(4001, 5000)]),
                    generate_data_block(ValueType::Unsigned, vec![(5001, 6000)]),
                    generate_data_block(ValueType::Unsigned, vec![(6001, 6500)]),
                ]),
                // 2, 1~5000
                (2, vec![
                    generate_data_block(ValueType::Integer, vec![(1, 1000)]),
                    generate_data_block(ValueType::Integer, vec![(1001, 2000)]),
                    generate_data_block(ValueType::Integer, vec![(2001, 3000)]),
                    generate_data_block(ValueType::Integer, vec![(3001, 4000)]),
                    generate_data_block(ValueType::Integer, vec![(4001, 5000)]),
                ]),
                // 3, 1~3500
                (3, vec![
                    generate_data_block(ValueType::Boolean, vec![(1, 1000)]),
                    generate_data_block(ValueType::Boolean, vec![(1001, 2000)]),
                    generate_data_block(ValueType::Boolean, vec![(2001, 3000)]),
                    generate_data_block(ValueType::Boolean, vec![(3001, 3500)]),
                ]),
                // 4, 1~2500
                (4, vec![
                    generate_data_block(ValueType::Float, vec![(1, 1000)]),
                    generate_data_block(ValueType::Float, vec![(1001, 2000)]),
                    generate_data_block(ValueType::Float, vec![(2001, 2500)]),
                ]),
            ]
        );

        let dir = "/tmp/test/compaction/3";
        let database = Arc::new("dba".to_string());
        let opt = create_options(dir.to_string());
        let dir = opt.storage.tsm_dir(&database, 1);
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }

        let mut column_files = Vec::new();
        for (tsm_sequence, args) in data_desc.iter() {
            let mut tsm_writer = tsm::new_tsm_writer(&dir, *tsm_sequence, false, 0)
                .await
                .unwrap();
            for arg in args.iter() {
                tsm_writer
                    .write_block(arg.1, &generate_data_block(arg.0, vec![(arg.2, arg.3)]))
                    .await
                    .unwrap();
            }
            tsm_writer.write_index().await.unwrap();
            tsm_writer.finish().await.unwrap();
            column_files.push(Arc::new(ColumnFile::new(
                *tsm_sequence,
                2,
                TimeRange::new(tsm_writer.min_ts(), tsm_writer.max_ts()),
                tsm_writer.size(),
                false,
                tsm_writer.path(),
            )));
        }

        let next_file_id = 4_u64;

        let (compact_req, kernel) =
            prepare_compact_req_and_kernel(database, opt, next_file_id, column_files);

        let (version_edit, _) = run_compaction_job(compact_req, kernel)
            .await
            .unwrap()
            .unwrap();

        check_column_file(dir, version_edit, expected_data).await;
    }

    #[allow(clippy::type_complexity)]
    async fn write_data_block_desc(
        dir: impl AsRef<Path>,
        data_desc: &[(
            ColumnFileId,
            Vec<(ValueType, FieldId, Timestamp, Timestamp)>,
            Vec<(FieldId, Timestamp, Timestamp)>,
        )],
    ) -> Vec<Arc<ColumnFile>> {
        let mut column_files = Vec::new();
        for (tsm_sequence, tsm_desc, tombstone_desc) in data_desc.iter() {
            let mut tsm_writer = tsm::new_tsm_writer(&dir, *tsm_sequence, false, 0)
                .await
                .unwrap();
            for &(val_type, fid, min_ts, max_ts) in tsm_desc.iter() {
                tsm_writer
                    .write_block(fid, &generate_data_block(val_type, vec![(min_ts, max_ts)]))
                    .await
                    .unwrap();
            }
            tsm_writer.write_index().await.unwrap();
            tsm_writer.finish().await.unwrap();
            let tsm_tombstone = TsmTombstone::open(&dir, *tsm_sequence).await.unwrap();
            for (fid, min_ts, max_ts) in tombstone_desc.iter() {
                tsm_tombstone
                    .add_range(&[*fid][..], &TimeRange::new(*min_ts, *max_ts), None)
                    .await
                    .unwrap();
            }

            tsm_tombstone.flush().await.unwrap();
            column_files.push(Arc::new(ColumnFile::new(
                *tsm_sequence,
                2,
                TimeRange::new(tsm_writer.min_ts(), tsm_writer.max_ts()),
                tsm_writer.size(),
                false,
                tsm_writer.path(),
            )));
        }

        column_files
    }

    #[tokio::test]
    async fn test_compaction_4() {
        #[rustfmt::skip]
        let data_desc = [
            // [( tsm_sequence, vec![(ValueType, FieldId, Timestamp_Begin, Timestamp_end)], vec![Option<(FieldId, MinTimestamp, MaxTimestamp)>] )]
            (1_u64, vec![
                // 1, 1~2500
                (ValueType::Unsigned, 1_u64, 1_i64, 1000_i64), (ValueType::Unsigned, 1, 1001, 2000), (ValueType::Unsigned, 1, 2001, 2500),
            ], vec![(1_u64, 1_i64, 2_i64), (1, 2001, 2100)]),
            (2, vec![
                // 1, 2001~4500
                // 2101~3100, 3101~4100, 4101~4499
                (ValueType::Unsigned, 1, 2001, 3000), (ValueType::Unsigned, 1, 3001, 4000), (ValueType::Unsigned, 1, 4001, 4500),
            ], vec![(1, 2001, 2100), (1, 4500, 4501)]),
            (3, vec![
                // 1, 4001~6500
                // 4001~4499, 4502~5501, 5502~6500
                (ValueType::Unsigned, 1, 4001, 5000), (ValueType::Unsigned, 1, 5001, 6000), (ValueType::Unsigned, 1, 6001, 6500),
            ], vec![(1, 4500, 4501)]),
        ];
        #[rustfmt::skip]
        let expected_data: HashMap<FieldId, Vec<DataBlock>> = HashMap::from(
            [
                // 1, 1~6500
                (1, vec![
                    generate_data_block(ValueType::Unsigned, vec![(3, 1002)]),
                    generate_data_block(ValueType::Unsigned, vec![(1003, 2000), (2101, 2102)]),
                    generate_data_block(ValueType::Unsigned, vec![(2103, 3102)]),
                    generate_data_block(ValueType::Unsigned, vec![(3103, 4102)]),
                    generate_data_block(ValueType::Unsigned, vec![(4103, 4499), (4502, 5104)]),
                    generate_data_block(ValueType::Unsigned, vec![(5105, 6104)]),
                    generate_data_block(ValueType::Unsigned, vec![(6105, 6500)]),
                ]),
            ]
        );

        let dir = "/tmp/test/compaction/4";
        let database = Arc::new("dba".to_string());
        let opt = create_options(dir.to_string());
        let dir = opt.storage.tsm_dir(&database, 1);
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }

        let column_files = write_data_block_desc(&dir, &data_desc).await;
        let next_file_id = 4_u64;
        let (compact_req, kernel) =
            prepare_compact_req_and_kernel(database, opt, next_file_id, column_files);

        let (version_edit, _) = run_compaction_job(compact_req, kernel)
            .await
            .unwrap()
            .unwrap();

        check_column_file(dir, version_edit, expected_data).await;
    }

    #[tokio::test]
    async fn test_compaction_5() {
        #[rustfmt::skip]
        let data_desc = [
            // [( tsm_sequence, vec![(ValueType, FieldId, Timestamp_Begin, Timestamp_end)], vec![Option<(FieldId, MinTimestamp, MaxTimestamp)>] )]
            (1_u64, vec![
                // 1, 1~2500
                (ValueType::Unsigned, 1_u64, 1_i64, 1000_i64), (ValueType::Unsigned, 1, 1001, 2000),  (ValueType::Unsigned, 1, 2001, 2500),
                // 2, 1~1500
                (ValueType::Integer, 2, 1, 1000), (ValueType::Integer, 2, 1001, 1500),
                // 3, 1~1500
                (ValueType::Boolean, 3, 1, 1000), (ValueType::Boolean, 3, 1001, 1500),
            ], vec![
                (1_u64, 1_i64, 2_i64), (1, 2001, 2100),
                (2, 1001, 1002),
                (3, 1499, 1500),
            ]),
            (2, vec![
                // 1, 2001~4500
                (ValueType::Unsigned, 1, 2001, 3000), (ValueType::Unsigned, 1, 3001, 4000), (ValueType::Unsigned, 1, 4001, 4500),
                // 2, 1001~3000
                (ValueType::Integer, 2, 1001, 2000), (ValueType::Integer, 2, 2001, 3000),
                // 3, 1001~2500
                (ValueType::Boolean, 3, 1001, 2000), (ValueType::Boolean, 3, 2001, 2500),
                // 4, 1~1500
                (ValueType::Float, 4, 1, 1000), (ValueType::Float, 4, 1001, 1500),
            ], vec![
                (1, 2001, 2100), (1, 4500, 4501),
                (2, 1001, 1002), (2, 2501, 2502),
                (3, 1499, 1500),
            ]),
            (3, vec![
                // 1, 4001~6500
                (ValueType::Unsigned, 1, 4001, 5000), (ValueType::Unsigned, 1, 5001, 6000), (ValueType::Unsigned, 1, 6001, 6500),
                // 2, 3001~5000
                (ValueType::Integer, 2, 3001, 4000), (ValueType::Integer, 2, 4001, 5000),
                // 3, 2001~3500
                (ValueType::Boolean, 3, 2001, 3000), (ValueType::Boolean, 3, 3001, 3500),
                // 4. 1001~2500
                (ValueType::Float, 4, 1001, 2000), (ValueType::Float, 4, 2001, 2500),
            ], vec![
                (1, 4500, 4501),
                (2, 4001, 4002),
            ]),
        ];
        #[rustfmt::skip]
        let expected_data: HashMap<FieldId, Vec<DataBlock>> = HashMap::from(
            [
                // 1, 1~6500
                (1, vec![
                    generate_data_block(ValueType::Unsigned, vec![(3, 1002)]),
                    generate_data_block(ValueType::Unsigned, vec![(1003, 2000), (2101, 2102)]),
                    generate_data_block(ValueType::Unsigned, vec![(2103, 3102)]),
                    generate_data_block(ValueType::Unsigned, vec![(3103, 4102)]),
                    generate_data_block(ValueType::Unsigned, vec![(4103, 4499), (4502, 5104)]),
                    generate_data_block(ValueType::Unsigned, vec![(5105, 6104)]),
                    generate_data_block(ValueType::Unsigned, vec![(6105, 6500)]),
                ]),
                // 2, 1~5000
                (2, vec![
                    generate_data_block(ValueType::Integer, vec![(1, 1000)]),
                    generate_data_block(ValueType::Integer, vec![(1003, 2002)]),
                    generate_data_block(ValueType::Integer, vec![(2003, 2500), (2503, 3004)]),
                    generate_data_block(ValueType::Integer, vec![(3005, 4000), (4003, 4006)]),
                    generate_data_block(ValueType::Integer, vec![(4007, 5000)]),
                ]),
                // 3, 1~3500
                (3, vec![
                    generate_data_block(ValueType::Boolean, vec![(1, 1000)]),
                    generate_data_block(ValueType::Boolean, vec![(1001, 1498), (1501, 2002)]),
                    generate_data_block(ValueType::Boolean, vec![(2003, 3002)]),
                    generate_data_block(ValueType::Boolean, vec![(3003, 3500)]),
                ]),
                // 4, 1~2500
                (4, vec![
                    generate_data_block(ValueType::Float, vec![(1, 1000)]),
                    generate_data_block(ValueType::Float, vec![(1001, 2000)]),
                    generate_data_block(ValueType::Float, vec![(2001, 2500)]),
                ]),
            ]
        );

        let dir = "/tmp/test/compaction/5";
        let database = Arc::new("dba".to_string());
        let opt = create_options(dir.to_string());
        let dir = opt.storage.tsm_dir(&database, 1);
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }

        let column_files = write_data_block_desc(&dir, &data_desc).await;
        let next_file_id = 4_u64;
        let (compact_req, kernel) =
            prepare_compact_req_and_kernel(database, opt, next_file_id, column_files);

        let (version_edit, _) = run_compaction_job(compact_req, kernel)
            .await
            .unwrap()
            .unwrap();

        check_column_file(dir, version_edit, expected_data).await;
    }
}
