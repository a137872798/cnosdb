use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayBuilder, ArrayRef, BooleanArray, BooleanBuilder, Float64Builder, Int64Builder,
    PrimitiveArray, PrimitiveBuilder, StringArray, StringBuilder, TimestampMicrosecondBuilder,
    TimestampMillisecondBuilder, TimestampNanosecondBuilder, TimestampSecondBuilder, UInt64Builder,
};
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, Float64Type, Int64Type, SchemaRef, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt64Type,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::metrics::{self, ExecutionPlanMetricsSet, MetricBuilder};
use futures::future::join_all;
use models::meta_data::VnodeId;
use models::predicate::domain::{self, QueryArgs, QueryExpr, TimeRanges};
use models::predicate::PlacedSplit;
use models::schema::{PhysicalCType as ColumnType, TableColumn, TskvTableSchemaRef};
use models::utils::{min_num, unite_id};
use models::{FieldId, PhysicalDType as ValueType, SeriesId, Timestamp};
use protos::kv_service::QueryRecordBatchRequest;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use trace::{debug, error, SpanRecorder};

use crate::compute::count::count_column_non_null_values;
use crate::error::Result;
use crate::memcache::DataType;
use crate::reader::Cursor;
use crate::tseries_family::{ColumnFile, SuperVersion, Version};
use crate::tsm::{BlockMetaIterator, DataBlockReader, TsmReader};
use crate::{EngineRef, Error};

// 游标每次移动只能得到一个列值
pub type CursorPtr = Box<dyn Cursor>;

// 存储某个列的数据

pub struct ArrayBuilderPtr {
    // 这个指针可以根据需要灵活的变换类型 但是实际上一旦确定了 就不会改变了
    pub ptr: Box<dyn ArrayBuilder>,
    pub column_type: ColumnType,
}

impl ArrayBuilderPtr {
    pub fn new(ptr: Box<dyn ArrayBuilder>, column_type: ColumnType) -> Self {
        Self { ptr, column_type }
    }

    #[inline(always)]
    fn builder<T: ArrowPrimitiveType>(&mut self) -> Option<&mut PrimitiveBuilder<T>> {
        self.ptr.as_any_mut().downcast_mut::<PrimitiveBuilder<T>>()
    }

    // 往容器中存储数据
    pub fn append_primitive<T: ArrowPrimitiveType>(&mut self, t: T::Native) {
        if let Some(b) = self.builder::<T>() {
            b.append_value(t);
        } else {
            error!(
                "Failed to get primitive-type array builder to insert {:?} array",
                self.column_type
            );
        }
    }

    pub fn append_timestamp(&mut self, unit: &TimeUnit, timestamp: Timestamp) {
        match unit {
            TimeUnit::Second => self.append_primitive::<TimestampSecondType>(timestamp),
            TimeUnit::Millisecond => self.append_primitive::<TimestampMillisecondType>(timestamp),
            TimeUnit::Microsecond => self.append_primitive::<TimestampMicrosecondType>(timestamp),
            TimeUnit::Nanosecond => self.append_primitive::<TimestampNanosecondType>(timestamp),
        }
    }

    // 添加某个列值
    pub fn append_value(
        &mut self,
        value_type: ValueType,  // 描述插入的数据类型
        value: Option<DataType>,   // 列值 以及类型
        column_name: &str,  // 本次插入的列名称
    ) -> Result<()> {

        // 根据情况调用不同的 append api
        match value_type {
            ValueType::Unknown => {
                return Err(Error::CommonError {
                    reason: format!("unknown type of column '{}'", column_name),
                });
            }
            ValueType::String => match value {
                Some(DataType::Str(_, val)) => {
                    // Safety
                    // All val is valid UTF-8 String
                    let str = unsafe { std::str::from_utf8_unchecked(val.as_slice()) };
                    self.append_string(str)
                }
                Some(DataType::StrRef(_, val)) => {
                    // Safety
                    // All val is valid UTF-8 String
                    let str = unsafe { std::str::from_utf8_unchecked(val.as_slice()) };
                    self.append_string(str)
                }
                _ => self.append_null_string(),
            },
            ValueType::Boolean => {
                if let Some(DataType::Bool(_, val)) = value {
                    self.append_bool(val);
                } else {
                    self.append_null_bool();
                }
            }
            ValueType::Float => {
                if let Some(DataType::F64(_, val)) = value {
                    self.append_primitive::<Float64Type>(val);
                } else {
                    self.append_primitive_null::<Float64Type>();
                }
            }
            ValueType::Integer => {
                if let Some(DataType::I64(_, val)) = value {
                    self.append_primitive::<Int64Type>(val);
                } else {
                    self.append_primitive_null::<Int64Type>();
                }
            }
            ValueType::Unsigned => {
                if let Some(DataType::U64(_, val)) = value {
                    self.append_primitive::<UInt64Type>(val);
                } else {
                    self.append_primitive_null::<UInt64Type>();
                }
            }
        }
        Ok(())
    }

    pub fn append_primitive_null<T: ArrowPrimitiveType>(&mut self) {
        if let Some(b) = self.builder::<T>() {
            b.append_null();
        } else {
            error!(
                "Failed to get primitive-type array builder to insert {:?} array",
                self.column_type
            );
        }
    }

    pub fn append_bool(&mut self, data: bool) {
        if let Some(b) = self.ptr.as_any_mut().downcast_mut::<BooleanBuilder>() {
            b.append_value(data);
        } else {
            error!(
                "Failed to get boolean array builder to insert {:?} array",
                self.column_type
            );
        }
    }

    pub fn append_null_bool(&mut self) {
        if let Some(b) = self.ptr.as_any_mut().downcast_mut::<BooleanBuilder>() {
            b.append_null();
        } else {
            error!(
                "Failed to get boolean array builder to insert {:?} array",
                self.column_type
            );
        }
    }

    pub fn append_string(&mut self, data: &str) {
        if let Some(b) = self.ptr.as_any_mut().downcast_mut::<StringBuilder>() {
            b.append_value(data);
        } else {
            error!(
                "Failed to get string array builder to insert {:?} array",
                self.column_type
            );
        }
    }

    pub fn append_null_string(&mut self) {
        if let Some(b) = self.ptr.as_any_mut().downcast_mut::<StringBuilder>() {
            b.append_null();
        } else {
            error!(
                "Failed to get string array builder to insert {:?} array",
                self.column_type
            );
        }
    }

    fn extend_primitive_array<T: ArrowPrimitiveType>(&mut self, array: ArrayRef) {
        let builder = self.builder::<T>();
        let array = array.as_any().downcast_ref::<PrimitiveArray<T>>();
        if let (Some(b), Some(a)) = (builder, array) {
            b.extend(a.iter())
        } else {
            error!(
                "Failed to get primitive-type array and array builder to insert {:?} array",
                self.column_type
            );
        }
    }

    fn extend_bool_array(&mut self, array: ArrayRef) {
        let builder = self.ptr.as_any_mut().downcast_mut::<BooleanBuilder>();
        let array = array.as_any().downcast_ref::<BooleanArray>();
        if let (Some(b), Some(a)) = (builder, array) {
            b.extend(a.iter())
        } else {
            error!(
                "Failed to get boolean array and array builder to insert {:?} array",
                self.column_type
            );
        }
    }

    fn extend_string_array(&mut self, array: ArrayRef) {
        let builder = self.ptr.as_any_mut().downcast_mut::<StringBuilder>();
        let array = array.as_any().downcast_ref::<StringArray>();
        if let (Some(b), Some(a)) = (builder, array) {
            b.extend(a)
        } else {
            error!(
                "Failed to get string array and array builder to insert {:?} array",
                self.column_type
            );
        }
    }

    // 将2个array数据合并
    pub fn append_column_data(&mut self, column: ArrayRef) {
        match self.column_type {
            ColumnType::Tag | ColumnType::Field(ValueType::String) => {
                self.extend_string_array(column);
            }
            ColumnType::Time(ref unit) => match unit {
                TimeUnit::Second => self.extend_primitive_array::<TimestampSecondType>(column),
                TimeUnit::Millisecond => {
                    self.extend_primitive_array::<TimestampMillisecondType>(column)
                }
                TimeUnit::Microsecond => {
                    self.extend_primitive_array::<TimestampMicrosecondType>(column)
                }
                TimeUnit::Nanosecond => {
                    self.extend_primitive_array::<TimestampNanosecondType>(column)
                }
            },
            ColumnType::Field(ValueType::Float) => {
                self.extend_primitive_array::<Float64Type>(column);
            }
            ColumnType::Field(ValueType::Integer) => {
                self.extend_primitive_array::<Int64Type>(column);
            }
            ColumnType::Field(ValueType::Unsigned) => {
                self.extend_primitive_array::<UInt64Type>(column);
            }
            ColumnType::Field(ValueType::Boolean) => {
                self.extend_bool_array(column);
            }
            _ => {
                error!("Trying to get unknown-type array builder");
            }
        }
    }
}

/// Stores metrics about the table writer execution.
/// 先忽略指标
#[derive(Debug, Clone)]
pub struct SeriesGroupRowIteratorMetrics {
    elapsed_series_scan: metrics::Time,
    elapsed_build_resp_stream: metrics::Time,
    elapsed_get_data_from_memcache: metrics::Time,
    elapsed_get_field_location: metrics::Time,
    elapsed_collect_row_time: metrics::Time,
    elapsed_collect_aggregate_time: metrics::Time,
}

impl SeriesGroupRowIteratorMetrics {
    /// Create new metrics
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let elapsed_series_scan =
            MetricBuilder::new(metrics).subset_time("elapsed_series_scan", partition);

        let elapsed_build_resp_stream =
            MetricBuilder::new(metrics).subset_time("elapsed_build_resp_stream", partition);

        let elapsed_get_data_from_memcache =
            MetricBuilder::new(metrics).subset_time("elapsed_get_data_from_memcache", partition);

        let elapsed_get_field_location =
            MetricBuilder::new(metrics).subset_time("elapsed_get_field_location", partition);

        let elapsed_collect_row_time =
            MetricBuilder::new(metrics).subset_time("elapsed_collect_row_time", partition);

        let elapsed_collect_aggregate_time =
            MetricBuilder::new(metrics).subset_time("elapsed_collect_aggregate_time", partition);

        Self {
            elapsed_series_scan,
            elapsed_build_resp_stream,
            elapsed_get_data_from_memcache,
            elapsed_get_field_location,
            elapsed_collect_row_time,
            elapsed_collect_aggregate_time,
        }
    }

    pub fn elapsed_series_scan(&self) -> &metrics::Time {
        &self.elapsed_series_scan
    }

    pub fn elapsed_build_resp_stream(&self) -> &metrics::Time {
        &self.elapsed_build_resp_stream
    }

    pub fn elapsed_get_data_from_memcache(&self) -> &metrics::Time {
        &self.elapsed_get_data_from_memcache
    }

    pub fn elapsed_get_field_location(&self) -> &metrics::Time {
        &self.elapsed_get_field_location
    }

    pub fn elapsed_collect_row_time(&self) -> &metrics::Time {
        &self.elapsed_collect_row_time
    }

    pub fn elapsed_collect_aggregate_time(&self) -> &metrics::Time {
        &self.elapsed_collect_aggregate_time
    }
}

// 1. Tsm文件遍历： KeyCursor
//  功能：根据输入参数遍历Tsm文件
//  输入参数： SeriesKey、FieldName、StartTime、EndTime、Ascending
//  功能函数：调用Peek()—>(value, timestamp)得到一个值；调用Next()方法游标移到下一个值。
// 2. Field遍历： FiledCursor
//  功能：一个Field特定SeriesKey的遍历
//  输入输出参数同KeyCursor，区别是需要读取缓存数据，并按照特定顺序返回
// 3. Fields->行转换器
//  一行数据是由同一个时间点的多个Field得到。借助上面的FieldCursor按照时间点对齐多个Field-Value拼接成一行数据。其过程类似于多路归并排序。
// 4. Iterator接口抽象层
//  调用Next接口返回一行数据，并且屏蔽查询是本机节点数据还是其他节点数据
// 5. 行数据到DataFusion的RecordBatch转换器
//  调用Iterator.Next得到行数据，然后转换行数据为RecordBatch结构
#[derive(Debug, Clone)]
pub struct QueryOption {
    pub batch_size: usize,
    pub split: PlacedSplit,  // 简单理解就是针对某个副本级的谓语条件
    pub df_schema: SchemaRef,
    pub table_schema: TskvTableSchemaRef,  // cnosdb定义的schema
    // 参与聚合的列 group by ?
    pub aggregates: Option<Vec<TableColumn>>, // TODO: Use PushedAggregateFunction
}

impl QueryOption {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        batch_size: usize,
        split: PlacedSplit,
        aggregates: Option<Vec<TableColumn>>, // TODO: Use PushedAggregateFunction
        df_schema: SchemaRef,
        table_schema: TskvTableSchemaRef,
    ) -> Self {
        Self {
            batch_size,
            split,
            aggregates,
            df_schema,
            table_schema,
        }
    }

    pub fn tenant_name(&self) -> &str {
        &self.table_schema.tenant
    }

    // 产生查询RecordBatch的请求
    pub fn to_query_record_batch_request(
        &self,
        vnode_ids: Vec<VnodeId>,
    ) -> Result<QueryRecordBatchRequest, models::Error> {
        let args = QueryArgs {
            vnode_ids,  // 代表要查询哪些 分片/副本
            limit: self.split.limit(),  // 限制查多少条
            batch_size: self.batch_size,
        };
        let expr = QueryExpr {
            split: self.split.clone(),  // 查询条件
            df_schema: self.df_schema.as_ref().clone(),  // 查询为什么要指定schema
            table_schema: self.table_schema.clone(),
        };

        let args_bytes = QueryArgs::encode(&args)?;
        let expr_bytes = QueryExpr::encode(&expr)?;
        // 对聚合条件进行编码
        let aggs_bytes = domain::encode_agg(&self.aggregates)?;

        Ok(QueryRecordBatchRequest {
            args: args_bytes,
            expr: expr_bytes,
            aggs: aggs_bytes,
        })
    }
}

pub struct FieldFileLocation {
    // 用于读取底层数据文件  换句话说 每个本对象只对应一个数据文件
    reader: Arc<TsmReader>,
    // 遍历用于读取block索引数据
    block_meta_iter: BlockMetaIterator,

    // 与data_block_reader 配合使用 代表只读取范围内的数据
    time_ranges: Arc<TimeRanges>,
    // 通过该对象 读取数据块
    data_block_reader: DataBlockReader,
}

struct DataTypeWithFileId {
    // 包含一个字段id
    file_id: u64,
    data_type: DataType,
}
impl DataTypeWithFileId {
    pub fn new(data_type: DataType, file_id: u64) -> Self {
        Self { file_id, data_type }
    }
    pub fn take(self) -> DataType {
        self.data_type
    }
}

impl Eq for DataTypeWithFileId {}

impl PartialEq for DataTypeWithFileId {
    fn eq(&self, other: &Self) -> bool {
        self.data_type.eq(&other.data_type) && self.file_id.eq(&other.file_id)
    }
}

impl PartialOrd for DataTypeWithFileId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DataTypeWithFileId {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.data_type.cmp(&other.data_type) {
            Ordering::Equal => self.file_id.cmp(&other.file_id),
            other => other,
        }
    }
}

// 因为有5个级别

pub struct Level0TSDataStream {
    // 2层 外层是并行的 需要通过heap帮助排序  内层是顺序的 挨个读取文件中的数据
    field_file_locations: Vec<std::vec::IntoIter<FieldFileLocation>>,
    // 针对外部二层此时正在使用的数据文件
    peeked_file_locations: Vec<Option<FieldFileLocation>>,
    // Reverse 将结果反向排序   BinaryHeap 存储多个列值 并进行排序
    // 注意 DataTypeWithFileId 只对应一个列值
    data_heap: BinaryHeap<Reverse<DataTypeWithFileId>>,
    // 某个被缓存的列数据
    cached_data_type: Option<DataTypeWithFileId>,
}

// 该对象是针对一个field的 代表1级～4级都是使用该对象
pub struct Level14TSDataStream {
    field_file_location: std::vec::IntoIter<FieldFileLocation>,
    peeked_file_locations: Option<FieldFileLocation>,
}

async fn open_field_file_location(
    column_file: Arc<ColumnFile>,  // 该文件对应一个列数据
    version: Arc<Version>,  // 版本中包含了不同级别对应时间的颗粒度
    time_ranges: Arc<TimeRanges>,  // 多个时间范围
    field_id: FieldId,  // 该列id
    value_type: ValueType,    // 描述列数据类型
) -> Result<Vec<FieldFileLocation>> {

    // 打开TsmReader
    let tsm_reader = version.get_tsm_reader(column_file.file_path()).await?;

    // 一个数据文件中 可能存储多个列数据   通过index_iterator_opt定位到该field的数据
    let res = tsm_reader
        .index_iterator_opt(field_id)
        .map(move |index_meta| {
            // 根据产生的元数据 生成FieldFileLocation
            FieldFileLocation::new(
                tsm_reader.clone(),
                time_ranges.clone(),
                // 只返回时间范围内的数据
                index_meta.block_iterator_opt(time_ranges.clone()),
                value_type,
            )
        })
        .collect();
    Ok(res)
}

// level 1～4 的数据流   对应多个TsmReader
impl Level14TSDataStream {
    pub async fn new(
        version: Arc<Version>,
        time_ranges: Arc<TimeRanges>,  // 使用同一个时间范围进行过滤
        column_files: Vec<Arc<ColumnFile>>,  // 每个对应一个reader
        field_id: FieldId,
        value_type: ValueType,
    ) -> Result<Self> {

        // 将每个 ColumnFile 转换成一个 FieldFileLocation
        let file_location_futures = column_files.into_iter().map(move |f| {
            open_field_file_location(
                f,
                version.clone(),
                time_ranges.clone(),
                field_id,
                value_type,
            )
        });

        // 将他们组合成一个迭代器
        let file_locations = join_all(file_location_futures)
            .await
            .into_iter()
            .collect::<Result<Vec<Vec<FieldFileLocation>>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<FieldFileLocation>>()
            .into_iter();
        Ok(Self {
            field_file_location: file_locations,
            peeked_file_locations: None,
        })
    }

    // 读取下一条记录
    async fn next_data(&mut self) -> Result<Option<DataType>> {
        loop {
            // 代表首次读取 或者上个FieldFileLocation被读完了
            match &mut self.peeked_file_locations {

                // 获取下个 FieldFileLocation
                None => match self.field_file_location.next() {
                    // 代表已经没有数据了
                    None => return Ok(None),
                    // 替换FieldFileLocation
                    Some(location) => {
                        self.peeked_file_locations.replace(location);
                    }
                },

                // 读取下个列值
                Some(location) => match location.next_data().await? {
                    None => {
                        // 代表这个FieldFileLocation已经读完了 要切换到下个
                        self.peeked_file_locations.take();
                    }
                    other => return Ok(other),
                },
            }
        }
    }
}


// level0 级别的数据流
impl Level0TSDataStream {
    pub async fn new(
        version: Arc<Version>,
        time_ranges: Arc<TimeRanges>,
        column_files: Vec<Arc<ColumnFile>>,  // 每个文件对应一个reader
        field_id: FieldId,
        value_type: ValueType,
    ) -> Result<Self> {

        // 读取每个数据文件
        let locations_future = column_files.into_iter().map(|f| {
            open_field_file_location(
                f,
                version.clone(),
                time_ranges.clone(),
                field_id,
                value_type,
            )
        });

        // 这里产生二层文件
        let field_file_locations = join_all(locations_future)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|ls| ls.into_iter())
            .collect::<Vec<_>>();
        let peeked_file_locations = field_file_locations.iter().map(|_| None).collect();

        // 1～4 都是顺序铺开的  挨个读取   level0估计是没有这种顺序关系  所以需要借助heap来进行排序
        Ok(Self {
            field_file_locations,
            peeked_file_locations,
            data_heap: BinaryHeap::new(),
            cached_data_type: None,
        })
    }

    // 读取下一个列数据
    async fn next_data(&mut self) -> Result<Option<DataType>> {
        let mut has_finished = false;

        // 遍历得到外层每个元素
        for (peeked_location, files_location) in self
            .peeked_file_locations
            .iter_mut()
            .zip(self.field_file_locations.iter_mut())
        {
            // 在同一层中 files_location的文件数据是顺序的
            if peeked_location.is_none() {
                *peeked_location = files_location.next();
            }

            loop {
                if let Some(location) = peeked_location {
                    match location.next_data().await? {
                        // 当前文件没数据了 切换到下个文件
                        None => {
                            *peeked_location = files_location.next();
                        }
                        // 将数据存储到堆中
                        Some(data) => {
                            let data = DataTypeWithFileId::new(data, location.get_file_id());
                            self.data_heap.push(Reverse(data));
                            break;
                        }
                    }
                } else {
                    // 所有数据都读完了
                    has_finished = true;
                    break;
                }
            }
        }

        // clean finished file_location_iterator
        // 代表某组文件被读取完了
        if has_finished {
            // SAFETY  去掉读完的field_file_location对象
            unsafe {
                let mut un_finish_iter = self.peeked_file_locations.iter().map(|a| a.is_some());
                debug_assert!(un_finish_iter.len().eq(&self.field_file_locations.len()));
                self.field_file_locations
                    .retain(|_| un_finish_iter.next().unwrap_unchecked());
            }
            self.peeked_file_locations.retain(|e| e.is_some());
        }

        loop {
            // 此时所有外层最小的值都在heap中了  通过它弹出最小列值
            return match self.data_heap.pop() {
                Some(Reverse(data)) => {
                    // 忽略重复的数据
                    if let Some(Reverse(next_data)) = self.data_heap.peek() {
                        // deduplication
                        if data.data_type.eq(&next_data.data_type) {
                            continue;
                        }
                    }
                    Ok(Some(data.take()))
                }
                None => Ok(None),
            };
        }
    }
}

// 可以通过它读取数据文件
impl FieldFileLocation {
    pub fn new(
        reader: Arc<TsmReader>,
        time_ranges: Arc<TimeRanges>,
        block_meta_iter: BlockMetaIterator,
        vtype: ValueType,
    ) -> Self {
        Self {
            reader,
            block_meta_iter,
            time_ranges,
            // 初始化时 代表读取到了一个空的数据块
            data_block_reader: DataBlockReader::new_uninit(vtype),
        }
    }

    // if return None
    // 操纵reader对象 读取下个列值
    pub async fn next_data(&mut self) -> Result<Option<DataType>> {
        let res = self.data_block_reader.next();
        if res.is_some() {
            return Ok(res);
        }

        // 切换到下个block块
        if let Some(reader) = self.next_data_block_reader().await? {
            self.data_block_reader = reader;
            debug_assert!(self.data_block_reader.has_next());
            return Ok(self.data_block_reader.next());
        }
        Ok(None)
    }

    /// Iterates the ramaining BlockMeta in `block_meta_iter`, if there are no remaining BlockMeta's,
    /// then return Ok(false).
    ///
    /// Iteration will continue until there are intersected time range between DataBlock and `time_ranges`.
    /// 切换到下个block
    async fn next_data_block_reader(&mut self) -> Result<Option<DataBlockReader>> {
        // Get next BlockMeta to locate the next DataBlock from file.
        // 通过迭代block元数据 可以拿到下个block所在的位置
        for meta in self.block_meta_iter.by_ref() {
            if meta.count() == 0 {
                continue;
            }

            // 该block数据的时间范围
            let time_range = meta.time_range();
            // Check if the time range of the BlockMeta intersected with the given time ranges.
            // 该block与指定的时间范围 有交集才有查询的必要
            if let Some(intersected_tr) = self.time_ranges.intersect(&time_range) {
                // Load a DataBlock from reader by BlockMeta.
                // 通过元数据信息 读取数据文件 顺带解码 和剔除坟墓数据
                let block = self.reader.get_data_block(&meta).await?;
                // 只展示时间交集的部分数据
                let mut data_block_reader = DataBlockReader::new(block, intersected_tr);
                if data_block_reader.has_next() {
                    return Ok(Some(data_block_reader));
                }
            }
        }

        Ok(None)
    }

    pub fn get_file_id(&self) -> u64 {
        self.reader.file_id()
    }
}

impl std::fmt::Debug for FieldFileLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FieldFileLocation")
            .field("file_id", &self.reader.file_id())
            .field("time_ranges", &self.time_ranges)
            .field("data_block_reader", &self.data_block_reader)
            .finish()
    }
}

//-----------Time Cursor----------------
// 描述某个时间列
pub struct TimeCursor {
    ts: i64,   // 时序值
    name: String,  // 时间列的名称
    unit: TimeUnit,  // 时间单位
}

impl TimeCursor {
    pub fn new(ts: i64, name: String, unit: TimeUnit) -> Self {
        Self { ts, name, unit }
    }
}

#[async_trait::async_trait]
impl Cursor for TimeCursor {
    fn name(&self) -> &String {
        &self.name
    }

    fn column_type(&self) -> ColumnType {
        ColumnType::Time(self.unit.clone())
    }

    async fn next(&mut self) -> Result<Option<DataType>> {
        let data = DataType::I64(self.ts, self.ts);

        Ok(Some(data))
    }
}

//-----------Tag Cursor----------------
// 扫描标签列
pub struct TagCursor {
    name: String,
    value: Option<DataType>,
}

impl TagCursor {
    pub fn new(name: String, value: Option<Vec<u8>>) -> Self {
        Self {
            name,
            value: value.map(|v| DataType::StrRef(0, Arc::new(v))),
        }
    }
}

#[async_trait::async_trait]
impl Cursor for TagCursor {
    fn name(&self) -> &String {
        &self.name
    }

    fn column_type(&self) -> ColumnType {
        ColumnType::Tag
    }

    async fn next(&mut self) -> Result<Option<DataType>> {
        Ok(self.value.clone())
    }
}

//-----------Field Cursor----------------
// 列字段游标
pub struct FieldCursor {
    name: Arc<String>,  // 列名
    value_type: ValueType,  // 类型

    cache_data: Box<dyn Iterator<Item = DataType> + 'static>,  // 可能还能读取到缓存数据
    peeked_cache: Option<DataType>,  // 上次查到的值

    // 通过这2个流 来读取列数据文件
    level0_data_stream: Option<Level0TSDataStream>,
    level14_data_stream: Option<Level14TSDataStream>,

    // 分别表示2种级别查到的列数据
    peeked_l0: Option<DataType>,
    peeked_l14: Option<DataType>,
}

unsafe impl Sync for FieldCursor {}
unsafe impl Send for FieldCursor {}

impl FieldCursor {
    pub fn empty(value_type: ValueType, name: Arc<String>) -> Self {
        Self {
            name,
            value_type,
            cache_data: Box::new(std::iter::empty()),
            peeked_cache: None,
            level14_data_stream: None,
            level0_data_stream: None,
            peeked_l0: None,
            peeked_l14: None,
        }
    }

    pub fn new(
        name: Arc<String>,
        value_type: ValueType,
        cache_data: Box<dyn Iterator<Item = DataType> + 'static>,
        level0_data_stream: Option<Level0TSDataStream>,
        level14_data_stream: Option<Level14TSDataStream>,
    ) -> Self {
        Self {
            name,
            value_type,
            cache_data,
            peeked_cache: None,
            level0_data_stream,
            level14_data_stream,
            peeked_l0: None,
            peeked_l14: None,
        }
    }

    // 读取下个 level0 级别的数据
    async fn next_l0_data(&mut self) -> Result<Option<DataType>> {
        match &mut self.level0_data_stream {
            None => Ok(None),
            Some(stream) => match stream.next_data().await? {
                None => {
                    self.level0_data_stream.take();
                    Ok(None)
                }
                other => Ok(other),
            },
        }
    }

    // 读取下个level1～level4级别的数据
    async fn next_l14_data(&mut self) -> Result<Option<DataType>> {
        match &mut self.level14_data_stream {
            None => Ok(None),
            Some(stream) => match stream.next_data().await? {
                None => {
                    self.level14_data_stream.take();
                    Ok(None)
                }
                other => Ok(other),
            },
        }
    }
}

#[async_trait::async_trait]
impl Cursor for FieldCursor {
    fn name(&self) -> &String {
        &self.name
    }

    fn column_type(&self) -> ColumnType {
        ColumnType::Field(self.value_type)
    }

    // 获取下个列值
    async fn next(&mut self) -> Result<Option<DataType>> {
        // 从不同的地方拿到值
        if self.peeked_cache.is_none() {
            self.peeked_cache = self.cache_data.next();
        }

        if self.peeked_l0.is_none() {
            self.peeked_l0 = self.next_l0_data().await?;
        }

        if self.peeked_l14.is_none() {
            self.peeked_l14 = self.next_l14_data().await?;
        }

        let peeked_file_data = match (&self.peeked_l0, &self.peeked_l14) {
            // 这里还有一层比较
            (Some(l0), Some(l14)) => match l0.timestamp().cmp(&l14.timestamp()) {
                Ordering::Less => Some(&mut self.peeked_l0),
                Ordering::Equal => {
                    self.peeked_l14.take();
                    Some(&mut self.peeked_l0)
                }
                Ordering::Greater => Some(&mut self.peeked_l14),
            },
            (Some(_), None) => Some(&mut self.peeked_l0),
            (None, Some(_)) => Some(&mut self.peeked_l14),
            (None, None) => None,
        };

        let peeked_cache_data = &mut self.peeked_cache;

        // 这里还有一层比较   从3个来源中抉择出时间序列最小的值
        match (peeked_file_data, peeked_cache_data.as_ref()) {
            (Some(file_data_opt), Some(cache_data)) => match file_data_opt {
                None => Ok(peeked_cache_data.take()),
                Some(file_data) => match file_data.timestamp().cmp(&cache_data.timestamp()) {
                    Ordering::Less => Ok(file_data_opt.take()),
                    Ordering::Equal => {
                        file_data_opt.take();
                        Ok(peeked_cache_data.take())
                    }
                    Ordering::Greater => Ok(peeked_cache_data.take()),
                },
            },
            (Some(res), None) => Ok(res.take()),
            (None, Some(_)) => Ok(peeked_cache_data.take()),
            (None, None) => Ok(None),
        }
    }
}

// 这个对象在 table_scan中出现  感觉像是获取table的
pub struct RowIterator {
    runtime: Arc<Runtime>,
    engine: EngineRef,  // 通过引擎与底层数据文件交互
    query_option: Arc<QueryOption>,  // 类似于查询条件
    vnode_id: VnodeId,

    /// Super version of vnode_id, maybe None.  该版本号包含更多信息
    super_version: Option<Arc<SuperVersion>>,
    /// List of series id filtered from engine.   本次要查询的多个系列
    series_ids: Arc<Vec<SeriesId>>,
    /// 使用该对象接收从后台任务发送过来的数据
    series_iter_receiver: Receiver<Option<Result<RecordBatch>>>,
    series_iter_closer: CancellationToken,
    /// Whether this iterator was finsihed.  迭代器的数据是否被迭代完
    is_finished: bool,
    #[allow(unused)]
    span_recorder: SpanRecorder,
    metrics_set: ExecutionPlanMetricsSet,  // 执行datafusion时的一些测量数据
}

impl RowIterator {
    pub async fn new(
        runtime: Arc<Runtime>,
        engine: EngineRef,
        query_option: QueryOption,
        vnode_id: VnodeId,
        span_recorder: SpanRecorder,
    ) -> Result<Self> {
        // TODO refac: None 代表没有数据，后续不需要执行
        // 通过指定 租户/db/vnode 可以得到一个版本号信息
        let super_version = {
            let mut span_recorder = span_recorder.child("get super version");
            engine
                .get_db_version(
                    &query_option.table_schema.tenant,
                    &query_option.table_schema.db,
                    vnode_id,
                )
                .await
                .map_err(|err| {
                    span_recorder.error(err.to_string());
                    err
                })?
        };

        // 获取需要的系列
        let series_ids = {
            let mut span_recorder = span_recorder.child("get series ids by filter");
            engine
                .get_series_id_by_filter(
                    &query_option.table_schema.tenant,
                    &query_option.table_schema.db,
                    &query_option.table_schema.name,
                    vnode_id,
                    query_option.split.tags_filter(),  // 这个是根据某个标签的值或范围来进行限定
                )
                .await
                .map_err(|err| {
                    span_recorder.error(err.to_string());
                    err
                })?
        };

        debug!(
            "Iterating rows: vnode_id: {vnode_id}, serie_ids_count: {}",
            series_ids.len()
        );
        let metrics_set = ExecutionPlanMetricsSet::new();
        let query_option = Arc::new(query_option);
        let series_len = series_ids.len();
        let (tx, rx) = channel(1);

        if query_option.aggregates.is_some() {
            // TODO: Correct the aggregate columns order.
            let mut row_iterator = Self {
                runtime,
                engine,
                query_option,
                vnode_id,
                super_version,
                series_ids: Arc::new(series_ids),
                series_iter_receiver: rx,
                series_iter_closer: CancellationToken::new(),
                is_finished: false,
                span_recorder,
                metrics_set,
            };

            // 因为有聚合条件 可能不能利用cpu并行处理? 因为中间结果集不可用
            row_iterator.new_series_group_iteration(0, series_len, tx);

            Ok(row_iterator)
        } else {
            let mut row_iterator = Self {
                runtime,
                engine,
                query_option,
                vnode_id,
                super_version,
                series_ids: Arc::new(series_ids),
                series_iter_receiver: rx,
                series_iter_closer: CancellationToken::new(),
                is_finished: false,
                span_recorder,
                metrics_set,
            };

            // 系列为空的 时候 直接范围迭代器
            if series_len == 0 {
                return Ok(row_iterator);
            }

            // TODO：get series_group_size more intelligently
            // 每个cpu 处理一个series_group
            let series_group_num = num_cpus::get().min(series_len);
            // `usize::div_ceil(self, Self)` is now unstable, so do it by-hand.
            // 每个cpu处理多少series
            let series_group_size = series_len / series_group_num
                + if series_len % series_group_num == 0 {
                    0
                } else {
                    1
                };

            for i in 0..series_group_num {
                let start = series_group_size * i;

                // 得到每个series的首尾下标
                let mut end = start + series_group_size;
                if end > series_len {
                    end = series_len
                }

                row_iterator.new_series_group_iteration(start, end, tx.clone());
            }

            Ok(row_iterator)
        }
    }

    // 根据系列组  产生一个异步任务 提交到tokio
    fn new_series_group_iteration(
        &mut self,
        start: usize,
        end: usize,
        sender: Sender<Option<Result<RecordBatch>>>,
    ) {

        // 表示一行有哪些列
        let row_cols: Vec<Option<DataType>> =
            vec![None; self.query_option.table_schema.columns().len()];

        // 产生一个系列行组迭代器
        let mut iter = SeriesGroupRowIterator {
            runtime: self.runtime.clone(),
            engine: self.engine.clone(),
            query_option: self.query_option.clone(),
            vnode_id: self.vnode_id,
            super_version: self.super_version.clone(),
            series_ids: self.series_ids.clone(),
            start,
            end,
            batch_size: self.query_option.batch_size,
            i: start,
            columns: Vec::with_capacity(self.query_option.table_schema.columns().len()),
            is_finished: false,
            span_recorder: self
                .span_recorder
                .child(format!("SeriesGroupRowIterator [{}, {})", start, end)),
            metrics: SeriesGroupRowIteratorMetrics::new(&self.metrics_set, start),
            row_cols,
        };
        let can_tok = self.series_iter_closer.clone();

        // 将任务提交到异步运行时
        self.runtime.spawn(async move {
            loop {
                tokio::select! {
                    // 发现任务被取消了
                    _ = can_tok.cancelled() => {
                        break;
                    }

                    // 在后台任务中不断拉取数据 并通过sender发送
                    iter_ret = iter.next() => {
                        if let Some(ret) = iter_ret {
                            if sender.send(Some(ret)).await.is_err() {
                                return;
                            }
                        } else {
                            let _ = sender.send(None).await;
                            break;
                        }
                    }
                }
            }
        });
    }

    // 生成用于存储数据的 arrow array builder
    fn build_record_builders(query_option: &QueryOption) -> Result<Vec<ArrayBuilderPtr>> {
        // Get builders for aggregating.
        // 分组列   如果有分组条件 产生存储分组列数据的array
        if let Some(aggregates) = query_option.aggregates.as_ref() {
            let mut builders: Vec<ArrayBuilderPtr> = Vec::with_capacity(aggregates.len());
            for _ in 0..aggregates.len() {
                builders.push(ArrayBuilderPtr::new(
                    Box::new(Int64Builder::with_capacity(query_option.batch_size)),
                    ColumnType::Field(ValueType::Integer),
                ));
            }
            return Ok(builders);
        }

        // Get builders for table scan.  正常情况下使用所有列
        let mut builders: Vec<ArrayBuilderPtr> =
            Vec::with_capacity(query_option.table_schema.columns().len());
        for item in query_option.table_schema.columns().iter() {
            debug!(
                "Building record builder: schema info {:02X} {}",
                item.id, item.name
            );
            let kv_dt = item.column_type.to_physical_type();
            let builder_item = Self::new_column_builder(&kv_dt, query_option.batch_size)?;
            builders.push(ArrayBuilderPtr::new(builder_item, kv_dt))
        }
        Ok(builders)
    }

    // 构建列builder 注意是单列
    fn new_column_builder(
        column_type: &ColumnType,
        batch_size: usize,
    ) -> Result<Box<dyn ArrayBuilder>> {
        Ok(match column_type {
            ColumnType::Tag => Box::new(StringBuilder::with_capacity(batch_size, batch_size * 32)),
            ColumnType::Time(unit) => match unit {
                TimeUnit::Second => Box::new(TimestampSecondBuilder::with_capacity(batch_size)),
                TimeUnit::Millisecond => {
                    Box::new(TimestampMillisecondBuilder::with_capacity(batch_size))
                }
                TimeUnit::Microsecond => {
                    Box::new(TimestampMicrosecondBuilder::with_capacity(batch_size))
                }
                TimeUnit::Nanosecond => {
                    Box::new(TimestampNanosecondBuilder::with_capacity(batch_size))
                }
            },
            ColumnType::Field(t) => match t {
                ValueType::Float => Box::new(Float64Builder::with_capacity(batch_size)),
                ValueType::Integer => Box::new(Int64Builder::with_capacity(batch_size)),
                ValueType::Unsigned => Box::new(UInt64Builder::with_capacity(batch_size)),
                ValueType::Boolean => Box::new(BooleanBuilder::with_capacity(batch_size)),
                ValueType::String => {
                    Box::new(StringBuilder::with_capacity(batch_size, batch_size * 32))
                }
                ValueType::Unknown => {
                    return Err(Error::CommonError {
                        reason: "failed to create column builder: unkown column type".to_string(),
                    })
                }
            },
        })
    }
}


// 行迭代器
impl RowIterator {
    pub async fn next(&mut self) -> Option<Result<RecordBatch>> {

        // 代表已经读完所有数据了
        if self.is_finished {
            return None;
        }

        // 在没有指定系列时 返回空数据
        if self.series_ids.is_empty() {
            self.is_finished = true;
            // Build an empty result.
            // TODO record elapsed_point_to_record_batch
            // let timer = self.metrics.elapsed_point_to_record_batch().timer();
            let mut empty_builders = match Self::build_record_builders(self.query_option.as_ref()) {
                Ok(builders) => builders,
                Err(e) => return Some(Err(e)),
            };
            let mut empty_cols = vec![];
            for item in empty_builders.iter_mut() {
                empty_cols.push(item.ptr.finish())
            }
            let empty_result =
                RecordBatch::try_new(self.query_option.df_schema.clone(), empty_cols).map_err(
                    |err| Error::CommonError {
                        reason: format!("iterator fail, {}", err),
                    },
                );
            // timer.done();

            return Some(empty_result);
        }

        // 接收后台任务读取到的数据 并返回
        while let Some(ret) = self.series_iter_receiver.recv().await {
            match ret {
                Some(Ok(r)) => {
                    return Some(Ok(r));
                }
                Some(Err(e)) => {
                    self.series_iter_closer.cancel();
                    return Some(Err(e));
                }
                None => {
                    // Do nothing
                    debug!("One of series group iterator finished.");
                }
            }
        }

        None
    }
}

impl Drop for RowIterator {
    fn drop(&mut self) {
        // 本对象被废弃时 取消后台任务
        self.series_iter_closer.cancel();

        // trace相关的  先忽略
        if self.span_recorder.span_ctx().is_some() {
            let version_number = self.super_version.as_ref().map(|v| v.version_number);
            let ts_family_id = self.super_version.as_ref().map(|v| v.ts_family_id);

            self.span_recorder
                .set_metadata("version_number", format!("{version_number:?}"));
            self.span_recorder
                .set_metadata("ts_family_id", format!("{ts_family_id:?}"));
            self.span_recorder.set_metadata("vnode_id", self.vnode_id);
            self.span_recorder
                .set_metadata("series_ids_num", self.series_ids.len());

            let metrics = self
                .metrics_set
                .clone_inner()
                .aggregate_by_name()
                .sorted_for_display()
                .timestamps_removed();

            metrics.iter().for_each(|e| {
                self.span_recorder
                    .set_metadata(e.value().name().to_string(), e.value().to_string());
            });
        }
    }
}

// 遍历系列行组的迭代器  会将读取到的数据发送到 row迭代器
struct SeriesGroupRowIterator {
    runtime: Arc<Runtime>,
    engine: EngineRef,
    query_option: Arc<QueryOption>,
    vnode_id: u32,
    super_version: Option<Arc<SuperVersion>>,

    // 因为每个迭代器 可能对应多个系列 要挨个拉取
    series_ids: Arc<Vec<u32>>,
    start: usize,

    /// 准备读取到第几个系列
    end: usize,
    batch_size: usize,

    /// The index of series_ids.  当前读取到第几个系列
    i: usize,
    /// The temporary columns of the series_id.
    /// 当前系列的 列
    columns: Vec<CursorPtr>,
    /// Whether this iterator was finsihed.
    is_finished: bool,

    #[allow(unused)]
    span_recorder: SpanRecorder,
    metrics: SeriesGroupRowIteratorMetrics,
    // row_cols_cache   最近一行数据
    row_cols: Vec<Option<DataType>>,
}

impl SeriesGroupRowIterator {
    pub async fn next(&mut self) -> Option<Result<RecordBatch>> {
        if self.is_finished {
            return None;
        }

        // 构建存储数据的 array builder
        let mut builders = match RowIterator::build_record_builders(self.query_option.as_ref()) {
            Ok(builders) => builders,
            Err(e) => return Some(Err(e)),
        };

        // record fetch_next_row time
        let timer = if self.query_option.aggregates.is_some() {
            self.metrics.elapsed_collect_aggregate_time().clone()
        } else {
            self.metrics.elapsed_collect_row_time().clone()
        };
        let timer_guard = timer.timer();

        // 代表每次返回多少行
        for _ in 0..self.batch_size {

            // 一行行读取 并设置到builder中
            match self.fetch_next_row(&mut builders).await {
                Ok(Some(_)) => {}
                Ok(None) => {
                    // 提前发现没数据了
                    self.is_finished = true;
                    break;
                }
                // 收到取消信号
                Err(err) => return Some(Err(err)),
            };
        }

        timer_guard.done();

        let mut cols = Vec::with_capacity(builders.len());
        for builder in builders.iter_mut() {
            cols.push(builder.ptr.finish())
        }

        // 通过schema + 数据 构成 RecordBatch
        match RecordBatch::try_new(self.query_option.df_schema.clone(), cols) {
            Ok(batch) => Some(Ok(batch)),
            Err(err) => Some(Err(Error::CommonError {
                reason: format!("iterator fail, {}", err),
            })),
        }
    }
}

impl SeriesGroupRowIterator {
    /// Try to fetch next row into array builders.
    ///
    /// If there is no remaning data to fetch, return Ok(None), otherwise return Ok(Some(())).
    /// 读取下一行数据 并将每列分别设置到builder中
    async fn fetch_next_row(&mut self, builder: &mut [ArrayBuilderPtr]) -> Result<Option<()>> {
        if self.query_option.aggregates.is_some() {
            // 收集聚合用的行
            self.collect_aggregate_row_data(builder).await
        } else {
            loop {
                // 已经读取完所有系列了 返回 None
                if self.columns.is_empty() && self.next_series().await?.is_none() {
                    return Ok(None);
                }

                // 采集行数据
                if self.collect_row_data(builder).await?.is_some() {
                    return Ok(Some(()));
                }
            }
        }
    }

    /// Try to fetch next series of this iterator.
    ///
    /// If series ids of this iterator are all consumed, return Ok(None),
    /// otherwise return Ok(Some(())).
    /// 读取下一个系列
    async fn next_series(&mut self) -> Result<Option<()>> {
        if self.i >= self.end {
            return Ok(None);
        }
        // 重新申请col内存
        self.build_series_columns(self.series_ids[self.i]).await?;
        self.i += 1;

        Ok(Some(()))
    }

    /// Build cursors to read data.
    ///
    /// Get series key by series id, for each of columns in the given schema
    /// (may be Time, Tag of Field) build a Cursor.
    /// 读取该系列的数据
    async fn build_series_columns(&mut self, series_id: SeriesId) -> Result<()> {
        let start = Instant::now();

        // 通过系列id  拿到系列key  会关联一组tab列
        if let Some(key) = self
            .engine
            .get_series_key(
                &self.query_option.table_schema.tenant,
                &self.query_option.table_schema.db,
                self.vnode_id,
                series_id,
            )
            .await?
        {
            // 清空之前的数据
            self.columns.clear();
            for item in self.query_option.table_schema.columns() {
                debug!(
                    "Building series columns: sid={:02X}, column={:?}",
                    series_id, item
                );
                let kv_dt = item.column_type.to_physical_type();

                // 每个列都被转换成游标
                let column_cursor: CursorPtr = match kv_dt {
                    ColumnType::Time(ref unit) => {
                        Box::new(TimeCursor::new(0, item.name.clone(), unit.clone()))
                    }

                    ColumnType::Tag => {
                        let tag_val = key.tag_val(&item.name);
                        // 将标签值转换成游标
                        Box::new(TagCursor::new(item.name.clone(), tag_val))
                    }

                    ColumnType::Field(vtype) => match vtype {
                        ValueType::Unknown => {
                            error!("Unknown field type of column {}", &item.name);
                            todo!("return an empty-cursor for unknown type field");
                        }
                        _ => {
                            let cursor = self
                                .build_field_cursor(
                                    unite_id(item.id, series_id),
                                    Arc::new(item.name.clone()),
                                    vtype,
                                )
                                .await?;
                            Box::new(cursor)
                        }
                    },
                };

                self.columns.push(column_cursor);
            }
        }

        // Record elapsed_series_scan
        // 先忽略统计数据
        self.metrics
            .elapsed_series_scan()
            .add_duration(start.elapsed());

        Ok(())
    }

    // 生成不同级别的2个流 不同级别的流底层连接col数据文件 并可以迭代列数据
    async fn build_level_ts_stream(
        &self,
        version: Arc<Version>,
        time_ranges: Arc<TimeRanges>,
        field_id: FieldId,
        value_type: ValueType,
    ) -> Result<(Option<Level0TSDataStream>, Option<Level14TSDataStream>)> {
        // 分别获取 5个级别各自关联的数据文件
        let mut level_files = version.get_level_files(&time_ranges, field_id);

        // 产生level0的stream
        let l0 = match level_files[0].take() {
            Some(fs) => Some(
                Level0TSDataStream::new(
                    version.clone(),
                    time_ranges.clone(),
                    fs,
                    field_id,
                    value_type,
                )
                .await?,
            ),
            None => None,
        };
        let fs: Vec<Arc<ColumnFile>> = level_files
            .into_iter()
            .skip(1)
            .rev()
            .flatten()
            .flatten()
            .collect::<Vec<_>>();

        // assert column file of level 1-4 is not overlap and is sorted
        if cfg!(debug_assertions) {
            debug!("debug assertion level file l1 l4");
            let time_range = fs.iter().map(|a| *a.time_range()).collect::<Vec<_>>();

            let mut time_range_cp = time_range
                .clone()
                .into_iter()
                .collect::<HashSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            time_range_cp.sort();
            debug_assert!(time_range.len().eq(&time_range_cp.len()));
            debug_assert!(time_range_cp.eq(&time_range));
        }

        let l14 = if fs.is_empty() {
            None
        } else {
            // 剩下的生成 1～4的流
            Some(
                Level14TSDataStream::new(
                    version.clone(),
                    time_ranges.clone(),
                    fs,
                    field_id,
                    value_type,
                )
                .await?,
            )
        };
        Ok((l0, l14))
    }

    /// Build a FieldCursor with cached data and file locations.
    /// 根据不同col类型 构成不同游标对象
    async fn build_field_cursor(
        &self,
        field_id: FieldId,
        field_name: Arc<String>,
        field_type: ValueType,
    ) -> Result<FieldCursor> {
        let super_version = match self.super_version {
            Some(ref v) => v.clone(),
            None => return Ok(FieldCursor::empty(field_type, field_name)),
        };

        // 这个代表要查询的时间范围
        let time_ranges_ref = self.query_option.split.time_ranges();
        // 代表只要时间范围内的数据
        let time_predicate = |ts| time_ranges_ref.is_boundless() || time_ranges_ref.contains(ts);
        debug!("Pushed down time range filter: {:?}", time_ranges_ref);
        // Get data from im_memcache and memcache
        let mut cache_data: Vec<DataType> = Vec::new();

        // 读取该字段满足时间条件的所有数据  注意是从缓存拿取
        super_version.caches.read_field_data(
            field_id,
            time_predicate,
            |_| true,
            |d| cache_data.push(d),
        );

        cache_data.sort_by_key(|data| data.timestamp());
        cache_data.reverse();
        cache_data.dedup_by_key(|data| data.timestamp());

        debug!(
            "build memcache data id: {:02X}, len: {}",
            field_id,
            cache_data.len()
        );

        // 这是一列的数据
        let cache_data_iter = cache_data.into_iter().rev();

        let (l0_stream, l14_stream) = self
            .build_level_ts_stream(
                super_version.version.clone(),
                time_ranges_ref.clone(),
                field_id,
                field_type,
            )
            .await?;

        // 2个流是能读取到数据的   把他们和缓存数据合并成cursor
        let cursor = FieldCursor::new(
            field_name.clone(),
            field_type,
            Box::new(cache_data_iter),
            l0_stream,
            l14_stream,
        );

        Ok(cursor)
    }

    // 每次调用 仅采集一行数据
    async fn collect_row_data(&mut self, builders: &mut [ArrayBuilderPtr]) -> Result<Option<()>> {
        trace::trace!("======collect_row_data=========");

        // 如何判别哪些列的数据是同一行呢   先取每列最小的值 如果他们的时间戳一致且最小  那么就是最前的一行
        let mut min_time = i64::MAX;

        // For each column, peek next (timestamp, value), set column_values, and
        // specify the next min_time (if column is a `Field`).
        for (col_cursor, row_col) in self.columns.iter_mut().zip(self.row_cols.iter_mut()) {
            // 标签的游标返回的是一个固定值 所以next多次调用没有影响
            if !col_cursor.is_field() || (col_cursor.is_field() && row_col.is_none()) {
                *row_col = col_cursor.next().await?;
            }
            // 记录最小时间
            if let Some(ref d) = row_col {
                if col_cursor.is_field() {
                    min_time = min_num(min_time, d.timestamp());
                }
            }
        }

        // For the specified min_time, fill each column data.
        // If a column data is for later time, set min_time_column_flag.
        // 这个是表示 每列最小的值 有哪些匹配最小时间戳  代表他们是要被返回的第一行
        let mut min_time_column_flag = vec![false; self.columns.len()];

        // 代表有多少列被判为第一行
        let mut test_collected_col_num = 0_usize;

        // 合起来一起遍历
        for ((col_cursor, ts_val), min_flag) in self
            .columns
            .iter_mut()
            .zip(self.row_cols.iter_mut())
            .zip(min_time_column_flag.iter_mut())
        {
            trace::trace!("field: {}, value: {:?}", col_cursor.name(), ts_val);
            // 跳过非数据列
            if !col_cursor.is_field() {
                continue;
            }

            if let Some(d) = ts_val {
                let ts = d.timestamp();
                if ts == min_time {
                    test_collected_col_num += 1;
                    *min_flag = true
                }
            }
        }

        // Step field_scan completed.
        trace::trace!(
                "Collected data, series_id: {}, column count: {test_collected_col_num}, timestamp: {min_time}",
                self.series_ids[self.i - 1],
            );

        // 代表没有读取到数据 可以切换到下一个系列了
        if min_time == i64::MAX {
            // If peeked no data, return.
            self.columns.clear();
            return Ok(None);
        }

        for (i, (value, min_flag)) in self
            .row_cols
            .iter_mut()
            .zip(min_time_column_flag.iter_mut())
            .enumerate()
        {
            match self.columns[i].column_type() {
                // 时间列 是填写每次检查到的最小时间戳  上面那个时间列游标其实没作用
                ColumnType::Time(unit) => {
                    builders[i].append_timestamp(&unit, min_time);
                }
                ColumnType::Tag => {
                    builders[i].append_value(
                        ValueType::String,
                        value.take(),  // 读取标签列的值
                        self.columns[i].name(),
                    )?;
                }
                ColumnType::Field(value_type) => {
                    // 有填写 无则写入 null
                    if *min_flag {
                        builders[i].append_value(
                            value_type,
                            value.take(),
                            self.columns[i].name(),
                        )?;
                    } else {
                        builders[i].append_value(value_type, None, self.columns[i].name())?;
                    }
                }
            }
        }

        Ok(Some(()))
    }

    // 读取聚合列相关的数据作为行数据
    async fn collect_aggregate_row_data(
        &mut self,
        builder: &mut [ArrayBuilderPtr],
    ) -> Result<Option<()>> {
        if self.is_finished {
            return Ok(None);
        }
        self.is_finished = true;
        trace::trace!("======collect_aggregate_row_data=========");
        match (
            self.super_version.as_ref(),
            self.query_option.aggregates.as_ref(),
        ) {
            (Some(version), Some(aggregates)) => {
                // 遍历聚合列
                for (i, item) in aggregates.iter().enumerate() {
                    let kv_dt = item.column_type.to_physical_type();
                    match kv_dt {
                        ColumnType::Tag => todo!("collect count for tag"),
                        ColumnType::Time(_) => {
                            // 相当于 `SELECT count(<column>) FROM <table> WHERE <time_range_predicates>`
                            let agg_ret = count_column_non_null_values(
                                self.runtime.clone(),
                                version.clone(),
                                self.series_ids.clone(),
                                None,
                                self.query_option.split.time_ranges(),
                            )
                            .await?;
                            builder[i].append_primitive::<Int64Type>(agg_ret as i64);
                        }
                        ColumnType::Field(vtype) => match vtype {
                            ValueType::Unknown => {
                                return Err(Error::CommonError {
                                    reason: format!("unknown type of {}", item.name),
                                });
                            }
                            _ => {
                                let agg_ret = count_column_non_null_values(
                                    self.runtime.clone(),
                                    version.clone(),
                                    self.series_ids.clone(),
                                    Some(item.id),
                                    self.query_option.split.time_ranges(),
                                )
                                .await?;
                                builder[i].append_primitive::<Int64Type>(agg_ret as i64);
                            }
                        },
                    };
                }

                Ok(Some(()))
            }
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_field_cursor() {
        // TODO: Test multi-level contains the same timestamp with different values.
    }
}
