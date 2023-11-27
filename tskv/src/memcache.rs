use std::collections::{HashMap, LinkedList};
use std::fmt::Display;
use std::mem::size_of_val;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use flatbuffers::{ForwardsUOffset, Vector};
use memory_pool::{MemoryConsumer, MemoryPoolRef, MemoryReservation};
use minivec::{mini_vec, MiniVec};
use models::predicate::domain::{TimeRange, TimeRanges};
use models::schema::{
    timestamp_convert, Precision, TableColumn, TskvTableSchema, TskvTableSchemaRef,
};
use models::utils::split_id;
use models::{
    ColumnId, FieldId, PhysicalDType as ValueType, RwLockRef, SchemaId, SeriesId, Timestamp,
};
use parking_lot::RwLock;
use protos::models::{Column, FieldType};
use trace::error;
use utils::bitset::ImmutBitSet;

use crate::error::Result;
use crate::{byte_utils, Error, TseriesFamilyId};

// 代表一个列值
#[derive(Debug, Clone)]
pub enum FieldVal {
    Float(f64),
    Integer(i64),
    Unsigned(u64),
    Boolean(bool),
    Bytes(MiniVec<u8>),  // 一组字节
}

impl FieldVal {

    // 获取数值的类型
    pub fn value_type(&self) -> ValueType {
        match self {
            FieldVal::Float(..) => ValueType::Float,
            FieldVal::Integer(..) => ValueType::Integer,
            FieldVal::Unsigned(..) => ValueType::Unsigned,
            FieldVal::Boolean(..) => ValueType::Boolean,
            FieldVal::Bytes(..) => ValueType::String,
        }
    }

    // 将值和时间戳 合在一起变成 DataType
    pub fn data_value(&self, ts: i64) -> DataType {
        match self {
            FieldVal::Float(val) => DataType::F64(ts, *val),
            FieldVal::Integer(val) => DataType::I64(ts, *val),
            FieldVal::Unsigned(val) => DataType::U64(ts, *val),
            FieldVal::Boolean(val) => DataType::Bool(ts, *val),
            FieldVal::Bytes(val) => DataType::Str(ts, val.clone()),
        }
    }

    pub fn new(val: MiniVec<u8>, vtype: ValueType) -> FieldVal {
        match vtype {
            // 未声明类型 按照u64解读
            ValueType::Unsigned => {
                let val = byte_utils::decode_be_u64(&val);
                FieldVal::Unsigned(val)
            }
            ValueType::Integer => {
                let val = byte_utils::decode_be_i64(&val);
                FieldVal::Integer(val)
            }
            ValueType::Float => {
                let val = byte_utils::decode_be_f64(&val);
                FieldVal::Float(val)
            }
            ValueType::Boolean => {
                let val = byte_utils::decode_be_bool(&val);
                FieldVal::Boolean(val)
            }
            ValueType::String => {
                //let val = Vec::from(val);
                FieldVal::Bytes(val)
            }
            _ => todo!(),
        }
    }

    // 只有 bytes (指针) 消耗内存 其他都是栈内存
    pub fn heap_size(&self) -> usize {
        if let FieldVal::Bytes(val) = self {
            val.capacity()
        } else {
            0
        }
    }
}

impl Display for FieldVal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldVal::Unsigned(val) => write!(f, "{}", val),
            FieldVal::Integer(val) => write!(f, "{}", val),
            FieldVal::Float(val) => write!(f, "{}", val),
            FieldVal::Boolean(val) => write!(f, "{}", val),
            FieldVal::Bytes(val) => write!(f, "{:?})", val),
        }
    }
}

impl PartialEq for FieldVal {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (FieldVal::Unsigned(a), FieldVal::Unsigned(b)) => a == b,
            (FieldVal::Integer(a), FieldVal::Integer(b)) => a == b,
            (FieldVal::Float(a), FieldVal::Float(b)) => a.eq(b),
            (FieldVal::Boolean(a), FieldVal::Boolean(b)) => a == b,
            (FieldVal::Bytes(a), FieldVal::Bytes(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for FieldVal {}

// 代表一行数据
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RowData {
    // 某行数据对应一个时间戳
    pub ts: i64,
    // 该行下各字段数据
    pub fields: Vec<Option<FieldVal>>,
}

impl RowData {

    // 从各向量中读取某列的值  并转换成行数据
    pub fn point_to_row_data(
        schema: &TskvTableSchema,  // 记录当前表的schema
        from_precision: Precision,
        columns: &Vector<ForwardsUOffset<Column>>,  // 包含列数据 以及偏移量信息   这个Column数据中是包含位图的
        fields_idx: &[usize],  // 应该是代表值需要读取这几列的数据
        ts_idx: usize,  // 描述存储时间戳的列的下标
        row_count: usize,  // 代表读取每列的第几行数据
    ) -> Result<RowData> {

        // 代表该行至少有一列有数据
        let mut has_fields = false;
        // 这个向量用于存储行数据
        let mut fields = vec![None; schema.field_num()];

        // 发现也是列式存储

        // value是序列
        let fields_id = schema.fields_id();
        for field_id in fields_idx {
            // 得到col信息
            let column = columns.get(*field_id);
            // 读取4字节长度 得到name
            let column_name = column.name_ext()?;
            // 用12字节存储位图信息
            let column_nullbit = column.nullbit_ext()?;

            // 根据不同类型 知道要读取多少长度
            match column.field_type() {
                FieldType::Integer => {
                    // 描述记录了多少个值
                    let len = column.int_values_len()?;
                    let column_nullbits =
                        ImmutBitSet::new_without_check(len, column_nullbit.bytes());
                    // 通过查看位图对应的位置是否为1  确认该列在该行是否有数据
                    if !column_nullbits.get(row_count) {
                        continue;
                    }

                    // 读取该行的值
                    let val = column.int_values()?.get(row_count);
                    match schema.column(column_name) {
                        None => {
                            error!("column {} not found in schema", column_name);
                        }
                        // 获取列的元数据信息
                        Some(column) => {
                            let field_id = column.id;
                            // 通过列id得到序号  因为fields_id 对col进行过排序
                            let field_idx = fields_id.get(&field_id).unwrap();
                            // 设置列的值
                            fields[*field_idx] = Some(FieldVal::Integer(val));
                            has_fields = true;
                        }
                    }
                }
                FieldType::Float => {
                    let len = column.float_values_len()?;
                    let column_nullbits =
                        ImmutBitSet::new_without_check(len, column_nullbit.bytes());
                    if !column_nullbits.get(row_count) {
                        continue;
                    }
                    let val = column.float_values()?.get(row_count);
                    match schema.column(column_name) {
                        None => {
                            error!("column {} not found in schema", column_name);
                        }
                        Some(column) => {
                            let field_id = column.id;
                            let field_idx = fields_id.get(&field_id).unwrap();
                            fields[*field_idx] = Some(FieldVal::Float(val));
                            has_fields = true;
                        }
                    }
                }
                FieldType::Unsigned => {
                    let len = column.uint_values_len()?;
                    let column_nullbits =
                        ImmutBitSet::new_without_check(len, column_nullbit.bytes());
                    if !column_nullbits.get(row_count) {
                        continue;
                    }
                    let val = column.uint_values()?.get(row_count);
                    match schema.column(column_name) {
                        None => {
                            error!("column {} not found in schema", column_name);
                        }
                        Some(column) => {
                            let field_id = column.id;
                            let field_idx = fields_id.get(&field_id).unwrap();
                            fields[*field_idx] = Some(FieldVal::Unsigned(val));
                            has_fields = true;
                        }
                    }
                }
                FieldType::Boolean => {
                    let len = column.bool_values_len()?;
                    let column_nullbits =
                        ImmutBitSet::new_without_check(len, column_nullbit.bytes());
                    if !column_nullbits.get(row_count) {
                        continue;
                    }
                    let val = column.bool_values()?.get(row_count);
                    match schema.column(column_name) {
                        None => {
                            error!("column {} not found in schema", column_name);
                        }
                        Some(column) => {
                            let field_id = column.id;
                            let field_idx = fields_id.get(&field_id).unwrap();
                            fields[*field_idx] = Some(FieldVal::Boolean(val));
                            has_fields = true;
                        }
                    }
                }
                FieldType::String => {
                    let len = column.string_values_len()?;
                    let column_nullbits =
                        ImmutBitSet::new_without_check(len, column_nullbit.bytes());
                    if !column_nullbits.get(row_count) {
                        continue;
                    }
                    let val = column.string_values()?.get(row_count);
                    match schema.column(column_name) {
                        None => {
                            error!("column {} not found in schema", column_name);
                        }
                        Some(column) => {
                            let field_id = column.id;
                            let field_idx = fields_id.get(&field_id).unwrap();
                            fields[*field_idx] =
                                Some(FieldVal::Bytes(MiniVec::from(val.as_bytes())));
                            has_fields = true;
                        }
                    }
                }
                _ => {
                    error!("unsupported field type");
                }
            }
        }

        // 代表该行无数据
        if !has_fields {
            return Err(Error::InvalidPoint);
        }

        // 获取时间列
        let ts_column = columns.get(ts_idx);
        // 获取该行对应的时间戳
        let ts = ts_column.int_values()?.get(row_count);
        // 获取时间戳精度
        let to_precision = schema.time_column_precision();
        let ts = timestamp_convert(from_precision, to_precision, ts).ok_or(Error::CommonError {
            reason: "timestamp overflow".to_string(),
        })?;

        // 将时间戳和这些列信息组合在一起  作为行数据
        Ok(RowData { ts, fields })
    }

    // 该对象占用的内存大小
    pub fn size(&self) -> usize {
        let mut size = 0;
        for i in self.fields.iter() {
            match i {
                None => {
                    size += size_of_val(i);
                }
                Some(v) => {
                    size += size_of_val(i) + v.heap_size();
                }
            }
        }
        size += size_of_val(&self.ts);
        size += size_of_val(&self.fields);
        size
    }
}

// 行组 将多行看作一个group
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RowGroup {
    // 包含各col的信息  每当col发生变化 版本号会递增
    pub schema: Arc<TskvTableSchema>,
    // 该行组的时间范围  (最小行到最大行对应的时间)
    pub range: TimeRange,
    // 行数据以链表形式连接
    pub rows: LinkedList<RowData>,
    /// total size in stack and heap
    pub size: usize,
}

// 系列数据 简单看是 RowGroup更上一层维度
#[derive(Debug)]
pub struct SeriesData {
    pub series_id: SeriesId,
    // 更大的时间范围
    pub range: TimeRange,
    pub groups: LinkedList<RowGroup>,
}

impl SeriesData {

    // 初始化系列数据对象
    fn new(series_id: SeriesId) -> Self {
        Self {
            series_id,
            range: TimeRange {
                min_ts: i64::MAX,
                max_ts: i64::MIN,
            },
            groups: LinkedList::new(),
        }
    }

    // 系列数据每次以 行组为单位进行写入
    pub fn write(&mut self, mut group: RowGroup) {
        self.range.merge(&group.range);

        // 相同schema的行组数据会进行合并
        for item in self.groups.iter_mut() {
            if item.schema.schema_id == group.schema.schema_id {
                item.range.merge(&group.range);
                item.rows.append(&mut group.rows);
                item.schema = group.schema;
                return;
            }
        }

        // 不同schema的数据 会挂到链表上
        self.groups.push_back(group);
    }

    // 在各schema上 去掉该col数据
    pub fn drop_column(&mut self, column_id: ColumnId) {
        for item in self.groups.iter_mut() {
            let name = match item.schema.column_name(column_id) {
                None => continue,
                Some(name) => name.to_string(),
            };
            let index = match item.schema.fields_id().get(&column_id) {
                None => continue,
                Some(index) => *index,
            };
            // 在每行数据上去掉该col
            for row in item.rows.iter_mut() {
                row.fields.remove(index);
            }
            // 更新schema信息
            let mut schema_t = item.schema.as_ref().clone();
            schema_t.drop_column(&name);
            //schema_t.schema_id += 1;
            item.schema = Arc::new(schema_t)
        }
    }

    // 新增或者修改schema 在查询时会产生什么影响呢

    // 修改某列信息  但是看起来不会影响到 RowGroup 的数据？
    pub fn change_column(&mut self, column_name: &str, new_column: &TableColumn) {
        for item in self.groups.iter_mut() {
            let mut schema_t = item.schema.as_ref().clone();
            schema_t.change_column(column_name, new_column.clone());
            schema_t.schema_id += 1;
            item.schema = Arc::new(schema_t)
        }
    }

    // 添加一列 只修改schema 不影响原来行组的数据
    pub fn add_column(&mut self, new_column: &TableColumn) {
        for item in self.groups.iter_mut() {
            let mut schema_t = item.schema.as_ref().clone();
            schema_t.add_column(new_column.clone());
            schema_t.schema_id += 1;
            item.schema = Arc::new(schema_t)
        }
    }

    // 删除时间范围覆盖的数据
    pub fn delete_series(&mut self, range: &TimeRange) {
        // 时间无交集 不需要删除
        if range.max_ts < self.range.min_ts || range.min_ts > self.range.max_ts {
            return;
        }

        for item in self.groups.iter_mut() {
            // 只保留时间范围在range之外的
            item.rows = item
                .rows
                .iter()
                .filter(|row| row.ts < range.min_ts || row.ts > range.max_ts)
                .cloned()
                .collect();
        }
    }

    // 根据一组时间范围删除
    pub fn delete_by_time_ranges(&mut self, time_ranges: &TimeRanges) {
        for time_range in time_ranges.time_ranges() {
            if time_range.max_ts < self.range.min_ts || time_range.min_ts > self.range.max_ts {
                continue;
            }

            for item in self.groups.iter_mut() {
                item.rows = item
                    .rows
                    .iter()
                    .filter(|row| row.ts < time_range.min_ts || row.ts > time_range.max_ts)
                    .cloned()
                    .collect();
            }
        }
    }

    // 根据谓语查询数据
    pub fn read_data(
        &self,
        column_id: ColumnId,
        mut time_predicate: impl FnMut(Timestamp) -> bool,
        mut value_predicate: impl FnMut(&FieldVal) -> bool,
        mut handle_data: impl FnMut(DataType),
    ) {
        for group in self.groups.iter() {
            // 这些行组的schema都是不同的   相同schema的数据将会合并到一个行组中
            let field_index = group.schema.fields_id();
            let index = match field_index.get(&column_id) {
                None => continue,
                Some(v) => v,
            };
            group
                .rows
                .iter()
                // 先根据时间谓语过滤
                .filter(|row| time_predicate(row.ts))
                .for_each(|row| {
                    // 读取该列的值 并判断是否满足条件
                    if let Some(Some(field)) = row.fields.get(*index) {
                        if value_predicate(field) {
                            // 处理数据
                            handle_data(field.data_value(row.ts))
                        }
                    }
                });
        }
    }

    // 处理满足时间谓语的数据
    pub fn read_timestamps(
        &self,
        mut time_predicate: impl FnMut(Timestamp) -> bool,
        mut handle_data: impl FnMut(Timestamp),
    ) {
        for group in self.groups.iter() {
            group
                .rows
                .iter()
                .filter(|row| time_predicate(row.ts))
                .for_each(|row| handle_data(row.ts));
        }
    }

    // 平铺开展示
    pub fn flat_groups(&self) -> Vec<(SchemaId, TskvTableSchemaRef, &LinkedList<RowData>)> {
        self.groups
            .iter()
            .map(|g| (g.schema.schema_id, g.schema.clone(), &g.rows))
            .collect()
    }
}

// 缓存对象
#[derive(Debug)]
pub struct MemCache {

    // 对应一个vnodeId  因为一个vnode下可能有多个系列 所以又被称作 列族
    tf_id: TseriesFamilyId,

    // 是否正在刷盘
    flushing: AtomicBool,

    // 缓存大小
    max_size: u64,
    // 缓存中最小数据的序列
    min_seq_no: u64,

    // wal seq number  当前序列号  当写入新数据时 序列号也会跟着更新
    seq_no: AtomicU64,
    // 该对象描述内存消耗
    memory: RwLock<MemoryReservation>,

    part_count: usize,
    // This u64 comes from split_id(SeriesId) % part_count
    // 二重维度 外层代表分区 内层是系列id 与系列数据
    // 应该是考虑到某个节点的多个分片有可能出现在同一个节点上
    partions: Vec<RwLock<HashMap<u32, RwLockRef<SeriesData>>>>,
}


// 针对每个vnode都有一个 缓存对象
impl MemCache {
    pub fn new(
        tf_id: TseriesFamilyId,  // vnodeid
        max_size: u64,
        part_count: usize,  // 有多少分区数据落在该节点上
        seq: u64,  // 序列号
        pool: &MemoryPoolRef,
    ) -> Self {
        let mut partions = Vec::with_capacity(part_count);
        for _i in 0..part_count {
            partions.push(RwLock::new(HashMap::new()));
        }

        // 将消费者注册到pool上 可以追踪该对象的内存消耗
        let res =
            RwLock::new(MemoryConsumer::new(format!("memcache-{}-{}", tf_id, seq)).register(pool));
        Self {
            tf_id,
            flushing: AtomicBool::new(false),

            max_size,
            min_seq_no: seq,

            part_count,
            partions,

            seq_no: AtomicU64::new(seq),
            memory: res,
        }
    }

    // 将行组数据写入缓存
    pub fn write_group(&self, sid: SeriesId, seq: u64, group: RowGroup) -> Result<()> {
        self.seq_no.store(seq, Ordering::Relaxed);
        // 从pool中申请内存
        self.memory
            .write()
            .try_grow(group.size)
            .map_err(|_| Error::MemoryExhausted)?;

        // 系列id 的分配是跟分区数挂钩的
        let index = (sid as usize) % self.part_count;
        let mut series_map = self.partions[index].write();

        // 写入行组数据
        if let Some(series_data) = series_map.get(&sid) {
            let series_data_ptr = series_data.clone();
            let mut series_data_ptr_w = series_data_ptr.write();
            drop(series_map);
            series_data_ptr_w.write(group);
        } else {
            let mut series_data = SeriesData::new(sid);
            series_data.write(group);
            series_map.insert(sid, Arc::new(RwLock::new(series_data)));
        }
        Ok(())
    }

    pub fn read_field_data(
        &self,
        field_id: FieldId,  // 同时包含了系列和列信息
        time_predicate: impl FnMut(Timestamp) -> bool,
        value_predicate: impl FnMut(&FieldVal) -> bool,
        handle_data: impl FnMut(DataType),
    ) {
        let (column_id, sid) = split_id(field_id);
        let index = (sid as usize) % self.part_count;
        let series_data = self.partions[index].read().get(&sid).cloned();

        // 针对该系列下所有行组 进行处理
        if let Some(series_data) = series_data {
            series_data
                .read()
                .read_data(column_id, time_predicate, value_predicate, handle_data)
        }
    }

    // 同上
    pub fn read_series_timestamps(
        &self,
        series_ids: &[SeriesId],
        mut time_predicate: impl FnMut(Timestamp) -> bool,
        mut handle_data: impl FnMut(Timestamp),
    ) {
        for sid in series_ids.iter() {
            let index = (*sid as usize) % self.part_count;
            let series_data = self.partions[index].read().get(sid).cloned();
            if let Some(series_data) = series_data {
                series_data
                    .read()
                    .read_timestamps(&mut time_predicate, &mut handle_data);
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        for part in self.partions.iter() {
            if !part.read().is_empty() {
                return false;
            }
        }

        true
    }

    // 丢弃某些列的数据
    pub fn drop_columns(&self, field_ids: &[FieldId]) {
        for fid in field_ids {
            let (column_id, sid) = split_id(*fid);
            let index = (sid as usize) % self.part_count;
            let series_data = self.partions[index].read().get(&sid).cloned();
            if let Some(series_data) = series_data {
                series_data.write().drop_column(column_id);
            }
        }
    }

    // 下面都是一些转发操作了 主要就是解析seriesId 找到目标 seriesData

    pub fn change_column(&self, sids: &[SeriesId], column_name: &str, new_column: &TableColumn) {
        for sid in sids {
            let index = (*sid as usize) % self.part_count;
            let series_data = self.partions[index].read().get(sid).cloned();
            if let Some(series_data) = series_data {
                series_data.write().change_column(column_name, new_column);
            }
        }
    }

    pub fn add_column(&self, sids: &[SeriesId], new_column: &TableColumn) {
        for sid in sids {
            let index = (*sid as usize) % self.part_count;
            let series_data = self.partions[index].read().get(sid).cloned();
            if let Some(series_data) = series_data {
                series_data.write().add_column(new_column);
            }
        }
    }

    // 删除某个系列某个范围数据
    pub fn delete_series(&self, sids: &[SeriesId], range: &TimeRange) {
        for sid in sids {
            let index = (*sid as usize) % self.part_count;
            let series_data = self.partions[index].read().get(sid).cloned();
            if let Some(series_data) = series_data {
                series_data.write().delete_series(range);
            }
        }
    }

    pub fn delete_series_by_time_ranges(&self, sids: &[SeriesId], time_ranges: &TimeRanges) {
        for sid in sids {
            let index = (*sid as usize) % self.part_count;
            let series_data = self.partions[index].read().get(sid).cloned();
            if let Some(series_data) = series_data {
                series_data.write().delete_by_time_ranges(time_ranges);
            }
        }
    }

    // 读取一份数据副本
    pub fn read_series_data(&self) -> Vec<(SeriesId, Arc<RwLock<SeriesData>>)> {
        let mut ret = Vec::new();
        self.partions.iter().for_each(|p| {
            let p_rlock = p.read();
            for (k, v) in p_rlock.iter() {
                ret.push((*k, v.clone()));
            }
        });
        ret
    }

    // 标记成正在刷盘
    pub fn mark_flushing(&self) -> bool {
        self.flushing
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    pub fn is_flushing(&self) -> bool {
        self.flushing.load(Ordering::Relaxed)
    }

    pub fn is_full(&self) -> bool {
        self.memory.read().size() >= self.max_size as usize
    }

    pub fn tf_id(&self) -> TseriesFamilyId {
        self.tf_id
    }

    pub fn seq_no(&self) -> u64 {
        self.seq_no.load(Ordering::Relaxed)
    }

    pub fn min_seq_no(&self) -> u64 {
        self.min_seq_no
    }

    pub fn max_buf_size(&self) -> u64 {
        self.max_size
    }

    pub fn cache_size(&self) -> u64 {
        self.memory.read().size() as u64
    }
}

// 代表一个列值  (在cnosdb中每个列值都要关联时间戳)
#[derive(Debug, Clone)]
pub enum DataType {
    U64(i64, u64),
    I64(i64, i64),
    Str(i64, MiniVec<u8>),
    F64(i64, f64),
    Bool(i64, bool),
    /// Notice.
    /// This variant is used for multiple Clone.
    /// If not, please use [`DataType::Str`].
    StrRef(i64, Arc<Vec<u8>>),
}

impl PartialEq for DataType {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp().eq(&other.timestamp())
    }
}

impl Eq for DataType {}

impl PartialOrd for DataType {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Only care about timestamps when comparing
impl Ord for DataType {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.timestamp().cmp(&other.timestamp())
    }
}

impl DataType {

    // 将某列的值 与时间戳关联起来
    pub fn new(vtype: ValueType, ts: i64) -> Self {
        match vtype {
            ValueType::Unsigned => DataType::U64(ts, 0),
            ValueType::Integer => DataType::I64(ts, 0),
            ValueType::Float => DataType::F64(ts, 0.0),
            ValueType::Boolean => DataType::Bool(ts, false),
            ValueType::String => DataType::Str(ts, mini_vec![]),
            _ => todo!(),
        }
    }
    pub fn timestamp(&self) -> i64 {
        match *self {
            DataType::U64(ts, ..) => ts,
            DataType::I64(ts, ..) => ts,
            DataType::Str(ts, ..) => ts,
            DataType::F64(ts, ..) => ts,
            DataType::Bool(ts, ..) => ts,
            DataType::StrRef(ts, ..) => ts,
        }
    }

    pub fn with_field_val(ts: Timestamp, field_val: FieldVal) -> Self {
        match field_val {
            FieldVal::Float(val) => Self::F64(ts, val),
            FieldVal::Integer(val) => Self::I64(ts, val),
            FieldVal::Unsigned(val) => Self::U64(ts, val),
            FieldVal::Boolean(val) => Self::Bool(ts, val),
            FieldVal::Bytes(val) => Self::Str(ts, val),
        }
    }

    #[cfg(test)]
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            DataType::U64(t, val) => {
                let mut buf = vec![0; 16];
                buf[0..8].copy_from_slice(t.to_be_bytes().as_slice());
                buf[8..16].copy_from_slice(val.to_be_bytes().as_slice());
                buf
            }
            DataType::I64(t, val) => {
                let mut buf = vec![0; 16];
                buf[0..8].copy_from_slice(t.to_be_bytes().as_slice());
                buf[8..16].copy_from_slice(val.to_be_bytes().as_slice());
                buf
            }
            DataType::F64(t, val) => {
                let mut buf = vec![0; 16];
                buf[0..8].copy_from_slice(t.to_be_bytes().as_slice());
                buf[8..16].copy_from_slice(val.to_be_bytes().as_slice());
                buf
            }
            DataType::Str(t, val) => {
                let buf_len = 8 + val.len();
                let mut buf = vec![0; buf_len];
                buf[0..8].copy_from_slice(t.to_be_bytes().as_slice());
                buf[8..buf_len].copy_from_slice(val);
                buf
            }
            DataType::Bool(t, val) => {
                let mut buf = vec![0; 9];
                buf[0..8].copy_from_slice(t.to_be_bytes().as_slice());
                buf[8] = if *val { 1_u8 } else { 0_u8 };
                buf
            }
            DataType::StrRef(t, val) => {
                let buf_len = 8 + val.len();
                let mut buf = vec![0; buf_len];
                buf[0..8].copy_from_slice(t.to_be_bytes().as_slice());
                buf[8..buf_len].copy_from_slice(val);
                buf
            }
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::U64(ts, val) => write!(f, "({}, {})", ts, val),
            DataType::I64(ts, val) => write!(f, "({}, {})", ts, val),
            DataType::Str(ts, val) => write!(f, "({}, {:?})", ts, val),
            DataType::F64(ts, val) => write!(f, "({}, {})", ts, val),
            DataType::Bool(ts, val) => write!(f, "({}, {})", ts, val),
            DataType::StrRef(ts, val) => write!(f, "({}, {:?})", ts, val),
        }
    }
}

pub(crate) mod test {
    use std::collections::{HashMap, LinkedList};
    use std::mem::size_of;
    use std::sync::Arc;

    use models::predicate::domain::TimeRange;
    use models::schema::TskvTableSchema;
    use models::{SchemaId, SeriesId, Timestamp};
    use parking_lot::RwLock;

    use super::{FieldVal, MemCache, RowData, RowGroup};

    pub fn put_rows_to_cache(
        cache: &MemCache,
        series_id: SeriesId,
        schema_id: SchemaId,
        mut schema: TskvTableSchema,
        time_range: (Timestamp, Timestamp),
        put_none: bool,
    ) {
        let mut rows = LinkedList::new();
        let mut size: usize = schema.size();
        for ts in time_range.0..=time_range.1 {
            let mut fields = Vec::new();
            for _ in 0..schema.columns().len() {
                size += size_of::<Option<FieldVal>>();
                if put_none {
                    fields.push(None);
                } else {
                    fields.push(Some(FieldVal::Float(ts as f64)));
                    size += 8;
                }
            }
            size += 8;
            rows.push_back(RowData { ts, fields });
        }

        schema.schema_id = schema_id;
        let row_group = RowGroup {
            schema: schema.into(),
            range: TimeRange::from(time_range),
            rows,
            size: size_of::<RowGroup>() + size,
        };
        cache.write_group(series_id, 1, row_group).unwrap();
    }

    pub fn get_one_series_cache_data(
        cache: Arc<RwLock<MemCache>>,
    ) -> HashMap<String, Vec<(Timestamp, FieldVal)>> {
        let mut fname_vals_map: HashMap<String, Vec<(Timestamp, FieldVal)>> = HashMap::new();
        let series_data = cache.read().read_series_data();
        for (_sid, sdata) in series_data {
            let sdata_rlock = sdata.read();
            let schema_groups = sdata_rlock.flat_groups();
            for (_sch_id, sch, row) in schema_groups {
                let fields = sch.fields();
                for r in row {
                    for (i, f) in r.fields.iter().enumerate() {
                        if let Some(fv) = f {
                            if let Some(c) = fields.get(i) {
                                if &c.name != "time" {
                                    fname_vals_map
                                        .entry(c.name.clone())
                                        .or_default()
                                        .push((r.ts, fv.clone()))
                                }
                            };
                        }
                    }
                }
            }
        }

        fname_vals_map
    }
}

#[cfg(test)]
mod test_memcache {
    use std::collections::LinkedList;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::TimeUnit;
    use memory_pool::{GreedyMemoryPool, MemoryPool};
    use models::predicate::domain::TimeRange;
    use models::schema::{ColumnType, TableColumn, TskvTableSchema};
    use models::{SeriesId, ValueType};

    use super::{FieldVal, MemCache, RowData, RowGroup};

    #[test]
    fn test_write_group() {
        let sid: SeriesId = 1;

        let memory_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024));
        let mem_cache = MemCache::new(1, 1000, 2, 1, &memory_pool);
        {
            let series_part = &mem_cache.partions[sid as usize].read();
            let series_data = series_part.get(&sid);
            assert!(series_data.is_none());
        }

        #[rustfmt::skip]
        let mut schema_1 = TskvTableSchema::new(
            "test_tenant".to_string(), "test_db".to_string(), "test_table".to_string(),
            vec![
                TableColumn::new_time_column(1, TimeUnit::Nanosecond),
                TableColumn::new_tag_column(2, "tag_col_1".to_string()),
                TableColumn::new_tag_column(3, "tag_col_2".to_string()),
                TableColumn::new(4, "f_col_1".to_string(), ColumnType::Field(ValueType::Float), Default::default()),
            ],
        );
        schema_1.schema_id = 1;
        #[rustfmt::skip]
        let row_group_1 = RowGroup {
            schema: Arc::new(schema_1),
            range: TimeRange::new(1, 3),
            rows: LinkedList::from([
                RowData { ts: 1, fields: vec![Some(FieldVal::Float(1.0))] },
                RowData { ts: 3, fields: vec![Some(FieldVal::Float(3.0))] },
            ]),
            size: 10,
        };
        mem_cache.write_group(sid, 1, row_group_1.clone()).unwrap();
        {
            let series_part = &mem_cache.partions[sid as usize].read();
            let series_data = series_part.get(&sid);
            assert!(series_data.is_some());
            let series_data = series_data.unwrap().read();
            assert_eq!(sid, series_data.series_id);
            assert_eq!(TimeRange::new(1, 3), series_data.range);
            assert_eq!(1, series_data.groups.len());
            assert_eq!(row_group_1, series_data.groups.front().unwrap().clone());
        }

        #[rustfmt::skip]
        let mut schema_2 = TskvTableSchema::new(
            "test_tenant".to_string(), "test_db".to_string(), "test_table".to_string(),
            vec![
                TableColumn::new_time_column(1, TimeUnit::Nanosecond),
                TableColumn::new_tag_column(2, "tag_col_1".to_string()),
                TableColumn::new_tag_column(3, "tag_col_2".to_string()),
                TableColumn::new(4, "f_col_1".to_string(), ColumnType::Field(ValueType::Float), Default::default()),
                TableColumn::new(5, "f_col_2".to_string(), ColumnType::Field(ValueType::Integer), Default::default()),
            ],
        );
        schema_2.schema_id = 2;
        #[rustfmt::skip]
        let row_group_2 = RowGroup {
            schema: Arc::new(schema_2),
            range: TimeRange::new(3, 5),
            rows: LinkedList::from([
                RowData { ts: 3, fields: vec![None, Some(FieldVal::Integer(3))] },
                RowData { ts: 5, fields: vec![Some(FieldVal::Float(5.0)), Some(FieldVal::Integer(5))] }
            ]),
            size: 10,
        };
        mem_cache.write_group(sid, 2, row_group_2.clone()).unwrap();
        {
            let series_part = &mem_cache.partions[sid as usize].read();
            let series_data = series_part.get(&sid);
            assert!(series_data.is_some());
            let series_data = series_data.unwrap().read();
            assert_eq!(sid, series_data.series_id);
            assert_eq!(TimeRange::new(1, 5), series_data.range);
            assert_eq!(2, series_data.groups.len());
            assert_eq!(row_group_2, series_data.groups.back().unwrap().clone());
        }
    }
}
