pub mod checker;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::logical_plan::AggWithGrouping;
use datafusion::logical_expr::{TableProviderAggregationPushDown, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use meta::model::MetaClientRef;
use models::schema::{StreamTable, Watermark};

use self::checker::SchemaChecker;
use crate::QueryError;

pub type StreamProviderManagerRef = Arc<StreamProviderManager>;

/// Maintain and manage all registered streaming data sources
/// 该manager 维护不同stream_type的提供者
#[derive(Default)]
pub struct StreamProviderManager {
    factories: HashMap<String, StreamProviderFactoryRef>,
}

impl StreamProviderManager {
    pub fn register_stream_provider_factory(
        &mut self,
        stream_type: impl Into<String>,
        factory: StreamProviderFactoryRef,
    ) -> Result<(), QueryError> {
        let stream_type = stream_type.into();

        // 拒绝重复创建
        if self.factories.contains_key(&stream_type) {
            return Err(QueryError::StreamSourceFactoryAlreadyExists { stream_type });
        }

        let _ = self.factories.insert(stream_type, factory);

        Ok(())
    }

    // 实际上是把 streamTable 转换成了一个 stream 并开放相关的api
    pub fn create_provider(
        &self,
        meta: MetaClientRef,
        table: &StreamTable,
    ) -> Result<StreamProviderRef, QueryError> {
        let stream_type = table.stream_type();
        self.factories
            .get(stream_type)
            .ok_or_else(|| QueryError::UnsupportedStreamType {
                stream_type: stream_type.to_string(),
            })?
            .create(meta, table)
    }
}

pub type StreamProviderFactoryRef = Arc<dyn StreamProviderFactory + Send + Sync>;

/// Each type of [`StreamTable`] corresponds to a unique [`StreamProviderFactory`]\
/// When supporting new streaming data sources, this interface needs to be implemented and registered with [`StreamProviderManager`].
pub trait StreamProviderFactory: SchemaChecker<StreamTable> {
    /// Create the corresponding [`StreamProviderRef`] according to the type of the given [`StreamTable`].\
    /// [`MetaClientRef`] is for possible stream tables associated with internal tables
    fn create(
        &self,
        meta: MetaClientRef,
        table: &StreamTable,
    ) -> Result<StreamProviderRef, QueryError>;
}

pub type Offset = i64;
pub type RangeOffset = (Option<Offset>, Offset);
pub type StreamProviderRef<T = Offset> = Arc<dyn StreamProvider<Offset = T> + Send + Sync>;

/// The table that implements this trait can be used as the source of the stream processing
#[async_trait]
pub trait StreamProvider {
    type Offset;

    // 每个stream 有自己的id
    fn id(&self) -> String;

    /// Event time column of stream table
    /// 看来水位字段 就是时间字段了  水位会携带一个时间 是代表要限流的时间吗 ?
    fn watermark(&self) -> &Watermark;

    /// Returns the latest (highest) available offsets
    /// 获取此时最高的偏移量  代表此时能被访问到的最新数据
    async fn latest_available_offset(&self) -> Result<Option<Self::Offset>>;

    // scan的条件 实际上就是一个select语句被解析后得到的各种对象 将他们变成执行计划
    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        agg_with_grouping: Option<&AggWithGrouping>,
        range: Option<&(Option<Self::Offset>, Self::Offset)>,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Informs the source that stream has completed processing all data for offsets less than or
    /// equal to `end` and will only request offsets greater than `end` in the future.
    /// 代表该offset之前的数据都可以访问了
    async fn commit(&self, end: Self::Offset) -> Result<()>;

    /// 获取底层stream table 相关的schema
    fn schema(&self) -> SchemaRef;

    /// Tests whether the table provider can make use of a filter expression
    /// to optimise data retrieval.
    /// 判断是否支持过滤条件下推  也就是提前执行过滤条件 减少中间数据集
    /// 是否支持跟expr本身也有一定关系
    fn supports_filter_pushdown(&self, _filter: &Expr) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Unsupported)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        filters
            .iter()
            .map(|f| self.supports_filter_pushdown(f))
            .collect()
    }

    /// true if the aggregation can be pushed down to datasource, false otherwise.
    /// 是否支持聚合条件的下推   默认不支持
    fn supports_aggregate_pushdown(
        &self,
        _group_expr: &[Expr],
        _aggr_expr: &[Expr],
    ) -> Result<TableProviderAggregationPushDown> {
        Ok(TableProviderAggregationPushDown::Unsupported)
    }
}
