use std::fmt::Display;
use std::pin::Pin;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use coordinator::service::CoordinatorRef;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{Stream, StreamExt, TryStreamExt};
use meta::model::MetaRef;
use trace::SpanContext;

use super::dispatcher::{QueryInfo, QueryStatus};
use super::logical_planner::Plan;
use super::session::SessionCtx;
use crate::service::protocol::{Query, QueryId};
use crate::{QueryError, Result};

pub type QueryExecutionRef = Arc<dyn QueryExecution>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryType {
    Batch,
    Stream,
}

impl Display for QueryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Batch => write!(f, "batch"),
            Self::Stream => write!(f, "stream"),
        }
    }
}

// 代表一个查询的执行
#[async_trait]
pub trait QueryExecution: Send + Sync {

    // 代表单次直接获取全部结果 还是以流的形式 慢慢得到结果
    fn query_type(&self) -> QueryType {
        QueryType::Batch
    }
    // 发起查询 并得到结果
    async fn start(&self) -> Result<Output>;
    // 取消查询
    fn cancel(&self) -> Result<()>;
    // 获取本次查询相关的信息
    fn info(&self) -> QueryInfo;
    fn status(&self) -> QueryStatus;
    // 是否需要持久化query信息
    fn need_persist(&self) -> bool {
        false
    }
}

// 代表查询结果
pub enum Output {
    StreamData(SendableRecordBatchStream),
    Nil(()),
}

impl Output {

    // 获取本次结果相关的schema
    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::StreamData(stream) => stream.schema(),
            Self::Nil(_) => Arc::new(Schema::empty()),
        }
    }

    // 将结果拆分成多块
    pub async fn chunk_result(self) -> Result<Vec<RecordBatch>> {
        match self {
            Self::Nil(_) => Ok(vec![]),
            // 这里应该会一次将stream的所有数据都拉出来吧
            Self::StreamData(stream) => {
                let res: Vec<RecordBatch> = stream.try_collect::<Vec<RecordBatch>>().await?;
                Ok(res)
            }
        }
    }

    pub async fn num_rows(self) -> usize {
        match self.chunk_result().await {
            Ok(rb) => rb.iter().map(|e| e.num_rows()).sum(),
            Err(_) => 0,
        }
    }

    /// Returns the number of records affected by the query operation
    ///
    /// If it is a select statement, returns the number of rows in the result set
    ///
    /// -1 means unknown
    ///
    /// panic! when StreamData's number of records greater than i64::Max
    pub async fn affected_rows(self) -> i64 {
        self.num_rows().await as i64
    }
}

impl Stream for Output {
    type Item = std::result::Result<RecordBatch, QueryError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this {
            // 每次poll 得到部分数据
            Output::StreamData(stream) => stream.poll_next_unpin(cx).map_err(|e| e.into()),
            Output::Nil(_) => Poll::Ready(None),
        }
    }
}

// pub struct FlightDataEncoderWrapper {
//     inner: FlightDataEncoder,
//     done: bool,
// }

// impl FlightDataEncoderWrapper {
//     fn new(inner: FlightDataEncoder) -> Self {
//         Self { inner, done: false }
//     }
// }

// pub struct FlightDataEncoderBuilderWrapper {
//     inner: FlightDataEncoderBuilder,
// }

// impl FlightDataEncoderBuilderWrapper {
//     pub fn new(schema: SchemaRef) -> Self {
//         Self {
//             inner: FlightDataEncoderBuilder::new().with_schema(Arc::clone(&schema)),
//         }
//     }

//     pub fn build<S>(self, input: S) -> FlightDataEncoderWrapper
//         where
//             S: Stream<Item=datafusion::common::Result<RecordBatch>> + Send + 'static,
//     {
//         FlightDataEncoderWrapper::new(
//             self.inner
//                 .build(input.map_err(|e| FlightError::ExternalError(e.into()))),
//         )
//     }
// }

// impl Stream for FlightDataEncoderWrapper {
//     type Item = arrow_flight::error::Result<FlightData>;

//     fn poll_next(
//         mut self: Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//     ) -> Poll<Option<Self::Item>> {
//         if self.done {
//             return Poll::Ready(None);
//         }

//         let res = ready!(self.inner.poll_next_unpin(cx));
//         match res {
//             None => {
//                 self.done = true;
//                 Poll::Ready(None)
//             }
//             Some(Ok(data)) => Poll::Ready(Some(Ok(data))),
//             Some(Err(e)) => {
//                 self.done = true;
//                 Poll::Ready(Some(Err(e)))
//             }
//         }
//     }
// }

// 通过工厂对象生成 execution对象 该对象可以执行查询 并得到结果
pub trait QueryExecutionFactory {
    fn create_query_execution(
        &self,
        plan: Plan,  // 这个计划是cnosdb层的
        query_state_machine: QueryStateMachineRef,
    ) -> Result<QueryExecutionRef>;
}

pub type QueryStateMachineRef = Arc<QueryStateMachine>;

// 这个就是查询状态机   记录查询执行的状态
pub struct QueryStateMachine {
    // 包含datafusion信息
    pub session: SessionCtx,
    // 包含查询信息
    pub query_id: QueryId,
    pub query: Query,
    // 各种元数据信息
    pub meta: MetaRef,
    // 协调中心
    pub coord: CoordinatorRef,

    // 查询当前的状态
    state: AtomicPtr<QueryState>,
    // 查询开始时间
    start: Instant,
}


// 查询状态机
impl QueryStateMachine {
    /// only for test
    pub fn test(query: Query, span_context: Option<SpanContext>) -> Self {
        use coordinator::service_mock::MockCoordinator;
        use datafusion::execution::memory_pool::UnboundedMemoryPool;

        use super::session::SessionCtxFactory;

        let factory = SessionCtxFactory::new("/tmp".into());
        let ctx = query.context().clone();
        QueryStateMachine::begin(
            QueryId::next_id(),
            query,
            factory
                .create_session_ctx(
                    "session_id",
                    ctx,
                    0,
                    Arc::new(UnboundedMemoryPool::default()),
                    span_context,
                )
                .expect("create test session ctx"),
            Arc::new(MockCoordinator {}),
        )
    }

    pub fn begin(
        query_id: QueryId,
        query: Query,
        session: SessionCtx,
        coord: CoordinatorRef,
    ) -> Self {
        // 通过协调对象获取此时最新的元数据
        let meta = coord.meta_manager();

        // 将相关信息包装起来 变成状态机对象
        Self {
            query_id,
            session,
            query,
            meta,
            coord,
            state: AtomicPtr::new(Box::into_raw(Box::new(QueryState::ACCEPTING))),
            start: Instant::now(),
        }
    }

    // 代表查询进入分析阶段
    pub fn begin_analyze(&self) {
        // TODO record time
        self.translate_to(Box::new(QueryState::RUNNING(RUNNING::ANALYZING)));
    }

    pub fn end_analyze(&self) {
        // TODO record time
    }

    // 对语句进行优化
    pub fn begin_optimize(&self) {
        // TODO record time
        self.translate_to(Box::new(QueryState::RUNNING(RUNNING::OPTMIZING)));
    }

    pub fn end_optimize(&self) {
        // TODO
    }

    // 应该是要把查询分派到不同节点进行
    pub fn begin_schedule(&self) {
        // TODO
        self.translate_to(Box::new(QueryState::RUNNING(RUNNING::SCHEDULING)));
    }

    pub fn end_schedule(&self) {
        // TODO
    }

    // 代表查询结束了
    pub fn finish(&self) {
        // TODO
        self.translate_to(Box::new(QueryState::DONE(DONE::FINISHED)));
    }

    // 查询被取消
    pub fn cancel(&self) {
        // TODO
        self.translate_to(Box::new(QueryState::DONE(DONE::CANCELLED)));
    }

    // 本次查询失败
    pub fn fail(&self) {
        // TODO
        self.translate_to(Box::new(QueryState::DONE(DONE::FAILED)));
    }

    pub fn state(&self) -> &QueryState {
        unsafe { &*self.state.load(Ordering::Relaxed) }
    }

    pub fn duration(&self) -> Duration {
        self.start.elapsed()
    }

    fn translate_to(&self, state: Box<QueryState>) {
        self.state.store(Box::into_raw(state), Ordering::Relaxed);
    }

    // 关联一个链路追踪的上下文
    pub fn with_span_ctx(&self, span_ctx: Option<SpanContext>) -> Self {
        let state = AtomicPtr::new(Box::into_raw(Box::new(self.state().clone())));
        Self {
            session: self.session.with_span_ctx(span_ctx),
            query_id: self.query_id,
            query: self.query.clone(),
            meta: self.meta.clone(),
            coord: self.coord.clone(),
            state,
            start: self.start,
        }
    }
}

// 表示查询的状态
#[derive(Debug, Clone)]
pub enum QueryState {
    // 刚接收到查询请求
    ACCEPTING,
    // 表示处在查询的几个阶段
    RUNNING(RUNNING),
    // 已经结束了 无论是正常结束 或者非正常结束
    DONE(DONE),
}

impl AsRef<str> for QueryState {
    fn as_ref(&self) -> &str {
        match self {
            QueryState::ACCEPTING => "ACCEPTING",
            QueryState::RUNNING(e) => e.as_ref(),
            QueryState::DONE(e) => e.as_ref(),
        }
    }
}

// 查询的几个阶段
#[derive(Debug, Clone)]
pub enum RUNNING {
    DISPATCHING,
    ANALYZING,
    OPTMIZING,
    SCHEDULING,
}

impl AsRef<str> for RUNNING {
    fn as_ref(&self) -> &str {
        match self {
            Self::DISPATCHING => "DISPATCHING",
            Self::ANALYZING => "ANALYZING",
            Self::OPTMIZING => "OPTMIZING",
            Self::SCHEDULING => "SCHEDULING",
        }
    }
}

#[derive(Debug, Clone)]
pub enum DONE {
    FINISHED,
    FAILED,
    CANCELLED,
}

impl AsRef<str> for DONE {
    fn as_ref(&self) -> &str {
        match self {
            Self::FINISHED => "FINISHED",
            Self::FAILED => "FAILED",
            Self::CANCELLED => "CANCELLED",
        }
    }
}
