use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};

pub type SchedulerRef = Arc<dyn Scheduler + Send + Sync>;

// 调度器对象
#[async_trait]
pub trait Scheduler {
    /// Schedule the provided [`ExecutionPlan`] on this [`Scheduler`].
    ///
    /// Returns a [`ExecutionResults`] that can be used to receive results as they are produced,
    /// as a [`futures::Stream`] of [`RecordBatch`]
    async fn schedule(
        &self,
        plan: Arc<dyn ExecutionPlan>,  // 已经产生执行计划了  也就是过了优化阶段
        context: Arc<TaskContext>,  // 维护一次查询相关的信息
    ) -> Result<ExecutionResults>;
}

pub struct ExecutionResults {
    stream: SendableRecordBatchStream,
}

impl ExecutionResults {
    pub fn new(stream: SendableRecordBatchStream) -> Self {
        Self { stream }
    }

    /// Returns a [`SendableRecordBatchStream`] of this execution
    pub fn stream(self) -> SendableRecordBatchStream {
        self.stream
    }
}
