use std::sync::Arc;

use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::ExtensionPlanner;

use super::session::SessionCtx;
use crate::Result;

#[async_trait]
pub trait PhysicalPlanner {
    /// Given a `LogicalPlan`, create an `ExecutionPlan` suitable for execution
    /// 暴露一个将逻辑计划变成物理计划的api
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionCtx,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    // 通过该对象可以对执行计划进行拓展
    fn inject_physical_transform_rule(&mut self, rule: Arc<dyn ExtensionPlanner + Send + Sync>);
}
