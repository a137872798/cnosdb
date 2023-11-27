use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;

use super::session::SessionCtx;
use crate::Result;

pub type AnalyzerRef = Arc<dyn Analyzer + Send + Sync>;

// 分析器
pub trait Analyzer {
    // 对datafusion的逻辑计划进行分析   还需要一个上下文(其中包含datafusion的上下文)
    // 分析后得到的还是一个 逻辑计划
    fn analyze(&self, plan: &LogicalPlan, session: &SessionCtx) -> Result<LogicalPlan>;
}
