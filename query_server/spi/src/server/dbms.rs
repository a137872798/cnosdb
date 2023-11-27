use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Float32Array, Float64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use models::auth::role::UserRole;
use models::auth::user::{User, UserDesc, UserInfo, UserOptionsBuilder};
use trace::SpanContext;

use crate::query::execution::{Output, QueryStateMachine, QueryStateMachineRef};
use crate::query::logical_planner::Plan;
use crate::query::recordbatch::RecordBatchStreamWrapper;
use crate::service::protocol::{Query, QueryHandle, QueryId};
use crate::Result;

pub type DBMSRef = Arc<dyn DatabaseManagerSystem + Send + Sync>;

// 模拟一个数据库系统
#[async_trait]
pub trait DatabaseManagerSystem {
    // 启动数据库
    async fn start(&self) -> Result<()>;

    // 对某个用户进行认证 应该是判断该用户是否有访问某些库表的权限
    async fn authenticate(&self, user_info: &UserInfo, tenant_name: Option<&str>) -> Result<User>;

    // 执行某个查询  query中包含sql语句
    async fn execute(
        &self,
        query: &Query,
        span_context: Option<&SpanContext>,
    ) -> Result<QueryHandle>;

    // 基于Query生成状态机对象  状态机会随着查询的进行而切换状态
    async fn build_query_state_machine(
        &self,
        query: Query,
        span_context: Option<&SpanContext>,
    ) -> Result<QueryStateMachineRef>;

    // 构建逻辑计划  这个逻辑计划 还不是datafusion层的
    async fn build_logical_plan(
        &self,
        query_state_machine: QueryStateMachineRef,
    ) -> Result<Option<Plan>>;

    // 执行逻辑计划 生成的句柄是cnosdb自己封装的 而不是datafusion层
    async fn execute_logical_plan(
        &self,
        logical_plan: Plan,
        query_state_machine: QueryStateMachineRef,
    ) -> Result<QueryHandle>;

    // 获取一些数据库的指标
    fn metrics(&self) -> String;

    // 可以取消某个查询
    fn cancel(&self, query_id: &QueryId);
}

pub struct DatabaseManagerSystemMock {}


// 代表一个模拟的数据库系统
#[async_trait]
impl DatabaseManagerSystem for DatabaseManagerSystemMock {
    async fn start(&self) -> Result<()> {
        Ok(())
    }
    async fn authenticate(&self, user_info: &UserInfo, _tenant_name: Option<&str>) -> Result<User> {
        let options = unsafe {
            UserOptionsBuilder::default()
                .password(user_info.password.clone())
                .build()
                .unwrap_unchecked()
        };
        let mock_desc = UserDesc::new(0_u128, user_info.user.to_string(), options, true);
        let mock_user = User::new(mock_desc, UserRole::Dba.to_privileges());
        Ok(mock_user)
    }

    async fn execute(
        &self,
        query: &Query,
        _span_context: Option<&SpanContext>,
    ) -> Result<QueryHandle> {
        println!("DatabaseManagerSystemMock::execute({:?})", query.content());

        // define a schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("f32", DataType::Float32, false),
            Field::new("f64", DataType::Float64, false),
        ]));

        // define data.
        let batch_size = 2;
        let batches = (0..10 / batch_size)
            .map(|i| {
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Float32Array::from(vec![i as f32; batch_size])),
                        Arc::new(Float64Array::from(vec![i as f64; batch_size])),
                    ],
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let stream = Box::pin(RecordBatchStreamWrapper::new(schema, batches));
        Ok(QueryHandle::new(
            QueryId::next_id(),
            query.clone(),
            Output::StreamData(stream),
        ))
    }

    async fn build_query_state_machine(
        &self,
        query: Query,
        span_context: Option<&SpanContext>,
    ) -> Result<QueryStateMachineRef> {
        Ok(Arc::new(QueryStateMachine::test(
            query,
            span_context.cloned(),
        )))
    }

    async fn build_logical_plan(
        &self,
        _query_state_machine: QueryStateMachineRef,
    ) -> Result<Option<Plan>> {
        Ok(None)
    }

    async fn execute_logical_plan(
        &self,
        _logical_plan: Plan,
        query_state_machine: QueryStateMachineRef,
    ) -> Result<QueryHandle> {
        self.execute(
            &query_state_machine.query,
            query_state_machine
                .session
                .get_child_span_recorder("mock execute logical plan")
                .span_ctx(),
        )
        .await
    }

    fn metrics(&self) -> String {
        "todo!()".to_string()
    }

    fn cancel(&self, query_id: &QueryId) {
        println!("DatabaseManagerSystemMock::cancel({:?})", query_id);
    }
}
