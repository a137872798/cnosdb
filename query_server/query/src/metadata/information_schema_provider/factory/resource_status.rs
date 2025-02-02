use std::any::Any;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::logical_plan::AggWithGrouping;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use meta::model::MetaClientRef;
use models::auth::user::User;

use crate::dispatcher::query_tracker::QueryTracker;
use crate::metadata::information_schema_provider::builder::resource_status::{
    self, InformationSchemaResourceStatusBuilder,
};
use crate::metadata::information_schema_provider::InformationSchemaTableFactory;

pub const INFORMATION_SCHEMA_RESOURCE_STATUS: &str = "RESOURCE_STATUS";

pub struct InformationSchemaResourceStatusFactory {}

impl InformationSchemaTableFactory for InformationSchemaResourceStatusFactory {
    fn table_name(&self) -> &'static str {
        INFORMATION_SCHEMA_RESOURCE_STATUS
    }

    fn create(
        &self,
        user: &User,
        metadata: MetaClientRef,
        _query_tracker: Arc<QueryTracker>,
    ) -> Arc<dyn TableProvider> {
        Arc::new(InformationSchemaResourceStatusTable::new(
            metadata,
            user.clone(),
        ))
    }
}

pub struct InformationSchemaResourceStatusTable {
    user: User,
    metadata: MetaClientRef,
}

impl InformationSchemaResourceStatusTable {
    /// Create a new in-memory table from the provided schema and record batches
    pub fn new(metadata: MetaClientRef, user: User) -> Self {
        Self { user, metadata }
    }
}

#[async_trait]
impl TableProvider for InformationSchemaResourceStatusTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        resource_status::RESOURCE_STATUS_SCHEMA.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _agg_with_grouping: Option<&AggWithGrouping>,
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let mut builder = InformationSchemaResourceStatusBuilder::default();

        if let Ok(resourceinfos) =
            self.metadata.read_resourceinfos(&[]).await.map_err(|e| {
                DataFusionError::Internal(format!("Failed to read resourceinfo: {}", e))
            })
        {
            //resourceinfos.retain(|resourceinfo| resourceinfo.names().get(0).unwrap() == tenant_name);
            for resourceinfo in resourceinfos {
                // Check if the current user has at least read permission on this db, skip if not
                let names = resourceinfo.get_names();
                let tenant_id = resourceinfo.get_tenant_id();
                if !self.user.can_access_role(tenant_id)
                    || (names.len() > 1
                        && !self
                            .user
                            .can_read_database(tenant_id, names.get(1).unwrap()))
                {
                    continue;
                }

                let duration = std::time::Duration::from_nanos(resourceinfo.get_time() as u64);
                let datetime = UNIX_EPOCH + duration;
                let time_str = chrono::DateTime::<chrono::Utc>::from(datetime)
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string();

                builder.append_row(
                    time_str,
                    resourceinfo.get_names().join("/"),
                    resourceinfo.get_operator().to_string(),
                    resourceinfo.get_try_count().to_string(),
                    resourceinfo.get_status().to_string(),
                    resourceinfo.get_comment(),
                );
            }
        }

        let rb: RecordBatch = builder.try_into()?;

        Ok(Arc::new(MemoryExec::try_new(
            &[vec![rb]],
            self.schema(),
            projection.cloned(),
        )?))
    }
}
