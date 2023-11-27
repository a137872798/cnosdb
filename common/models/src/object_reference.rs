use std::fmt::Display;

use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::sql::TableReference;

// 根据catalog和schema 能够定位到某张表
pub trait Resolve {
    fn resolve_object(self, default_catalog: &str, default_schema: &str)
        -> DFResult<ResolvedTable>;
}

impl Resolve for TableReference<'_> {
    fn resolve_object(
        self,
        default_catalog: &str,
        default_schema: &str,
    ) -> DFResult<ResolvedTable> {
        let result = match self {
            // 代表本对象同时声明了 catalog schema table
            Self::Full { .. } => {
                // check table reference name    但是这种情况在cnosdb中是不合理的
                return Err(DataFusionError::Plan(format!(
                    "Database object names must have at most two parts, but found: '{}'",
                    self
                )));
            }

            // catalog 映射到租户 也就是各租户在上层是数据隔离的
            // schema映射成database
            Self::Partial { schema, table } => ResolvedTable {
                tenant: default_catalog.into(),
                database: schema.into(),
                table: table.into(),
            },
            // 为指定情况下 使用默认数据库
            Self::Bare { table } => ResolvedTable {
                tenant: default_catalog.into(),
                database: default_schema.into(),
                table: table.into(),
            },
        };

        Ok(result)
    }
}

// 代表被定位到的某张表
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedTable {
    // 表所属的租户
    tenant: String,
    // 定位到的数据库
    database: String,
    // 定位到的表
    table: String,
}

impl ResolvedTable {
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    pub fn table(&self) -> &str {
        &self.table
    }
}

impl Display for ResolvedTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            tenant,
            database,
            table,
        } = self;
        write!(f, "{tenant}.{database}.{table}")
    }
}
