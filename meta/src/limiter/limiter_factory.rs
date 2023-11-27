use std::fmt::Debug;
use std::sync::Arc;

use models::schema::Tenant;

use crate::client::MetaHttpClient;
use crate::error::{MetaError, MetaResult};
use crate::limiter::limiter_manager::LimiterKey;
use crate::limiter::{LimiterConfig, LocalRequestLimiter, RequestLimiter};
use crate::store::command::ReadCommand;

#[async_trait::async_trait]
pub trait LimiterFactory: Debug + Send + Sync {
    async fn create_default(&self, _key: LimiterKey) -> MetaResult<Arc<dyn RequestLimiter>> {
        Err(MetaError::LimiterCreate {
            msg: "not implements".to_string(),
        })
    }
    async fn create_limiter(&self, _config: LimiterConfig) -> MetaResult<Arc<dyn RequestLimiter>> {
        Err(MetaError::LimiterCreate {
            msg: "not implements".to_string(),
        })
    }
}

#[derive(Debug)]
pub struct LocalRequestLimiterFactory {
    cluster_name: String,
    meta_http_client: MetaHttpClient,  // 因为要访问元数据服务器  所以需要client  在client内部维护了所有元数据服务器地址 会自动做路由
}
impl LocalRequestLimiterFactory {
    pub fn new(cluster_name: String, meta_http_client: MetaHttpClient) -> Self {
        Self {
            cluster_name,
            meta_http_client,
        }
    }
}

// 本地限流器工厂
#[async_trait::async_trait]
impl LimiterFactory for LocalRequestLimiterFactory {
    async fn create_default(&self, key: LimiterKey) -> MetaResult<Arc<dyn RequestLimiter>> {
        let LimiterKey(_, tenant_name) = key;
        let command = ReadCommand::Tenant(self.cluster_name.clone(), tenant_name.clone());
        let tenant = self
            .meta_http_client
            .read::<Option<Tenant>>(&command)
            .await?
            .ok_or_else(|| MetaError::TenantNotFound {
                tenant: tenant_name.clone(),
            })?;

        // 这是目前的限流配置   也就是基于元数据服务器上该租户相关的限流信息生成本地限流器
        let limiter_config = LimiterConfig::TenantRequestLimiterConfig {
            tenant: tenant_name,
            config: Box::new(tenant.options().request_config().cloned()),
        };

        // 生成限流器
        let limiter = self.create_limiter(limiter_config).await?;
        Ok(limiter)
    }

    async fn create_limiter(&self, config: LimiterConfig) -> MetaResult<Arc<dyn RequestLimiter>> {
        match config {
            LimiterConfig::TenantRequestLimiterConfig { tenant, config } => {
                Ok(Arc::new(LocalRequestLimiter::new(
                    self.cluster_name.as_str(),
                    &tenant,
                    config.as_ref().as_ref(),
                    self.meta_http_client.clone(),
                )))
            }
            _ => Err(MetaError::LimiterCreate {
                msg: "limiter config invalid".to_string(),
            }),
        }
    }
}
