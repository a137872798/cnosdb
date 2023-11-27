use std::sync::Arc;

use async_trait::async_trait;
use models::auth::user::{User, UserInfo};
use models::auth::AuthError;
use models::oid::Oid;

pub type Result<T> = std::result::Result<T, AuthError>;

pub type AccessControlRef = Arc<dyn AccessControl + Send + Sync>;

// 一个访问控制对象
#[async_trait]
pub trait AccessControl {

    // 检查该用户是否有访问权限
    async fn access_check(&self, user_info: &UserInfo, tenant_name: Option<&str>) -> Result<User>;

    // 通过name反查id
    async fn tenant_id(&self, tenant_name: &str) -> Result<Oid>;
}
