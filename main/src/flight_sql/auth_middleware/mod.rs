pub mod basic_call_header_authenticator;
pub mod generated_bearer_token_authenticator;

use async_trait::async_trait;
use models::auth::user::User;
use tonic::metadata::MetadataMap;
use tonic::Status;

/// Interface for Server side authentication handlers.
/// 认证模块
#[async_trait]
pub trait CallHeaderAuthenticator {
    type AuthResult: AuthResult + Send + Sync;
    /// Implementations of CallHeaderAuthenticator should
    /// take care not to provide leak confidential details
    /// for security reasons when reporting errors back to clients.
    /// 通过grpc携带的请求头 就可以完成认证了
    async fn authenticate(&self, req_headers: &MetadataMap) -> Result<Self::AuthResult, Status>;
}

pub trait AuthResult {
    // 获取认证到的用户
    fn identity(&self) -> User;
    // 结果可以设置到响应头上
    fn append_to_outgoing_headers(&self, resp_headers: &mut MetadataMap) -> Result<(), Status>;
}

// 把认证结果公共的部分抽取出来 就是User
pub struct CommonAuthResult {
    user: User,
}

impl CommonAuthResult {
    pub fn new(user: User) -> Self {
        Self { user }
    }
}

impl AuthResult for CommonAuthResult {
    fn identity(&self) -> User {
        self.user.clone()
    }

    fn append_to_outgoing_headers(&self, _resp_headers: &mut MetadataMap) -> Result<(), Status> {
        Ok(())
    }
}
