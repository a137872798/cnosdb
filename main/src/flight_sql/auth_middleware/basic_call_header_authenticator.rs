use http_protocol::header::{self, PRIVATE_KEY};
use spi::server::dbms::DBMSRef;
use tonic::metadata::MetadataMap;
use tonic::Status;
use trace::debug;

use super::{CallHeaderAuthenticator, CommonAuthResult};
use crate::flight_sql::utils;
use crate::http::header::Header;

#[derive(Clone)]
pub struct BasicCallHeaderAuthenticator {
    // 这个对应的是下层的数据库对象
    instance: DBMSRef,
}

impl BasicCallHeaderAuthenticator {
    pub fn new(instance: DBMSRef) -> Self {
        Self { instance }
    }
}

#[async_trait::async_trait]
impl CallHeaderAuthenticator for BasicCallHeaderAuthenticator {
    type AuthResult = CommonAuthResult;

    // 认证逻辑 就是从请求头获取部分信息 并通过下层的数据库 完成认证
    async fn authenticate(&self, req_headers: &MetadataMap) -> Result<Self::AuthResult, Status> {
        debug!("authenticate, request headers: {:?}", req_headers);

        // 获取认证相关头部
        let authorization = utils::get_value_from_auth_header(req_headers, "")
            .ok_or_else(|| Status::unauthenticated("authorization field not present"))?;
        // 获取 X-CnosDB-PrivateKey 头 这是一个自定义请求头
        let private_key = utils::get_value_from_header(req_headers, PRIVATE_KEY, "");

        // 从认证头中拆解出用户名/密码
        let user_info = Header::with_private_key(None, authorization, private_key)
            .try_get_basic_auth()
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        // 获取租户信息
        let tenant = utils::get_value_from_header(req_headers, header::TENANT, "");

        // 通过与db交互 拿到用户信息  因为请求头只能拆解出用户名密码  但是这些不一定正确 还是要通过下层数据库系统进行查询
        let user = self
            .instance
            .authenticate(&user_info, tenant.as_deref())
            .await
            .map_err(|e| Status::unauthenticated(e.to_string()))?;

        debug!("authenticate success, user: {}", user_info.user);

        Ok(CommonAuthResult { user })
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use http_protocol::header::AUTHORIZATION;
    use spi::server::dbms::DatabaseManagerSystemMock;
    use tonic::metadata::{AsciiMetadataValue, MetadataMap};

    use super::BasicCallHeaderAuthenticator;
    use crate::flight_sql::auth_middleware::{AuthResult, CallHeaderAuthenticator};

    #[tokio::test]
    async fn test() {
        let instance = Arc::new(DatabaseManagerSystemMock {});
        let authenticator = BasicCallHeaderAuthenticator::new(instance);

        let mut req_headers = MetadataMap::default();

        assert!(authenticator.authenticate(&req_headers).await.is_err());

        let val = AsciiMetadataValue::from_static("Basic eHg6eHgK");

        req_headers.insert(AUTHORIZATION.as_str(), val);

        let auth_result = authenticator
            .authenticate(&req_headers)
            .await
            .expect("authenticate");

        auth_result
            .append_to_outgoing_headers(&mut req_headers)
            .expect("append_to_outgoing_headers");

        assert_eq!(req_headers.len(), 1)
    }
}
