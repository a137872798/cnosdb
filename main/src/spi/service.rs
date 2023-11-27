use crate::server;

pub type ServiceRef = Box<dyn Service + Send + Sync>;

/// 作为cnosdb服务 提供最基础的接口
#[async_trait::async_trait]
pub trait Service {
    fn start(&mut self) -> server::Result<()>;
    async fn stop(&mut self, force: bool);
}
