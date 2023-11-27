use std::collections::HashMap;
use std::sync::Arc;

use meta::model::MetaClientRef;
use models::schema::StreamTable;

use crate::QueryError;

pub type StreamCheckerManagerRef = Arc<StreamCheckerManager>;

/// Maintain and manage all registered streaming data sources
/// 一个类型对应一个checker对象
#[derive(Default)]
pub struct StreamCheckerManager {
    checkers: HashMap<String, StreamTableCheckerRef>,
}

impl StreamCheckerManager {

    // 注册一个需要被检查的stream table
    pub fn register_stream_checker(
        &mut self,
        stream_type: impl Into<String>,   // stream的类型
        checker: StreamTableCheckerRef,   // 添加一个检查对象
    ) -> Result<(), QueryError> {
        let stream_type = stream_type.into();

        if self.checkers.contains_key(&stream_type) {
            return Err(QueryError::StreamTableCheckerAlreadyExists { stream_type });
        }

        let _ = self.checkers.insert(stream_type, checker);

        Ok(())
    }

    pub fn checker(&self, stream_type: &str) -> Option<StreamTableCheckerRef> {
        self.checkers.get(stream_type).cloned()
        // .ok_or_else(|| QueryError::UnsupportedStreamType {
        //     stream_type: stream_type.to_string(),
        // })
    }
}

pub type StreamTableCheckerRef = Arc<dyn SchemaChecker<StreamTable> + Send + Sync>;

// 用于检验元数据是否有效
pub trait SchemaChecker<T> {
    /// Check whether [`T`] meets the conditions
    fn check(&self, client: &MetaClientRef, element: &T) -> Result<(), QueryError>;
}
