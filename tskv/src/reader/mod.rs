use std::pin::Pin;

use datafusion::arrow::record_batch::RecordBatch;
use futures::Stream;
pub use iterator::*;
use models::schema::PhysicalCType;

use crate::memcache::DataType;
use crate::{Error, Result};

mod iterator;
pub mod query_executor;
pub mod serialize;
pub mod table_scan;
pub mod tag_scan;

pub type SendableTskvRecordBatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>;

#[async_trait::async_trait]
pub trait Cursor: Send + Sync {
    fn name(&self) -> &String;
    fn is_field(&self) -> bool {
        matches!(self.column_type(), PhysicalCType::Field(_))
    }
    // 描述此时读取的数据列类型  分为 Tag/Time/Field    cnosdb中的列分为3类  第一类是数据列 field 第二类是标签列 tab 第三列是时间序列 time
    fn column_type(&self) -> PhysicalCType;

    // 遍历每次得到一个列值
    async fn next(&mut self) -> Result<Option<DataType>, Error>;
}
