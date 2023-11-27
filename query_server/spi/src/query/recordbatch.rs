use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::RecordBatchStream;
use futures::Stream;

// 将一组 RecordBatch 包装成stream
pub struct RecordBatchStreamWrapper {
    inner: Vec<RecordBatch>,
    schema: SchemaRef,  // 数据相关的schema
    index: usize,
}

impl RecordBatchStreamWrapper {
    pub fn new(schema: SchemaRef, inner: Vec<RecordBatch>) -> Self {
        Self {
            inner,
            schema,
            index: 0,
        }
    }
}

impl RecordBatchStream for RecordBatchStreamWrapper {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for RecordBatchStreamWrapper {
    type Item = datafusion::common::Result<RecordBatch>;

    // 每次poll 推进下标得到新数据
    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(if self.index < self.inner.len() {
            let batch = self.inner[self.index].clone();
            self.index += 1;
            Some(Ok(batch))
        } else {
            None
        })
    }
}
