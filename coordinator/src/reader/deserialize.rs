use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::arrow::record_batch::RecordBatch;
use futures::{ready, Stream, StreamExt};
use models::record_batch_decode;
use protos::kv_service::BatchBytesResponse;
use tonic::Streaming;

use crate::errors::{CoordinatorError, CoordinatorResult};

pub struct TonicRecordBatchDecoder {
    stream: Streaming<BatchBytesResponse>,
}

impl TonicRecordBatchDecoder {
    pub fn new(stream: Streaming<BatchBytesResponse>) -> Self {
        Self { stream }
    }
}

impl Stream for TonicRecordBatchDecoder {
    type Item = CoordinatorResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(self.stream.poll_next_unpin(cx)) {
            // 通过grpc拉取到的数据流 可以通过arrow-ipc进行反序列化 得到 RecordBatch
            Some(Ok(received)) => match record_batch_decode(&received.data) {
                Ok(batch) => Poll::Ready(Some(Ok(batch))),
                // 这个是arrow的解析失败
                Err(err) => Poll::Ready(Some(Err(err.into()))),
            },
            Some(Err(err)) => Poll::Ready(Some(Err(CoordinatorError::TskvError {
                source: err.into(),
            }))),
            None => Poll::Ready(None),
        }
    }
}
