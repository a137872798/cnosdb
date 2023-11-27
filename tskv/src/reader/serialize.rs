use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{ready, Stream, StreamExt};
use models::record_batch_encode;
use protos::kv_service::BatchBytesResponse;
use trace::SpanRecorder;

use crate::error::{Error as TskvError, Result};
use crate::reader::SendableTskvRecordBatchStream;

pub struct TonicRecordBatchEncoder {
    // 代表record的数据流   tableScan和tabScan 分别是针对table和tab的
    input: SendableTskvRecordBatchStream,
    #[allow(unused)]
    span_recorder: SpanRecorder,
}

impl TonicRecordBatchEncoder {
    pub fn new(input: SendableTskvRecordBatchStream, span_recorder: SpanRecorder) -> Self {
        Self {
            input,
            span_recorder,
        }
    }
}

impl Stream for TonicRecordBatchEncoder {
    type Item = Result<BatchBytesResponse>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(self.input.poll_next_unpin(cx)) {
            // 结果是通过 arrow-ipc 进行序列化的  所以这里进行编码 还原数据结果
            Some(Ok(batch)) => match record_batch_encode(&batch) {
                Ok(body) => {
                    let resp = BatchBytesResponse {
                        data: body,
                        ..Default::default()
                    };
                    Poll::Ready(Some(Ok(resp)))
                }
                Err(err) => {
                    let err = TskvError::Arrow { source: err };
                    Poll::Ready(Some(Err(err)))
                }
            },
            Some(Err(err)) => Poll::Ready(Some(Err(err))),
            None => Poll::Ready(None),
        }
    }
}
