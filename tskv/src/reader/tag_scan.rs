use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use futures::future::BoxFuture;
use futures::{ready, Stream, StreamExt, TryFutureExt};
use models::arrow::stream::MemoryRecordBatchStream;
use models::arrow_array::build_arrow_array_builders;
use models::meta_data::VnodeId;
use models::SeriesKey;
use trace::SpanRecorder;

use crate::error::Result;
use crate::reader::{QueryOption, SendableTskvRecordBatchStream};
use crate::EngineRef;

// 这个对象就是用来遍历某个table下的所有key   (key包含多tab  每个tab就是一个col)
pub struct LocalTskvTagScanStream {
    // 描述是同步流 还是异步流
    state: StreamState,
    // 忽略链路追踪的
    #[allow(unused)]
    span_recorder: SpanRecorder,
}

// 本地标签扫描流
impl LocalTskvTagScanStream {
    pub fn new(
        vnode_id: VnodeId,
        option: QueryOption,  // 表示本次查询针对的租户/库/表
        kv: EngineRef,   // 功能性接口统一由engine提供
        span_recorder: SpanRecorder,  // trace相关的
    ) -> Self {
        let futrue = async move {
            let (tenant, db, table) = (
                option.table_schema.tenant.as_str(),
                option.table_schema.db.as_str(),
                option.table_schema.name.as_str(),
            );

            let mut keys = Vec::new();

            // 通过该api 获取table下所有seriesId
            for series_id in kv
                .get_series_id_by_filter(tenant, db, table, vnode_id, option.split.tags_filter())
                .await?
                .into_iter()
            {
                // 查询该系列的key  (包含table/tag)
                if let Some(key) = kv.get_series_key(tenant, db, vnode_id, series_id).await? {
                    keys.push(key)
                }
            }

            let mut batches = vec![];
            // 代表每次遍历 同时返回多个 seriesKey
            for chunk in keys.chunks(option.batch_size) {
                // df_schema 描述表结构
                let record_batch = series_keys_to_record_batch(option.df_schema.clone(), chunk)?;
                batches.push(record_batch)
            }

            Ok(Box::pin(MemoryRecordBatchStream::new(batches)) as SendableTskvRecordBatchStream)
        };

        let state = StreamState::Open(Box::pin(futrue));

        Self {
            state,
            span_recorder,
        }
    }

    // 拉取结果集
    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            match &mut self.state {
                StreamState::Open(future) => match ready!(future.try_poll_unpin(cx)) {
                    Ok(stream) => {
                        self.state = StreamState::Scan(stream);
                    }
                    Err(err) => return Poll::Ready(Some(Err(err))),
                },
                StreamState::Scan(stream) => return stream.poll_next_unpin(cx),
            }
        }
    }
}

impl Stream for LocalTskvTagScanStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_inner(cx)
    }
}

pub type StreamFuture = BoxFuture<'static, Result<SendableTskvRecordBatchStream>>;

enum StreamState {
    // 底层就是 Stream<Item = Result<RecordBatch>>
    Open(StreamFuture),
    // 与上面的区别 就是非异步的
    Scan(SendableTskvRecordBatchStream),
}

// 返回的就是系列名
fn series_keys_to_record_batch(
    schema: SchemaRef,
    series_keys: &[SeriesKey],
) -> Result<RecordBatch, ArrowError> {

    // 每个列名 变成了标签名  也就是cnosdb下每个列 实际上是一个tab  然后根据不同的tab 可以划分不同的series
    let tag_key_array = schema.fields.iter().map(|f| f.name()).collect::<Vec<_>>();

    // 构建 arrow容器 用于存储结果
    let mut array_builders = build_arrow_array_builders(&schema, series_keys.len())?;

    // 遍历系列
    for key in series_keys {
        // 遍历某个系列的key的所有列  并存入arrow
        for (k, array_builder) in tag_key_array.iter().zip(&mut array_builders) {

            // 从key中 找到对应的标签
            let tag_value = key
                .tag_string_val(k)
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

            // 将标签 或者说列名 存入arrow容器
            let builder = array_builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .expect("Cast failed for List Builder<StringBuilder> during nested data parsing");
            builder.append_option(tag_value)
        }
    }
    let columns = array_builders
        .into_iter()
        .map(|mut b| b.finish())
        .collect::<Vec<_>>();
    let record_batch = RecordBatch::try_new(schema.clone(), columns)?;
    Ok(record_batch)
}
