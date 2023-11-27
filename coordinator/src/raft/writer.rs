use std::sync::Arc;
use std::time::Duration;

use meta::model::MetaRef;
use models::meta_data::*;
use protos::kv_service::tskv_service_client::TskvServiceClient;
use protos::kv_service::RaftWriteCommand;
use protos::models_helper::to_prost_bytes;
use replication::raft_node::RaftNode;
use tonic::transport;
use tower::timeout::Timeout;
use trace::{debug, info, SpanContext, SpanRecorder};
use tskv::EngineRef;

use super::manager::RaftNodesManager;
use crate::errors::*;

pub struct RaftWriter {
    // 数据节点会维护该对象 用于与元数据服务交互
    meta: MetaRef,
    // 数据节点的各种配置
    config: config::Config,
    // tskv存储
    kv_inst: Option<EngineRef>,
    raft_manager: Arc<RaftNodesManager>,  // 该对象管理本地所有的副本节点  某些操作当本节点为副本的leader节点时 才有权限发起
}

impl RaftWriter {
    pub fn new(
        meta: MetaRef,
        config: config::Config,
        kv_inst: Option<EngineRef>,
        raft_manager: Arc<RaftNodesManager>,
    ) -> Self {
        Self {
            meta,
            config,
            kv_inst,
            raft_manager,
        }
    }

    // 基于raft协议 接收某个写入请求
    pub async fn write_to_replica(
        &self,
        replica: &ReplicationSet,
        request: RaftWriteCommand,
        span_recorder: SpanRecorder,
    ) -> CoordinatorResult<()> {
        let node_id = self.config.node_basic.node_id;
        let leader_id = replica.leader_node_id;

        // 作为leader节点接收写入请求
        if leader_id == node_id && self.kv_inst.is_some() {
            let span_recorder = span_recorder.child("write to local node or forward");
            let result = self
                .write_to_local_or_forward(replica, request, span_recorder.span_ctx())
                .await;

            debug!("write to local {} {:?} {:?}", node_id, replica, result);

            result
        } else {
            let span_recorder = span_recorder.child("write to remote node");
            // 非leader节点  转发到leader节点
            let result = self
                .write_to_remote(leader_id, request.clone(), span_recorder.span_ctx())
                .await;
            debug!("write to remote {} {:?} {:?}", leader_id, replica, result);

            if let Err(CoordinatorError::FailoverNode { .. }) = result {
                for vnode in replica.vnodes.iter() {
                    if vnode.node_id == leader_id {
                        continue;
                    }

                    let result = self
                        .write_to_remote(vnode.node_id, request.clone(), span_recorder.span_ctx())
                        .await;
                    debug!(
                        "try write to remote {} {:?} {:?}",
                        vnode.node_id, replica, result
                    );

                    if result.is_ok() {
                        return Ok(());
                    }

                    if let Err(CoordinatorError::FailoverNode { .. }) = result {
                        continue;
                    } else {
                        return result;
                    }
                }
            }

            result
        }
    }

    // 通过raft协议写数据
    // write_to_remote 也会被转发到该方法
    pub async fn write_to_local_or_forward(
        &self,
        replica: &ReplicationSet,
        request: RaftWriteCommand,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<()> {

        let raft = self
            .raft_manager
            .get_node_or_build(&request.tenant, &request.db_name, replica)
            .await?;

        // 这个库是序列化/反序列化 protocol文件的
        let raft_data = to_prost_bytes(request.clone());

        let result = self.write_to_raft(raft, raft_data).await;
        if let Err(CoordinatorError::ForwardToLeader {
            replica_id: _,
            leader_vnode_id,
        }) = result
        {
            self.process_leader_change(leader_vnode_id, request, span_ctx)
                .await
        } else {
            result
        }
    }

    // 发现leader变更时
    async fn process_leader_change(
        &self,
        leader_vnode_id: VnodeId,
        request: RaftWriteCommand,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<()> {
        let vnode_id = leader_vnode_id;
        let all_info =
            crate::get_vnode_all_info(self.meta.clone(), &request.tenant, vnode_id).await?;

        let rsp = self
            .meta
            .tenant_meta(&request.tenant)
            .await
            .ok_or(CoordinatorError::TenantNotFound {
                name: request.tenant.clone(),
            })?
            // 发送请求 更新leader
            .change_repl_set_leader(
                &all_info.db_name,
                all_info.bucket_id,
                request.replica_id,
                all_info.node_id,
                vnode_id,
            )
            .await;

        info!(
            "change replica set({}) leader to vnode({}); {:?}",
            request.replica_id, vnode_id, rsp
        );

        // 将请求转发给leader
        self.write_to_remote(all_info.node_id, request, span_ctx)
            .await
    }

    // 一般就是转发给leader节点
    async fn write_to_remote(
        &self,
        leader_id: u64,
        request: RaftWriteCommand,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<()> {
        let channel = self.meta.get_node_conn(leader_id).await.map_err(|error| {
            CoordinatorError::FailoverNode {
                id: leader_id,
                error: error.to_string(),
            }
        })?;
        let timeout = self.config.query.write_timeout_ms;
        let timeout_channel = Timeout::new(channel, Duration::from_millis(timeout));
        let mut client = TskvServiceClient::<Timeout<transport::Channel>>::new(timeout_channel);

        let mut cmd = tonic::Request::new(request);
        trace_http::ctx::append_trace_context(span_ctx, cmd.metadata_mut()).map_err(|_| {
            CoordinatorError::CommonError {
                msg: "Parse trace_id, this maybe a bug".to_string(),
            }
        })?;

        let begin_time = models::utils::now_timestamp_millis();
        let response = client
            .exec_raft_write_command(cmd)
            .await
            .map_err(|err| match err.code() {
                tonic::Code::Internal => CoordinatorError::TskvError { source: err.into() },
                _ => CoordinatorError::FailoverNode {
                    id: leader_id,
                    error: format!("{err:?}"),
                },
            })?
            .into_inner();

        let use_time = models::utils::now_timestamp_millis() - begin_time;
        if use_time > 200 {
            debug!(
                "write points to node:{}, use time too long {}",
                leader_id, use_time
            )
        }

        crate::status_response_to_result(&response)
    }

    // 将请求写入raft状态机 成功时 会回调写入各节点  首先写入leader节点
    async fn write_to_raft(&self, raft: Arc<RaftNode>, data: Vec<u8>) -> CoordinatorResult<()> {
        // 作为leader节点才能调用该api
        if let Err(err) = raft.raw_raft().client_write(data).await {
            if let Some(openraft::error::ForwardToLeader {
                leader_id: Some(leader_id),
                leader_node: Some(leader_node),
            }) = err.forward_to_leader()
            {
                Err(CoordinatorError::ForwardToLeader {
                    leader_vnode_id: (*leader_id) as u32,
                    replica_id: leader_node.group_id,
                })
            } else {
                Err(CoordinatorError::RaftWriteError {
                    msg: err.to_string(),
                })
            }
        } else {
            Ok(())
        }
    }
}
