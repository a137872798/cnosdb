use std::convert::Infallible as StdInfallible;
use std::sync::Arc;
use std::time::Duration;

use futures::TryFutureExt;
use models::meta_data::{NodeId, NodeMetrics};
use models::node_info::NodeStatus;
use openraft::SnapshotPolicy;
use protos::raft_service::raft_service_server::RaftServiceServer;
use replication::entry_store::HeedEntryStorage;
use replication::multi_raft::MultiRaft;
use replication::network_grpc::RaftCBServer;
use replication::network_http::{EitherBody, RaftHttpAdmin, SyncSendError};
use replication::node_store::NodeStorage;
use replication::raft_node::RaftNode;
use replication::state_store::StateStorage;
use replication::{EntryStorageRef, RaftNodeInfo};
use tokio::sync::RwLock;
use tower::Service;
use tracing::warn;
use warp::hyper;

use super::init::init_meta;
use crate::error::{MetaError, MetaResult};
use crate::store::command::*;
use crate::store::config::{HeartBeatConfig, MetaInit};
use crate::store::key_path::KeyPath;
use crate::store::storage::StateMachine;
use crate::store::{self};

fn openraft_config() -> openraft::Config {
    let hb: u64 = 1000;
    let config = openraft::Config {
        enable_tick: true,
        enable_elect: true,
        enable_heartbeat: true,
        heartbeat_interval: hb,
        election_timeout_min: 3 * hb,
        election_timeout_max: 5 * hb,
        install_snapshot_timeout: 300 * 1000,
        replication_lag_threshold: 10000,
        snapshot_policy: SnapshotPolicy::LogsSinceLast(10000),
        max_in_snapshot_log_to_keep: 10000,
        cluster_name: "cnosdb_meta".to_string(),
        ..Default::default()
    };

    config.validate().unwrap()
}

// 根据opt提供的信息 启动元数据服务器
pub async fn start_raft_node(opt: store::config::Opt) -> MetaResult<()> {
    let id = opt.id;
    let path = std::path::Path::new(&opt.data_path);
    // 生成http地址
    let http_addr = models::utils::build_address(opt.host.clone(), opt.port);

    // 在该目录下初始化3个组件
    // 存储raft协议的一些状态
    let state = StateStorage::open(path.join(format!("{}_state", id)))?;
    // 使用heed框架 存储raft日志
    let entry = HeedEntryStorage::open(path.join(format!("{}_entry", id)))?;
    // 状态机对象 处理分派到单个raft节点上的请求
    let engine = StateMachine::open(path.join(format!("{}_data", id)))?;

    // 这些对象将被多线程共享 所以用Arc包裹
    let state = Arc::new(state);
    let engine = Arc::new(engine);
    let entry: EntryStorageRef = Arc::new(entry);

    // 把自己包装成raft协议中的一个节点
    let info = RaftNodeInfo {
        group_id: 2222,
        address: http_addr.clone(),
    };

    let storage = NodeStorage::open(id, info.clone(), state, engine.clone(), entry)?;
    let storage = Arc::new(storage);

    let node = RaftNode::new(id, info, openraft_config(), storage, engine.clone())
        .await
        .unwrap();

    // 初始化元数据服务
    init_meta(&engine, opt.meta_init.clone()).await;

    // 这个心跳检测是定期把节点上报的测量数据同步到其他follower节点的操作
    tokio::spawn(detect_node_heartbeat(
        node.clone(),
        engine.clone(),
        opt.meta_init.clone(),
        opt.heartbeat.clone(),
    ));

    let bind_addr = models::utils::build_address("0.0.0.0".to_string(), opt.port);
    start_warp_grpc_server(bind_addr, node, engine).await?;

    Ok(())
}

async fn detect_node_heartbeat(
    node: RaftNode,
    storage: Arc<StateMachine>,
    init_data: MetaInit,
    heartbeat_config: HeartBeatConfig,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(
        heartbeat_config.heartbeat_recheck_interval,
    ));

    let metrics_path = KeyPath::data_nodes_metrics(&init_data.cluster_name);
    loop {
        interval.tick().await;

        if let Ok(_leader) = node.raw_raft().is_leader().await {
            let opt_list = storage.children_data::<NodeMetrics>(&metrics_path);

            if let Ok(list) = opt_list {
                let node_metrics_list: Vec<NodeMetrics> = list.into_values().collect();

                // 查看与其他节点的通信信息
                let time = models::utils::now_timestamp_secs();
                for node_metrics in node_metrics_list.iter() {
                    // 需要发送心跳
                    if time - heartbeat_config.heartbeat_expired_interval as i64 > node_metrics.time
                    {
                        let mut now_node_metrics = node_metrics.clone();
                        now_node_metrics.status = NodeStatus::Unreachable;
                        warn!(
                            "Data node '{}' report heartbeat late, maybe unreachable.",
                            node_metrics.id
                        );

                        // 该数据节点的测量数据
                        let req = WriteCommand::ReportNodeMetrics(
                            init_data.cluster_name.clone(),
                            now_node_metrics,
                        );

                        if let Ok(data) = serde_json::to_vec(&req) {

                            // 传播到raft集群
                            if node.raw_raft().client_write(data).await.is_err() {
                                tracing::error!("failed to change node status to unreachable");
                            }
                        }

                        let resourceinfos_mark_path =
                            KeyPath::resourceinfosmark(&init_data.cluster_name);
                        let result =
                            storage.children_data::<(NodeId, bool)>(&resourceinfos_mark_path);
                        if let Ok(opt) = result {
                            let node_id_is_lock_vec: Vec<(NodeId, bool)> =
                                opt.into_values().collect();
                            for node_id_is_lock in node_id_is_lock_vec {

                                // 定期将下面所有节点解锁
                                if node_id_is_lock.0 == node_metrics.id && node_id_is_lock.1 {
                                    let req = WriteCommand::ResourceInfosMark(
                                        init_data.cluster_name.clone(),
                                        node_metrics.id,
                                        false,
                                    );

                                    if let Ok(data) = serde_json::to_vec(&req) {
                                        if node.raw_raft().client_write(data).await.is_err() {
                                            tracing::error!("write resourceinfos_mark failed");
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

// **************************** http and grpc server ************************************** //
async fn start_warp_grpc_server(
    addr: String,
    node: RaftNode,
    storage: Arc<StateMachine>,
) -> MetaResult<()> {
    let node = Arc::new(node);
    let raft_admin = RaftHttpAdmin::new(node.clone());

    // 启动http服务以接受metaClient的请求
    let http_server = super::http::HttpServer {
        node: node.clone(),
        storage: storage.clone(),
        raft_admin: Arc::new(raft_admin),
    };

    let mut multi_raft = MultiRaft::new();
    multi_raft.add_node(node);
    let nodes = Arc::new(RwLock::new(multi_raft));

    let addr = addr.parse().unwrap();
    hyper::Server::bind(&addr)
        .serve(hyper::service::make_service_fn(move |_| {
            let mut http_service = warp::service(http_server.routes());
            let raft_service = RaftServiceServer::new(RaftCBServer::new(nodes.clone()));

            // raft节点间的通信是通过grpc的
            let mut grpc_service = tonic::transport::Server::builder()
                .add_service(raft_service)
                .into_service();

            // 根据请求路径 判断是一个普通http请求 还是一个grpc请求
            futures::future::ok::<_, StdInfallible>(tower::service_fn(
                move |req: hyper::Request<hyper::Body>| {
                    if req.uri().path().starts_with("/raft_service.RaftService/") {
                        futures::future::Either::Right(
                            grpc_service
                                .call(req)
                                .map_ok(|res| res.map(EitherBody::Right))
                                .map_err(SyncSendError::from),
                        )
                    } else {
                        futures::future::Either::Left(
                            http_service
                                .call(req)
                                .map_ok(|res| res.map(EitherBody::Left))
                                .map_err(SyncSendError::from),
                        )
                    }
                },
            ))
        }))
        .await
        .map_err(|err| MetaError::CommonError {
            msg: err.to_string(),
        })?;

    Ok(())
}
