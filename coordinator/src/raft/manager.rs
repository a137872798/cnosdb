use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use meta::model::MetaRef;
use models::meta_data::*;
use models::schema::Precision;
use openraft::SnapshotPolicy;
use protos::kv_service::tskv_service_client::TskvServiceClient;
use protos::kv_service::*;
use replication::multi_raft::MultiRaft;
use replication::node_store::NodeStorage;
use replication::raft_node::RaftNode;
use replication::state_store::{RaftNodeSummary, StateStorage};
use replication::{ApplyStorageRef, EntryStorageRef, RaftNodeId, RaftNodeInfo};
use tokio::sync::RwLock;
use tonic::transport;
use tower::timeout::Timeout;
use tracing::info;
use tskv::{wal, EngineRef};

use crate::errors::*;
use crate::{get_replica_all_info, update_replication_set};

// 代表一个基于raft协议的写入请求
// 如果采用副本集部署的方式 副本集通过raft协议来确保数据同步
pub struct RaftWriteRequest {
    pub points: WritePointsRequest,  // 内部包含数据
    pub precision: Precision,  // 时间精度
}

// 每个参与raft集群的节点 都要部署一个 RaftNodesManager
pub struct RaftNodesManager {
    meta: MetaRef,  // 用于与元数据服务交互
    config: config::Config,
    kv_inst: Option<EngineRef>,  // tskv存储
    raft_state: Arc<StateStorage>,  // 存储raft状态信息
    raft_nodes: Arc<RwLock<MultiRaft>>,  // 维护每个副本集中的一个节点
}

impl RaftNodesManager {

    // 初始化管理器
    pub fn new(config: config::Config, meta: MetaRef, kv_inst: Option<EngineRef>) -> Self {
        let path = PathBuf::from(config.storage.path.clone()).join("raft-state");
        let state = StateStorage::open(path).unwrap();

        Self {
            meta,
            config,
            kv_inst,
            raft_state: Arc::new(state),
            raft_nodes: Arc::new(RwLock::new(MultiRaft::new())),
        }
    }

    pub fn node_id(&self) -> u64 {
        self.config.node_basic.node_id
    }

    pub fn multi_raft(&self) -> Arc<RwLock<MultiRaft>> {
        self.raft_nodes.clone()
    }

    pub async fn metrics(&self, group_id: u32) -> String {
        if let Some(node) = self.raft_nodes.read().await.get_node(group_id) {
            serde_json::to_string(&node.raft_metrics())
                .unwrap_or("encode raft metrics to json failed".to_string())
        } else {
            format!("Not found raft group: {}", group_id)
        }
    }

    // 启动所有副本节点
    pub async fn start_all_raft_node(&self) -> CoordinatorResult<()> {
        // 获取所有节点的描述信息
        let nodes_summary = self.raft_state.all_nodes_summary()?;
        let mut nodes = self.raft_nodes.write().await;
        for summary in nodes_summary {
            let node = self
                // 启动所有raft节点   实际上这个组件内的每个raft节点都会关联到某个副本集的副本  这样就能在集群层面确保副本数据一致性
                .open_raft_node(
                    &summary.tenant,
                    &summary.db_name,
                    summary.raft_id as VnodeId,
                    summary.group_id,
                )
                .await?;

            info!("start raft node: {:?} Success", summary);

            nodes.add_node(node);
        }

        Ok(())
    }

    // 获取某个副本集相关的raft节点   不存在则创建
    pub async fn get_node_or_build(
        &self,
        tenant: &str,
        db_name: &str,
        replica: &ReplicationSet,
    ) -> CoordinatorResult<Arc<RaftNode>> {
        if let Some(node) = self.raft_nodes.read().await.get_node(replica.id) {
            return Ok(node);
        }

        // 一定要leader节点才有权限构建组  同时会通知副本组其他节点 自动加入副本组
        let result = self.build_replica_group(tenant, db_name, replica).await;
        if let Err(err) = &result {
            info!("build replica group failed: {:?}, {:?}", replica, err);
        } else {
            info!("build replica group success: {:?}", replica);
        }

        result
    }

    // 启动一个raft节点
    pub async fn exec_open_raft_node(
        &self,
        tenant: &str,
        db_name: &str,
        id: VnodeId,
        group_id: ReplicationSetId,
    ) -> CoordinatorResult<()> {
        info!("exec open raft node: {}.{}", group_id, id);
        let mut nodes = self.raft_nodes.write().await;
        if nodes.get_node(group_id).is_some() {
            return Ok(());
        }

        let node = self.open_raft_node(tenant, db_name, id, group_id).await?;
        nodes.add_node(node);

        Ok(())
    }

    // 本节点不再维护某个副本组的副本
    pub async fn exec_drop_raft_node(
        &self,
        tenant: &str,
        db_name: &str,
        id: VnodeId,
        group_id: ReplicationSetId,
    ) -> CoordinatorResult<()> {
        info!("exec drop raft node: {}.{}", group_id, id);
        let mut nodes = self.raft_nodes.write().await;
        if let Some(raft_node) = nodes.get_node(group_id) {
            // 终止raft节点  并清理本地数据
            raft_node.shutdown().await?;
            nodes.rm_node(group_id);

            let vnode_id = raft_node.raft_id() as VnodeId;
            if let Some(storage) = &self.kv_inst {
                // 删除本地数据
                let err = storage.remove_tsfamily(tenant, db_name, vnode_id).await;
                info!("drop raft node remove tsfamily: {:?}", err);
            }

            info!("success remove raft node({}) from group({})", id, group_id)
        } else {
            info!("can't found raft node({}) from group({})", id, group_id)
        }

        Ok(())
    }

    // 构建副本组
    async fn build_replica_group(
        &self,
        tenant: &str,
        db_name: &str,
        replica: &ReplicationSet,
    ) -> CoordinatorResult<Arc<RaftNode>> {

        // leader节点才有权限生成副本组
        if replica.leader_node_id != self.node_id() {
            return Err(CoordinatorError::LeaderIsWrong {
                replica: replica.clone(),
            });
        }

        // 获取锁后 先检查一次
        let mut nodes = self.raft_nodes.write().await;
        if let Some(node) = nodes.get_node(replica.id) {
            return Ok(node);
        }

        let mut cluster_nodes = BTreeMap::new();

        // 启动leader节点
        let leader_id = replica.leader_vnode_id;
        let raft_node = self
            .open_raft_node(tenant, db_name, leader_id, replica.id)
            .await?;

        // leader首次创建副本组后 远程命令其他节点启动副本
        for vnode in &replica.vnodes {
            let raft_id = vnode.id as RaftNodeId;
            let info = RaftNodeInfo {
                group_id: replica.id,
                address: self.meta.node_info_by_id(vnode.node_id).await?.grpc_addr,
            };
            cluster_nodes.insert(raft_id, info);

            if vnode.node_id == self.node_id() {
                continue;
            }

            self.open_remote_raft_node(tenant, db_name, vnode, replica.id)
                .await?;
            info!("success open remote raft: {}", vnode.node_id,);
        }

        info!("init raft group: {:?}", replica);

        // 先在集群中通知各raft节点启动  然后初始化raft集群
        raft_node.raft_init(cluster_nodes).await?;

        // 等待leader选举结束
        self.try_wait_leader_elected(raft_node.clone()).await;

        // 增加本manager维护的副本节点
        nodes.add_node(raft_node.clone());

        Ok(raft_node)
    }

    async fn try_wait_leader_elected(&self, raft_node: Arc<RaftNode>) {
        for _ in 0..10 {
            let result = raft_node.raw_raft().is_leader().await;
            info!("try wait leader elected, check leader: {:?}", result);
            if let Err(err) = result {
                if let Some(openraft::error::ForwardToLeader {
                    leader_id: Some(_id),
                    leader_node: Some(_node),
                }) = err.forward_to_leader()
                {
                    break;
                }
            } else {
                // 本节点就是leader
                break;
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }

    // 销毁整个副本组
    pub async fn destory_replica_group(
        &self,
        tenant: &str,
        db_name: &str,
        replica_id: ReplicationSetId,
    ) -> CoordinatorResult<()> {
        // 获取副本集全信息
        let all_info = get_replica_all_info(self.meta.clone(), tenant, replica_id).await?;
        let replica = all_info.replica_set.clone();
        // 只有leader节点可以发起 销毁副本组的请求
        if replica.leader_node_id != self.node_id() {
            return Err(CoordinatorError::LeaderIsWrong {
                replica: replica.clone(),
            });
        }

        // 本副本节点从 raft集群中离开
        let raft_node = self.get_node_or_build(tenant, db_name, &replica).await?;
        let mut members = BTreeSet::new();
        members.insert(raft_node.raft_id());
        raft_node.raft_change_membership(members).await?;

        // 由leader通知其余节点从集群离开
        for vnode in replica.vnodes.iter() {
            if vnode.node_id == self.node_id() {
                continue;
            }

            let result = self
                .drop_remote_raft_node(tenant, db_name, vnode, replica.id)
                .await;
            info!("destory replica group drop vnode: {:?},{:?}", vnode, result);
        }

        // 等待所有节点从raft集群离开时  终止集群
        raft_node.shutdown().await?;

        // 将通知发往元数据服务
        update_replication_set(
            self.meta.clone(),
            tenant,
            db_name,
            all_info.bucket_id,
            replica.id,
            &replica.vnodes,
            &[],
        )
        .await?;

        // 删除本地数据
        let vnode_id = raft_node.raft_id() as VnodeId;
        if let Some(storage) = &self.kv_inst {
            let err = storage.remove_tsfamily(tenant, db_name, vnode_id).await;
            info!("destory replica group remove tsfamily: {:?}", err);
        }
        self.raft_nodes.write().await.rm_node(replica_id);

        Ok(())
    }


    // 代表往副本集中添加新成员
    pub async fn add_follower_to_group(
        &self,
        tenant: &str,
        db_name: &str,
        follower_nid: NodeId,  // 期望成为副本的节点
        replica_id: ReplicationSetId,
    ) -> CoordinatorResult<()> {
        let follower_addr = self.meta.node_info_by_id(follower_nid).await?.grpc_addr;
        let all_info = get_replica_all_info(self.meta.clone(), tenant, replica_id).await?;
        let replica = all_info.replica_set.clone();

        // 只有leader节点可以管理该请求
        if replica.leader_node_id != self.node_id() {
            return Err(CoordinatorError::LeaderIsWrong {
                replica: replica.clone(),
            });
        }

        let raft_node = self.get_node_or_build(tenant, db_name, &replica).await?;

        // retain_id 返回的是一个全局id  传入count1 代表递增1  得到一个新的id
        let new_vnode_id = self.meta.retain_id(1).await?;
        let new_vnode = VnodeInfo {
            id: new_vnode_id,
            node_id: follower_nid,
            status: VnodeStatus::Running,
        };
        self.open_remote_raft_node(tenant, db_name, &new_vnode, replica.id)
            .await?;

        let raft_node_info = RaftNodeInfo {
            group_id: replica_id,
            address: follower_addr,
        };
        raft_node
            .raft_add_learner(new_vnode_id.into(), raft_node_info)
            .await?;

        let mut members = BTreeSet::new();
        members.insert(new_vnode_id as RaftNodeId);
        for vnode in replica.vnodes.iter() {
            members.insert(vnode.id as RaftNodeId);
        }
        // 更新集群节点
        raft_node.raft_change_membership(members).await?;

        // 副本增加了节点 推送到元数据服务
        update_replication_set(
            self.meta.clone(),
            tenant,
            db_name,
            all_info.bucket_id,
            replica.id,
            &[],
            &[new_vnode],
        )
        .await?;

        Ok(())
    }

    // 某节点从副本集中离开
    pub async fn remove_node_from_group(
        &self,
        tenant: &str,
        db_name: &str,
        vnode_id: VnodeId,
        replica_id: ReplicationSetId,
    ) -> CoordinatorResult<()> {
        let all_info = get_replica_all_info(self.meta.clone(), tenant, replica_id).await?;
        let replica = all_info.replica_set.clone();
        if replica.leader_node_id != self.node_id() {
            return Err(CoordinatorError::LeaderIsWrong {
                replica: replica.clone(),
            });
        }

        let mut members = BTreeSet::new();
        for vnode in replica.vnodes.iter() {
            if vnode.id != vnode_id {
                members.insert(vnode.id as RaftNodeId);
            }
        }

        if let Some(vnode) = replica.vnode(vnode_id) {
            let raft_node = self.get_node_or_build(tenant, db_name, &replica).await?;
            raft_node.raft_change_membership(members).await?;

            // 通知远程下线 or 本地节点下线
            if vnode.node_id == self.node_id() {
                self.exec_drop_raft_node(tenant, db_name, vnode.id, replica.id)
                    .await?;
            } else {
                self.drop_remote_raft_node(tenant, db_name, &vnode, replica.id)
                    .await?;
            }

            // 更新副本集
            update_replication_set(
                self.meta.clone(),
                tenant,
                db_name,
                all_info.bucket_id,
                replica.id,
                &[vnode],
                &[],
            )
            .await?;
        }

        Ok(())
    }

    fn raft_config(&self) -> openraft::Config {
        let logs_to_keep = self.config.raft_logs_to_keep;

        let heartbeat = 3000;
        openraft::Config {
            heartbeat_interval: heartbeat,
            election_timeout_min: 3 * heartbeat,
            election_timeout_max: 5 * heartbeat,
            replication_lag_threshold: logs_to_keep,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(logs_to_keep),
            max_in_snapshot_log_to_keep: logs_to_keep,
            ..Default::default()
        }
    }

    // 启动指定的节点
    async fn open_raft_node(
        &self,
        tenant: &str,
        db_name: &str,
        vnode_id: VnodeId,
        group_id: ReplicationSetId,   // 每个副本集 对应一个raft组 有点像kafka
    ) -> CoordinatorResult<Arc<RaftNode>> {
        info!("open local raft node: {}.{}", group_id, vnode_id);

        // 该对象可以用于访问 vnode节点相关的wal数据文件
        // 在该模块中 将wal文件作为  raft的日志存储
        let entry = self
            .raft_node_logs(tenant, db_name, vnode_id, group_id)
            .await?;

        // 基于存储引擎 包装应用处理器
        let engine = self.raft_node_engine(tenant, db_name, vnode_id).await?;

        // 开放grpc端口
        let grpc_addr = models::utils::build_address(
            self.config.host.clone(),
            self.config.cluster.grpc_listen_port,
        );

        // 产生本节点信息
        let info = RaftNodeInfo {
            group_id,
            address: grpc_addr,
        };

        let raft_id = vnode_id as u64;

        // 将各种信息组合起来 变成了NodeStorage 内部包含了raft协议需要的各个组件
        let storage = NodeStorage::open(
            raft_id,
            info.clone(),
            self.raft_state.clone(),
            engine.clone(),
            entry,
        )?;
        let storage = Arc::new(storage);

        // 包装成node
        let node = RaftNode::new(raft_id, info, self.raft_config(), storage, engine).await?;

        // 将相关信息包装成summary 通过 state统一管理
        let summary = RaftNodeSummary {
            raft_id,
            group_id,
            tenant: tenant.to_string(),
            db_name: db_name.to_string(),
        };

        self.raft_state.set_node_summary(group_id, &summary)?;

        Ok(Arc::new(node))
    }

    // 加载本地节点相关的wal日志
    async fn raft_node_logs(
        &self,
        tenant: &str,
        db_name: &str,
        vnode_id: VnodeId,
        group_id: ReplicationSetId,  // 某个副本集id  也是一个raft group id
    ) -> CoordinatorResult<EntryStorageRef> {

        // 生成一个key
        let owner = models::schema::make_owner(tenant, db_name);
        // 根据当前数据节点的配置 生成wal opt
        let wal_option = tskv::kv_option::WalOptions::from(&self.config);

        // 每个vnode节点会关联一个wal文件目录
        let wal = wal::VnodeWal::new(Arc::new(wal_option), Arc::new(owner), vnode_id).await?;
        let raft_logs = wal::raft_store::RaftEntryStorage::new(wal);

        let _apply_id = self.raft_state.get_last_applied_log(group_id)?;
        // 主要是重建wal文件索引
        raft_logs.recover().await?;

        Ok(Arc::new(raft_logs))
    }

    // 生成应用处理器 主要用于调用存储引擎的快照相关接口
    async fn raft_node_engine(
        &self,
        tenant: &str,
        db_name: &str,
        vnode_id: VnodeId,
    ) -> CoordinatorResult<ApplyStorageRef> {
        let kv_inst = self.kv_inst.clone();
        let storage = kv_inst.ok_or(CoordinatorError::KvInstanceNotFound {
            node_id: self.node_id(),
        })?;

        let engine = super::TskvEngineStorage::open(
            self.node_id(),
            tenant,
            db_name,
            vnode_id,
            self.meta.clone(),
            storage,
        );
        let engine: ApplyStorageRef = Arc::new(engine);
        Ok(engine)
    }

    // 通知其他节点离开raft集群
    async fn drop_remote_raft_node(
        &self,
        tenant: &str,
        db_name: &str,
        vnode: &VnodeInfo,
        replica_id: ReplicationSetId,
    ) -> CoordinatorResult<()> {
        let channel = self.meta.get_node_conn(vnode.node_id).await?;
        let timeout_channel = Timeout::new(channel, Duration::from_secs(5));
        let mut client = TskvServiceClient::<Timeout<transport::Channel>>::new(timeout_channel);
        let cmd = tonic::Request::new(DropRaftNodeRequest {
            replica_id,
            vnode_id: vnode.id,
            db_name: db_name.to_string(),
            tenant: tenant.to_string(),
        });

        let response = client
            .exec_drop_raft_node(cmd)
            .await
            .map_err(|err| CoordinatorError::GRPCRequest {
                msg: err.to_string(),
            })?
            .into_inner();

        crate::status_response_to_result(&response)
    }

    // 远程通知某个节点 开启副本节点
    async fn open_remote_raft_node(
        &self,
        tenant: &str,
        db_name: &str,
        vnode: &VnodeInfo,
        replica_id: ReplicationSetId,
    ) -> CoordinatorResult<()> {
        info!(
            "open remote raft node: {}.{}.{}",
            vnode.node_id, replica_id, vnode.id
        );

        let channel = self.meta.get_node_conn(vnode.node_id).await?;
        let timeout_channel = Timeout::new(channel, Duration::from_secs(5));
        let mut client = TskvServiceClient::<Timeout<transport::Channel>>::new(timeout_channel);
        // 发送请求
        let cmd = tonic::Request::new(OpenRaftNodeRequest {
            replica_id,
            vnode_id: vnode.id,
            db_name: db_name.to_string(),
            tenant: tenant.to_string(),
        });

        let response = client
            .exec_open_raft_node(cmd)
            .await
            .map_err(|err| CoordinatorError::GRPCRequest {
                msg: err.to_string(),
            })?
            .into_inner();

        crate::status_response_to_result(&response)
    }
}
