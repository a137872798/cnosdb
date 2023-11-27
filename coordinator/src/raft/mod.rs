use std::path::Path;
use std::time::Duration;

use meta::model::MetaRef;
use models::meta_data::{NodeId, VnodeId};
use models::schema::Precision;
use protos::kv_service::tskv_service_client::TskvServiceClient;
use protos::kv_service::{
    raft_write_command, GetFilesMetaResponse, GetVnodeSnapFilesMetaRequest, RaftWriteCommand,
    WriteDataRequest,
};
use protos::models_helper::parse_prost_bytes;
use replication::errors::{ReplicationError, ReplicationResult};
use replication::{ApplyContext, ApplyStorage};
use tonic::transport::Channel;
use tower::timeout::Timeout;
use tracing::info;
use tskv::VnodeSnapshot;

use crate::errors::{CoordinatorError, CoordinatorResult};
use crate::file_info::get_file_info;
use crate::vnode_mgr::VnodeManager;

pub mod manager;
pub mod writer;

// 维护存储引擎   在每个服务节点上 除了启动存储引擎本身外 还要启动该对象
pub struct TskvEngineStorage {
    node_id: NodeId,  // 这个节点是物理层面的节点   vnode不是物理层面节点
    tenant: String,
    db_name: String,
    vnode_id: VnodeId,
    meta: MetaRef,  // 通过它与元数据服务交互
    storage: tskv::EngineRef,   // 引用存储引擎
}

impl TskvEngineStorage {
    pub fn open(
        node_id: NodeId,
        tenant: &str,
        db_name: &str,
        vnode_id: VnodeId,
        meta: MetaRef,
        storage: tskv::EngineRef,
    ) -> Self {
        Self {
            meta,
            node_id,
            vnode_id,
            storage,
            tenant: tenant.to_owned(),
            db_name: db_name.to_owned(),
        }
    }

    // 从远端节点下载快照
    pub async fn download_snapshot(&self, snapshot: &VnodeSnapshot) -> CoordinatorResult<()> {
        let channel = self.meta.get_node_conn(snapshot.node_id).await?;
        // 元数据服务记录了每个节点暴露的grpc地址 可以生成连接
        let timeout_channel = Timeout::new(channel, Duration::from_secs(60 * 60));
        // 通过client来访问 grpc的服务接口
        let mut client = TskvServiceClient::<Timeout<Channel>>::new(timeout_channel);

        let owner = models::schema::make_owner(&snapshot.tenant, &snapshot.database);

        // 准备好存储快照数据的目录
        let path = self.storage.get_storage_options().snapshot_sub_dir(
            &owner,
            self.vnode_id,
            &snapshot.snapshot_id,  // 在发起创建快照请求时 生成的一个随机id
        );
        info!("snapshot path: {:?}", path);

        // 从远端下载快照文件
        if let Err(err) = self
            .download_snapshot_files(&path, snapshot, &mut client)
            .await
        {
            tokio::fs::remove_dir_all(&path).await?;
            return Err(err);
        }
        info!("success download snapshot all files");

        Ok(())
    }

    // 从远端下载快照文件
    async fn download_snapshot_files(
        &self,
        data_path: &Path,
        snapshot: &VnodeSnapshot,
        client: &mut TskvServiceClient<Timeout<Channel>>,
    ) -> CoordinatorResult<()> {

        // 获取到快照相关的元数据
        let files_meta = self.get_snapshot_files_meta(snapshot, client).await?;
        for info in files_meta.infos.iter() {

            let relative_filename = info
                .name
                .strip_prefix(&(files_meta.path.clone() + "/"))
                .unwrap();

            // 解出文件名
            let filename = data_path.join(relative_filename);
            info!("begin download file: {:?} -> {:?}", info.name, filename);
            VnodeManager::download_file(&info.name, &filename, client).await?;

            // 此时已经将文件下载到本地了  通过检查md5 确保文件数据完整性
            let filename = filename.to_string_lossy().to_string();
            let tmp_info = get_file_info(&filename).await?;
            if tmp_info.md5 != info.md5 {
                return Err(CoordinatorError::CommonError {
                    msg: "download file md5 not match ".to_string(),
                });
            }
        }

        Ok(())
    }

    // 发送获取快照文件信息的请求
    async fn get_snapshot_files_meta(
        &self,
        snapshot: &VnodeSnapshot,
        client: &mut TskvServiceClient<Timeout<Channel>>,
    ) -> CoordinatorResult<GetFilesMetaResponse> {
        let request = tonic::Request::new(GetVnodeSnapFilesMetaRequest {
            // 通过这些信息可以定位到一个快照数据
            tenant: snapshot.tenant.to_string(),
            db: snapshot.database.to_string(),
            vnode_id: snapshot.vnode_id,
            snapshot_id: snapshot.snapshot_id.to_string(),
        });

        // 得到这些文件的元数据信息
        let resp = client
            .get_vnode_snap_files_meta(request)
            .await
            .map_err(tskv::Error::from)?
            .into_inner();
        info!(
            "vnode id: {}, snapshot files meta: {:?}",
            snapshot.vnode_id, resp
        );

        Ok(resp)
    }

    // 作为raft节点接收请求时 调用该方法
    async fn apply_write_data(
        &self,
        ctx: &ApplyContext,
        request: WriteDataRequest,
    ) -> ReplicationResult<Vec<u8>> {
        // 直接将数据写入cache 不生成wal数据
        self.storage
            .write_memcache(
                ctx.index,
                &self.tenant,
                request.data,
                self.vnode_id,
                Precision::from(request.precision as u8),
                None,
            )
            .await
            .map_err(|err| ReplicationError::ApplyEngineErr {
                msg: err.to_string(),
            })?;

        Ok(vec![])
    }
}


// kv存储引擎 也实现了ApplyStorage特征 代表需要借助raft协议
#[async_trait::async_trait]
impl ApplyStorage for TskvEngineStorage {

    // 接收raft集群中同步的请求
    async fn apply(
        &self,
        ctx: &ApplyContext,
        req: &replication::Request,
    ) -> ReplicationResult<replication::Response> {
        let request = parse_prost_bytes::<RaftWriteCommand>(req)?;
        if let Some(command) = request.command {
            match command {
                // 接收写入数据请求
                raft_write_command::Command::WriteData(request) => {
                    return self.apply_write_data(ctx, request).await;
                }

                raft_write_command::Command::DropTab(_request) => {}

                raft_write_command::Command::DropColumn(_request) => {}

                raft_write_command::Command::AddColumn(_request) => {}

                raft_write_command::Command::AlterColumn(_request) => {}

                raft_write_command::Command::RenameColumn(_request) => {}

                raft_write_command::Command::UpdateTags(_request) => {}
            }
        }

        Ok(vec![])
    }

    // 下面这些有关数据的快照 恢复等等 都是基于raft协议的   当一个新的节点启动时 会自动拉取快照数据 确保参与raft集群的几个节点数据都是一致的
    // 如果将同一个数据节点的副本放在一个raft集群下 也就是副本集  那么他们的数据就会自动同步了
    async fn snapshot(&self) -> ReplicationResult<Vec<u8>> {
        // 针对底层数据生成快照
        let mut snapshot = self
            .storage
            .create_snapshot(self.vnode_id)
            .await
            .map_err(|err| ReplicationError::ApplyEngineErr {
                msg: err.to_string(),
            })?;

        // 因为要传输整个副本 数据量非常大 考虑到 快照仅传输一个元数据信息
        snapshot.node_id = self.node_id;

        let data = bincode::serialize(&snapshot)?;
        Ok(data)
    }

    // 基于一个元数据信息 从远端拉取快照并恢复数据
    async fn restore(&self, data: &[u8]) -> ReplicationResult<()> {
        let snapshot = bincode::deserialize::<VnodeSnapshot>(data)?;
        self.download_snapshot(&snapshot).await.map_err(|err| {
            ReplicationError::ApplyEngineErr {
                msg: err.to_string(),
            }
        })?;

        let owner = models::schema::make_owner(&snapshot.tenant, &snapshot.database);

        // 修改目录路径
        let vnode_move_dir = self
            .storage
            .get_storage_options()
            .move_dir(&owner, self.vnode_id);
        let snapshot_dir = self.storage.get_storage_options().snapshot_sub_dir(
            &owner,
            self.vnode_id,
            &snapshot.snapshot_id,
        );
        info!(
            "rename snpshot dir to move dir: {:?} -> {:?}",
            snapshot_dir, vnode_move_dir
        );
        tokio::fs::rename(&snapshot_dir, vnode_move_dir).await?;

        let mut snapshot = snapshot.clone();
        snapshot.vnode_id = self.vnode_id;

        // 现在有数据文件了 在本地还原数据
        self.storage.apply_snapshot(snapshot).await.map_err(|err| {
            ReplicationError::ApplyEngineErr {
                msg: err.to_string(),
            }
        })?;

        Ok(())
    }

    // 销毁本地数据
    async fn destory(&self) -> ReplicationResult<()> {
        self.storage
            .remove_tsfamily(&self.tenant, &self.db_name, self.vnode_id)
            .await
            .map_err(|err| ReplicationError::ApplyEngineErr {
                msg: err.to_string(),
            })?;

        Ok(())
    }
}
