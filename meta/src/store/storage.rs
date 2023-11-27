use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::sync::Arc;

use models::auth::privilege::DatabasePrivilege;
use models::auth::role::{CustomTenantRole, SystemTenantRole, TenantRoleIdentifier};
use models::auth::user::{UserDesc, UserOptions};
use models::meta_data::*;
use models::oid::{Identifier, Oid, UuidGenerator};
use models::schema::{DatabaseSchema, ResourceInfo, TableSchema, Tenant, TenantOptions};
use replication::errors::ReplicationResult;
use replication::{ApplyContext, ApplyStorage, Request, Response};
use serde::{Deserialize, Serialize};
use trace::{debug, error, info};

use super::command::*;
use super::key_path;
use crate::error::{MetaError, MetaResult};
use crate::limiter::local_request_limiter::{LocalBucketRequest, LocalBucketResponse};
use crate::limiter::remote_request_limiter::RemoteRequestLimiter;
use crate::store::key_path::KeyPath;

pub type CommandResp = String;

pub fn value_encode<T: Serialize>(d: &T) -> MetaResult<String> {
    serde_json::to_string(d).map_err(|e| MetaError::SerdeMsgInvalid { err: e.to_string() })
}

// 对处理结果进行json编码
pub fn response_encode<T: Serialize>(d: MetaResult<T>) -> String {
    match serde_json::to_string(&d) {
        Ok(val) => val,
        Err(err) => {
            let err_rsp = MetaError::SerdeMsgInvalid {
                err: err.to_string(),
            };

            serde_json::to_string(&err_rsp).unwrap()
        }
    }
}

// 使用btree存储快照数据
#[derive(Serialize, Deserialize)]
pub struct BtreeMapSnapshotData {
    pub map: BTreeMap<String, String>,
}

// 状态机 也是使用heed框架读写数据
pub struct StateMachine {
    env: heed::Env,
    db: heed::Database<heed::types::Str, heed::types::Str>,
    // 该对象内包含了一组日志 还有一个sender对象 用于发送消息
    pub watch: Arc<Watch>,
}

// cnosdb在接入openRaft后 将业务处理逻辑转发到该对象的api  元数据服务器有组件实现了该特征就代表 元数据服务节点也使用了raft协议
#[async_trait::async_trait]
impl ApplyStorage for StateMachine {

    async fn apply(&self, _ctx: &ApplyContext, req: &Request) -> ReplicationResult<Response> {
        // 收到的是一个json 转换成命令并执行
        let req: WriteCommand = serde_json::from_slice(req)?;

        Ok(self.process_write_command(&req).into())
    }

    // 将db中所有数据以btree结构写出 作为快照数据
    async fn snapshot(&self) -> ReplicationResult<Vec<u8>> {
        let mut hash_map = BTreeMap::new();

        let reader = self.env.read_txn()?;
        let iter = self.db.iter(&reader)?;
        for pair in iter {
            let (key, val) = pair?;
            hash_map.insert(key.to_string(), val.to_string());
        }

        let data = BtreeMapSnapshotData { map: hash_map };
        let json_str = serde_json::to_string(&data).unwrap();

        Ok(json_str.as_bytes().to_vec())
    }

    // 从之前的快照数据还原
    async fn restore(&self, snapshot: &[u8]) -> ReplicationResult<()> {
        let data: BtreeMapSnapshotData = serde_json::from_slice(snapshot).unwrap();

        let mut writer = self.env.write_txn()?;
        self.db.clear(&mut writer)?;
        for (key, val) in data.map.iter() {
            self.db.put(&mut writer, key, val)?;
        }
        writer.commit()?;

        Ok(())
    }

    async fn destory(&self) -> ReplicationResult<()> {
        Ok(())
    }
}

impl StateMachine {
    // 状态机对象 也是在元数据服务器上被创建
    pub fn open(path: impl AsRef<Path>) -> MetaResult<Self> {
        fs::create_dir_all(&path)?;

        let env = heed::EnvOpenOptions::new()
            .map_size(1024 * 1024 * 1024)
            .max_dbs(16)
            .open(path)?;

        let db: heed::Database<heed::types::Str, heed::types::Str> =
            env.create_database(Some("data"))?;
        let storage = Self {
            env,
            db,
            watch: Arc::new(Watch::new()),
        };

        Ok(storage)
    }

    // 如果相关key存在数据 代表完成了初始化
    pub fn is_meta_init(&self) -> MetaResult<bool> {
        self.contains_key(&KeyPath::already_init())
    }

    // 设置完成初始化
    pub fn set_already_init(&self) -> MetaResult<()> {
        let mut writer = self.env.write_txn()?;
        self.db.put(&mut writer, &KeyPath::already_init(), "true")?;
        writer.commit()?;

        Ok(())
    }

    // 将快照数据输出
    pub async fn dump(&self) -> MetaResult<String> {
        let data = self.snapshot().await.map_err(MetaError::from)?;

        let data: BtreeMapSnapshotData = serde_json::from_slice(&data).map_err(MetaError::from)?;

        let mut rsp = "****** ------------------------------------- ******\n".to_string();
        for (key, val) in data.map.iter() {
            rsp = rsp + &format!("* {}: {}\n", key, val);
        }
        rsp += "****** ------------------------------------- ******\n";

        Ok(rsp)
    }

    //********************************************************************************* */
    pub fn get(&self, key: &str) -> MetaResult<Option<String>> {
        let reader = self.env.read_txn()?;
        if let Some(data) = self.db.get(&reader, key)? {
            Ok(Some(data.to_owned()))
        } else {
            Ok(None)
        }
    }

    fn contains_key(&self, key: &str) -> MetaResult<bool> {
        if self.get(key)?.is_some() {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn version(&self) -> MetaResult<u64> {
        let key = KeyPath::version();
        if let Some(data) = self.get(&key)? {
            Ok(data.parse::<u64>().unwrap_or(0))
        } else {
            Ok(0)
        }
    }

    // 找到并递增id
    fn fetch_and_add_incr_id(&self, cluster: &str, count: u32) -> MetaResult<u32> {
        let key = KeyPath::incr_id(cluster);

        let mut writer = self.env.write_txn()?;
        let data = self.db.get(&writer, &key)?.unwrap_or("1");
        let id = data.parse::<u32>().unwrap_or(1);

        self.db.put(&mut writer, &key, &(id + count).to_string())?;
        writer.commit()?;

        Ok(id)
    }

    // 插入的键值对 作为日志
    fn insert(&self, key: &str, val: &str) -> MetaResult<()> {
        let version = self.version()? + 1;

        let mut writer = self.env.write_txn()?;
        self.db.put(&mut writer, key, val)?;
        self.db
            .put(&mut writer, &KeyPath::version(), &version.to_string())?;
        writer.commit()?;

        info!("METADATA WRITE(ver: {}): {} :{}", version, key, val);
        let log = EntryLog {
            tye: ENTRY_LOG_TYPE_SET,
            ver: version,
            key: key.to_string(),
            val: val.to_string(),
        };

        // 同时会通知所有监听日志变化的 receiver
        self.watch.writer_log(log);

        Ok(())
    }

    // 移除某个kv 并产生一条del日志
    fn remove(&self, key: &str) -> MetaResult<()> {
        let version = self.version()? + 1;

        let mut writer = self.env.write_txn()?;
        self.db.delete(&mut writer, key)?;
        self.db
            .put(&mut writer, &KeyPath::version(), &version.to_string())?;
        writer.commit()?;

        info!("METADATA REMOVE(ver: {}): {}", version, key);
        let log = EntryLog {
            tye: ENTRY_LOG_TYPE_DEL,
            ver: version,
            key: key.to_string(),
            val: "".to_string(),
        };

        self.watch.writer_log(log);

        Ok(())
    }

    pub fn get_struct<T>(&self, key: &str) -> MetaResult<Option<T>>
    where
        for<'a> T: Deserialize<'a>,
    {
        let val = self.get(key)?;
        if let Some(data) = val {
            let info: T = serde_json::from_str(&data)?;
            Ok(Some(info))
        } else {
            Ok(None)
        }
    }

    // 返回path下所有子路径
    pub fn children_fullpath(&self, path: &str) -> MetaResult<Vec<String>> {
        let mut path = path.to_owned();
        if !path.ends_with('/') {
            path.push('/');
        }

        let mut list = vec![];
        let reader = self.env.read_txn()?;
        let iter = self.db.prefix_iter(&reader, &path)?;
        for pair in iter {
            let (key, _) = pair?;
            match key.strip_prefix(path.as_str()) {
                Some(val) => {
                    if val.find('/').is_some() {
                        continue;
                    }
                    if val.is_empty() {
                        continue;
                    }

                    list.push(key.to_string());
                }

                None => break,
            }
        }

        Ok(list)
    }

    // 将路径下所有数据读取出来
    pub fn children_data<T>(&self, path: &str) -> MetaResult<HashMap<String, T>>
    where
        for<'a> T: Deserialize<'a>,
    {
        let mut path = path.to_owned();
        if !path.ends_with('/') {
            path.push('/');
        }

        let mut result = HashMap::new();
        let reader = self.env.read_txn()?;
        let iter = self.db.prefix_iter(&reader, &path)?;
        for pair in iter {
            let (key, val) = pair?;
            match key.strip_prefix(path.as_str()) {
                Some(sub_key) => {
                    if sub_key.find('/').is_some() {
                        continue;
                    }
                    if sub_key.is_empty() {
                        continue;
                    }

                    let info: T = serde_json::from_str(val)?;
                    result.insert(sub_key.to_string(), info);
                }

                None => break,
            }
        }

        Ok(result)
    }

    // 查询这些租户相关的日志 并且从base序列号开始
    // 认为key中会包含租户 所以可以使用租户作为过滤条件
    pub fn read_change_logs(
        &self,
        cluster: &str,
        tenants: &HashSet<String>,
        base_ver: u64,
    ) -> WatchData {
        let mut data = WatchData {
            full_sync: false,
            entry_logs: vec![],
            min_ver: self.watch.min_version().unwrap_or(0),
            max_ver: self.watch.max_version().unwrap_or(0),
        };

        if base_ver == self.version().unwrap_or(0) {
            return data;
        }

        let (logs, status) = self.watch.read_entry_logs(cluster, tenants, base_ver);

        // 日志不存在会返回-1
        if status < 0 {
            data.full_sync = true;
        } else {
            data.entry_logs = logs;
        }

        data
    }

    // 获取租户相关的元数据信息
    pub fn to_tenant_meta_data(&self, cluster: &str, tenant: &str) -> MetaResult<TenantMetaData> {

        // 就是根据各种key 从db中读取数据 并组合成租户数据
        let mut meta = TenantMetaData::new();
        meta.version = self.version()?;
        meta.roles =
            self.children_data::<CustomTenantRole<Oid>>(&KeyPath::roles(cluster, tenant))?;
        meta.members =
            self.children_data::<TenantRoleIdentifier>(&KeyPath::members(cluster, tenant))?;
        let db_schemas =
            self.children_data::<DatabaseSchema>(&KeyPath::tenant_dbs(cluster, tenant))?;

        for (key, schema) in db_schemas.iter() {
            let buckets = self
                .children_data::<BucketInfo>(&KeyPath::tenant_db_buckets(cluster, tenant, key))?;
            let tables =
                self.children_data::<TableSchema>(&KeyPath::tenant_schemas(cluster, tenant, key))?;

            let info = DatabaseInfo {
                tables,
                schema: schema.clone(),
                buckets: buckets.into_values().collect(),
            };

            meta.dbs.insert(key.clone(), info);
        }

        Ok(meta)
    }

    // 处理收到的命令
    pub fn process_read_command(&self, req: &ReadCommand) -> CommandResp {
        debug!("meta process read command {:?}", req);
        match req {
            ReadCommand::DataNodes(cluster) => {
                response_encode(self.process_read_data_nodes(cluster))
            }
            ReadCommand::NodeMetrics(cluster) => {
                response_encode(self.process_read_node_metrics(cluster))
            }
            ReadCommand::TenaneMetaData(cluster, tenant) => {
                response_encode(self.to_tenant_meta_data(cluster, tenant))
            }

            // 通过key 得到value数据 并转换成CustomTenantRole
            ReadCommand::CustomRole(cluster, role_name, tenant_name) => {
                let path = KeyPath::role(cluster, tenant_name, role_name);
                response_encode(self.get_struct::<CustomTenantRole<Oid>>(&path))
            }

            // 获取所有自定义角色
            ReadCommand::CustomRoles(cluster, tenant_name) => {
                response_encode(self.process_read_roles(cluster, tenant_name))
            }

            // 获取某用户的角色   一个用户只能对应一个角色吗?
            ReadCommand::MemberRole(cluster, tenant_name, user_id) => {
                let path = KeyPath::member(cluster, tenant_name, user_id);
                response_encode(self.get_struct::<TenantRoleIdentifier>(&path))
            }

            // 获取某个租户下所有成员与其角色的关系
            ReadCommand::Members(cluster, tenant_name) => {
                response_encode(self.process_read_members(cluster, tenant_name))
            }

            // 获取某个用户的描述信息
            ReadCommand::User(cluster, user_name) => {
                let path = KeyPath::user(cluster, user_name);
                response_encode(self.get_struct::<UserDesc>(&path))
            }
            // 获取集群下所有用户
            ReadCommand::Users(cluster) => response_encode(self.process_read_users(cluster)),

            // 获取租户信息
            ReadCommand::Tenant(cluster, tenant_name) => {
                response_encode(self.process_read_tenant(cluster, tenant_name))
            }

            // 获取集群下所有租户
            ReadCommand::Tenants(cluster) => response_encode(self.process_read_tenants(cluster)),

            // 读取某张表的schema信息
            ReadCommand::TableSchema(cluster, tenant_name, db_name, table_name) => {
                // 拼接成path
                let path = KeyPath::tenant_schema_name(cluster, tenant_name, db_name, table_name);
                response_encode(self.get_struct::<TableSchema>(&path))
            }

            // 读取集群下某些资源信息
            ReadCommand::ResourceInfos(cluster, names) => {
                response_encode(self.process_read_resourceinfos(cluster, names))
            }
            ReadCommand::ResourceInfosMark(cluster) => {
                response_encode(self.process_read_resourceinfos_mark(cluster))
            }
        }
    }

    // 获取该集群所有数据节点
    pub fn process_read_data_nodes(&self, cluster: &str) -> MetaResult<(Vec<NodeInfo>, u64)> {
        let response: Vec<NodeInfo> = self
            .children_data::<NodeInfo>(&KeyPath::data_nodes(cluster))?
            .into_values()
            .collect();

        let ver = self.version()?;
        Ok((response, ver))
    }

    // 获取节点的测量数据  每个节点应该会通过心跳 将自身测量数据发送到leader元数据节点
    pub fn process_read_node_metrics(&self, cluster: &str) -> MetaResult<Vec<NodeMetrics>> {
        let response: Vec<NodeMetrics> = self
            .children_data::<NodeMetrics>(&KeyPath::data_nodes_metrics(cluster))?
            .into_values()
            .collect();

        Ok(response)
    }

    // 获取集群下所有用户
    pub fn process_read_users(&self, cluster: &str) -> MetaResult<Vec<UserDesc>> {
        let path = KeyPath::users(cluster);
        let users: Vec<UserDesc> = self
            .children_data::<UserDesc>(&path)?
            .into_values()
            .collect();

        Ok(users)
    }

    // 获取租户信息
    pub fn process_read_tenant(&self, cluster: &str, tenant_name: &str) -> MetaResult<Tenant> {
        let path = KeyPath::tenant(cluster, tenant_name);
        let tenant = match self.get_struct::<Tenant>(&path)? {
            Some(t) => t,
            None => {
                return Err(MetaError::TenantNotFound {
                    tenant: tenant_name.to_string(),
                });
            }
        };
        if !tenant.options().get_tenant_is_hidden() {
            Ok(tenant)
        } else {
            // 如果租户被隐藏的话 会返回notfound
            Err(MetaError::TenantNotFound {
                tenant: tenant_name.to_string(),
            })
        }
    }

    // 获取集群下所有租户
    pub fn process_read_tenants(&self, cluster: &str) -> MetaResult<Vec<Tenant>> {
        let path = KeyPath::tenants(cluster);
        let mut tenants: Vec<Tenant> = self.children_data::<Tenant>(&path)?.into_values().collect();
        tenants.retain(|tenant| !tenant.options().get_tenant_is_hidden());

        Ok(tenants)
    }

    // 获取所有自定义角色
    pub fn process_read_roles(
        &self,
        cluster: &str,
        tenant_name: &str,
    ) -> MetaResult<Vec<CustomTenantRole<Oid>>> {
        let path = KeyPath::roles(cluster, tenant_name);

        let roles: Vec<CustomTenantRole<Oid>> = self
            .children_data::<CustomTenantRole<Oid>>(&path)?
            .into_values()
            .collect();

        Ok(roles)
    }

    // 获取租户下每个用户名 与 角色的关联关系
    pub fn process_read_members(
        &self,
        cluster: &str,
        tenant_name: &str,
    ) -> MetaResult<HashMap<String, TenantRoleIdentifier>> {
        let path = KeyPath::members(cluster, tenant_name);

        let members = self.children_data::<TenantRoleIdentifier>(&path)?;
        let users: HashMap<String, UserDesc> = self
            .children_data::<UserDesc>(&KeyPath::users(cluster))?
            .into_values()
            .map(|desc| (format!("{}", desc.id()), desc))
            .collect();

        trace::trace!("members of path {}: {:?}", path, members);
        trace::trace!("all users: {:?}", users);

        let members: HashMap<String, TenantRoleIdentifier> = members
            .into_iter()
            // id 对应的是路径
            .filter_map(|(id, role)| users.get(&id).map(|e| (e.name().to_string(), role)))
            .collect();

        debug!("returned members of path {}: {:?}", path, members);

        Ok(members)
    }

    pub fn process_read_resourceinfos(
        &self,
        cluster: &str,
        names: &[String],
    ) -> MetaResult<Vec<ResourceInfo>> {
        let path = KeyPath::resourceinfos(cluster, names);
        let resourceinfos: Vec<ResourceInfo> = self
            .children_data::<ResourceInfo>(&path)?
            .into_values()
            .collect();

        Ok(resourceinfos)
    }

    pub fn process_read_resourceinfos_mark(&self, cluster: &str) -> MetaResult<(NodeId, bool)> {
        let path = KeyPath::resourceinfosmark(cluster);
        match self.get_struct::<(NodeId, bool)>(&path)? {
            Some((node_id, is_lock)) => Ok((node_id, is_lock)),
            None => Ok((0, false)),
        }
    }


    // 处理write命令
    pub fn process_write_command(&self, req: &WriteCommand) -> CommandResp {
        debug!("meta process write command {:?}", req);

        match req {
            // 往元数据服务追加一组键值对  其他元数据节点通过监控日记可以自动进行同步
            WriteCommand::Set { key, value } => response_encode(self.process_write_set(key, value)),

            WriteCommand::AddDataNode(cluster, node) => {
                response_encode(self.process_add_date_node(cluster, node))
            }

            // 每个数据节点会上报自己的测量数据 那么在创建数据库时 在分配分片和副本时 就会考虑每个节点此时的状态
            WriteCommand::ReportNodeMetrics(cluster, node_metrics) => {
                response_encode(self.process_add_node_metrics(cluster, node_metrics))
            }

            // 创建db
            WriteCommand::CreateDB(cluster, tenant, schema) => {
                response_encode(self.process_create_db(cluster, tenant, schema))
            }

            // 修改db数据
            WriteCommand::AlterDB(cluster, tenant, schema) => {
                response_encode(self.process_alter_db(cluster, tenant, schema))
            }

            WriteCommand::SetDBIsHidden(cluster, tenant, db, db_is_hidden) => {
                response_encode(self.process_db_is_hidden(cluster, tenant, db, *db_is_hidden))
            }
            WriteCommand::DropDB(cluster, tenant, db_name) => {
                response_encode(self.process_drop_db(cluster, tenant, db_name))
            }
            WriteCommand::DropTable(cluster, tenant, db_name, table_name) => {
                response_encode(self.process_drop_table(cluster, tenant, db_name, table_name))
            }
            WriteCommand::CreateTable(cluster, tenant, schema) => {
                response_encode(self.process_create_table(cluster, tenant, schema))
            }
            WriteCommand::UpdateTable(cluster, tenant, schema) => {
                response_encode(self.process_update_table(cluster, tenant, schema))
            }

            // 数据按照时间戳划分为多个bucket  bucket的信息会记录在元数据服务器中
            WriteCommand::CreateBucket(cluster, tenant, db, ts) => {
                response_encode(self.process_create_bucket(cluster, tenant, db, ts))
            }

            // 删除某个bucket
            WriteCommand::DeleteBucket(cluster, tenant, db, id) => {
                response_encode(self.process_delete_bucket(cluster, tenant, db, *id))
            }
            WriteCommand::CreateUser(cluster, user) => {
                response_encode(self.process_create_user(cluster, user))
            }
            WriteCommand::AlterUser(cluster, name, options) => {
                response_encode(self.process_alter_user(cluster, name, options))
            }
            WriteCommand::RenameUser(cluster, old_name, new_name) => {
                response_encode(self.process_rename_user(cluster, old_name, new_name))
            }
            WriteCommand::DropUser(cluster, name) => {
                response_encode(self.process_drop_user(cluster, name))
            }
            WriteCommand::CreateTenant(cluster, tenant) => {
                response_encode(self.process_create_tenant(cluster, tenant))
            }
            WriteCommand::AlterTenant(cluster, name, options) => {
                response_encode(self.process_alter_tenant(cluster, name, options))
            }
            WriteCommand::SetTenantIsHidden(cluster, name, tenant_is_hidden) => {
                response_encode(self.process_tenant_is_hidden(cluster, name, *tenant_is_hidden))
            }
            WriteCommand::RenameTenant(cluster, old_name, new_name) => {
                response_encode(self.process_rename_tenant(cluster, old_name, new_name))
            }
            WriteCommand::DropTenant(cluster, name) => {
                response_encode(self.process_drop_tenant(cluster, name))
            }
            WriteCommand::AddMemberToTenant(cluster, user_id, role, tenant_name) => {
                response_encode(self.process_add_member_to_tenant(
                    cluster,
                    user_id,
                    role,
                    tenant_name,
                ))
            }
            WriteCommand::RemoveMemberFromTenant(cluster, user_id, tenant_name) => {
                response_encode(self.process_remove_member_to_tenant(cluster, user_id, tenant_name))
            }
            WriteCommand::ReasignMemberRole(cluster, user_id, role, tenant_name) => {
                response_encode(self.process_reasign_member_role(
                    cluster,
                    user_id,
                    role,
                    tenant_name,
                ))
            }

            WriteCommand::CreateRole(cluster, role_name, sys_role, privileges, tenant_name) => {
                response_encode(self.process_create_role(
                    cluster,
                    role_name,
                    sys_role,
                    privileges,
                    tenant_name,
                ))
            }
            WriteCommand::DropRole(cluster, role_name, tenant_name) => {
                response_encode(self.process_drop_role(cluster, role_name, tenant_name))
            }
            WriteCommand::GrantPrivileges(cluster, privileges, role_name, tenant_name) => {
                response_encode(self.process_grant_privileges(
                    cluster,
                    privileges,
                    role_name,
                    tenant_name,
                ))
            }
            WriteCommand::RevokePrivileges(cluster, privileges, role_name, tenant_name) => {
                response_encode(self.process_revoke_privileges(
                    cluster,
                    privileges,
                    role_name,
                    tenant_name,
                ))
            }
            WriteCommand::RetainID(cluster, count) => {
                response_encode(self.process_retain_id(cluster, *count))
            }
            WriteCommand::UpdateVnodeReplSet(args) => {
                response_encode(self.process_update_vnode_repl_set(args))
            }
            WriteCommand::ChangeReplSetLeader(args) => {
                response_encode(self.process_change_repl_set_leader(args))
            }
            WriteCommand::UpdateVnode(args) => response_encode(self.process_update_vnode(args)),
            WriteCommand::LimiterRequest {
                cluster,
                tenant,
                request,
            } => response_encode(self.process_limiter_request(cluster, tenant, request)),
            WriteCommand::ResourceInfo(cluster, names, res_info) => {
                response_encode(self.process_write_resourceinfo(cluster, names, res_info))
            }
            WriteCommand::ResourceInfosMark(cluster, node_id, is_lock) => {
                response_encode(self.process_write_resourceinfos_mark(cluster, *node_id, *is_lock))
            }
        }
    }

    fn process_write_set(&self, key: &str, val: &str) -> MetaResult<()> {
        self.insert(key, val)
    }

    // 更新某个vnode的信息
    fn process_update_vnode(&self, args: &UpdateVnodeArgs) -> MetaResult<()> {
        let key = key_path::KeyPath::tenant_bucket_id(
            &args.cluster,
            &args.vnode_info.tenant,
            &args.vnode_info.db_name,
            args.vnode_info.bucket_id,
        );
        let mut bucket = match self.get_struct::<BucketInfo>(&key)? {
            Some(b) => b,
            None => {
                return Err(MetaError::BucketNotFound {
                    id: args.vnode_info.bucket_id,
                });
            }
        };

        for set in bucket.shard_group.iter_mut() {
            if set.id != args.vnode_info.repl_set_id {
                continue;
            }
            for vnode in set.vnodes.iter_mut() {
                if vnode.id == args.vnode_info.vnode_id {
                    // 更新status
                    vnode.status = args.vnode_info.status;
                    break;
                }
            }
        }

        self.insert(&key, &value_encode(&bucket)?)?;
        Ok(())
    }

    // 更新vnode的副本集
    fn process_update_vnode_repl_set(&self, args: &UpdateVnodeReplSetArgs) -> MetaResult<()> {
        // 先找到副本集相关的bucket
        let key = key_path::KeyPath::tenant_bucket_id(
            &args.cluster,
            &args.tenant,
            &args.db_name,
            args.bucket_id,
        );
        let mut bucket = match self.get_struct::<BucketInfo>(&key)? {
            Some(b) => b,
            None => {
                return Err(MetaError::BucketNotFound { id: args.bucket_id });
            }
        };

        // 遍历分片组
        for set in bucket.shard_group.iter_mut() {
            if set.id != args.repl_id {
                continue;
            }

            // 变更副本集的节点
            for info in args.del_info.iter() {
                set.vnodes
                    .retain(|item| !((item.id == info.id) && (item.node_id == info.node_id)));
            }

            for info in args.add_info.iter() {
                set.vnodes.push(info.clone());
            }

            // process if the leader is deleted....
            // 如果发现leader节点已经下线 进行变更
            if set.vnode(set.leader_vnode_id).is_none() && !set.vnodes.is_empty() {
                set.leader_vnode_id = set.vnodes[0].id;
                set.leader_node_id = set.vnodes[0].node_id;
            }
        }

        // delete the vnodes is empty replication
        // 如果某副本下的节点已经为空 自动从bucket去除
        bucket
            .shard_group
            .retain(|replica| !replica.vnodes.is_empty());

        if bucket.shard_group.is_empty() {
            self.remove(&key)
        } else {
            self.insert(&key, &value_encode(&bucket)?)
        }
    }

    // 变更副本集的leader
    fn process_change_repl_set_leader(&self, args: &ChangeReplSetLeaderArgs) -> MetaResult<()> {
        let key = key_path::KeyPath::tenant_bucket_id(
            &args.cluster,
            &args.tenant,
            &args.db_name,
            args.bucket_id,
        );
        let mut bucket = match self.get_struct::<BucketInfo>(&key)? {
            Some(b) => b,
            None => {
                return Err(MetaError::BucketNotFound { id: args.bucket_id });
            }
        };

        for repl in bucket.shard_group.iter_mut() {
            if repl.id == args.repl_id {
                repl.leader_node_id = args.leader_node_id;
                repl.leader_vnode_id = args.leader_vnode_id;
            }
        }

        self.insert(&key, &value_encode(&bucket)?)?;
        Ok(())
    }

    // 获取当前最新的id  每当创建bucket时会推进id的变化
    fn process_retain_id(&self, cluster: &str, count: u32) -> MetaResult<u32> {
        let id = self.fetch_and_add_incr_id(cluster, count)?;

        Ok(id)
    }

    // 检查node地址是否已经添加到集群中
    fn check_node_ip_address(&self, cluster: &str, node: &NodeInfo) -> MetaResult<bool> {
        for value in self
            .children_data::<NodeInfo>(&KeyPath::data_nodes(cluster))?
            .values()
        {
            if value.id != node.id
                && (value.http_addr == node.http_addr || value.grpc_addr == node.grpc_addr)
            {
                error!(
                    "ip address has been added, add node failed, the added node is : {:?}",
                    value
                );
                return Ok(false);
            }
        }
        Ok(true)
    }

    // 往集群中添加一个数据节点
    fn process_add_date_node(&self, cluster: &str, node: &NodeInfo) -> MetaResult<()> {

        // 地址已经被使用 返回错误信息
        if !self.check_node_ip_address(cluster, node)? {
            return Err(MetaError::DataNodeExist {
                addr: format! {"{} {}", node.grpc_addr,node.http_addr},
            });
        }

        // 生成path 并存储到db中
        let key = KeyPath::data_node_id(cluster, node.id);
        let value = value_encode(node)?;
        // 另外每次写入数据都会产生日志  这个日志应该是用来同步到其他元数据服务器的  环形缓存区则决定了可以同步多少日志 跟redis的副本模块一样
        self.insert(&key, &value)
    }

    // 接收某个节点上报的测量数据
    fn process_add_node_metrics(
        &self,
        cluster: &str,
        node_metrics: &NodeMetrics,  // 节点测量数据
    ) -> MetaResult<()> {
        let key = KeyPath::data_node_metrics(cluster, node_metrics.id);
        let value = value_encode(node_metrics)?;
        self.insert(&key, &value)
    }

    // 删除某个db
    fn process_drop_db(&self, cluster: &str, tenant: &str, db_name: &str) -> MetaResult<()> {
        let key = KeyPath::tenant_db_name(cluster, tenant, db_name);
        let _ = self.remove(&key);

        // 移除db下所有bucket
        let buckets_path = KeyPath::tenant_db_buckets(cluster, tenant, db_name);
        for it in self.children_fullpath(&buckets_path)?.iter() {
            let _ = self.remove(it);
        }

        // 移除db的schema信息
        let schemas_path = KeyPath::tenant_schemas(cluster, tenant, db_name);
        for it in self.children_fullpath(&schemas_path)?.iter() {
            let _ = self.remove(it);
        }

        Ok(())
    }

    // 删除某张表的数据
    fn process_drop_table(
        &self,
        cluster: &str,
        tenant: &str,
        db_name: &str,
        table_name: &str,
    ) -> MetaResult<()> {
        let key = KeyPath::tenant_schema_name(cluster, tenant, db_name, table_name);
        if !self.contains_key(&key)? {
            return Err(MetaError::TableNotFound {
                table: table_name.to_owned(),
            });
        }

        self.remove(&key)
    }

    // 创建数据库
    fn process_create_db(
        &self,
        cluster: &str,
        tenant: &str,
        schema: &DatabaseSchema,  // 数据库级别的schema  实际上是一些数据库opt 不包含字段信息
    ) -> MetaResult<TenantMetaData> {
        let key = KeyPath::tenant_db_name(cluster, tenant, schema.database_name());

        // 表示数据库已经存在
        if self.contains_key(&key)? {
            return Err(MetaError::DatabaseAlreadyExists {
                database: schema.database_name().to_string(),
            });
        }

        // 检查数据库的schema是否有效    数据库在创建时应该还没有指定存储数据的节点的
        self.check_db_schema_valid(cluster, schema)?;
        // 当insert后 同样会同步到follower节点
        self.insert(&key, &value_encode(schema)?)?;

        // 返回最新的租户信息
        self.to_tenant_meta_data(cluster, tenant)
    }

    // 修改db数据
    fn process_alter_db(
        &self,
        cluster: &str,
        tenant: &str,
        schema: &DatabaseSchema,
    ) -> MetaResult<TenantMetaData> {
        let key = KeyPath::tenant_db_name(cluster, tenant, schema.database_name());
        if !self.contains_key(&key)? {
            return Err(MetaError::DatabaseNotFound {
                database: schema.database_name().to_string(),
            });
        }

        self.check_db_schema_valid(cluster, schema)?;
        self.insert(&key, &value_encode(schema)?)?;

        self.to_tenant_meta_data(cluster, tenant)
    }

    // 修改db的hidden属性
    fn process_db_is_hidden(
        &self,
        cluster: &str,
        tenant: &str,
        db: &str,
        db_is_hidden: bool,
    ) -> MetaResult<TenantMetaData> {
        let key = KeyPath::tenant_db_name(cluster, tenant, db);
        if let Some(mut db_schema) = self.get_struct::<DatabaseSchema>(&key)? {
            db_schema.config.set_db_is_hidden(db_is_hidden);
            self.insert(&key, &value_encode(&db_schema)?)?;
            self.to_tenant_meta_data(cluster, tenant)
        } else {
            Err(MetaError::DatabaseNotFound {
                database: db.to_string(),
            })
        }
    }


    // 检查数据库schema是否有效
    fn check_db_schema_valid(&self, cluster: &str, db_schema: &DatabaseSchema) -> MetaResult<()> {
        // 获取当前集群中所有可用的节点
        let node_list = self.get_valid_node_list(cluster)?;
        // 检查节点数量是否足够  不考虑分片数量吗?
        check_node_enough(db_schema.config.replica_or_default(), &node_list)?;

        // 分片数量异常
        if db_schema.config.shard_num_or_default() == 0 {
            return Err(MetaError::DatabaseSchemaInvalid {
                name: db_schema.database_name().to_string(),
            });
        }

        Ok(())
    }

    // 创建某张表
    fn process_create_table(
        &self,
        cluster: &str,
        tenant: &str,
        schema: &TableSchema,   // 表相关的schema信息
    ) -> MetaResult<TenantMetaData> {
        // 先定位到db
        let key = KeyPath::tenant_db_name(cluster, tenant, schema.db());
        if !self.contains_key(&key)? {
            return Err(MetaError::DatabaseNotFound {
                database: schema.db().to_string(),
            });
        }
        // 在db下创建table
        let key = KeyPath::tenant_schema_name(cluster, tenant, schema.db(), schema.name());
        if self.contains_key(&key)? {
            return Err(MetaError::TableAlreadyExists {
                table_name: schema.name().to_string(),
            });
        }

        self.insert(&key, &value_encode(schema)?)?;

        self.to_tenant_meta_data(cluster, tenant)
    }

    // 处理更新表的请求
    fn process_update_table(
        &self,
        cluster: &str,
        tenant: &str,
        schema: &TableSchema,
    ) -> MetaResult<()> {
        // 查询获得table信息
        let key = KeyPath::tenant_schema_name(cluster, tenant, schema.db(), schema.name());
        if let Some(val) = self.get_struct::<TableSchema>(&key)? {
            match (val, schema) {
                // 外部表不支持更新 仅支持更新 TsKvTableSchema   stream表也不支持更新
                (TableSchema::TsKvTableSchema(val), TableSchema::TsKvTableSchema(schema)) => {
                    if val.schema_id + 1 != schema.schema_id {
                        return Err(MetaError::UpdateTableConflict {
                            name: schema.name.clone(),
                        });
                    }
                }
                _ => {
                    return Err(MetaError::NotSupport {
                        msg: "update external table".to_string(),
                    });
                }
            }
        }

        self.insert(&key, &value_encode(schema)?)?;
        Ok(())
    }

    // 获取所有有效的节点
    fn get_valid_node_list(&self, cluster: &str) -> MetaResult<Vec<NodeInfo>> {
        // 先获取所有节点
        let node_info_list: Vec<NodeInfo> = self
            .children_data::<NodeInfo>(&KeyPath::data_nodes(cluster))?
            .into_values()
            .collect();

        // 获取节点的指标
        let node_metrics_list: HashMap<NodeId, NodeMetrics> = self
            .children_data::<NodeMetrics>(&KeyPath::data_nodes_metrics(cluster))?
            .into_values()
            .map(|m| (m.id, m))
            .collect();

        // 根据指标信息 获取健康的节点
        let node_info_list = node_info_list
            .into_iter()
            .filter_map(|n| node_metrics_list.get(&n.id).map(|m| (n, m)))
            .filter(|(_, m)| m.is_healthy())
            .collect::<Vec<_>>();

        // 先检查热点节点
        let temp_node_info_list = node_info_list
            .iter()
            .filter(|(n, _)| !n.is_cold())
            .cloned()
            .collect::<Vec<_>>();

        // 优选热点节点
        let mut res = if temp_node_info_list.is_empty() {
            node_info_list
        } else {
            temp_node_info_list
        };

        // 根据磁盘的空闲量排序
        res.sort_by_key(|(_, m)| Reverse(m.disk_free));
        // 这个节点数量不一定够的 (与数据库的分片/副本数比较)
        let res = res.into_iter().map(|(n, _)| n).collect();
        Ok(res)
    }

    // 给某个db创建时间桶  作为时序数据库  一定有什么跟时间相关的优化措施  就比如时间桶
    fn process_create_bucket(
        &self,
        cluster: &str,
        tenant: &str,
        db: &str,
        ts: &i64,
    ) -> MetaResult<TenantMetaData> {
        let db_path = KeyPath::tenant_db_name(cluster, tenant, db);
        // 该数据库下所有的buckets
        let buckets = self.children_data::<BucketInfo>(&(db_path.clone() + "/buckets"))?;

        // 代表已经存在该时间相关的桶了
        for (_, val) in buckets.iter() {
            if *ts >= val.start_time && *ts < val.end_time {
                return self.to_tenant_meta_data(cluster, tenant);
            }
        }

        // 找到数据库
        let db_schema =
            self.get_struct::<DatabaseSchema>(&db_path)?
                .ok_or(MetaError::DatabaseNotFound {
                    database: db.to_string(),
                })?;

        // 获取所有可用的节点
        let node_list = self.get_valid_node_list(cluster)?;
        check_node_enough(db_schema.config.replica_or_default(), &node_list)?;

        // 验证db配置有效性
        if db_schema.config.shard_num_or_default() == 0 {
            return Err(MetaError::DatabaseSchemaInvalid {
                name: db.to_string(),
            });
        }

        // 计算当前存在数据的最小时间戳 (通过ttl推算) 如果比最小时间还小 就无法创建bucket
        if *ts < db_schema.time_to_expired() {
            return Err(MetaError::NotSupport {
                msg: "create expired bucket".to_string(),
            });
        }

        // 生成bucket
        let mut bucket = BucketInfo {
            id: self.fetch_and_add_incr_id(cluster, 1)?,
            start_time: 0,
            end_time: 0,
            shard_group: vec![],
        };

        // 计算分片的时间范围
        (bucket.start_time, bucket.end_time) = get_time_range(
            *ts,
            db_schema
                .config
                .vnode_duration_or_default()
                .to_precision(*db_schema.config.precision_or_default()),
        );
        // 之前看到创建db/table都没有进行副本/分片的操作 只是做了基础校验  当创建bucket时 才考虑到这些
        // 这样看来只有创建了bucket 才能开始存入数据?
        let (group, used) = allocation_replication_set(
            node_list,
            db_schema.config.shard_num_or_default() as u32,
            db_schema.config.replica_or_default() as u32,
            bucket.id + 1,
        );

        // 该bucket 对应的分片组 每个元素是一个副本集
        bucket.shard_group = group;
        // used 代表总共增加了多少计数 (副本*分片)   所以每个分片每个副本集 都是有一个唯一id的
        self.fetch_and_add_incr_id(cluster, used)?;

        // 将bucket存入租户中
        let key = KeyPath::tenant_bucket_id(cluster, tenant, db, bucket.id);
        self.insert(&key, &value_encode(&bucket)?)?;

        self.to_tenant_meta_data(cluster, tenant)
    }

    // 删除
    fn process_delete_bucket(
        &self,
        cluster: &str,
        tenant: &str,
        db: &str,
        id: u32,
    ) -> MetaResult<()> {
        // 拼接生成bucket的路径
        let key = KeyPath::tenant_bucket_id(cluster, tenant, db, id);
        self.remove(&key)
    }

    // 创建用户
    fn process_create_user(&self, cluster: &str, user_desc: &UserDesc) -> MetaResult<()> {
        let key = KeyPath::user(cluster, user_desc.name());

        if self.contains_key(&key)? {
            return Err(MetaError::UserAlreadyExists {
                user: user_desc.name().to_string(),
            });
        }

        self.insert(&key, &value_encode(&user_desc)?)?;
        Ok(())
    }

    // 处理修改用户的请求
    fn process_alter_user(
        &self,
        cluster: &str,
        user_name: &str,
        user_options: &UserOptions,
    ) -> MetaResult<()> {
        let key = KeyPath::user(cluster, user_name);
        if let Some(old_user_desc) = self.get_struct::<UserDesc>(&key)? {
            let old_options = old_user_desc.options().to_owned();
            let new_options = user_options.clone().merge(old_options);

            let new_user_desc = UserDesc::new(
                *old_user_desc.id(),
                user_name.to_string(),
                new_options,
                old_user_desc.is_root_admin(),
            );

            Ok(self.insert(&key, &value_encode(&new_user_desc)?)?)
        } else {
            Err(MetaError::UserNotFound {
                user: user_name.to_string(),
            })
        }
    }

    fn process_rename_user(
        &self,
        _cluster: &str,
        _old_name: &str,
        _new_name: &str,
    ) -> MetaResult<()> {
        Err(MetaError::NotSupport {
            msg: "rename user".to_string(),
        })
    }

    fn process_drop_user(&self, cluster: &str, user_name: &str) -> MetaResult<bool> {
        let key = KeyPath::user(cluster, user_name);

        if self.contains_key(&key)? {
            self.remove(&key)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn set_tenant_limiter(
        &self,
        cluster: &str,
        tenant: &str,
        limiter: Option<RemoteRequestLimiter>,
    ) -> MetaResult<()> {
        let key = KeyPath::limiter(cluster, tenant);

        let limiter = match limiter {
            Some(limiter) => limiter,
            None => {
                return self.remove(&key);
            }
        };

        self.insert(&key, &value_encode(&limiter)?)
    }

    // 创建租户
    fn process_create_tenant(&self, cluster: &str, tenant: &Tenant) -> MetaResult<()> {
        let key = KeyPath::tenant(cluster, tenant.name());

        if self.contains_key(&key)? {
            return Err(MetaError::TenantAlreadyExists {
                tenant: tenant.name().to_string(),
            });
        }

        let limiter = tenant
            .options()
            .request_config()
            .map(RemoteRequestLimiter::new);

        // 更新限制器
        self.set_tenant_limiter(cluster, tenant.name(), limiter)?;

        self.insert(&key, &value_encode(&tenant)?)?;

        Ok(())
    }

    // 更新租户信息
    fn process_alter_tenant(
        &self,
        cluster: &str,
        name: &str,
        options: &TenantOptions,
    ) -> MetaResult<Tenant> {
        let key = KeyPath::tenant(cluster, name);
        if let Some(tenant) = self.get_struct::<Tenant>(&key)? {
            if !tenant.options().get_tenant_is_hidden() {
                let new_tenant = Tenant::new(*tenant.id(), name.to_string(), options.to_owned());
                self.insert(&key, &value_encode(&new_tenant)?)?;

                let limiter = options.request_config().map(RemoteRequestLimiter::new);

                self.set_tenant_limiter(cluster, name, limiter)?;

                Ok(new_tenant)
            } else {
                Err(MetaError::TenantNotFound {
                    tenant: name.to_string(),
                })
            }
        } else {
            Err(MetaError::TenantNotFound {
                tenant: name.to_string(),
            })
        }
    }

    // 设置租户的隐藏方式
    fn process_tenant_is_hidden(
        &self,
        cluster: &str,
        name: &str,
        tenant_is_hidden: bool,
    ) -> MetaResult<Tenant> {
        let key = KeyPath::tenant(cluster, name);
        if let Some(tenant) = self.get_struct::<Tenant>(&key)? {
            let mut options = tenant.options().clone();
            options.set_tenant_is_hidden(tenant_is_hidden);
            let new_tenant = Tenant::new(*tenant.id(), name.to_string(), options.to_owned());
            self.insert(&key, &value_encode(&new_tenant)?)?;
            let limiter = options.request_config().map(RemoteRequestLimiter::new);

            self.set_tenant_limiter(cluster, name, limiter)?;
            Ok(new_tenant)
        } else {
            Err(MetaError::TenantNotFound {
                tenant: name.to_string(),
            })
        }
    }

    fn process_rename_tenant(
        &self,
        _cluster: &str,
        _old_name: &str,
        _new_name: &str,
    ) -> MetaResult<()> {
        Err(MetaError::NotSupport {
            msg: "rename tenant".to_string(),
        })
    }

    // 删除某个租户
    fn process_drop_tenant(&self, cluster: &str, name: &str) -> MetaResult<()> {
        // remove members in the tenant
        // 获取该租户下所有成员信息
        let members = self.process_read_members(cluster, name)?;
        // 找到所有匹配的user 并删除
        let mut users = self.process_read_users(cluster)?;
        users.retain(|user| members.iter().any(|member| user.name() == member.0));
        for user in users {
            self.process_remove_member_to_tenant(cluster, user.id(), name)?;
        }

        // drop role in the tenant
        let roles = self.process_read_roles(cluster, name)?;
        for role in roles {
            self.process_drop_role(cluster, role.name(), name)?;
        }

        // drop tenant meta
        // 删除租户 已经相关的limiter
        let key = KeyPath::tenant(cluster, name);
        let limiter_key = KeyPath::limiter(cluster, name);

        self.remove(&key)?;
        self.remove(&limiter_key)?;

        Ok(())
    }

    fn process_add_member_to_tenant(
        &self,
        cluster: &str,
        user_id: &Oid,
        role: &TenantRoleIdentifier,
        tenant_name: &str,
    ) -> MetaResult<()> {
        let key = KeyPath::member(cluster, tenant_name, user_id);

        if self.contains_key(&key)? {
            return Err(MetaError::UserAlreadyExists {
                user: user_id.to_string(),
            });
        }

        self.insert(&key, &value_encode(&role)?)
    }

    // 从某个租户下删除某个用户
    fn process_remove_member_to_tenant(
        &self,
        cluster: &str,
        user_id: &Oid,
        tenant_name: &str,
    ) -> MetaResult<()> {
        let key = KeyPath::member(cluster, tenant_name, user_id);

        if self.contains_key(&key)? {
            self.remove(&key)?;

            Ok(())
        } else {
            Err(MetaError::UserNotFound {
                user: user_id.to_string(),
            })
        }
    }

    fn process_reasign_member_role(
        &self,
        cluster: &str,
        user_id: &Oid,
        role: &TenantRoleIdentifier,
        tenant_name: &str,
    ) -> MetaResult<()> {
        let key = KeyPath::member(cluster, tenant_name, user_id);

        if !self.contains_key(&key)? {
            return Err(MetaError::UserNotFound {
                user: user_id.to_string(),
            });
        }

        self.insert(&key, &value_encode(&role)?)
    }

    fn process_create_role(
        &self,
        cluster: &str,
        role_name: &str,
        sys_role: &SystemTenantRole,
        privileges: &HashMap<String, DatabasePrivilege>,
        tenant_name: &str,
    ) -> MetaResult<()> {
        let key = KeyPath::role(cluster, tenant_name, role_name);

        if self.contains_key(&key)? {
            return Err(MetaError::RoleAlreadyExists {
                role: role_name.to_string(),
            });
        }

        let oid = UuidGenerator::default().next_id();
        let role = CustomTenantRole::new(
            oid,
            role_name.to_string(),
            sys_role.clone(),
            privileges.clone(),
        );

        self.insert(&key, &value_encode(&role)?)
    }

    // 删除租户下某个角色
    fn process_drop_role(
        &self,
        cluster: &str,
        role_name: &str,
        tenant_name: &str,
    ) -> MetaResult<bool> {
        let key = KeyPath::role(cluster, tenant_name, role_name);

        if !self.contains_key(&key)? {
            return Err(MetaError::RoleNotFound {
                role: role_name.to_string(),
            });
        }

        self.remove(&key)?;
        Ok(true)
    }

    // 为某个角色赋予权限
    fn process_grant_privileges(
        &self,
        cluster: &str,
        privileges: &[(DatabasePrivilege, String)],
        role_name: &str,
        tenant_name: &str,
    ) -> MetaResult<()> {
        let key = KeyPath::role(cluster, tenant_name, role_name);
        if let Some(mut role) = self.get_struct::<CustomTenantRole<Oid>>(&key)? {
            for (privilege, database_name) in privileges {
                // 给role追加权限
                let _ = role.grant_privilege(database_name.clone(), privilege.clone());
            }

            Ok(self.insert(&key, &value_encode(&role)?)?)
        } else {
            Err(MetaError::RoleNotFound {
                role: role_name.to_string(),
            })
        }
    }

    fn process_revoke_privileges(
        &self,
        cluster: &str,
        privileges: &[(DatabasePrivilege, String)],
        role_name: &str,
        tenant_name: &str,
    ) -> MetaResult<()> {
        let key = KeyPath::role(cluster, tenant_name, role_name);
        if let Some(mut role) = self.get_struct::<CustomTenantRole<Oid>>(&key)? {
            for (privilege, database_name) in privileges {
                let _ = role.revoke_privilege(database_name, privilege);
            }

            Ok(self.insert(&key, &value_encode(&role)?)?)
        } else {
            Err(MetaError::RoleNotFound {
                role: role_name.to_string(),
            })
        }
    }

    // 当本地限流器permit不足时 会从元数据服务请求集群级别的permit
    fn process_limiter_request(
        &self,
        cluster: &str,
        tenant: &str,
        requests: &LocalBucketRequest,
    ) -> MetaResult<LocalBucketResponse> {
        let mut rsp = LocalBucketResponse {
            kind: requests.kind,
            alloc: requests.expected.max,
            remote_remain: -1,
        };
        let key = KeyPath::limiter(cluster, tenant);

        let limiter = match self.get_struct::<RemoteRequestLimiter>(&key)? {
            Some(b) => b,
            None => {
                return Ok(rsp);
            }
        };

        let bucket = match limiter.buckets.get(&requests.kind) {
            Some(bucket) => bucket,
            None => {
                return Ok(rsp);
            }
        };
        // 尝试获取permit
        let alloc = bucket.acquire_closed(requests.expected.max as usize);

        rsp.alloc = alloc as i64;
        rsp.remote_remain = bucket.balance() as i64;
        // permit数量发生了变化 所以要更新limiter
        self.set_tenant_limiter(cluster, tenant, Some(limiter))?;

        Ok(rsp)
    }

    fn process_write_resourceinfo(
        &self,
        cluster: &str,
        names: &[String],
        res_info: &ResourceInfo,
    ) -> MetaResult<()> {
        let key = KeyPath::resourceinfos(cluster, names);
        self.insert(&key, &value_encode(&res_info)?)
    }

    fn process_write_resourceinfos_mark(
        &self,
        cluster: &str,
        node_id: NodeId,
        is_lock: bool,
    ) -> MetaResult<()> {
        let key = KeyPath::resourceinfosmark(cluster);
        self.insert(&key, &value_encode(&(node_id, is_lock))?)
    }
}

// 判断节点数量是否足够
fn check_node_enough(need: u64, node_list: &[NodeInfo]) -> MetaResult<()> {
    if need > node_list.len() as u64 {
        return Err(MetaError::ValidNodeNotEnough {
            need,
            valid_node_num: node_list.len() as u32,
        });
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;
    use std::println;

    use serde::{Deserialize, Serialize};

    #[test]
    fn test_btree_map() {
        let mut map = BTreeMap::new();
        map.insert("/root/tenant".to_string(), "tenant_v".to_string());
        map.insert("/root/tenant/db1".to_string(), "123_v".to_string());
        map.insert("/root/tenant/db2".to_string(), "456_v".to_string());
        map.insert("/root/tenant/db1/".to_string(), "123/_v".to_string());
        map.insert("/root/tenant/db1/table1".to_string(), "123_v".to_string());
        map.insert("/root/tenant/123".to_string(), "123_v".to_string());
        map.insert("/root/tenant/456".to_string(), "456_v".to_string());

        let begin = "/root/tenant/".to_string();
        let end = "/root/tenant/|".to_string();
        for (key, value) in map.range(begin..end) {
            println!("{key}  : {value}");
        }
    }

    //{"Set":{"key":"foo","value":"bar111"}}
    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct Command1 {
        id: u32,
        name: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct Command2 {
        id: u32,
        name: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub enum Command {
        // Test1 { id: u32, name: String },
        // Test2 { id: u32, name: String },
        Test1(Command1),
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct RequestCommand {
        key: String,
        value: String,
    }

    #[test]
    fn test_json() {
        let command = RequestCommand {
            key: "xxxxxxxk".to_string(),
            value: "xxxxxxxv".to_string(),
        };
        let data = serde_json::to_string(&command).unwrap();
        println!("{}", data);

        let cmd = Command::Test1(Command1 {
            id: 100,
            name: "test".to_string(),
        });

        let str = serde_json::to_vec(&cmd).unwrap();
        print!("\n1 === {}=== \n", String::from_utf8(str).unwrap());

        let str = serde_json::to_string(&cmd).unwrap();
        print!("\n2 === {}=== \n", str);

        let tup = ("test1".to_string(), "test2".to_string());
        let str = serde_json::to_string(&tup).unwrap();
        print!("\n3 === {}=== \n", str);

        let str = serde_json::to_string(&"xxx".to_string()).unwrap();
        print!("\n4 === {}=== \n", str);
    }
}
