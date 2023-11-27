#![allow(clippy::field_reassign_with_default)]

use std::collections::{HashMap, HashSet};

use models::auth::privilege::DatabasePrivilege;
use models::auth::role::{SystemTenantRole, TenantRoleIdentifier};
use models::auth::user::{UserDesc, UserOptions};
use models::meta_data::*;
use models::oid::Oid;
use models::schema::{DatabaseSchema, ResourceInfo, TableSchema, Tenant, TenantOptions};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use super::key_path::KeyPath;
use crate::limiter::local_request_limiter::LocalBucketRequest;

// 更新vnode的副本集参数
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UpdateVnodeReplSetArgs {

    // 关于哪个集群
    pub cluster: String,
    pub tenant: String,
    pub db_name: String,
    pub bucket_id: u32,
    // 副本集id (每个分片对应一个副本集)
    pub repl_id: u32,
    // 要删除的节点
    pub del_info: Vec<VnodeInfo>,
    // 要增加的节点
    pub add_info: Vec<VnodeInfo>,
}

// 更新副本集的leader
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ChangeReplSetLeaderArgs {
    // 发现前面几个参数都是一样的
    pub cluster: String,
    pub tenant: String,
    pub db_name: String,
    pub bucket_id: u32,
    pub repl_id: u32,

    // 更新的leader节点id
    pub leader_node_id: NodeId,
    pub leader_vnode_id: VnodeId,
}

// 更新某个vnode的信息  推测node是物理节点  vnode是将某个副本模拟成node
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UpdateVnodeArgs {
    pub cluster: String,
    pub vnode_info: VnodeAllInfo,
}

/******************* write command *************************/
// 在 StateMachine 中可以接受命令
// 注意 StateMachine 是元数据服务器所有 所以相当于节点本身可以处理下面的请求
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WriteCommand {
    // retain increment id  cluster, count
    RetainID(String, u32),

    // 更新副本集
    UpdateVnodeReplSet(UpdateVnodeReplSetArgs),

    // 修改副本集的leader
    ChangeReplSetLeader(ChangeReplSetLeaderArgs),

    // 更新某个vnode的信息
    UpdateVnode(UpdateVnodeArgs),

    // cluster, node info
    // 往集群中添加一个节点
    AddDataNode(String, NodeInfo),

    //cluster, node metrics
    // 某个节点将自己的统计数据上报给元数据节点
    ReportNodeMetrics(String, NodeMetrics),

    // cluster, tenant, db schema
    // 元数据节点收到一个创建db的请求  也就是cnosdb收到创建db的请求后 应该是转发到这里?
    CreateDB(String, String, DatabaseSchema),

    // cluster, tenant, db schema
    // 修改某个db信息
    AlterDB(String, String, DatabaseSchema),

    // cluster, tenant, db, db_is_hidden
    // 将db修改成隐藏状态
    SetDBIsHidden(String, String, String, bool),

    // cluster, tenant, db name
    DropDB(String, String, String),

    // cluster, tenant, db name, timestamp
    // 在某个db下创建一个bucket 还要指定时间戳
    CreateBucket(String, String, String, i64),

    // cluster, tenant, db name, id
    // 删除某个bucket
    DeleteBucket(String, String, String, u32),

    // cluster, tenant, table schema
    // table相关的命令
    CreateTable(String, String, TableSchema),
    UpdateTable(String, String, TableSchema),
    // cluster, tenant, db name, table name
    DropTable(String, String, String, String),

    // 用户相关的命令
    // cluster, user_name, user_options, is_admin
    CreateUser(String, UserDesc),
    // cluster, user_id, user_options
    AlterUser(String, String, UserOptions),
    // cluster, old_name, new_name
    RenameUser(String, String, String),
    // cluster, user_name
    DropUser(String, String),

    // 租户相关的命令
    // cluster, tenant_name, tenant_options
    CreateTenant(String, Tenant),
    // cluster, tenant_name, tenant_options
    AlterTenant(String, String, TenantOptions),
    // cluster, tenant_name, tenant_is_hidden
    SetTenantIsHidden(String, String, bool),
    // cluster, old_name, new_name
    RenameTenant(String, String, String),
    // cluster, tenant_name
    DropTenant(String, String),

    // member相关的命令
    // cluster, user_id, role, tenant_name
    AddMemberToTenant(String, Oid, TenantRoleIdentifier, String),
    // cluster, user_id, tenant_name
    RemoveMemberFromTenant(String, Oid, String),
    // cluster, user_id, role, tenant_name
    ReasignMemberRole(String, Oid, TenantRoleIdentifier, String),

    // 为租户创建角色 并且还有该角色拥有的特权
    // cluster, role_name, sys_role, privileges, tenant_name
    CreateRole(
        String,
        String,
        SystemTenantRole,
        HashMap<String, DatabasePrivilege>,
        String,
    ),
    // cluster, role_name, tenant_name
    DropRole(String, String, String),

    // 赋予角色权限 和撤销权限

    // cluster, privileges, role_name, tenant_name
    GrantPrivileges(String, Vec<(DatabasePrivilege, String)>, String, String),
    // cluster, privileges, role_name, tenant_name
    RevokePrivileges(String, Vec<(DatabasePrivilege, String)>, String, String),

    // 往元数据中添加 key/value
    Set {
        key: String,
        value: String,
    },
    // cluster, tenant, requests
    // 请求获取permit
    LimiterRequest {
        cluster: String,
        tenant: String,
        request: LocalBucketRequest,
    },
    // cluster, [tenant, db, table,...], ResourceInfo
    // 对资源发起操作
    ResourceInfo(String, Vec<String>, ResourceInfo),
    // cluster, node_id, is_lock
    // 描述某个节点是否被锁定
    ResourceInfosMark(String, NodeId, bool),
}

/******************* read command *************************/
// 上面的都是修改命令 元数据服务器还提供查询能力
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ReadCommand {

    // 查询某个集群下所有节点
    DataNodes(String),              //cluster

    // 查询集群，租户下所有数据
    TenaneMetaData(String, String), // cluster tenant

    // 获取某节点的测量数据
    NodeMetrics(String), //cluster

    // cluster, role_name, tenant_name
    CustomRole(String, String, String),
    // cluster, tenant_name
    CustomRoles(String, String),
    // cluster, tenant_name, user_id
    MemberRole(String, String, Oid),
    // cluster, tenant_name
    Members(String, String),
    // cluster, user_name
    User(String, String),
    // cluster
    Users(String),
    // cluster, tenant_name
    Tenant(String, String),
    // cluster
    Tenants(String),
    // cluster, tenant, db, table
    TableSchema(String, String, String, String),
    // cluster, [tenant, db, table,...]
    ResourceInfos(String, Vec<String>),
    // cluster
    ResourceInfosMark(String),
}

pub const ENTRY_LOG_TYPE_SET: i32 = 1;
pub const ENTRY_LOG_TYPE_DEL: i32 = 2;
pub const ENTRY_LOG_TYPE_NOP: i32 = 10;

// 日志数据
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct EntryLog {
    // 日志类型
    pub tye: i32,
    // 应该是序列号
    pub ver: u64,

    // 一组kv数据

    /// store mache key
    pub key: String,
    /// store mache val
    pub val: String,
}

// 代表这组日志处于被监控状态
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct WatchData {
    // 当没有监听到对应的日志时 代表与元数据服务的数据差距较大了 需要全量更新
    pub full_sync: bool,
    // 这组日志最小序号和最大序号
    pub min_ver: u64,
    pub max_ver: u64,
    // 一组日志对象
    pub entry_logs: Vec<EntryLog>,
}

impl WatchData {
    pub fn need_return(&self, base_ver: u64) -> bool {
        if self.full_sync {
            return true;
        }

        if !self.entry_logs.is_empty() {
            return true;
        }

        if base_ver + 100 < self.max_ver {
            return true;
        }

        false
    }
}

// 环形buffer
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct CircleBuf {
    // 代表未读数据
    count: usize,
    // 代表写指针
    writer: usize,
    // 总大小
    capacity: usize,
    // buf中就是存储一组日志
    buf: Vec<EntryLog>,
}

impl CircleBuf {

    // 初始化环形缓冲区
    pub fn new(capacity: usize) -> Self {
        let mut buf = Vec::new();
        buf.resize(capacity, EntryLog::default());

        Self {
            buf,
            count: 0,
            writer: 0,
            capacity,
        }
    }

    // 添加数据
    pub fn append(&mut self, log: EntryLog) {
        self.buf[self.writer] = log;

        self.writer += 1;

        // 每当写到末尾时 下次就会覆盖旧数据
        if self.writer == self.capacity {
            self.writer = 0;
        }

        if self.count < self.capacity {
            self.count += 1;
        }
    }

    pub fn is_empty(&self) -> bool {
        if self.count == 0 {
            return true;
        }

        false
    }

    // 获取第一个数据的序列号
    pub fn min_version(&self) -> Option<u64> {
        if self.is_empty() {
            return None;
        }

        let index = if self.count == self.capacity {
            self.writer
        } else {
            0
        };

        Some(self.buf[index].ver)
    }

    // 获取最新数据的序列号
    pub fn max_version(&self) -> Option<u64> {
        if self.is_empty() {
            return None;
        }

        let index = if self.writer == 0 {
            self.capacity - 1
        } else {
            self.writer - 1
        };

        Some(self.buf[index].ver)
    }

    // -1: the logs is empty
    // -2: min version < ver
    // 找到序列号为 ver的 日志的下标
    pub fn find_index(&self, ver: u64) -> i32 {
        if self.is_empty() {
            return -1;
        }

        let mut index = self.writer;

        // 在循环内 index 会递减 从大往小去匹配
        for _ in 0..self.count {
            index = if index == 0 {
                self.capacity - 1
            } else {
                index - 1
            };

            if self.buf[index].ver <= ver {
                return index as i32;
            }
        }

        -2
    }

    // 从index开始读取 将满足条件的实体返回
    pub fn read_entrys<F>(&self, filter: F, index: usize) -> Vec<EntryLog>
    where
        F: Fn(&EntryLog) -> bool,
    {
        let mut entrys = vec![];

        let mut index = (index + 1) % self.capacity;
        while index != self.writer {
            let entry = &self.buf[index];
            if filter(entry) {
                entrys.push(entry.clone());
            }

            index = (index + 1) % self.capacity;
        }

        entrys
    }
}

// 可以监控一组日志
pub struct Watch {
    // 代表被监控的日志 同时有一个capacity
    pub logs: RwLock<CircleBuf>,
    // 通过该对象可以进行通知
    pub sender: broadcast::Sender<()>,
}

impl Watch {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(1);
        Self {
            sender,
            logs: RwLock::new(CircleBuf::new(8 * 1024)),
        }
    }

    // 每当写入日志时 就通过sender进行通知
    pub fn writer_log(&self, log: EntryLog) {
        self.logs.write().append(log);

        let _ = self.sender.send(());
    }

    // 可以通过订阅方法 产生一个receiver
    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.sender.subscribe()
    }

    pub fn min_version(&self) -> Option<u64> {
        self.logs.read().min_version()
    }
    pub fn max_version(&self) -> Option<u64> {
        self.logs.read().max_version()
    }

    // -1: the logs is empty
    // -2: min version < ver
    // 检索符合条件的log
    pub fn read_entry_logs(
        &self,
        cluster: &str,
        tenants: &HashSet<String>,  // 代表要检索这些租户的日志
        base_ver: u64,
    ) -> (Vec<EntryLog>, i32) {
        let filter = |entry: &EntryLog| -> bool {
            // 不是这些日志
            if entry.key == KeyPath::version()
                || entry.key == KeyPath::already_init()
                || entry.key == KeyPath::incr_id(cluster)
            {
                return false;
            }

            if !entry.key.starts_with(&KeyPath::cluster_prefix(cluster)) {
                return false;
            }

            // 这里写错了吧?
            if !entry.key.starts_with(&KeyPath::tenants(cluster)) {
                return true;
            }

            // 这是无效参数 无法命中任何数据
            if tenants.is_empty() {
                return false;
            }

            // 代表支持所有租户
            if tenants.contains(&"".to_string()) {
                return true;
            }

            // 命中租户
            let prefix = KeyPath::tenants(cluster);
            if let Some(sub_str) = entry.key.strip_prefix(&prefix) {
                if let Some((tenant, _)) = sub_str.split_once('/') {
                    if tenants.contains(tenant) {
                        return true;
                    }
                }
            }

            false
        };

        self.read_start_version(filter, base_ver)
    }

    // 从目标序列号开始 查询满足条件的所有日志
    fn read_start_version<F>(&self, filter: F, base_ver: u64) -> (Vec<EntryLog>, i32)
    where
        F: Fn(&EntryLog) -> bool,
    {
        let logs = self.logs.read();
        // 代表该序列号的日志不存在
        let index = logs.find_index(base_ver);
        if index < 0 {
            return (vec![], index);
        }

        (logs.read_entrys(filter, index as usize), 0)
    }
}

impl Default for Watch {
    fn default() -> Self {
        Self::new()
    }
}
