use std::fs::File;
use std::io::prelude::Read;
use std::path::Path;

use config::LogConfig;
use serde::{Deserialize, Serialize};

// 元数据模块的配置信息
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaInit {
    // 每个元数据服务器 与一个集群关联
    pub cluster_name: String,
    // 集群管理员账号
    pub admin_user: String,
    // 系统租户
    pub system_tenant: String,
    // 默认创建的数据库
    pub default_database: Vec<String>,
}

impl Default for MetaInit {
    fn default() -> Self {
        Self {
            cluster_name: String::from("cluster_xxx"),
            admin_user: String::from("root"),
            system_tenant: String::from("cnosdb"),
            default_database: vec![String::from("public"), String::from("usage_schema")],
        }
    }
}

impl MetaInit {
    pub fn default_db_config(tenant: &str, db: &str) -> String {
        format!(
            "{{\"tenant\":\"{}\",\"database\":\"{}\",\"config\":{{\"ttl\":null,\"shard_num\":null,\"vnode_duration\":null,\"replica\":null,\"precision\":null,\"db_is_hidden\":false}}}}",
            tenant, db
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeartBeatConfig {
    pub heartbeat_recheck_interval: u64,
    pub heartbeat_expired_interval: u64,
}

impl Default for HeartBeatConfig {
    fn default() -> Self {
        Self {
            // 应该是代表续约时间为 300 秒 而一旦超过600秒还没有续约 就可以认为节点下线了
            heartbeat_recheck_interval: 300,
            heartbeat_expired_interval: 600,
        }
    }
}


// 代表元数据服务器的相关配置
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Opt {
    // 服务器id
    pub id: u64,
    pub host: String,
    pub port: u16,
    // 存储元数据的地址
    pub data_path: String,
    // 日记信息
    pub log: LogConfig,
    // 元数据的描述信息
    pub meta_init: MetaInit,
    // 元数据需要一个心跳配置
    pub heartbeat: HeartBeatConfig,
}

impl Default for Opt {
    fn default() -> Self {
        Self {
            id: 1,
            host: String::from("127.0.0.1"),
            port: 8901,
            data_path: String::from("/var/lib/cnosdb/meta"),
            log: Default::default(),
            meta_init: Default::default(),
            heartbeat: Default::default(),
        }
    }
}

// 加载配置文件 得到opt信息
pub fn get_opt(path: Option<impl AsRef<Path>>) -> Opt {
    // 返回默认配置
    if path.is_none() {
        return Default::default();
    }
    let path = path.unwrap();
    let path = path.as_ref();

    // 打开文件
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(err) => panic!(
            "Failed to open configurtion file '{}': {}",
            path.display(),
            err
        ),
    };
    let mut content = String::new();
    if let Err(err) = file.read_to_string(&mut content) {
        panic!(
            "Failed to read configurtion file '{}': {}",
            path.display(),
            err
        );
    }

    // 调用toml库 解析toml文件
    let config: Opt = match toml::from_str(&content) {
        Ok(config) => config,
        Err(err) => panic!(
            "Failed to parse configurtion file '{}': {}",
            path.display(),
            err
        ),
    };
    config
}

#[cfg(test)]
mod test {
    use crate::store::config::Opt;

    #[test]
    fn test() {
        let config_str = r#"
id = 1
host = "127.0.0.1"
port = 8901
data_path = "/tmp/cnosdb/meta"

[log]
level = "warn"
path = "/tmp/cnosdb/logs"

[meta_init]
cluster_name = "cluster_xxx"
admin_user = "root"
system_tenant = "cnosdb"
default_database = ["public", "usage_schema"]

[heartbeat]
heartbeat_recheck_interval = 300
heartbeat_expired_interval = 600
"#;

        let config: Opt = toml::from_str(config_str).unwrap();
        assert!(toml::to_string_pretty(&config).is_ok());
        dbg!(config);
    }
}
