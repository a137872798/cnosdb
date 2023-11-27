use serde::{Deserialize, Serialize};

// 租户限制配置
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TenantLimiterConfig {
    pub object_config: Option<TenantObjectLimiterConfig>,
    // 内部包含各种bucket 记录了容量信息
    pub request_config: Option<RequestLimiterConfig>,
}

// 这个是有关租户对象的限制
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct TenantObjectLimiterConfig {
    // add user limit    该租户的用户上限
    pub max_users_number: Option<usize>,
    /// create database limit  数据库数量限制
    pub max_databases: Option<usize>,
    // 分片数量限制
    pub max_shard_number: Option<usize>,
    // 副本数量限制
    pub max_replicate_number: Option<usize>,
    // 数据最大保留时间
    pub max_retention_time: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct RateBucketConfig {
    pub max: Option<usize>,
    pub initial: usize,
    pub refill: usize,
    // ms
    pub interval: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct CountBucketConfing {
    // 初始计数和最大计数
    pub max: Option<i64>,
    pub initial: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Bucket {
    // 存储2种配置对象
    pub remote_bucket: RateBucketConfig,
    pub local_bucket: CountBucketConfing,
}

// 请求限流配置
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct RequestLimiterConfig {
    // 这里的bucket可以理解为存储数据的容器 当数据超过容器容量时 自然就没法继续操作了 比如将无法write
    pub coord_data_in: Option<Bucket>,
    pub coord_data_out: Option<Bucket>,
    pub coord_queries: Option<Bucket>,
    pub coord_writes: Option<Bucket>,
    pub http_data_in: Option<Bucket>,
    pub http_data_out: Option<Bucket>,
    pub http_queries: Option<Bucket>,
    pub http_writes: Option<Bucket>,
}

#[test]
fn test_config() {
    let config_str = r#"
[object_config]
# add user limit
max_users_number = 1
# create database limit
max_databases = 3
max_shard_number = 2
max_replicate_number = 2
max_retention_time = 30


[request_config.coord_data_in]
local_bucket = {max = 100, initial = 0}
remote_bucket = {max = 100, initial = 0, refill = 100, interval = 100}


[request_config.coord_data_out]
local_bucket = {max = 100, initial = 0}
remote_bucket = {max = 100, initial = 0, refill = 100, interval = 100}

[request_config.coord_data_writes]
local_bucket = {max = 100, initial = 0}
remote_bucket = {max = 100, initial = 0, refill = 100, interval = 100}

[request_config.coord_data_queries]
local_bucket = {max = 100, initial = 0}
remote_bucket = {max = 100, initial = 0, refill = 100, interval = 100}

[request_config.http_data_in]
local_bucket = {max = 100, initial = 0}
remote_bucket = {max = 100, initial = 0, refill = 100, interval = 100}

[request_config.http_data_out]
local_bucket = {max = 100, initial = 0}
remote_bucket = {max = 100, initial = 0, refill = 100, interval = 100}

[request_config.http_queries]
local_bucket = {max = 100, initial = 0}
remote_bucket = {max = 100, initial = 0, refill = 100, interval = 100}

[request_config.http_writes]
local_bucket = {max = 100, initial = 0}
remote_bucket = {max = 100, initial = 0, refill = 100, interval = 100}
"#;

    let config: TenantLimiterConfig = toml::from_str(config_str).unwrap();
    dbg!(config);
}
