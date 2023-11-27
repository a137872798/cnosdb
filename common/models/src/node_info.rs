use serde::{Deserialize, Serialize};

use crate::Timestamp;

#[allow(dead_code)]
pub struct Vnode {
    ///  same with time series family name
    name: String,
    node_id: String,
    ip: String,
}

#[allow(dead_code)]
pub struct Node {
    node_id: String,
    ip: String,
    flavor: Flavor,
    status: NodeStatus,
    located: Location,
    last_updated: Timestamp,
}


// 节点在上报自己的指标数据时 会告知自身是否处于健康状态
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
pub enum NodeStatus {
    #[default]
    Healthy,
    Broken,
    Unreachable,
    NoDiskSpace,
    Cordon,
}

#[allow(dead_code)]
pub struct Location {
    ///  aws / huawei / google / local
    provider: String,
    region: String,
    az: String,
}

#[allow(dead_code)]
pub struct Flavor {
    // M
    memory: f64,
    // core
    cpu: f64,
    // G
    disk: f64,
}
