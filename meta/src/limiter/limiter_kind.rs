use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};


// 限流针对的类型
#[derive(Hash, Eq, PartialEq, Debug, Clone, Copy, Serialize, Deserialize)]
pub enum RequestLimiterKind {
    CoordDataIn,
    CoordDataOut,
    CoordQueries,
    CoordWrites,
    HttpDataIn,
    HttpDataOut,
    HttpQueries,
    HttpWrites,
}

impl Display for RequestLimiterKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
