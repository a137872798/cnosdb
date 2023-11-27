use std::collections::VecDeque;

use super::ast::ExtStatement;
use crate::Result;

pub trait Parser {

    // 解析sql 得到一组token  在cnosdb层 做了一层映射
    fn parse(&self, sql: &str) -> Result<VecDeque<ExtStatement>>;
}
