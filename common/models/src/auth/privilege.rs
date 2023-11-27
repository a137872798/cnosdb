use std::fmt::Display;
use std::hash::Hash;

use serde::{Deserialize, Serialize};

use crate::oid::Id;

// A权限检查B权限
pub trait PrivilegeChecker {
    fn check_privilege(&self, other: &Self) -> bool;
}

// 表示一个特权
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Privilege<T> {

    // 表示一个全局特权  可能被赋予某个用户或者租户
    Global(GlobalPrivilege<T>),
    // Some(T): tenantId
    // None: all tenants
    // 表示一个租户级别的权限  T为None 代表针对所有租户
    TenantObject(TenantObjectPrivilege, Option<T>),
}

impl<T> Display for Privilege<T>
where
    T: Id,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Global(g) => {
                write!(f, "{}", g)
            }
            Self::TenantObject(p, t) => match t {
                Some(t) => {
                    write!(f, "{} of tenant {}", p, t)
                }
                None => {
                    write!(f, "{} of all tenants", p)
                }
            },
        }
    }
}

impl<T: Id> PrivilegeChecker for Privilege<T> {
    fn check_privilege(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Global(s), Self::Global(o)) => s.check_privilege(o),
            (Self::TenantObject(s, None), Self::TenantObject(o, _)) => s.check_privilege(o),
            (Self::TenantObject(s, Some(s_t)), Self::TenantObject(o, Some(o_t))) => {
                s_t == o_t && s.check_privilege(o)
            }
            (_, _) => false,
        }
    }
}

// 全局权限 代表的是更高维度的权限   (下面的要么是针对租户概念的权限 或者数据库级别的权限)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GlobalPrivilege<T> {

    // 全局范围的权限 且与系统相关
    System,
    // Some(T): Administrative rights for the specify user, `T` represents the unique identifier of the user
    // None: Administrative rights for all user
    // 全局范围的权限 且与某个用户相关
    User(Option<T>),
    // Some(T): Administrative rights for the specify tenant, `T` represents the unique identifier of the tenant
    // None: Administrative rights for all tenants
    Tenant(Option<T>),
}

impl<T> Display for GlobalPrivilege<T>
where
    T: Id,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::System => {
                write!(f, "maintainer for system")
            }
            Self::User(u) => match u {
                Some(u) => {
                    write!(f, "maintainer for user {}", u)
                }
                None => {
                    write!(f, "maintainer for all users")
                }
            },
            Self::Tenant(t) => match t {
                Some(t) => {
                    write!(f, "maintainer for tenant {}", t)
                }
                None => {
                    write!(f, "maintainer for all tenants")
                }
            },
        }
    }
}

impl<T> PrivilegeChecker for GlobalPrivilege<T>
where
    T: Id,
{
    fn check_privilege(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::User(None), Self::User(_)) => true,
            (Self::User(Some(lo)), Self::User(Some(ro))) => lo == ro,
            (Self::Tenant(None), Self::Tenant(_)) => true,
            (Self::Tenant(Some(lo)), Self::Tenant(Some(ro))) => lo == ro,
            (l, r) => l == r,
        }
    }
}

// 描述一个租户级别的特权
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TenantObjectPrivilege {
    // All operation permissions related to system   代表这个权限是有关发起系统操作的
    // e.g. kill querys of tenant
    System,
    // All operation permissions related to members  代表这个权限是用于管理租户下所有用户的
    MemberFull,
    // All operation permissions related to roles    代表这个权限是用于管理租户下所有角色的
    RoleFull,
    // T: database_name
    // None: all databases in this tenant   代表这个权限是有关读写db的
    Database(DatabasePrivilege, Option<String>),
}

impl Display for TenantObjectPrivilege {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::System => {
                write!(f, "system admin")
            }
            Self::MemberFull => {
                write!(f, "maintainer for all members")
            }
            Self::RoleFull => {
                write!(f, "maintainer for all roles")
            }
            Self::Database(p, db) => match db {
                Some(db) => {
                    write!(f, "{:?} on database {}", p, db)
                }
                None => {
                    write!(f, "{:?} on all databases", p)
                }
            },
        }
    }
}

// 基于租户级别 检查是否具有这个权限 (当前权限是否覆盖另一个权限)
impl PrivilegeChecker for TenantObjectPrivilege {
    fn check_privilege(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::System, Self::System) => true,
            (Self::MemberFull, Self::MemberFull) => true,
            (Self::RoleFull, Self::RoleFull) => true,
            (Self::Database(s, None), Self::Database(o, _)) => s.check_privilege(o),
            (Self::Database(s, Some(s_t)), Self::Database(o, Some(o_t))) => {
                s_t == o_t && s.check_privilege(o)
            }
            (l, r) => l == r,
        }
    }
}

// 数据库级别的特权
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DatabasePrivilege {
    Read,
    Write,
    Full,
}

impl DatabasePrivilege {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Read => "Read",
            Self::Write => "Write",
            Self::Full => "All",
        }
    }
}

// 检验当前权限是否覆盖另一个权限
impl PrivilegeChecker for DatabasePrivilege {
    fn check_privilege(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Full, _) => true,
            (Self::Write, Self::Write | Self::Read) => true,
            (Self::Read, Self::Read) => true,
            (l, r) => l == r,
        }
    }
}
