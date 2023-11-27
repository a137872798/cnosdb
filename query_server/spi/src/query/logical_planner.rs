use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

use async_trait::async_trait;
use config::TenantLimiterConfig;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::file_format::file_type::{FileCompressionType, FileType};
use datafusion::logical_expr::type_coercion::aggregates::{
    DATES, NUMERICS, STRINGS, TIMES, TIMESTAMPS,
};
use datafusion::logical_expr::{
    expr, expr_fn, CreateExternalTable, LogicalPlan as DFPlan, ReturnTypeFunction, ScalarUDF,
    Signature, Volatility,
};
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion::prelude::{col, Expr};
use datafusion::sql::sqlparser::ast::{Ident, ObjectName, SqlOption, Value};
use datafusion::sql::sqlparser::parser::ParserError;
use lazy_static::lazy_static;
use models::auth::privilege::{DatabasePrivilege, GlobalPrivilege, Privilege};
use models::auth::role::{SystemTenantRole, TenantRoleIdentifier};
use models::auth::user::{UserOptions, UserOptionsBuilder};
use models::meta_data::{NodeId, ReplicationSetId, VnodeId};
use models::object_reference::ResolvedTable;
use models::oid::{Identifier, Oid};
use models::schema::{
    DatabaseOptions, Duration, TableColumn, Tenant, TenantOptions, TenantOptionsBuilder, Watermark,
};
use snafu::ResultExt;
use tempfile::NamedTempFile;

use super::ast::{parse_bool_value, parse_char_value, parse_string_value, ExtStatement};
use super::datasource::azure::{AzblobStorageConfig, AzblobStorageConfigBuilder};
use super::datasource::gcs::{
    GcsStorageConfig, ServiceAccountCredentials, ServiceAccountCredentialsBuilder,
};
use super::datasource::s3::{S3StorageConfig, S3StorageConfigBuilder};
use super::datasource::UriSchema;
use super::session::SessionCtx;
use super::AFFECTED_ROWS;
use crate::service::protocol::QueryId;
use crate::{ParserSnafu, QueryError, Result};

pub const TENANT_OPTION_LIMITER: &str = "_limiter";
pub const TENANT_OPTION_COMMENT: &str = "comment";

lazy_static! {
    static ref TABLE_WRITE_UDF: Arc<ScalarUDF> = Arc::new(ScalarUDF::new(
        "rows",
        &Signature::variadic(
            STRINGS
                .iter()
                .chain(NUMERICS)
                .chain(TIMESTAMPS)
                .chain(DATES)
                .chain(TIMES)
                .cloned()
                .collect::<Vec<_>>(),
            Volatility::Immutable
        ),
        &(Arc::new(move |_: &[DataType]| Ok(Arc::new(DataType::UInt64))) as ReturnTypeFunction),
        &make_scalar_function(|args: &[ArrayRef]| Ok(Arc::clone(&args[0]))),
    ));
}


// 将plan与一组权限关联在一起
#[derive(Clone)]
pub struct PlanWithPrivileges {
    pub plan: Plan,
    pub privileges: Vec<Privilege<Oid>>,
}

#[derive(Clone)]
pub enum Plan {
    /// Query plan   对于数据的操作
    Query(QueryPlan),
    /// Query plan   一些对表结构的改动
    DDL(DDLPlan),
    /// Ext DML plan  删除表
    DML(DMLPlan),
    /// Query plan   一些系统级别的操作 比如终止某个查询  或者查看其他查询此时的进度
    SYSTEM(SYSPlan),
}

impl Plan {

    // 各种类型的逻辑计划 都有对应的schema
    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::Query(p) => SchemaRef::from(p.df_plan.schema().as_ref()),
            Self::DDL(p) => p.schema(),
            Self::DML(p) => p.schema(),
            Self::SYSTEM(p) => p.schema(),
        }
    }
}

// datafusion的逻辑计划
#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub df_plan: DFPlan,
}

impl QueryPlan {
    pub fn is_explain(&self) -> bool {
        matches!(self.df_plan, DFPlan::Explain(_) | DFPlan::Analyze(_))
    }
}


// 代表一些对表的改动
#[derive(Clone)]
pub enum DDLPlan {
    // e.g. drop table
    DropDatabaseObject(DropDatabaseObject),
    // e.g. drop user/tenant   丢弃全局对象 比如用户或者租户
    DropGlobalObject(DropGlobalObject),
    // e.g. drop database/role   丢弃租户级别的数据 也就是某个数据库 或者角色
    DropTenantObject(DropTenantObject),

    /// Create external table. such as parquet\csv...
    /// 创建外部表 在datafusion的视角下 parquet/csv文件 实际上就是表
    CreateExternalTable(CreateExternalTable),

    // 创建cnosdb所认为的表
    CreateTable(CreateTable),

    // 创建stream表
    CreateStreamTable(CreateStreamTable),

    // 创建数据库
    CreateDatabase(CreateDatabase),

    // 创建一个新租户 会在上层创建新的 catalog
    CreateTenant(Box<CreateTenant>),

    // 在租户下创建一个用户
    CreateUser(CreateUser),

    // 创建角色
    CreateRole(CreateRole),

    // 修改database信息   请求体跟 CreateDatabase很相似
    AlterDatabase(AlterDatabase),

    // 修改表
    AlterTable(AlterTable),

    // 修改租户信息
    AlterTenant(AlterTenant),

    // 修改用户信息
    AlterUser(AlterUser),

    // 撤销授权   这个是以角色为单位的
    GrantRevoke(GrantRevoke),

    // Vnode 此时还不清楚是什么
    DropVnode(DropVnode),

    CopyVnode(CopyVnode),

    MoveVnode(MoveVnode),

    CompactVnode(CompactVnode),

    ChecksumGroup(ChecksumGroup),

    // 恢复某个数据的数据  看来数据是有做备份的  有关备份相关的时间周期又是怎么定的？
    RecoverDatabase(RecoverDatabase),

    // 恢复某个租户的数据
    RecoverTenant(RecoverTenant),
}

impl DDLPlan {
    pub fn schema(&self) -> SchemaRef {
        match self {
            DDLPlan::ChecksumGroup(_) => Arc::new(Schema::new(vec![
                Field::new("VNODE_ID", DataType::UInt32, false),
                Field::new("CHECK_SUM", DataType::Utf8, false),
            ])),
            _ => Arc::new(Schema::empty()),
        }
    }
}

// 副本集id
#[derive(Debug, Clone)]
pub struct ChecksumGroup {
    pub replication_set_id: ReplicationSetId,
}

// 将一些v节点的数据进行合并
#[derive(Debug, Clone)]
pub struct CompactVnode {
    pub vnode_ids: Vec<VnodeId>,
}

#[derive(Debug, Clone)]
pub struct MoveVnode {
    pub vnode_id: VnodeId,
    pub node_id: NodeId,
}

// 将某个v节点的数据拷贝到 另一个节点?
#[derive(Debug, Clone)]
pub struct CopyVnode {
    pub vnode_id: VnodeId,
    pub node_id: NodeId,
}

#[derive(Debug, Clone)]
pub struct DropVnode {
    pub vnode_id: VnodeId,
}

// 删除表的操作
#[derive(Debug, Clone)]
pub enum DMLPlan {
    DeleteFromTable(DeleteFromTable),
}

impl DMLPlan {
    pub fn schema(&self) -> SchemaRef {
        Arc::new(Schema::empty())
    }
}

/// TODO implement UserDefinedLogicalNodeCore
/// 根据sql语句将名中的table删除
#[derive(Debug, Clone)]
pub struct DeleteFromTable {
    pub table_name: ResolvedTable,
    pub selection: Option<Expr>,
}

#[derive(Debug, Clone)]
pub enum SYSPlan {
    ShowQueries,
    KillQuery(QueryId),
}

impl SYSPlan {
    pub fn schema(&self) -> SchemaRef {
        match self {
            // 当计划为展示 其他query的状态时 是有固定的schema的
            SYSPlan::ShowQueries => Arc::new(Schema::new(vec![
                Field::new("query_id", DataType::Utf8, false),
                Field::new("user", DataType::Utf8, false),
                Field::new("query", DataType::Utf8, false),
                Field::new("state", DataType::Utf8, false),
                Field::new("duration", DataType::UInt64, false),
            ])),
            // 如果是kill 就没有schema
            _ => Arc::new(Schema::empty()),
        }
    }
}

// 对应drop表的计划
#[derive(Debug, Clone)]
pub struct DropDatabaseObject {
    /// object name
    /// e.g. database_name.table_name
    /// 通过解析定位到的某张表
    pub object_name: ResolvedTable,
    /// If exists  是否当存在时才执行drop命令
    pub if_exist: bool,
    ///ObjectType  代表针对的对象类型 目前只有database
    pub obj_type: DatabaseObjectType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatabaseObjectType {
    Table,
}

// 丢弃租户级别的对象
#[derive(Debug, Clone)]
pub struct DropTenantObject {
    // 本次针对哪个租户
    pub tenant_name: String,
    // 要删除对象的名字
    pub name: String,
    pub if_exist: bool,
    // 要删除的是 role 还是database
    pub obj_type: TenantObjectType,
    // 也是支持延时删除
    pub after: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TenantObjectType {
    Role,
    Database,
}

// 丢弃一个全局对象
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropGlobalObject {
    // 通过名字来定位
    pub name: String,
    pub if_exist: bool,
    // user / tenant
    pub obj_type: GlobalObjectType,
    // 删除操作支持延时触发
    pub after: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GlobalObjectType {
    User,
    Tenant,
}

// 恢复某个租户的数据
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverTenant {
    pub tenant_name: String,
    pub if_exist: bool,
}

// 恢复某个数据库的数据
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverDatabase {
    pub tenant_name: String,
    pub db_name: String,
    pub if_exist: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverTable {
    pub table: ResolvedTable,
    pub if_exist: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CSVOptions {
    /// Whether the CSV file contains a header
    pub has_header: bool,
    /// Delimiter for CSV
    pub delimiter: char,
}

// 创建一个 cnosdb的表
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTable {
    /// The table schema   该table相关的多列
    pub schema: Vec<TableColumn>,
    /// The table name   resolvedTable 相当于是一个坐标 能够定位到某个租户的某张表
    pub name: ResolvedTable,
    /// Option to not error if table already exists
    pub if_not_exists: bool,
}

// 创建一张stream表  stream相比普通表 有一个水位概念
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateStreamTable {
    /// Option to not error if table already exists
    pub if_not_exists: bool,
    /// The table name
    pub name: ResolvedTable,
    /// 创建的表相关的schema
    pub schema: Schema,
    /// 流表有一个水位系统 可以在某个字段上关联是一个时间属性 目前还不清楚怎么进行水位控制
    pub watermark: Watermark,
    /// 描述这个流的类型
    pub stream_type: String,
    /// 一些额外的选项
    pub extra_options: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDatabase {
    pub name: String,

    pub if_not_exists: bool,

    // 数据库选项
    pub options: DatabaseOptions,
}

// 创建新租户
#[derive(Debug, Clone)]
pub struct CreateTenant {
    pub name: String,
    pub if_not_exists: bool,
    pub options: TenantOptions,
}

// 下面的是一些辅助方法 在执行相关的plan时会调用

// 代表要取消租户的某个option
pub fn unset_option_to_alter_tenant_action(
    tenant: Tenant,  // 租户信息
    ident: Ident,
) -> Result<(AlterTenantAction, Privilege<Oid>)> {
    // 获取租户id
    let tenant_id = *tenant.id();
    let mut tenant_options_builder = TenantOptionsBuilder::from(tenant.to_own_options());

    // 返回一个全局权限
    let privilege = match normalize_ident(&ident).as_str() {
        // 代表要取消的是 comment
        TENANT_OPTION_COMMENT => {
            tenant_options_builder.unset_comment();
            Privilege::Global(GlobalPrivilege::Tenant(Some(tenant_id)))
        }
        TENANT_OPTION_LIMITER => {
            tenant_options_builder.unset_limiter_config();
            Privilege::Global(GlobalPrivilege::System)
        }
        _ => {
            return Err(QueryError::Parser {
                source: ParserError::ParserError(format!(
                "Expected option [{TENANT_OPTION_COMMENT}], [{TENANT_OPTION_LIMITER}] found [{}]",
                ident
            )),
            })
        }
    };
    let tenant_options = tenant_options_builder.build()?;

    Ok((
        // 根据相关信息 产生了一个 action对象 该对象描述了如何修改tenant
        AlterTenantAction::SetOption(Box::new(tenant_options)),
        privilege,
    ))
}

// 这里是产生options 并追加到tenant上
pub fn sql_option_to_alter_tenant_action(
    tenant: Tenant,
    option: SqlOption,  // 是一个kv值
) -> std::result::Result<(AlterTenantAction, Privilege<Oid>), QueryError> {
    let SqlOption { name, value } = option;
    let tenant_id = *tenant.id();
    // 获得原options
    let mut tenant_options_builder = TenantOptionsBuilder::from(tenant.to_own_options());

    let privilege = match normalize_ident(&name).as_str() {
        TENANT_OPTION_COMMENT => {
            let value = parse_string_value(value)?;
            tenant_options_builder.comment(value);
            Privilege::Global(GlobalPrivilege::Tenant(Some(tenant_id)))
        }
        TENANT_OPTION_LIMITER => {
            let config =
                serde_json::from_str::<TenantLimiterConfig>(parse_string_value(value)?.as_str())
                    .map_err(|_| ParserError::ParserError("limiter format error".to_string()))?;
            tenant_options_builder.limiter_config(config);
            Privilege::Global(GlobalPrivilege::System)
        }
        _ => {
            return Err(QueryError::Parser {
                source: ParserError::ParserError(format!(
                "Expected option [{TENANT_OPTION_COMMENT}], [{TENANT_OPTION_LIMITER}] found [{}]",
                name
            )),
            })
        }
    };
    let tenant_options = tenant_options_builder.build()?;
    Ok((
        AlterTenantAction::SetOption(Box::new(tenant_options)),
        privilege,
    ))
}

// 使用一组选项去设置tenant
pub fn sql_options_to_tenant_options(options: Vec<SqlOption>) -> Result<TenantOptions> {
    let mut builder = TenantOptionsBuilder::default();

    for SqlOption { ref name, value } in options {
        match normalize_ident(name).as_str() {
            TENANT_OPTION_COMMENT => {
                builder.comment(parse_string_value(value).context(ParserSnafu)?);
            }
            TENANT_OPTION_LIMITER => {
                let config = serde_json::from_str::<TenantLimiterConfig>(
                    parse_string_value(value).context(ParserSnafu)?.as_str(),
                )?;
                builder.limiter_config(config);
            }
            _ => {
                return Err(QueryError::Parser {
                    source: ParserError::ParserError(format!(
                        "Expected option [{TENANT_OPTION_COMMENT}], [{TENANT_OPTION_LIMITER}] found [{}]",
                        name
                    )),
                })
            }
        }
    }

    builder.build().map_err(|e| QueryError::Parser {
        source: ParserError::ParserError(e.to_string()),
    })
}

// 创建一个用户
#[derive(Debug, Clone)]
pub struct CreateUser {
    pub name: String,
    pub if_not_exists: bool,
    // 用户相关的选项
    pub options: UserOptions,
}

// 从kv值读取数据 并添加到 userOptions上
pub fn sql_options_to_user_options(
    with_options: Vec<SqlOption>,
) -> std::result::Result<UserOptions, ParserError> {
    let mut builder = UserOptionsBuilder::default();

    for SqlOption { ref name, value } in with_options {
        match normalize_ident(name).as_str() {
            "password" => {
                builder.password(parse_string_value(value)?);
            }
            "must_change_password" => {
                builder.must_change_password(parse_bool_value(value)?);
            }
            "rsa_public_key" => {
                builder.rsa_public_key(parse_string_value(value)?);
            }
            "comment" => {
                builder.comment(parse_string_value(value)?);
            }
            "granted_admin" => {
                builder.granted_admin(parse_bool_value(value)?);
            }
            _ => {
                return Err(ParserError::ParserError(format!(
                "Expected option [password | rsa_public_key | comment | granted_admin], found [{}]",
                name
            )))
            }
        }
    }

    builder
        .build()
        .map_err(|e| ParserError::ParserError(e.to_string()))
}

// 创建角色
#[derive(Debug, Clone)]
pub struct CreateRole {
    pub tenant_name: String,
    pub name: String,
    pub if_not_exists: bool,
    // 描述角色与租户的关系
    pub inherit_tenant_role: SystemTenantRole,
}

// 撤销授权
#[derive(Debug, Clone)]
pub struct GrantRevoke {
    // 赋予/撤销
    pub is_grant: bool,
    // privilege, db name
    // 代表涉及到哪些数据库权限
    pub database_privileges: Vec<(DatabasePrivilege, String)>,
    // 租户名字
    pub tenant_name: String,
    // 角色名字
    pub role_name: String,
}

// 修改用户信息
#[derive(Debug, Clone)]
pub struct AlterUser {
    pub user_name: String,
    pub alter_user_action: AlterUserAction,
}

#[derive(Debug, Clone)]
pub enum AlterUserAction {
    // 重命名 或者修改选项
    RenameTo(String),
    Set(UserOptions),
}

#[derive(Debug, Clone)]
pub struct AlterTenant {
    pub tenant_name: String,
    pub alter_tenant_action: AlterTenantAction,
}

// 修改租户信息
#[derive(Debug, Clone)]
pub enum AlterTenantAction {
    AddUser(AlterTenantAddUser),
    SetUser(AlterTenantSetUser),
    RemoveUser(Oid),
    // 更新选项
    SetOption(Box<TenantOptions>),
}

// 给租户增加一个user 同时还指定了它的角色
#[derive(Debug, Clone)]
pub struct AlterTenantAddUser {
    pub user_id: Oid,
    pub role: TenantRoleIdentifier,
}

#[derive(Debug, Clone)]
pub struct AlterTenantSetUser {
    pub user_id: Oid,
    pub role: TenantRoleIdentifier,
}

// 修改数据库
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterDatabase {
    pub database_name: String,
    pub database_options: DatabaseOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTable {
    // 可以定位到表
    pub table_name: ResolvedTable,
    // 修改表的动作
    pub alter_action: AlterTableAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterTableAction {
    // 添加某列
    AddColumn {
        table_column: TableColumn,
    },
    // 修改某列
    AlterColumn {
        column_name: String,
        new_column: TableColumn,
    },
    // 丢弃某列
    DropColumn {
        column_name: String,
    },
    // 重命名某列
    RenameColumn {
        old_column_name: String,
        new_column_name: RenameColumnAction,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RenameColumnAction {
    RenameTag(String),
    RenameField(String),
}

// 该对象可以基于会话 创建逻辑计划  sql先变成 statement 然后再变成 logicalPlan
#[async_trait]
pub trait LogicalPlanner {
    async fn create_logical_plan(
        &self,
        statement: ExtStatement,
        session: &SessionCtx,
    ) -> Result<Plan>;
}

/// Additional output information
/// TODO
pub fn affected_row_expr(args: Vec<Expr>) -> Expr {
    let udf = expr::ScalarUDF::new(TABLE_WRITE_UDF.clone(), args);

    Expr::ScalarUDF(udf).alias(AFFECTED_ROWS.0)
}

// 一个计算总数的表达式
pub fn merge_affected_row_expr() -> Expr {
    expr_fn::sum(col(AFFECTED_ROWS.0)).alias(AFFECTED_ROWS.0)
}

/// Normalize a SQL object name
/// 将name抽取出来 按照"." 拼接
pub fn normalize_sql_object_name_to_string(sql_object_name: &ObjectName) -> String {
    sql_object_name
        .0
        .iter()
        .map(normalize_ident)
        .collect::<Vec<String>>()
        .join(".")
}

// Normalize an identifier to a lowercase string unless the identifier is quoted.
pub fn normalize_ident(id: &Ident) -> String {
    match id.quote_style {
        Some(_) => id.value.clone(),
        None => id.value.to_ascii_lowercase(),
    }
}

pub struct CopyOptions {
    pub auto_infer_schema: bool,
}

#[derive(Default)]
pub struct CopyOptionsBuilder {
    auto_infer_schema: Option<bool>,
}

// 有关copy的选项
impl CopyOptionsBuilder {
    // Convert sql options to supported parameters
    // perform value validation
    pub fn apply_options(
        mut self,
        options: Vec<SqlOption>,
    ) -> std::result::Result<Self, QueryError> {
        for SqlOption { ref name, value } in options {
            match normalize_ident(name).as_str() {
                "auto_infer_schema" => {
                    self.auto_infer_schema = Some(parse_bool_value(value)?);
                }
                option => {
                    return Err(QueryError::Semantic {
                        err: format!("Unsupported option [{}]", option),
                    })
                }
            }
        }

        Ok(self)
    }

    /// Construct CopyOptions and assign default value
    pub fn build(self) -> CopyOptions {
        CopyOptions {
            auto_infer_schema: self.auto_infer_schema.unwrap_or_default(),
        }
    }
}

// 有关文件格式的选项
pub struct FileFormatOptions {
    // arrow/avro/parquet/json/csv 等等
    pub file_type: FileType,
    pub delimiter: char,
    pub with_header: bool,
    // 数据文件的压缩方式
    pub file_compression_type: FileCompressionType,
}

#[derive(Debug, Default)]
pub struct FileFormatOptionsBuilder {
    file_type: Option<FileType>,
    delimiter: Option<char>,
    with_header: Option<bool>,
    file_compression_type: Option<FileCompressionType>,
}

impl FileFormatOptionsBuilder {
    fn parse_file_type(s: &str) -> Result<FileType> {
        let s = s.to_uppercase();
        match s.as_str() {
            "AVRO" => Ok(FileType::AVRO),
            "PARQUET" => Ok(FileType::PARQUET),
            "CSV" => Ok(FileType::CSV),
            "JSON" => Ok(FileType::JSON),
            _ => Err(QueryError::Semantic {
                err: format!(
                    "Unknown FileType: {}, only support AVRO | PARQUET | CSV | JSON",
                    s
                ),
            }),
        }
    }

    fn parse_file_compression_type(s: &str) -> Result<FileCompressionType> {
        let s = s.to_uppercase();
        match s.as_str() {
            "GZIP" | "GZ" => Ok(FileCompressionType::GZIP),
            "BZIP2" | "BZ2" => Ok(FileCompressionType::BZIP2),
            "" => Ok(FileCompressionType::UNCOMPRESSED),
            _ => Err(QueryError::Semantic {
                err: format!(
                    "Unknown FileCompressionType: {}, only support GZIP | BZIP2",
                    s
                ),
            }),
        }
    }

    // 将sql options转换为受支持的参数
    // 执行值校验
    pub fn apply_options(mut self, options: Vec<SqlOption>) -> Result<Self> {
        for SqlOption { ref name, value } in options {
            match normalize_ident(name).as_str() {
                "type" => {
                    let file_type = Self::parse_file_type(&parse_string_value(value)?)?;
                    self.file_type = Some(file_type);
                }
                "delimiter" => {
                    self.delimiter = Some(parse_char_value(value)?);
                }
                "with_header" => {
                    self.with_header = Some(parse_bool_value(value)?);
                }
                "file_compression_type" => {
                    let file_compression_type =
                        Self::parse_file_compression_type(&parse_string_value(value)?)?;
                    self.file_compression_type = Some(file_compression_type);
                }
                option => {
                    return Err(QueryError::Semantic {
                        err: format!("Unsupported option [{}]", option),
                    })
                }
            }
        }

        Ok(self)
    }

    /// Construct FileFormatOptions and assign default value
    pub fn build(self) -> FileFormatOptions {
        FileFormatOptions {
            file_type: self.file_type.unwrap_or(FileType::CSV),
            delimiter: self.delimiter.unwrap_or(','),
            with_header: self.with_header.unwrap_or(true),
            file_compression_type: self
                .file_compression_type
                .unwrap_or(FileCompressionType::UNCOMPRESSED),
        }
    }
}


// 有关连接到oss的选项
pub enum ConnectionOptions {
    S3(S3StorageConfig),
    Gcs(GcsStorageConfig),
    Azblob(AzblobStorageConfig),
    Local,
}

/// Construct ConnectionOptions and assign default value
/// Convert sql options to supported parameters
/// perform value validation
pub fn parse_connection_options(
    url: &UriSchema,
    bucket: Option<&str>,
    options: Vec<SqlOption>,
) -> Result<ConnectionOptions> {

    // 下面的套路都差不多 解析字符串 得到某个选项 然后进行设置
    let parsed_options = match (url, bucket) {
        (UriSchema::S3, Some(bucket)) => ConnectionOptions::S3(parse_s3_options(bucket, options)?),
        (UriSchema::Gcs, Some(bucket)) => {
            ConnectionOptions::Gcs(parse_gcs_options(bucket, options)?)
        }
        (UriSchema::Azblob, Some(bucket)) => {
            ConnectionOptions::Azblob(parse_azure_options(bucket, options)?)
        }
        (UriSchema::Local, _) => ConnectionOptions::Local,
        (UriSchema::Custom(schema), _) => {
            return Err(QueryError::Semantic {
                err: format!("Unsupported url schema [{}]", schema),
            })
        }
        (_, None) => {
            return Err(QueryError::Semantic {
                err: "Lost bucket in url".to_string(),
            })
        }
    };

    Ok(parsed_options)
}

/// s3://<bucket>/<path>
fn parse_s3_options(bucket: &str, options: Vec<SqlOption>) -> Result<S3StorageConfig> {
    let mut builder = S3StorageConfigBuilder::default();

    builder.bucket(bucket);

    for SqlOption { ref name, value } in options {
        match normalize_ident(name).as_str() {
            "endpoint_url" => {
                builder.endpoint_url(parse_string_value(value)?);
            }
            "region" => {
                builder.region(parse_string_value(value)?);
            }
            "access_key_id" => {
                builder.access_key_id(parse_string_value(value)?);
            }
            "secret_key" => {
                builder.secret_access_key(parse_string_value(value)?);
            }
            "token" => {
                builder.security_token(parse_string_value(value)?);
            }
            "virtual_hosted_style" => {
                builder.virtual_hosted_style_request(parse_bool_value(value)?);
            }
            _ => {
                return Err(QueryError::Semantic {
                    err: format!("Unsupported option [{}]", name),
                })
            }
        }
    }

    builder.build().map_err(|err| QueryError::Semantic {
        err: err.to_string(),
    })
}

/// gcs://<bucket>/<path>
fn parse_gcs_options(bucket: &str, options: Vec<SqlOption>) -> Result<GcsStorageConfig> {
    let mut sac_builder = ServiceAccountCredentialsBuilder::default();

    for SqlOption { ref name, value } in options {
        match normalize_ident(name).as_str() {
            "gcs_base_url" => {
                sac_builder.gcs_base_url(parse_string_value(value)?);
            }
            "disable_oauth" => {
                sac_builder.disable_oauth(parse_bool_value(value)?);
            }
            "client_email" => {
                sac_builder.client_email(parse_string_value(value)?);
            }
            "private_key" => {
                sac_builder.private_key(parse_string_value(value)?);
            }
            _ => {
                return Err(QueryError::Semantic {
                    err: format!("Unsupported option [{}]", name),
                })
            }
        }
    }

    let sac = sac_builder.build().map_err(|err| QueryError::Semantic {
        err: err.to_string(),
    })?;
    let mut temp = NamedTempFile::new()?;
    write_tmp_service_account_file(sac, &mut temp)?;

    Ok(GcsStorageConfig {
        bucket: bucket.to_string(),
        service_account_path: temp.into_temp_path(),
    })
}

/// https://<account>.blob.core.windows.net/<container>[/<path>]
/// azblob://<container>/<path>
fn parse_azure_options(bucket: &str, options: Vec<SqlOption>) -> Result<AzblobStorageConfig> {
    let mut builder = AzblobStorageConfigBuilder::default();
    builder.container_name(bucket);

    for SqlOption { ref name, value } in options {
        match normalize_ident(name).as_str() {
            "account" => {
                builder.account_name(parse_string_value(value)?);
            }
            "access_key" => {
                builder.access_key(parse_string_value(value)?);
            }
            "bearer_token" => {
                builder.bearer_token(parse_string_value(value)?);
            }
            "use_emulator" => {
                builder.use_emulator(parse_bool_value(value)?);
            }
            _ => {
                return Err(QueryError::Semantic {
                    err: format!("Unsupported option [{}]", name),
                })
            }
        }
    }

    builder.build().map_err(|err| QueryError::Semantic {
        err: err.to_string(),
    })
}

// gcs相关的  先忽略
fn write_tmp_service_account_file(
    sac: ServiceAccountCredentials,
    tmp: &mut NamedTempFile,
) -> Result<()> {
    let body = serde_json::to_vec(&sac)?;
    let _ = tmp.write(&body)?;
    tmp.flush()?;

    Ok(())
}

/// Convert SqlOption s to map, and convert value to lowercase
/// 将kv读取出来 存储到map中
pub fn sql_options_to_map(opts: &[SqlOption]) -> HashMap<String, String> {
    let mut map = HashMap::with_capacity(opts.len());
    for SqlOption { name, value } in opts {
        let value_str = match value {
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => s.clone(),
            _ => value.to_string().to_ascii_lowercase(),
        };
        map.insert(normalize_ident(name), value_str);
    }
    map
}
