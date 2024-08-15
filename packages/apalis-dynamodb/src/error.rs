use aws_sdk_dynamodb::{
    error::SdkError,
    operation::{
        create_table::CreateTableError, delete_item::{DeleteItemError, DeleteItemInput}, get_item::GetItemError, list_tables::ListTablesError, put_item::PutItemError, query::QueryError, scan::ScanError, update_item::UpdateItemError
    },
};

#[derive(thiserror::Error, Debug)]

pub enum LibError {
    #[error("Item Not Found")]
    ItemNotFound,

    #[error("Concurrency Error")]
    Concurrency,

    #[error("Malformed Object at field: {0}")]
    MalformedObject(String),

    #[error("Invalid data {0}")]
    InvalidData(std::io::Error),

    #[error("Dynamo QueryError: {0}")]
    DynamoQuery(#[from] SdkError<QueryError>),
    
    #[error("Dynamo PutItemError: {0}")]
    DynamoPut(#[from] SdkError<PutItemError>),

    #[error("Dynamo UpdateItemError: {0}")]
    DynamoUpdate(#[from] SdkError<UpdateItemError>),

    #[error("Dynamo DynamoDelete: {0}")]
    DynamoDelete(#[from] SdkError<DeleteItemError>),
    
    #[error("Dynamo ListTablesError: {0}")]
    DynamoListTables(#[from] SdkError<ListTablesError>),

    #[error("Dynamo CreateTableError: {0}")]
    DynamoCreateTable(#[from] SdkError<CreateTableError>),

    #[error("Dynamo GetItemError: {0}")]
    DynamoGetItem(#[from] SdkError<GetItemError>),

    #[error("Dynamo ScanItemError: {0}")]
    DynamoScanItems(#[from] SdkError<ScanError>),

    #[error("Dynamo BuildError: {0}")]
    DynamoBuild(#[from] aws_sdk_dynamodb::error::BuildError),

    #[error("serde_dynamo BuildError: {0}")]
    SerdeDynamo(#[from] serde_dynamo::Error),

    #[error("Serde Error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("Apalis Error: {0}")]
    Apalis(#[from] apalis_core::error::Error),
}

pub type Result<T> = std::result::Result<T, LibError>;
