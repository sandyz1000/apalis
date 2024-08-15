use crate::context::{DynamoTask, TaskState, Workers};
use crate::error::{LibError, Result};
use apalis_core::codec::json::JsonCodec;
use apalis_core::data::Extensions;
use apalis_core::layers;
use apalis_core::layers::{Ack, AckLayer};
use apalis_core::poller::controller::Controller;
use apalis_core::poller::stream::BackendStream;
use apalis_core::poller::Poller;
use apalis_core::request::{Request, RequestStream};
use apalis_core::storage::{Job, Storage};
use apalis_core::task::task_id::TaskId;
use apalis_core::worker::WorkerId;
use apalis_core::{Backend, Codec};
use async_stream::try_stream;
use aws_sdk_dynamodb::operation::query::QueryInput;
use aws_sdk_dynamodb::{
    client::Client,
    error::SdkError,
    operation::put_item::PutItemError,
    types::{
        AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ProvisionedThroughput,
        ScalarAttributeType,
    },
};
use chrono::Utc;
use futures::{lock, FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt};
use serde::{de::DeserializeOwned, Serialize};
use serde_dynamo::{from_item, from_items, to_item};
use serde_json::to_string;
use uuid::timestamp::context;
use std::collections::HashMap;
use std::convert::TryInto;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::{fmt, io};

const ATTR_TASK_STATUS: &str = "status";
const ATTR_TASK_RUNAT: &str = "run_at";
const ATTR_TASK_ATTEMPTS: &str = "attempts";
const ATTR_TASK_MAX_ATTEMPTS: &str = "max_attempts";
const ATTR_TASK_LAST_ERROR: &str = "last_error";
const ATTR_TASK_LOCK_AT: &str = "lock_at";
const ATTR_TASK_LOCK_BY: &str = "lock_by";
const ATTR_TASK_DONE_AT: &str = "done_at";
const ATTR_TASK_WORKER_ID: &str = "worker_id";
const ATTR_TASK_JOB: &str = "job";
const ATTR_TASK_JOB_TYPE: &str = "job_type";

const TASK_PARTITION_KEY_NAME: &str = "task";
const WORKER_PARTITION_KEY_NAME: &str = "worker";

const JOB_TABLE_NAME: &str = "apalis-jobs";

// use std::net::SocketAddr;
// use tokio::net::TcpStream;
// use tokio::net::TcpListener;
// fn bind_and_accept(addr: SocketAddr) -> impl Stream<Item = io::Result<TcpStream>>
// {
//     try_stream! {
//         let mut listener = TcpListener::bind(addr).await?;

//         loop {
//             let (stream, addr) = listener.accept().await?;
//             println!("received on {:?}", addr);
//             yield stream;
//         }
//     }
// }

#[derive(Debug, Clone)]
pub struct ApiRequest<T> {
    pub(crate) req: T,
    pub(crate) context: DynamoTask,
}

impl<T> From<ApiRequest<T>> for Request<T> {
    fn from(val: ApiRequest<T>) -> Self {
        let mut data = Extensions::new();
        data.insert(val.context.id().clone());
        data.insert(val.context.attempts().clone());
        data.insert(val.context);

        Request::new_with_data(val.req, data)
    }
}

/// Config for dynamo storages
#[derive(Debug, Clone)]
pub struct Config {
    keep_alive: Duration,
    buffer_size: usize,
    poll_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            keep_alive: Duration::from_secs(30),
            buffer_size: 10,
            poll_interval: Duration::from_millis(50),
        }
    }
}

impl Config {
    /// Interval between database poll queries
    ///
    /// Defaults to 30ms
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Interval between worker keep-alive database updates
    ///
    /// Defaults to 30s
    pub fn keep_alive(mut self, keep_alive: Duration) -> Self {
        self.keep_alive = keep_alive;
        self
    }

    /// Buffer size to use when querying for jobs
    ///
    /// Defaults to 10
    pub fn buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }
}

pub fn event_key(key: &str) -> String {
    format!("events/ev-{key}.json")
}

type ArcCodec<T> =
    Arc<Box<dyn Codec<T, String, Error = apalis_core::error::Error> + Sync + Send + 'static>>;

/// Represents a [Storage] that persists to DynamoDB
// Store the Job state to dynamo
// #[derive(Debug)]
pub struct DynamoStorage<T> {
    /// the aws-sdk DynamoDB client to use when managing towser-sessions.
    pub client: Client,
    /// the DynamoDB backend configuration properties.
    pub table_name: String,
    /// The Controller struct represents a thread-safe state manager. I
    pub controller: Controller,
    /// Config for dynamo storage
    pub config: Config,

    pub codec: ArcCodec<T>,
}

impl<T> fmt::Debug for DynamoStorage<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynamoStorage")
            .field("controller", &self.controller)
            .field("config", &self.config)
            .field(
                "codec",
                &"Arc<Box<dyn Codec<T, String, Error = Error> + Sync + Send + 'static>>",
            )
            // .field("ack_notify", &self.ack_notify)
            .finish()
    }
}

impl<T> Clone for DynamoStorage<T> {
    fn clone(&self) -> Self {
        let client = self.client.clone();

        DynamoStorage {
            client,
            controller: self.controller.clone(),
            config: self.config.clone(),
            codec: self.codec.clone(),
            table_name: self.table_name.clone(),
        }
    }
}

/// Create a Dynamo table if not exist
async fn create_table(
    client: &Client,
    table_name: String,
    partition_key: String,
    sort_key: Option<String>,
) -> Result<()> {
    let ad = AttributeDefinition::builder()
        .attribute_name(&partition_key)
        .attribute_type(ScalarAttributeType::S)
        .build()?;

    let ks = KeySchemaElement::builder()
        .attribute_name(&partition_key)
        .key_type(KeyType::Hash)
        .build()?;

    let pt = ProvisionedThroughput::builder()
        .read_capacity_units(5)
        .write_capacity_units(5)
        .build()?;

    let mut create_table_request = client
        .create_table()
        .table_name(table_name)
        .attribute_definitions(ad)
        .key_schema(ks)
        .provisioned_throughput(pt);

    if let Some(sk) = &sort_key {
        create_table_request = create_table_request
            .attribute_definitions(
                aws_sdk_dynamodb::types::AttributeDefinition::builder()
                    .attribute_name(sk.clone())
                    .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .key_schema(
                aws_sdk_dynamodb::types::KeySchemaElement::builder()
                    .attribute_name(sk.clone())
                    .key_type(aws_sdk_dynamodb::types::KeyType::Range)
                    .build()
                    .unwrap(),
            );
    }

    let _ = create_table_request.send().await?;

    Ok(())
}

type AttributeMap = HashMap<String, AttributeValue>;

async fn put(db: &Client, table_name: &String, item: AttributeMap) -> Result<()> {
    let request = db.put_item().table_name(table_name).set_item(Some(item));

    //Note: filter out conditional error
    if let Err(e) = request.send().await {
        if matches!(&e,SdkError::<PutItemError>::ServiceError (err)
        if matches!(
            err.err(),PutItemError::ConditionalCheckFailedException(_)

        )) {
            return Err(LibError::Concurrency);
        }

        return Err(LibError::DynamoPut(e));
    }

    Ok(())
}

fn task_to_attr(task: DynamoTask, partition_key: &str) -> AttributeMap {
    let mut item: AttributeMap = HashMap::new();
    let now = Utc::now().timestamp();
    let task_id = task.id.to_string();
    item.insert("#pk".into(), AttributeValue::S(partition_key.to_string()));
    item.insert(
        "#sk".into(),
        AttributeValue::S(format!("{partition_key}#{task_id}")),
    );

    item.insert(
        ATTR_TASK_STATUS.into(),
        AttributeValue::S(task.status.to_string()),
    );
    item.insert(ATTR_TASK_RUNAT.into(), AttributeValue::N(now.to_string()));
    item.insert(
        ATTR_TASK_ATTEMPTS.into(),
        AttributeValue::N(task.attempts().to_string()),
    );
    if let Some(err) = task.last_error() {
        item.insert(ATTR_TASK_LAST_ERROR.into(), AttributeValue::S(err.clone()));
    }
    if let Some(lock_by) = task.lock_by() {
        item.insert(
            ATTR_TASK_LOCK_AT.into(),
            AttributeValue::S(lock_by.to_string()),
        );
    }
    if let Some(done_at) = task.done_at() {
        item.insert(
            ATTR_TASK_DONE_AT.into(),
            AttributeValue::N(done_at.to_string()),
        );
    }
    item.insert(
        ATTR_TASK_WORKER_ID.into(),
        AttributeValue::S(task.worker_id.to_string()),
    );
    item.insert(ATTR_TASK_JOB.into(), AttributeValue::S(task.job.clone()));
    item.insert(
        ATTR_TASK_JOB_TYPE.into(),
        AttributeValue::S(task.job_type.clone()),
    );

    item
}

fn worker_to_attr(worker: Workers, partition_key: &str) -> AttributeMap {
    let mut item: AttributeMap = HashMap::new();

    item
}

impl<T: Job + Serialize + DeserializeOwned> DynamoStorage<T> {
    pub async fn new(
        client: aws_sdk_dynamodb::Client,
        check_table_exists: bool,
        table_name: String,
    ) -> Result<Self> {
        if check_table_exists {
            let resp = client.list_tables().send().await?;
            let names = resp.table_names();

            tracing::trace!("tables: {}", names.join(","));

            if !names.contains(&table_name) {
                tracing::info!("table not found, creating now");

                create_table(
                    &client,
                    table_name.clone(),
                    "#pk".to_string(),
                    Some("#sk".to_string()),
                )
                .await?;
            }
        }
        Ok(Self {
            client,
            controller: Controller::new(),
            config: Config::default(),
            codec: Arc::new(Box::new(JsonCodec)),
            table_name,
        })
    }

    /// Create a new instance with a custom config
    pub fn new_with_config(
        client: aws_sdk_dynamodb::Client,
        table_name: String,
        config: Config,
    ) -> Self {
        Self {
            client,
            controller: Controller::new(),
            config,
            codec: Arc::new(Box::new(JsonCodec)),
            table_name,
        }
    }

    /// Keeps a storage notified that the worker is still alive manually
    pub async fn keep_alive_at<Service>(
        &mut self,
        worker_id: &WorkerId,
        last_seen: i64,
    ) -> Result<()> {
        let worker_type = T::NAME;
        let storage_name = std::any::type_name::<Self>();
        let layers = std::any::type_name::<Service>();
        let workers = Workers::new(
            worker_id.to_string(),
            worker_type.to_string(),
            storage_name.to_string(),
            layers.to_string(),
            last_seen,
        );
        let task = DynamoTask::new(TaskId::new(), worker_id.clone());
        let item = task_to_attr(task, &TASK_PARTITION_KEY_NAME);
        put(&self.client, &self.table_name, item).await?;

        let worker_map = worker_to_attr(workers, &WORKER_PARTITION_KEY_NAME);
        put(&self.client, &self.table_name, worker_map).await?;
        Ok(())
    }

    /// Expose the pool for other functionality, eg custom migrations
    pub fn pool(&self) -> &aws_sdk_dynamodb::Client {
        &self.client
    }
}

// Scan all job where job_type == T::NAME; and status == TaskState::Pending
async fn fetch_next<T: Job>(
    db: Client,
    worker_id: &WorkerId,
    id: String,
    table_name: &String,
    partition_key: &String,
) -> Result<Option<ApiRequest<String>>> {
    let now: i64 = Utc::now().timestamp();
    let key = format!("{}-{}", id, worker_id.to_string());

    let job_type: String = T::NAME.to_string();

    let mut attribute_values = HashMap::new();

    let key_condition = "job_type = :job_type AND status = :status";

    attribute_values.insert("#pk".into(), AttributeValue::S(partition_key.clone()));
    attribute_values.insert(":job_type".into(), AttributeValue::S(job_type.clone()));
    attribute_values.insert(
        ":status".into(),
        AttributeValue::S(TaskState::Pending.to_string()),
    );
    let result = db
        .scan()
        .set_expression_attribute_values(Some(attribute_values))
        .filter_expression("job_type = :job_type AND status = :status")
        .send()
        .await?;

    // let tasks = vec![];
    if let Some(items) = result.items {
        for item in items {
            let id = item["id"]
                .as_s()
                .map_err(|e| LibError::MalformedObject("Task id is invalid".into()))
                .map(|id| TaskId::from_str(id).unwrap())?
                .clone();
            let run_at = item[ATTR_TASK_RUNAT]
                .as_n()
                .map_err(|_| LibError::MalformedObject(ATTR_TASK_RUNAT.into()))
                .map(|run_at| run_at.parse::<i64>().unwrap())?;

            let attempts = item[ATTR_TASK_ATTEMPTS]
                .as_n()
                .map_err(|_| LibError::MalformedObject(ATTR_TASK_ATTEMPTS.into()))
                .map(|run_at| run_at.parse::<i32>().unwrap())?;

            let max_attempts = item[ATTR_TASK_MAX_ATTEMPTS]
                .as_n()
                .map_err(|_| LibError::MalformedObject(ATTR_TASK_MAX_ATTEMPTS.into()))
                .map(|run_at| run_at.parse::<i32>().unwrap())?;

            let last_error = item[ATTR_TASK_LAST_ERROR]
                .as_n()
                .map_err(|_| LibError::MalformedObject(ATTR_TASK_LAST_ERROR.into()))
                .map(|run_at| run_at.clone())?;

            let lock_at = item[ATTR_TASK_LOCK_AT]
                .as_n()
                .map_err(|_| LibError::MalformedObject(ATTR_TASK_LOCK_AT.into()))
                .map(|run_at| run_at.parse::<i64>().unwrap())?;

            let lock_by = item[ATTR_TASK_LOCK_BY]
                .as_n()
                .map_err(|_| LibError::MalformedObject(ATTR_TASK_LOCK_BY.into()))
                .map(|run_at| run_at.parse::<i64>().unwrap())?;

            let done_at = item[ATTR_TASK_DONE_AT]
                .as_n()
                .map_err(|_| LibError::MalformedObject(ATTR_TASK_DONE_AT.into()))
                .map(|run_at| run_at.parse::<i64>().unwrap())?;

            let job = item[ATTR_TASK_JOB]
                .as_n()
                .map_err(|_| LibError::MalformedObject(ATTR_TASK_JOB.into()))
                .map(|run_at| run_at.clone())?;

            let task = DynamoTask {
                id,
                status: TaskState::Running, // Mark all the pending job to running
                run_at,
                attempts,
                max_attempts,
                last_error: Some(last_error),
                lock_at: Some(now),
                lock_by: Some(worker_id.clone()),
                done_at: Some(done_at),
                worker_id: worker_id.clone(),
                job,
                job_type: job_type.clone(),
            };

            let attr_value = task_to_attr(task.clone(), partition_key);
            // Update the value to db
            // put(&db, &table_name, job.clone()).await?;
            let request = ApiRequest {
                req: "".to_string(),
                context: task,
            };

            // Ok(Some(request))
        }
    }

    todo!()
}

impl<T: DeserializeOwned + Send + Unpin + Job> DynamoStorage<T> {
    // TODO: Fix this stream jobs
    async fn stream_jobs<'a>(
        &'a self,
        worker_id: &'a WorkerId,
        interval: Duration,
        buffer_size: usize,
    ) -> impl Stream<Item = Result<Option<Request<T>>>> + 'a {
        let client = self.client.clone();
        let worker_id = worker_id.clone();
        let codec = self.codec.clone();
        let partition_key = TASK_PARTITION_KEY_NAME.to_string();

        try_stream! {
            loop {
                apalis_core::sleep(interval).await;

                // let fetch_query = "SELECT id FROM Jobs
                // WHERE (status = 'Pending' OR (status = 'Failed' AND attempts < max_attempts))
                // AND run_at < ?1 AND job_type = ?2 LIMIT ?3";
                let job_type = T::NAME;
                let now: i64 = Utc::now().timestamp();
                let run_at_str = 0.to_string(); // Change this to valid value
                let max_attemps = 10; // Change this to valid value

                let filter_expression = "(status = :pending OR (status = :failed AND :attempts < max_attempts)) AND run_at < :run_at AND job_type = :job_type";

                let mut attr_value: AttributeMap = HashMap::new();
                attr_value.insert(
                    ":pending".into(),
                    AttributeValue::S(TaskState::Pending.to_string()),
                );
                attr_value.insert(
                    ":failed".into(),
                    AttributeValue::S(TaskState::Failed.to_string()),
                );
                attr_value.insert(
                    ":attempts".into(),
                    AttributeValue::N(max_attemps.to_string()),
                );
                attr_value.insert(":run_at".into(), AttributeValue::N(run_at_str));
                attr_value.insert(":job_type".into(), AttributeValue::S(job_type.to_string()));

                let result = client
                    .scan()
                    .set_expression_attribute_values(Some(attr_value))
                    .filter_expression(filter_expression)
                    .send()
                    .await?;

                let contexts: Vec<HashMap<String, AttributeValue>> = match result.items {
                    Some(context) => context,
                    None => vec![]
                };
                for ctx in contexts {
                    let id = ctx["id"].as_s().map_err(|_| LibError::MalformedObject("id".to_string()))?.clone();
                    let res = fetch_next::<T>(
                        client.clone(),
                        &worker_id,
                        id.to_string(),
                        &self.table_name,
                        &partition_key,
                    )
                    .await?;
                    
                    yield match res {
                        None => None::<Request<T>>,
                        Some(c) => Some(
                            ApiRequest {
                                context: c.context,
                                req: codec.decode(&c.req).map_err(|e| {
                                    LibError::InvalidData(io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        e,
                                    ))
                                })?,
                            }
                            .into(),
                        ),
                    }
                    .map(Into::into);
                }
            }
        }
    }
}

pub fn transform<T, U, F>(opt: &Option<T>, func: F) -> Option<U>
where
    F: FnOnce(&T) -> U,
{
    opt.as_ref().map(func)
}

impl<T> Storage for DynamoStorage<T>
where
    T: Job + Serialize + DeserializeOwned + Send + 'static + Unpin + Sync,
{
    type Job = T;

    type Error = LibError;

    type Identifier = TaskId;

    async fn push(&mut self, job: Self::Job) -> Result<TaskId> {
        let id = TaskId::new();
        let job = self.codec.encode(&job).map_err(|e| LibError::Apalis(e))?;
        let job_type = T::NAME;

        // TODO: Define a new worker or use existing worker
        let worker_id = WorkerId::new("test-worker");
        let mut context = DynamoTask::new(id.clone(), worker_id);
        context.job = job;
        context.job_type = job_type.to_string();
        context.status = TaskState::Pending;
        let item = task_to_attr(context, &TASK_PARTITION_KEY_NAME);
        put(&self.client, &self.table_name, item);

        Ok(id)
    }

    async fn schedule(&mut self, job: Self::Job, on: i64) -> Result<TaskId> {
        let id = TaskId::new();
        let job = self.codec.encode(&job).map_err(|e| LibError::Apalis(e))?;
        let job_type = T::NAME;
        let worker_id = WorkerId::new("test-worker");
        let search_key = format!("{0}#{1}", TASK_PARTITION_KEY_NAME, id.to_string());
        let run_at = Utc::now().timestamp() + 4; // Replace this with `on` args
        // let run_at = {
        //     let four = chrono::Duration::seconds(4);
        //     let now = Utc::now() + four;
        //     now.timestamp()
        // };

        let update_expr = "SET job = :job, job_type = :job_type, status = :status, run_at = :run_at";
        let mut attr_value: AttributeMap = HashMap::new();
        attr_value.insert("job".into(), AttributeValue::S(job));
        attr_value.insert("job_type".into(), AttributeValue::S(job_type.to_string()));
        attr_value.insert("status".into(), AttributeValue::S(TaskState::Pending.to_string()));
        attr_value.insert("run_at".into(), AttributeValue::N(run_at.to_string()));

        let _update_output = self.client
            .update_item()
            .key("#pk", AttributeValue::S(TASK_PARTITION_KEY_NAME.into()))
            .key("#sk", AttributeValue::S(search_key))
            .set_expression_attribute_values(Some(attr_value))
            .update_expression(update_expr)
            .send()
            .await.map_err(|e| LibError::DynamoUpdate(e))?;
        
        Ok(id)
    }

    async fn fetch_by_id(&self, job_id: &TaskId) -> Result<Option<Request<Self::Job>>> {
        let attr_value: AttributeMap = HashMap::new();
        let search_key = format!("{0}#{1}", TASK_PARTITION_KEY_NAME, job_id.to_string());
        attr_value.insert(
            "#pk".into(),
            AttributeValue::S(TASK_PARTITION_KEY_NAME.into()),
        );
        attr_value.insert("#sk".into(), AttributeValue::S(search_key));

        let query_output = self
            .client
            .query()
            .table_name(self.table_name)
            .set_expression_attribute_values(Some(attr_value))
            .limit(1)
            .send()
            .await
            .map_err(|e| LibError::DynamoQuery(e))?;

        match query_output.items {
            Some(items) => {
                
                let context = DynamoTask {
                    id: todo!(),
                    status: todo!(),
                    run_at: todo!(),
                    attempts: todo!(),
                    max_attempts: todo!(),
                    last_error: todo!(),
                    lock_at: todo!(),
                    lock_by: todo!(),
                    done_at: todo!(),
                    worker_id: todo!(),
                    job: todo!(),
                    job_type: todo!(),
                };
                let res = ApiRequest {
                    context,
                    req: self.codec.decode(&c.req).map_err(|e| LibError::Apalis(e))?,
                }
                .into();
                Ok(res)
            }
            _ => Err(LibError::ItemNotFound),
        }
    }

    async fn len(&self) -> Result<i64> {
        todo!()
    }

    async fn reschedule(&mut self, job: Request<T>, wait: Duration) -> Result<()> {
        fn safe_u64_to_i64(value: u64) -> Option<i64> {
            if value <= i64::MAX as u64 {
                Some(value as i64)
            } else {
                None
            }
        }
        let task_id = job.get::<TaskId>().ok_or(LibError::ItemNotFound)?;

        let Some(wait) = safe_u64_to_i64(wait.as_secs()) else {
            return Err(LibError::InvalidData(io::Error::new(
                io::ErrorKind::InvalidData,
                "Missing SqlContext",
            )));
        };

        // let query =
        //         "UPDATE Jobs SET status = 'Failed', done_at = NULL, lock_by = NULL, lock_at = NULL, run_at = ?2 WHERE id = ?1";
        let update_expression = "SET status = :status, run_at = :run_at, done_at = :null, lock_by = :null, lock_at = :null";

        let now: i64 = Utc::now().timestamp();
        let wait_until = now + wait;
        let search_key = format!("{0}#{1}", TASK_PARTITION_KEY_NAME, task_id.to_string());

        let mut attr_value: AttributeMap = HashMap::new();
        
        attr_value.insert(":run_at".into(), AttributeValue::N(wait_until.to_string()));
        attr_value.insert(
            ":status".into(),
            AttributeValue::S(TaskState::Failed.to_string()),
        );
        attr_value.insert(":null".to_string(), AttributeValue::Null(true));

        let _context = self
            .client
            .update_item()
            .key("#pk", AttributeValue::S(TASK_PARTITION_KEY_NAME.into()))
            .key("#sk", AttributeValue::S(search_key))
            .update_expression(update_expression)
            .set_expression_attribute_values(Some(attr_value))
            .send()
            .await
            .map_err(|e| LibError::DynamoUpdate(e))?;

        Ok(())
    }

    /// let query =
    /// "UPDATE Jobs SET status = ?1, attempts = ?2,
    ///  done_at = ?3, lock_by = ?4, lock_at = ?5, last_error = ?6 WHERE id = ?7";
    async fn update(&self, job: Request<Self::Job>) -> Result<()> {
        let ctx = job
            .get::<DynamoTask>()
            .ok_or(LibError::InvalidData(io::Error::new(
                io::ErrorKind::InvalidData,
                "Missing SqlContext",
            )))?;

        let status = ctx.status().clone();
        let attempts = ctx.attempts();
        let done_at = ctx.done_at().unwrap_or(0);
        let lock_by = transform(ctx.lock_by(), |w| w.clone()).unwrap();
        let lock_at = ctx.lock_at().unwrap_or(0);
        let last_error = transform(ctx.last_error(), |e| e.clone()).unwrap();
        let job_id = ctx.id();

        let update_expr = r#"
            SET 
                status = :status, attempts = :attempts, done_at = :done_at, 
                lock_by = :lock_by, lock_at = :lock_at, last_error = :last_error
        "#;
        let search_key = format!("{0}#{1}", TASK_PARTITION_KEY_NAME, job_id.to_string());
        let mut attr_values: AttributeMap = HashMap::new();

        attr_values.insert(":status".into(), AttributeValue::S(status.to_string()));
        attr_values.insert(":attempts".into(), AttributeValue::N(attempts.to_string()));
        attr_values.insert(":done_at".into(), AttributeValue::N(done_at.to_string()));
        attr_values.insert(":lock_by".into(), AttributeValue::S(lock_by.to_string()));
        attr_values.insert(":lock_at".into(), AttributeValue::N(lock_at.to_string()));
        attr_values.insert(
            ":last_error".into(),
            AttributeValue::S(last_error.to_string()),
        );

        let _context = self
            .client
            .update_item()
            .key("#pk", AttributeValue::S(TASK_PARTITION_KEY_NAME.into()))
            .key("#sk", AttributeValue::S(search_key))
            .update_expression(update_expr)
            .set_expression_attribute_values(Some(attr_values))
            .send()
            .await
            .map_err(|e| LibError::DynamoUpdate(e))?;

        Ok(())
    }

    async fn is_empty(&self) -> Result<bool> {
        self.len().map_ok(|c| c == 0).await
    }

    // let query = "Delete from Jobs where status='Done'";
    async fn vacuum(&self) -> Result<usize> {
        let query_output = self.client
            .query()
            .index_name("#gsi1") // Assume there is a GSI on status
            .key_condition_expression("status = :status")
            .expression_attribute_values(":status", AttributeValue::S(TaskState::Done.to_string()))
            .send()
            .await
            .map_err(|e| LibError::DynamoQuery(e))?;
        
        // If no items were found, return early
        let mut deleted: usize = 0;

        // Step 2: Delete each item
        for item in query_output.items().iter() {
            // Extract the primary key (PK and SK) from the item to delete
            if let (Some(pk), Some(sk)) = (item.get("#pk"), item.get("#sk")) {
                // Create a delete request for each item
                let _delete_item_input = self.client.delete_item()
                    .key("PK", pk.clone())
                    .key("SK", sk.clone())
                    .send().await.map_err(|e| LibError::DynamoDelete(e))?;
                
                deleted += 1;
            }
        }
        
        Ok(deleted)
    }
}

impl<T> DynamoStorage<T> {
    /// Puts the job instantly back into the queue
    /// Another [Worker] may consume
    pub async fn retry(&mut self, worker_id: &WorkerId, job_id: &TaskId) -> Result<()> {
        // let query =
        //         "UPDATE Jobs SET status = 'Pending', done_at = NULL, lock_by = NULL WHERE id = ?1 AND lock_by = ?2";

        let search_key = format!("{0}#{1}", TASK_PARTITION_KEY_NAME, job_id.to_string());
        let update_expr = "SET status = :status, done_at = :null, lock_by = :null";
        let mut attr_value: AttributeMap = HashMap::new();

        attr_value.insert(
            ":status".into(),
            AttributeValue::S(TaskState::Pending.to_string()),
        );
        attr_value.insert(":null".to_string(), AttributeValue::Null(true));

        let _context = self
            .client
            .update_item()
            .key("#pk", AttributeValue::S(TASK_PARTITION_KEY_NAME.into()))
            .key("#sk", AttributeValue::S(search_key))
            .update_expression(update_expr)
            .condition_expression("lock_by = :lock_by")
            .set_expression_attribute_values(Some(attr_value))
            .send()
            .await
            .map_err(|e| LibError::DynamoUpdate(e))?;

        Ok(())
    }

    /// Kill a job
    pub async fn kill(&mut self, worker_id: &WorkerId, job_id: &TaskId) -> Result<()> {
        // let query = r#"
        //     UPDATE Jobs
        //     SET status = 'Killed', done_at = strftime('%s','now')
        //         WHERE id = ?1 AND lock_by = ?2
        // "#;
        let search_key = format!("{0}#{1}", TASK_PARTITION_KEY_NAME, job_id.to_string());
        let update_expr = "SET status = :status, done_at = :done_at";
        let mut attr_value: AttributeMap = HashMap::new();
        let done_at = Utc::now().timestamp();
        attr_value.insert(
            ":status".into(),
            AttributeValue::S(TaskState::Pending.to_string()),
        );

        attr_value.insert(":done_at".into(), AttributeValue::N(done_at.to_string()));
        let _context = self
            .client
            .update_item()
            .key("#pk", AttributeValue::S(TASK_PARTITION_KEY_NAME.into()))
            .key("#sk", AttributeValue::S(search_key))
            .update_expression(update_expr)
            .set_expression_attribute_values(Some(attr_value))
            .send()
            .await
            .map_err(|e| LibError::DynamoUpdate(e))?;

        Ok(())
    }

    /// Add jobs that failed back to the queue if there are still remaining attempts
    pub async fn reenqueue_failed(&self) -> Result<()>
    where
        T: Job,
    {
        // let query = r#"
        // UPDATE Jobs
        // SET status = "Pending", done_at = NULL, lock_by = NULL, lock_at = NULL
        //     WHERE id in (
        //         SELECT Jobs.id from Jobs
        //         WHERE status= "Failed" AND Jobs.attempts < Jobs.max_attempts
        //         ORDER BY lock_at ASC LIMIT ?2
        // );"#;

        let max_limit = 10;
        let query_output = self
            .client
            .query()
            .index_name("#gsi1") // Assume there is a GSI on status
            .key_condition_expression("status = :status AND attempts < max_attempts")
            .expression_attribute_values(":status", AttributeValue::S("Failed".to_string()))
            .limit(max_limit)
            .scan_index_forward(true) // Ascending order (lock_at)
            .send()
            .await
            .map_err(|e| LibError::DynamoQuery(e))?;

        if let Some(items) = query_output.items {
            for item in items {
                if let Some(id) = item.get("id").and_then(|v| v.as_s().ok()) {
                    // Step 2: Update each item found
                    let update_expression = r#"
                        SET status = :pending,
                            done_at = :null,
                            lock_by = :null,
                            lock_at = :null
                    "#;

                    let mut attr_values = HashMap::new();
                    let search_key = format!("{0}#{1}", TASK_PARTITION_KEY_NAME, id.clone());
                    attr_values.insert(
                        ":pending".to_string(),
                        AttributeValue::S("Pending".to_string()),
                    );
                    attr_values.insert(":null".to_string(), AttributeValue::Null(true));

                    let _update_output = self
                        .client
                        .update_item()
                        .key("#pk", AttributeValue::S(TASK_PARTITION_KEY_NAME.into()))
                        .key("#sk", AttributeValue::S(search_key))
                        .update_expression(update_expression)
                        .set_expression_attribute_values(Some(attr_values))
                        .send()
                        .await
                        .map_err(|e| LibError::DynamoUpdate(e))?;
                }
            }
        }

        Ok(())
    }

    /// Add jobs that workers have disappeared to the queue
    pub async fn reenqueue_orphaned(&self, timeout: i64) -> Result<()>
    where
        T: Job,
    {
        // let query = r#"
        //     UPDATE Jobs
        //     SET status = "Pending", done_at = NULL, lock_by = NULL, lock_at = NULL, last_error ="Job was abandoned"
        //         WHERE id in (
        //             SELECT Jobs.id from Jobs INNER join Workers ON lock_by = Workers.id
        //             WHERE status= "Running" AND workers.last_seen < ?1
        //             AND Workers.worker_type = ?2 ORDER BY lock_at ASC LIMIT ?3
        // );"#;

        let max_limit = 10;
        let job_type = T::NAME;
        let query_output = self
            .client
            .query()
            .index_name("#gsi2") // Assume there's a GSI on worker_type
            .key_condition_expression("worker_type = :worker_type AND last_seen < :last_seen")
            .expression_attribute_values(":worker_type", AttributeValue::S(job_type.to_string()))
            .expression_attribute_values(":last_seen", AttributeValue::N(timeout.to_string()))
            .limit(max_limit)
            .scan_index_forward(true) // Ascending order (lock_at)
            .send()
            .await
            .map_err(|e| LibError::DynamoQuery(e))?;

        let mut worker_ids = vec![];

        if let Some(worker_items) = query_output.items {
            for item in worker_items {
                if let Some(worker_id) = item.get("id").and_then(|v| v.as_s().ok()) {
                    worker_ids.push(worker_id.clone());
                }
            }
        }

        // If no worker IDs were found, there's nothing to update
        if worker_ids.is_empty() {
            return Ok(());
        }

        // Step 2: Query Jobs that match the status and lock_by worker IDs
        let mut job_ids = vec![];
        for worker_id in worker_ids {
            let jobs_query_output = self
                .client
                .query()
                .index_name("#gsi1") // Assume there's a GSI on status
                .key_condition_expression("status = :status AND lock_by = :lock_by")
                .expression_attribute_values(":status", AttributeValue::S("Running".to_string()))
                .expression_attribute_values(":lock_by", AttributeValue::S(worker_id.clone()))
                .limit(max_limit)
                .scan_index_forward(true) // Ascending order (lock_at)
                .send()
                .await
                .map_err(|e| LibError::DynamoQuery(e))?;

            if let Some(job_items) = jobs_query_output.items {
                for item in job_items {
                    if let Some(job_id) = item.get("id").and_then(|v| v.as_s().ok()) {
                        job_ids.push(job_id.clone());
                    }
                }
            }
        }

        // Step 3: Update each job to mark it as "Pending" and clear fields
        for job_id in job_ids {
            let update_expression = r#"
                SET status = :pending,
                done_at = :null,
                lock_by = :null,
                lock_at = :null,
                last_error = :last_error
            "#;

            let search_key = format!("{0}#{1}", TASK_PARTITION_KEY_NAME, job_id.clone());
            let mut attr_values = HashMap::new();
            attr_values.insert(
                ":pending".to_string(),
                AttributeValue::S(TaskState::Pending.to_string()),
            );
            attr_values.insert(":null".to_string(), AttributeValue::Null(true));
            attr_values.insert(
                ":last_error".to_string(),
                AttributeValue::S("Job was abandoned".to_string()),
            );

            let _update_output = self
                .client
                .update_item()
                .key("#pk", AttributeValue::S(TASK_PARTITION_KEY_NAME.into()))
                .key("#sk", AttributeValue::S(search_key))
                .update_expression(update_expression)
                .set_expression_attribute_values(Some(attr_values))
                .send()
                .await
                .map_err(|e| LibError::DynamoUpdate(e))?;
        }

        Ok(())
    }
}

impl<T: Job + Serialize + DeserializeOwned + Sync + Send + Unpin + 'static> Backend<Request<T>>
    for DynamoStorage<T>
{
    type Stream = BackendStream<RequestStream<Request<T>>>;
    type Layer = AckLayer<DynamoStorage<T>, T>;

    fn common_layer(&self, worker_id: WorkerId) -> Self::Layer {
        AckLayer::new(self.clone(), worker_id)
    }

    fn poll(mut self, worker: WorkerId) -> Poller<Self::Stream> {
        let config = self.config.clone();
        let controller = self.controller.clone();
        let stream = self
            .stream_jobs(&worker, config.poll_interval, config.buffer_size)
            .map_err(|e| apalis_core::error::Error::SourceError(Box::new(e)));

        let stream = BackendStream::new(stream.boxed(), controller);
        let heartbeat = async move {
            loop {
                let now: i64 = Utc::now().timestamp();
                self.keep_alive_at::<Self::Layer>(&worker, now)
                    .await
                    .unwrap();
                apalis_core::sleep(Duration::from_secs(30)).await;
            }
        }
        .boxed();
        Poller::new(stream, heartbeat)
    }
}

impl<T: Sync> Ack<T> for DynamoStorage<T> {
    type Acknowledger = TaskId;
    type Error = LibError;
    async fn ack(&self, worker_id: &WorkerId, task_id: &Self::Acknowledger) -> Result<()> {
        // let mut context = get(&self.client, &self.table_name, &task_id.to_string()).await?;
        // context.status = TaskState::Done;
        // context.done_at = Some(Utc::now().timestamp());
        // context.lock_by = Some(worker_id.clone());
        // put(&self.client, &self.table_name, context);

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use crate::context::TaskState;

    use super::*;
    use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
    use chrono::Utc;
    use futures::StreamExt;
    use serde::Deserialize;

    const TEST_DYNAMO_TABLE: &str = "dynamo-local";

    #[derive(Debug, Deserialize, Serialize, Clone)]
    struct Email {
        to: String,
        subject: String,
        text: String,
    }

    impl Job for Email {
        const NAME: &'static str = "apalis::Email";
    }

    /// migrate DB and return a storage instance.
    async fn setup() -> DynamoStorage<Email> {
        // Because connections cannot be shared across async runtime
        // (different runtimes are created for each test),
        // we don't share the storage and tests must be run sequentially.

        let region_provider = RegionProviderChain::default_provider();
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(region_provider)
            .load()
            .await;
        let client = Client::new(&config);

        let storage = DynamoStorage::<Email>::new(client, true, TEST_DYNAMO_TABLE.to_string())
            .await
            .unwrap();

        storage
    }

    #[tokio::test]
    async fn test_inmemory_sqlite_worker() {
        let mut sqlite = setup().await;
        sqlite
            .push(Email {
                subject: "Test Subject".to_string(),
                to: "example@sqlite".to_string(),
                text: "Some Text".to_string(),
            })
            .await
            .expect("Unable to push job");
        let len = sqlite.len().await.expect("Could not fetch the jobs count");
        assert_eq!(len, 1);
    }

    struct DummyService {}

    fn example_email() -> Email {
        Email {
            subject: "Test Subject".to_string(),
            to: "example@postgres".to_string(),
            text: "Some Text".to_string(),
        }
    }

    async fn consume_one(
        storage: &mut DynamoStorage<Email>,
        worker_id: &WorkerId,
    ) -> Request<Email> {
        let mut stream = storage
            .stream_jobs(worker_id, std::time::Duration::from_secs(10), 1)
            .boxed();
        stream
            .next()
            .await
            .expect("stream is empty")
            .expect("failed to poll job")
            .expect("no job is pending")
    }

    async fn register_worker_at(storage: &mut DynamoStorage<Email>, last_seen: i64) -> WorkerId {
        let worker_id = WorkerId::new("test-worker");

        storage
            .keep_alive_at::<DummyService>(&worker_id, last_seen)
            .await
            .expect("failed to register worker");
        worker_id
    }

    async fn register_worker(storage: &mut DynamoStorage<Email>) -> WorkerId {
        register_worker_at(storage, Utc::now().timestamp()).await
    }

    async fn push_email(storage: &mut DynamoStorage<Email>, email: Email) {
        storage.push(email).await.expect("failed to push a job");
    }

    async fn get_job(storage: &mut DynamoStorage<Email>, job_id: &TaskId) -> Request<Email> {
        storage
            .fetch_by_id(job_id)
            .await
            .expect("failed to fetch job by id")
            .expect("no job found by id")
    }

    #[tokio::test]
    async fn test_consume_last_pushed_job() {
        let mut storage = setup().await;
        push_email(&mut storage, example_email()).await;

        let worker_id = register_worker(&mut storage).await;

        let job = consume_one(&mut storage, &worker_id).await;
        let ctx = job.get::<DynamoTask>().unwrap();
        assert_eq!(*ctx.status(), TaskState::Running);
        assert_eq!(*ctx.lock_by(), Some(worker_id.clone()));
        assert!(ctx.lock_at().is_some());
    }

    #[tokio::test]
    async fn test_acknowledge_job() {
        let mut storage = setup().await;
        push_email(&mut storage, example_email()).await;

        let worker_id = register_worker(&mut storage).await;

        let job = consume_one(&mut storage, &worker_id).await;
        let ctx = job.get::<DynamoTask>().unwrap();
        let job_id = ctx.id();

        storage
            .ack(&worker_id, job_id)
            .await
            .expect("failed to acknowledge the job");

        let job = get_job(&mut storage, job_id).await;
        let ctx = job.get::<DynamoTask>().unwrap();
        assert_eq!(*ctx.status(), TaskState::Done);
        assert!(ctx.done_at().is_some());
    }

    #[tokio::test]
    async fn test_kill_job() {
        let mut storage = setup().await;

        push_email(&mut storage, example_email()).await;

        let worker_id = register_worker(&mut storage).await;

        let job = consume_one(&mut storage, &worker_id).await;
        let ctx = job.get::<DynamoTask>().unwrap();
        let job_id = ctx.id();

        storage
            .kill(&worker_id, job_id)
            .await
            .expect("failed to kill job");

        let job = get_job(&mut storage, job_id).await;
        let ctx = job.get::<DynamoTask>().unwrap();
        assert_eq!(*ctx.status(), TaskState::Killed);
        assert!(ctx.done_at().is_some());
    }

    #[tokio::test]
    async fn test_heartbeat_renqueueorphaned_pulse_last_seen_6min() {
        let mut storage = setup().await;

        push_email(&mut storage, example_email()).await;

        let six_minutes_ago = Utc::now() - Duration::from_secs(6 * 60);

        let worker_id = register_worker_at(&mut storage, six_minutes_ago.timestamp()).await;

        let job = consume_one(&mut storage, &worker_id).await;
        let ctx = job.get::<DynamoTask>().unwrap();
        storage
            .reenqueue_orphaned(six_minutes_ago.timestamp())
            .await
            .expect("failed to heartbeat");

        let job_id = ctx.id();
        let job = get_job(&mut storage, job_id).await;
        let ctx = job.get::<DynamoTask>().unwrap();
        // TODO: rework these assertions
        // assert_eq!(*ctx.status(), State::Pending);
        // assert!(ctx.done_at().is_none());
        // assert!(ctx.lock_by().is_none());
        // assert!(ctx.lock_at().is_none());
        // assert_eq!(*ctx.last_error(), Some("Job was abandoned".to_string()));
    }

    #[tokio::test]
    async fn test_heartbeat_renqueueorphaned_pulse_last_seen_4min() {
        let mut storage = setup().await;

        push_email(&mut storage, example_email()).await;

        let four_minutes_ago = Utc::now() - Duration::from_secs(4 * 60);
        let worker_id = register_worker_at(&mut storage, four_minutes_ago.timestamp()).await;

        let job = consume_one(&mut storage, &worker_id).await;
        let ctx = job.get::<DynamoTask>().unwrap();
        storage
            .reenqueue_orphaned(four_minutes_ago.timestamp())
            .await
            .expect("failed to heartbeat");

        let job_id = ctx.id();
        let job = get_job(&mut storage, job_id).await;
        let ctx = job.get::<DynamoTask>().unwrap();
        assert_eq!(*ctx.status(), TaskState::Running);
        assert_eq!(*ctx.lock_by(), Some(worker_id));
    }
}
