use crate::context::{DynamoContext, TaskState, Workers};
use crate::error::{LibError, Result};
use apalis_core::layers;
use apalis_core::codec::json::JsonCodec;
use apalis_core::data::Extensions;
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
use futures::{FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt};
use serde::{de::DeserializeOwned, Serialize};
use serde_dynamo::{from_item, from_items, to_item};
use std::convert::TryInto;
use std::str::FromStr;
use std::sync::Arc;
use std::{fmt, io};
use std::{marker::PhantomData, time::Duration};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct ApiRequest<T> {
    pub(crate) req: T,
    pub(crate) context: DynamoContext,
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
    db: Client,
    table_name: String,
    job_type: PhantomData<T>,
    controller: Controller,
    config: Config,
    codec: ArcCodec<T>,
}

impl<T> fmt::Debug for DynamoStorage<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynamoStorage")
            .field("job_type", &"PhantomData<T>")
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
        let db = self.db.clone();
        DynamoStorage {
            db,
            job_type: PhantomData,
            controller: self.controller.clone(),
            config: self.config.clone(),
            codec: self.codec.clone(),
            table_name: self.table_name.clone(),
        }
    }
}

/// Create a Dynamo table if not exist
async fn create_table(client: &Client, table_name: String, key_name: String) -> Result<()> {
    let ad = AttributeDefinition::builder()
        .attribute_name(&key_name)
        .attribute_type(ScalarAttributeType::S)
        .build()?;

    let ks = KeySchemaElement::builder()
        .attribute_name(&key_name)
        .key_type(KeyType::Hash)
        .build()?;

    let pt = ProvisionedThroughput::builder()
        .read_capacity_units(5)
        .write_capacity_units(5)
        .build()?;

    client
        .create_table()
        .table_name(table_name)
        .attribute_definitions(ad)
        .key_schema(ks)
        .provisioned_throughput(pt)
        .send()
        .await?;

    Ok(())
}

async fn get(db: &Client, table_name: &String, id: &str) -> Result<DynamoContext> {
    // Get documents from DynamoDB
    let res = db
        .get_item()
        .table_name(table_name)
        .key("key", AttributeValue::S(id.into()))
        .send()
        .await?;

    let item = res.item().ok_or(LibError::ItemNotFound)?;

    // And deserialize them as strongly-typed data structures
    let item_type = from_item(item.clone())?;

    Ok(item_type)
}

async fn scan(db: &Client, table_name: &String) -> Result<Vec<DynamoContext>> {
    // Get documents from DynamoDB
    let result = db
        .scan()
        .table_name(table_name)
        .send()
        .await
        .map_err(|e| LibError::DynamoScanItems(e))?;

    // And deserialize them as strongly-typed data structures
    let Some(items) = result.items else {
        return Err(LibError::ItemNotFound);
    };

    let users: Vec<DynamoContext> = from_items(items).map_err(|e| LibError::SerdeDynamo(e))?;

    Ok(users)
}

async fn put(db: &Client, table_name: &String, item: DynamoContext) -> Result<()> {
    // Set the item by key
    // TODO: Key should be job_id and worker_id pair
    let item = to_item(item)?;

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

impl<T: Job + Serialize + DeserializeOwned> DynamoStorage<T> {
    pub async fn new(
        db: aws_sdk_dynamodb::Client,
        check_table_exists: bool,
        table_name: String,
    ) -> Result<Self> {
        if check_table_exists {
            let resp = db.list_tables().send().await?;
            let names = resp.table_names();

            tracing::trace!("tables: {}", names.join(","));

            if !names.contains(&table_name) {
                tracing::info!("table not found, creating now");

                create_table(&db, table_name.clone(), "key".into()).await?;
            }
        }

        Ok(Self {
            db,
            table_name,
            job_type: PhantomData,
            controller: Controller::new(),
            config: Config::default(),
            codec: Arc::new(Box::new(JsonCodec)),
        })
    }

    /// Create a new instance with a custom config
    pub fn new_with_config(
        db: aws_sdk_dynamodb::Client,
        table_name: String,
        config: Config,
    ) -> Self {
        Self {
            db,
            table_name,
            job_type: PhantomData,
            controller: Controller::new(),
            config,
            codec: Arc::new(Box::new(JsonCodec)),
        }
    }

    /// Keeps a storage notified that the worker is still alive manually
    pub async fn keep_alive_at<Service>(
        &mut self,
        worker_id: &WorkerId,
        last_seen: i64,
    ) -> Result<()> {
        let pool = self.db.clone();
        let worker_type = T::NAME;
        let storage_name = std::any::type_name::<Self>();
        let layers = std::any::type_name::<Service>();
        let mut item = DynamoContext::new(TaskId::new());
        item.workers = Workers::new(
            worker_id.to_string(),
            worker_type.to_string(),
            storage_name.to_string(),
            layers.to_string(),
            last_seen,
        );
        put(&self.db, &self.table_name, item).await?;
        Ok(())
    }

    /// Expose the pool for other functionality, eg custom migrations
    pub fn pool(&self) -> &aws_sdk_dynamodb::Client {
        &self.db
    }
}

const JOB_TABLE_NAME: &str = "apalis-jobs";

async fn fetch_next<T: Job>(
    db: Client,
    worker_id: &WorkerId,
    id: String,
) -> Result<Option<ApiRequest<String>>> {
    let now: i64 = Utc::now().timestamp();
    let key = format!("{}-{}", id, worker_id.to_string());
    let table_name = JOB_TABLE_NAME.to_owned();
    let mut job = get(&db, &table_name, &key).await?;
    // Scan all job where job_type == T::NAME; and status == TaskState::Pending
    //
    job.status = TaskState::Running;
    job.lock_by = Some(worker_id.clone());
    job.lock_at = Some(now);

    put(&db, &table_name, job.clone()).await?;
    let request = Some(ApiRequest {
        req: "".to_string(),
        context: job,
    });

    Ok(request)
}

impl<T: DeserializeOwned + Send + Unpin + Job> DynamoStorage<T> {
    // TODO: Fix this stream jobs
    fn stream_jobs(
        &self,
        worker_id: &WorkerId,
        interval: Duration,
        buffer_size: usize,
    ) -> impl Stream<Item = Result<Option<Request<T>>>> {
        let client = self.db.clone();
        let worker_id = worker_id.clone();
        let codec = self.codec.clone();
        // try_stream! {
        loop {
            apalis_core::sleep(interval).await;
            // let tx = self.db.clone();
            let table_name = self.table_name;
            let job_type = T::NAME;
            // let fetch_query = "SELECT id FROM Jobs
            //     WHERE (status = 'Pending' OR (status = 'Failed' AND attempts < max_attempts)) AND run_at < ?1 AND job_type = ?2 LIMIT ?3";
            let now: i64 = Utc::now().timestamp();
            let contexts: Vec<DynamoContext> = scan(&client, &table_name).await?;

            for ctx in contexts {
                let id = ctx.id;
                let res = fetch_next::<T>(client.clone(), &worker_id, id.to_string()).await?;
                match res {
                    None => None::<Request<T>>,
                    Some(c) => Some(
                        ApiRequest {
                            context: c.context,
                            req: codec.decode(&c.req).map_err(|e| {
                                LibError::InvalidData(io::Error::new(io::ErrorKind::InvalidData, e))
                            })?,
                        }
                        .into(),
                    ),
                }
                .map(Into::into);
            }
        }
        // }
    }
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
        let pool = self.db.clone();
        let job = self.codec.encode(&job).map_err(|e| LibError::Apalis(e))?;
        let job_type = T::NAME;

        let mut context = DynamoContext::new(id.clone());
        context.job = job;
        context.job_type = job_type.to_string();
        context.status = TaskState::Pending;

        put(&self.db, &self.table_name, context);

        Ok(id)
    }

    async fn schedule(&mut self, job: Self::Job, on: i64) -> Result<TaskId> {
        let pool = self.db.clone();
        let id = TaskId::new();
        let job = self.codec.encode(&job).map_err(|e| LibError::Apalis(e))?;
        let job_type = T::NAME;

        let mut context = DynamoContext::new(id.clone());
        context.job = job;
        context.job_type = job_type.to_string();
        context.status = TaskState::Pending;
        context.run_at = {
            let four = chrono::Duration::seconds(4);
            let now = Utc::now() + four;
            now
        };
        put(&self.db, &self.table_name, context);

        Ok(id)
    }

    async fn fetch_by_id(&self, job_id: &TaskId) -> Result<Option<Request<Self::Job>>> {
        let context = get(&self.db, &self.table_name, &job_id.to_string()).await?;

        Ok(Some(
            ApiRequest {
                context,
                req: self.codec.decode(&c.req).map_err(|e| LibError::Apalis(e))?,
            }
            .into(),
        ))
    }

    async fn len(&self) -> Result<i64> {
        let record = scan(&self.db, &self.table_name).await?;
        Ok(record.len() as i64)
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
        let now: i64 = Utc::now().timestamp();
        let wait_until = chrono::DateTime::from_timestamp(now + wait, 0).unwrap();
        let mut context = get(&self.db, &self.table_name, &task_id.to_string()).await?;
        context.status = TaskState::Failed;
        context.done_at = None;
        context.lock_by = None;
        context.lock_at = None;
        context.run_at = wait_until;
        put(&self.db, &self.table_name, context);
        Ok(())
    }

    async fn update(&self, job: Request<Self::Job>) -> Result<()> {
        // let query =
        //         "UPDATE Jobs SET status = ?1, attempts = ?2, done_at = ?3, lock_by = ?4, lock_at = ?5, last_error = ?6 WHERE id = ?7";
        let pool = self.db.clone();
        let ctx = job
            .get::<DynamoContext>()
            .ok_or(LibError::InvalidData(io::Error::new(
                io::ErrorKind::InvalidData,
                "Missing SqlContext",
            )))?;
        let status = ctx.status().clone();
        let attempts = ctx.attempts();
        let done_at = *ctx.done_at();
        let lock_by = ctx.lock_by().clone();
        let lock_at = *ctx.lock_at();
        let last_error = ctx.last_error().clone();
        let job_id = ctx.id();

        let mut context = get(&self.db, &self.table_name, &job_id.to_string()).await?;
        context.status = status;
        context.attempts = attempts;
        context.done_at = done_at;
        context.lock_by = lock_by;
        context.lock_at = lock_at;
        context.last_error = last_error;
        put(&self.db, &self.table_name, context);
        Ok(())
    }

    async fn is_empty(&self) -> Result<bool> {
        self.len().map_ok(|c| c == 0).await
    }

    async fn vacuum(&self) -> Result<usize> {
        let pool = self.db.clone();
        // let query = "Delete from Jobs where status='Done'";
        // let record = sqlx::query(query).execute(&pool).await?;
        let deleted: usize = 0;

        Ok(deleted)
    }
}

impl<T> DynamoStorage<T> {
    /// Puts the job instantly back into the queue
    /// Another [Worker] may consume
    pub async fn retry(&mut self, worker_id: &WorkerId, job_id: &TaskId) -> Result<()> {
        // let query =
        //         "UPDATE Jobs SET status = 'Pending', done_at = NULL, lock_by = NULL WHERE id = ?1 AND lock_by = ?2";

        let mut context = get(&self.db, &self.table_name, &job_id.to_string()).await?;
        context.status = TaskState::Pending;
        context.done_at = None;
        context.lock_by = None;
        put(&self.db, &self.table_name, context);
        Ok(())
    }

    /// Kill a job
    pub async fn kill(&mut self, worker_id: &WorkerId, job_id: &TaskId) -> Result<()> {
        // let query = r#"
        //     UPDATE Jobs
        //     SET status = 'Killed', done_at = strftime('%s','now')
        //         WHERE id = ?1 AND lock_by = ?2
        // "#;

        let mut context = get(&self.db, &self.table_name, &job_id.to_string()).await?;
        context.status = TaskState::Killed;
        context.done_at = Some(Utc::now().timestamp());
        context.lock_by = Some(worker_id.clone());
        put(&self.db, &self.table_name, context);

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

        let job_type = T::NAME;

        let contexts: Vec<DynamoContext> = scan(&self.db, &self.table_name).await?;
        let context = contexts
            .into_iter()
            .filter(|x| x.status == TaskState::Failed && x.attempts < x.max_attempts)
            .next();

        if context.is_some() {
            let id = context.unwrap().id;
            let mut context = get(&self.db, &self.table_name, &id.to_string()).await?;
            context.status = TaskState::Pending;
            context.done_at = None;
            context.lock_by = None;
            put(&self.db, &self.table_name, context);
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

        let job_type = T::NAME;
        let contexts: Vec<DynamoContext> = scan(&self.db, &self.table_name).await?;
        let context = contexts
            .into_iter()
            .filter(|x| {
                x.status == TaskState::Running
                    && x.workers.last_seen < timeout
                    && x.workers.worker_type == job_type.to_string()
            })
            .next();

        if context.is_some() {
            let id = context.unwrap().id;
            let mut context = get(&self.db, &self.table_name, &id.to_string()).await?;
            context.status = TaskState::Pending;
            context.done_at = None;
            context.lock_by = None;
            context.last_error = Some("Job was abandoned".to_string());
            put(&self.db, &self.table_name, context);
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
        let mut context = get(&self.db, &self.table_name, &task_id.to_string()).await?;
        context.status = TaskState::Done;
        context.done_at = Some(Utc::now().timestamp());
        context.lock_by = Some(worker_id.clone());
        put(&self.db, &self.table_name, context);

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

        let storage =
            DynamoStorage::<Email>::new(client, true, TEST_DYNAMO_TABLE.to_string()).await?;

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
        let ctx = job.get::<DynamoContext>().unwrap();
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
        let ctx = job.get::<DynamoContext>().unwrap();
        let job_id = ctx.id();

        storage
            .ack(&worker_id, job_id)
            .await
            .expect("failed to acknowledge the job");

        let job = get_job(&mut storage, job_id).await;
        let ctx = job.get::<DynamoContext>().unwrap();
        assert_eq!(*ctx.status(), TaskState::Done);
        assert!(ctx.done_at().is_some());
    }

    #[tokio::test]
    async fn test_kill_job() {
        let mut storage = setup().await;

        push_email(&mut storage, example_email()).await;

        let worker_id = register_worker(&mut storage).await;

        let job = consume_one(&mut storage, &worker_id).await;
        let ctx = job.get::<DynamoContext>().unwrap();
        let job_id = ctx.id();

        storage
            .kill(&worker_id, job_id)
            .await
            .expect("failed to kill job");

        let job = get_job(&mut storage, job_id).await;
        let ctx = job.get::<DynamoContext>().unwrap();
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
        let ctx = job.get::<DynamoContext>().unwrap();
        storage
            .reenqueue_orphaned(six_minutes_ago.timestamp())
            .await
            .expect("failed to heartbeat");

        let job_id = ctx.id();
        let job = get_job(&mut storage, job_id).await;
        let ctx = job.get::<DynamoContext>().unwrap();
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
        let ctx = job.get::<DynamoContext>().unwrap();
        storage
            .reenqueue_orphaned(four_minutes_ago.timestamp())
            .await
            .expect("failed to heartbeat");

        let job_id = ctx.id();
        let job = get_job(&mut storage, job_id).await;
        let ctx = job.get::<DynamoContext>().unwrap();
        assert_eq!(*ctx.status(), TaskState::Running);
        assert_eq!(*ctx.lock_by(), Some(worker_id));
    }
}
