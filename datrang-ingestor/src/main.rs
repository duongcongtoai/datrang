use datrang_ingestor::migration;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use std::{any, env, future};

use config::{Environment, File};
use etl::config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig};
use etl::destination::Destination;
use etl::destination::memory::MemoryDestination;
use etl::error::{ErrorKind, EtlError};
use etl::pipeline::Pipeline;
use etl::store::both::memory::MemoryStore;
use etl::store::both::postgres::PostgresStore;
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::{BeginEvent, Event, PipelineId, TableId};
use futures::StreamExt;
use futures::channel::mpsc;
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use tokio::time::{Sleep, sleep};
use tokio_postgres::NoTls;

async fn build_lakehouse_destination(
    host: &String,
    port: u16,
    user: &String,
    password: &str,
    db: &String,
    state: PostgresStore,
) -> Result<PostgresLakehouse<PostgresStore>, anyhow::Error> {
    let conn_str = format!("host=localhost user=postgres password=postgres dbname=app port=5432");
    // maybe using connection pool?

    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("failed to connect to postgres {err}");
        }
    });
    return Ok(PostgresLakehouse {
        client: Arc::new(client),
        replication_store: state,
    });
}
async fn main_ingestor(pg_connection_config: PgConnectionConfig) -> Result<(), anyhow::Error> {
    // Configure the pipeline
    let pipeline_config = PipelineConfig {
        id: 1,
        publication_name: "app_slot_pub".to_string(),
        pg_connection: pg_connection_config.clone(),
        batch: BatchConfig {
            max_size: 1,
            max_fill_ms: 5000,
        },
        table_error_retry_delay_ms: 1000,
        max_table_sync_workers: 1,
    };

    // Create in-memory store and destination for testing
    let store = PostgresStore::new(1, pg_connection_config.clone());
    // pub fn new(pipeline_id: PipelineId, source_config: PgConnectionConfig) -> Self {
    // store.get_table_schema(source_table_id)
    let destination = build_lakehouse_destination(
        &pg_connection_config.host,
        pg_connection_config.port,
        &pg_connection_config.username,
        &pg_connection_config.password.unwrap().expose_secret(),
        &pg_connection_config.name,
        store.clone(),
    )
    .await?;

    // Create and start the pipeline
    let mut pipeline = Pipeline::new(pipeline_config, store.clone(), destination);
    let maybe_result = pipeline.start().await?;

    pipeline.wait().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let pg_connection_config: PgConnectionConfig = config::Config::builder()
        .add_source(
            Environment::with_prefix("POSTGRES")
                .prefix_separator("__")
                .separator("__"),
        )
        .build()?
        .try_deserialize()?;
    let mut args = env::args();
    match args.len() {
        1 => main_ingestor(pg_connection_config).await,
        2 => {
            let cmd = args.nth(1).unwrap();
            match cmd.as_str() {
                "migrate" => migration::main_migrate(&pg_connection_config).await,
                _ => {
                    panic!("invalid {cmd}");
                }
            }
        }
        _ => {
            panic!("invalid cmd {args:?}");
        }
    }
}

// traditional lake house store metadata inside metadata log hosted on object stores
// this implementation use postgres as metastore
#[derive(Clone)]
pub struct PostgresLakehouse<S: StateStore + SchemaStore> {
    client: Arc<tokio_postgres::Client>,
    replication_store: S,
}

unsafe impl<S: StateStore + SchemaStore> Sync for PostgresLakehouse<S> {}

impl<S: StateStore + SchemaStore> PostgresLakehouse<S> {
    async fn handle_event(&self, event: &etl::types::Event) -> Result<(), anyhow::Error> {
        match event {
            // TODO: find out how this works, how do we store the schema of a newly created table
            Event::Relation(rela) => {
                return Ok(());
            }

            // TODO: implement checkpoint
            // else the lakehouse may contains inconsistent data comparing to the original
            // in postgres
            Event::Commit(commit) => Ok(()),
            Event::Insert(insert) => {
                // let mapping = self
                //     .replication_store
                //     .get_table_mapping(source_table_id)
                //     .await?
                //     .ok_or(anyhow::bail!("missing metadata for table {table_id}"))?;

                // let a = self
                //     .replication_store
                //     .get_table_replication_state(table_id)
                //     .await?
                //     .ok_or(anyhow::bail!("missing metadata for table {table_id}"))?;
                // a.insert.table_row.values
                // find event
                Ok(())
            }
            Event::Begin(begin_evt) => Ok(()),
            Event::Commit(commit) => {
                // pub start_lsn: PgLsn,
                // /// LSN position where the transaction committed.
                // pub commit_lsn: PgLsn,
                // /// Transaction commit flags from PostgreSQL.
                // pub flags: i8,
                // /// Final LSN position after the transaction.
                // pub end_lsn: u64,
                // /// Transaction commit timestamp in PostgreSQL format.
                // pub timestamp: i64,
                Ok(())
            }
            _ => {
                unimplemented!()
            }
        }
    }
    async fn handle_events(&self, events: Vec<etl::types::Event>) -> Result<(), anyhow::Error> {
        println!("received events {:?}", events);

        // each of these events in ops are continous mutating event
        // within 1 transaction
        for evt in events {
            self.handle_event(&evt).await?;
        }
        println!("done processing event");

        Ok(())
    }
}
impl<S: StateStore + SchemaStore> Destination for PostgresLakehouse<S> {
    /// This operation is called during initial table synchronization to ensure the
    /// destination table starts from a clean state before bulk loading. The operation
    /// should be atomic and handle cases where the table may not exist.
    fn truncate_table(
        &self,
        table_id: etl::types::TableId,
    ) -> impl Future<Output = etl::error::EtlResult<()>> + Send {
        future::ready(Ok(()))
    }
    fn write_table_rows(
        &self,
        table_id: etl::types::TableId,
        table_rows: Vec<etl::types::TableRow>,
    ) -> impl Future<Output = etl::error::EtlResult<()>> + Send {
        println!(
            "full sync is not implemented yet, to play around, create the table from scratch without any data"
        );

        future::ready(Ok(()))
    }
    fn write_events(
        &self,
        events: Vec<etl::types::Event>,
    ) -> impl Future<Output = etl::error::EtlResult<()>> + Send {
        println!("writing events {:?}", events);
        async move {
            let result = self.handle_events(events).await;
            match result {
                Ok(_) => Ok(()),
                Err(e) => {
                    println!("{}", e);
                    Err(EtlError::from((
                        ErrorKind::Unknown,
                        "unknown",
                        e.to_string(),
                    )))
                }
            }
        }
    }
}
