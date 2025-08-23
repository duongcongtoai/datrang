use etl::config::{IntoConnectOptions, PgConnectionConfig};
use sqlx::Executor;
use sqlx::postgres::PgPoolOptions;

pub async fn main_migrate(pg_connection_config: &PgConnectionConfig) -> Result<(), anyhow::Error> {
    let options = pg_connection_config.with_db();
    let pool = PgPoolOptions::new()
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                // Create the etl schema if it doesn't exist
                conn.execute("create schema if not exists etl;").await?;
                // We set the search_path to etl so that the _sqlx_migrations
                // metadata table is created inside that schema instead of the public
                // schema
                conn.execute("set search_path = 'etl';").await?;

                Ok(())
            })
        })
        .connect_with(options)
        .await?;

    let migrator = sqlx::migrate!("./migrations");
    migrator.run(&pool).await?;

    Ok(())
}
