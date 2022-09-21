use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};
use r2d2::Error;
use diesel_migrations::{FileBasedMigrations, MigrationHarness};
use std::time::Duration;
use diesel::connection::SimpleConnection;
use std::env;

pub(crate) fn build_pg_pool(database_url: String) -> Result<Pool<ConnectionManager<PgConnection>>, Error> {
    log::info!("building postgres connection pool for {}", database_url.clone());

    let manager = ConnectionManager::<PgConnection>::new(database_url.clone());

    if env::var("DATABASE_URL").is_err(){
        let mut conn = PgConnection::establish(&database_url).unwrap();
        let migrations = FileBasedMigrations::find_migrations_directory().unwrap();
        conn.run_pending_migrations(migrations).unwrap();
        conn.begin_test_transaction().unwrap();
    }

    Pool::builder()
        .test_on_check_out(true)
        .build(manager)
}

#[derive(Debug)]
pub struct ConnectionOptions {
    pub enable_wal: bool,
    pub enable_foreign_keys: bool,
    pub busy_timeout: Option<Duration>,
}

impl diesel::r2d2::CustomizeConnection<SqliteConnection, diesel::r2d2::Error> for ConnectionOptions {
    fn on_acquire(&self, conn: &mut SqliteConnection) -> Result<(), diesel::r2d2::Error> {
        (|| {
            if self.enable_wal {
                conn.batch_execute("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;")?;
            }
            if self.enable_foreign_keys {
                conn.batch_execute("PRAGMA foreign_keys = ON;")?;
            }
            if let Some(d) = self.busy_timeout {
                conn.batch_execute(&format!("PRAGMA busy_timeout = {};", d.as_millis()))?;
            }
            Ok(())
        })()
            .map_err(diesel::r2d2::Error::QueryError)
    }
}

pub(crate) fn build_sqlite_pool(database_url: String) -> Result<Pool<ConnectionManager<SqliteConnection>>, Error> {
    let manager = ConnectionManager::<SqliteConnection>::new(database_url.clone());
    if env::var("DATABASE_URL").is_err(){
        let mut conn = build_sqlite_connection(database_url.clone()).unwrap();
        let migrations = FileBasedMigrations::find_migrations_directory().unwrap();
        log::info!("building sqlite connection pool for {}", database_url.clone());
        let _ = conn.run_pending_migrations(migrations).unwrap();
        conn.begin_test_transaction().unwrap();
    }

    Pool::builder()
        .max_size(64)
        .connection_customizer(Box::new(ConnectionOptions {
            enable_wal: true,
            enable_foreign_keys: true,
            busy_timeout: Some(Duration::from_secs(60)),
        }))
        .test_on_check_out(true)
        .build(manager)
}

fn build_sqlite_connection(database_url: String) -> ConnectionResult<SqliteConnection> {
    SqliteConnection::establish(&database_url)
}
