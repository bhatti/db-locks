use std::time::Duration;

use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};
use diesel::result::DatabaseErrorKind;
use diesel_migrations::{FileBasedMigrations, MigrationHarness};
use r2d2::Error;

use crate::domain::models::{LockError, LocksConfig};

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

impl std::convert::From<diesel::result::Error> for LockError {
    fn from(err: diesel::result::Error) -> Self {
        let (retryable, reason) = retryable_db_error(&err);
        if retryable {
            LockError::unavailable(
                format!("database error {:?} {:?}", err, reason).as_str(), reason, true)
        } else if Some("NotFound".to_string()) == reason {
            LockError::not_found(
                format!("not found error {:?} {:?}", err, reason).as_str())
        } else {
            LockError::database(
                format!("database error {:?} {:?}", err, reason).as_str(), reason, false)
        }
    }
}

pub(crate) fn retryable_db_error(err: &diesel::result::Error) -> (bool, Option<String>) {
    match err {
        diesel::result::Error::InvalidCString(_) => { (false, Some("InvalidCString".to_string())) }
        diesel::result::Error::DatabaseError(kind, _) => {
            match kind {
                DatabaseErrorKind::UniqueViolation => { (false, Some("UniqueViolation".to_string())) }
                DatabaseErrorKind::ForeignKeyViolation => { (false, Some("ForeignKeyViolation".to_string())) }
                DatabaseErrorKind::UnableToSendCommand => { (true, Some("UnableToSendCommand".to_string())) }
                DatabaseErrorKind::SerializationFailure => { (false, Some("SerializationFailure".to_string())) }
                DatabaseErrorKind::ReadOnlyTransaction => { (false, Some("ReadOnlyTransaction".to_string())) }
                DatabaseErrorKind::NotNullViolation => { (false, Some("NotNullViolation".to_string())) }
                DatabaseErrorKind::CheckViolation => { (false, Some("CheckViolation".to_string())) }
                DatabaseErrorKind::ClosedConnection => { (true, Some("ClosedConnection".to_string())) }
                DatabaseErrorKind::Unknown => { (true, Some("Unknown".to_string())) }
                _ => { (true, None) }
            }
        }
        diesel::result::Error::NotFound => { (false, Some("NotFound".to_string())) }
        diesel::result::Error::QueryBuilderError(_) => { (false, Some("QueryBuilderError".to_string())) }
        diesel::result::Error::DeserializationError(_) => { (false, Some("DeserializationError".to_string())) }
        diesel::result::Error::SerializationError(_) => { (false, Some("SerializationError".to_string())) }
        diesel::result::Error::RollbackErrorOnCommit { .. } => { (false, Some("RollbackErrorOnCommit".to_string())) }
        diesel::result::Error::RollbackTransaction => { (false, Some("RollbackTransaction".to_string())) }
        diesel::result::Error::AlreadyInTransaction => { (false, Some("AlreadyInTransaction".to_string())) }
        diesel::result::Error::NotInTransaction => { (false, Some("NotInTransaction".to_string())) }
        diesel::result::Error::BrokenTransactionManager => { (false, Some("BrokenTransactionManager".to_string())) }
        _ => { (true, None) }
    }
}

pub(crate) fn build_pg_pool(config: &LocksConfig) -> Result<Pool<ConnectionManager<PgConnection>>, Error> {
    log::info!("building postgres connection pool for {:?}", config);

    let manager = ConnectionManager::<PgConnection>::new(config.get_database_url().clone());

    if config.should_run_database_migrations() {
        let mut conn = PgConnection::establish(&config.get_database_url()).unwrap();
        let migrations = FileBasedMigrations::find_migrations_directory().unwrap();
        conn.run_pending_migrations(migrations).unwrap();
        conn.begin_test_transaction().unwrap();
    }

    Pool::builder()
        .max_size(config.get_database_pool_size())
        .test_on_check_out(true)
        .build(manager)
}

pub(crate) fn build_sqlite_pool(config: &LocksConfig) -> Result<Pool<ConnectionManager<SqliteConnection>>, Error> {
    log::info!("building sqlite connection pool for {:?}", config);

    let manager = ConnectionManager::<SqliteConnection>::new(config.get_database_url().clone());

    if config.should_run_database_migrations() {
        let mut conn = build_sqlite_connection(&config.get_database_url()).unwrap();
        let migrations = FileBasedMigrations::find_migrations_directory().unwrap();
        let _ = conn.run_pending_migrations(migrations).unwrap();
        conn.begin_test_transaction().unwrap();
    }

    Pool::builder()
        .max_size(config.get_database_pool_size())
        .connection_customizer(Box::new(ConnectionOptions {
            enable_wal: true,
            enable_foreign_keys: true,
            busy_timeout: Some(Duration::from_secs(60)),
        }))
        .test_on_check_out(true)
        .build(manager)
}

fn build_sqlite_connection(database_url: &String) -> ConnectionResult<SqliteConnection> {
    SqliteConnection::establish(database_url)
}
