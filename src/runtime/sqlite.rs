use std::path::Path;

use anyhow::{Context, Result, anyhow};
use tokio_rusqlite::Connection;

const INITIAL_MIGRATION: &str = include_str!("migrations/0001_runtime.sql");

#[derive(Clone)]
pub struct SqliteRuntimeStore {
    connection: Connection,
}

impl SqliteRuntimeStore {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let connection = Connection::open(path)
            .await
            .context("failed to open runtime database")?;
        Self::initialize(connection, true).await
    }

    pub async fn open_in_memory() -> Result<Self> {
        let connection = Connection::open_in_memory()
            .await
            .context("failed to open in-memory runtime database")?;
        Self::initialize(connection, false).await
    }

    async fn initialize(connection: Connection, use_wal: bool) -> Result<Self> {
        connection
            .call(move |connection| -> tokio_rusqlite::rusqlite::Result<()> {
                connection
                    .execute_batch("PRAGMA foreign_keys = ON; PRAGMA busy_timeout = 5000;")?;
                if use_wal {
                    connection.execute_batch("PRAGMA journal_mode = WAL;")?;
                }
                connection.execute_batch(INITIAL_MIGRATION)?;
                Ok(())
            })
            .await
            .map_err(|error| anyhow!("failed to initialize runtime database: {error}"))?;

        Ok(Self { connection })
    }

    #[cfg(test)]
    async fn schema_probe(&self) -> Result<(bool, Vec<String>)> {
        self.connection
            .call(
                |connection| -> tokio_rusqlite::rusqlite::Result<(bool, Vec<String>)> {
                    let foreign_keys = connection
                        .query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))?
                        == 1;
                    let mut statement = connection.prepare(
                        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name",
                    )?;
                    let tables = statement
                        .query_map([], |row| row.get(0))?
                        .collect::<std::result::Result<Vec<String>, _>>()?;
                    Ok((foreign_keys, tables))
                },
            )
            .await
            .map_err(|error| anyhow!("failed to inspect runtime schema: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use super::SqliteRuntimeStore;

    #[tokio::test]
    async fn migration_creates_runtime_tables_and_enables_foreign_keys() {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        let (foreign_keys, tables) = store.schema_probe().await.unwrap();

        assert!(foreign_keys);
        for table in [
            "actors",
            "identities",
            "events",
            "work_items",
            "actor_leases",
            "runs",
            "run_events",
            "recent_messages",
            "tool_attempts",
            "outbox",
        ] {
            assert!(tables.contains(&table.to_string()), "missing {table}");
        }
    }
}
