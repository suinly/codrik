use std::path::Path;

use anyhow::{Context, Result, anyhow};
use tokio_rusqlite::Connection;
#[cfg(test)]
use tokio_rusqlite::rusqlite::OptionalExtension;
use tokio_rusqlite::rusqlite::TransactionBehavior;

mod checkpoint;
mod dispatch;
mod ingress;
mod outbox;

const INITIAL_MIGRATION: &str = include_str!("migrations/0001_runtime.sql");
const SERVE_MIGRATION: &str = include_str!("migrations/0002_serve.sql");

#[derive(Clone)]
pub struct SqliteRuntimeStore {
    connection: Connection,
}

fn map_call_error(error: tokio_rusqlite::Error<anyhow::Error>) -> anyhow::Error {
    match error {
        tokio_rusqlite::Error::Error(error) => error,
        other => anyhow!(other.to_string()),
    }
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
            .call(move |connection| -> Result<()> {
                connection
                    .execute_batch("PRAGMA foreign_keys = ON; PRAGMA busy_timeout = 5000;")?;
                if use_wal {
                    connection.execute_batch("PRAGMA journal_mode = WAL;")?;
                }
                let version =
                    connection.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))?;
                match version {
                    0 => {
                        let transaction = connection.transaction()?;
                        transaction.execute_batch(INITIAL_MIGRATION)?;
                        transaction.execute_batch("PRAGMA user_version = 1;")?;
                        transaction.commit()?;
                        migrate_to_v2(connection)?;
                    }
                    1 => migrate_to_v2(connection)?,
                    2 => {}
                    other => anyhow::bail!("unsupported runtime schema version: {other}"),
                }
                Ok(())
            })
            .await
            .map_err(map_call_error)
            .context("failed to initialize runtime database")?;

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

    #[cfg(test)]
    async fn v2_probe(&self) -> Result<V2Probe> {
        self.connection
            .call(|connection| -> tokio_rusqlite::rusqlite::Result<V2Probe> {
                let scalar = |sql: &str| connection.query_row(sql, [], |row| row.get(0));
                let foreign_key_errors = {
                    let mut statement = connection.prepare("PRAGMA foreign_key_check")?;
                    statement.query([])?.mapped(|row| row.get::<_, String>(0)).count()
                };
                Ok(V2Probe {
                    user_version: scalar("PRAGMA user_version")?,
                    archived_outbox: scalar("SELECT count(*) FROM legacy_outbox_archive")?,
                    archived_outbox_states: scalar(
                        "SELECT count(DISTINCT state) FROM legacy_outbox_archive",
                    )?,
                    archived_unmanaged_files: scalar(
                        "SELECT count(*) FROM legacy_outbox_archive WHERE payload_json LIKE '%/tmp/unmanaged.txt%'",
                    )?,
                    v2_outbox: scalar("SELECT count(*) FROM outbox")?,
                    active_runs: scalar("SELECT count(*) FROM runs WHERE state = 'active'")?,
                    pending_events: scalar(
                        "SELECT count(*) FROM events WHERE state IN ('pending', 'processing')",
                    )?,
                    quarantined_entities: scalar(
                        "SELECT count(*) FROM legacy_runtime_quarantine",
                    )?,
                    actor_leases: scalar("SELECT count(*) FROM actor_leases")?,
                    work_item_state: connection
                        .query_row("SELECT state FROM work_items WHERE id = 'work-1'", [], |row| {
                            row.get(0)
                        })
                        .optional()?,
                    run_state: connection
                        .query_row("SELECT state FROM runs WHERE id = 'run-1'", [], |row| {
                            row.get(0)
                        })
                        .optional()?,
                    event_state: connection
                        .query_row("SELECT state FROM events WHERE id = 'event-1'", [], |row| {
                            row.get(0)
                        })
                        .optional()?,
                    attempt_state: connection
                        .query_row(
                            "SELECT state FROM tool_attempts WHERE id = 'attempt-1'",
                            [],
                            |row| row.get(0),
                        )
                        .optional()?,
                    foreign_key_errors,
                })
            })
            .await
            .map_err(|error| anyhow!("failed to inspect v2 runtime schema: {error}"))
    }
}

fn migrate_to_v2(connection: &mut tokio_rusqlite::rusqlite::Connection) -> Result<()> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let source_outbox = transaction.query_row("SELECT count(*) FROM outbox", [], |row| {
        row.get::<_, i64>(0)
    })?;
    transaction.execute_batch(SERVE_MIGRATION)?;
    let archived_outbox =
        transaction.query_row("SELECT count(*) FROM legacy_outbox_archive", [], |row| {
            row.get::<_, i64>(0)
        })?;
    if archived_outbox != source_outbox {
        anyhow::bail!(
            "v1 outbox archive count mismatch: source={source_outbox}, archive={archived_outbox}"
        );
    }

    let foreign_key_errors = {
        let mut statement = transaction.prepare("PRAGMA foreign_key_check")?;
        statement
            .query([])?
            .mapped(|row| row.get::<_, String>(0))
            .count()
    };
    if foreign_key_errors != 0 {
        anyhow::bail!("schema v2 migration left {foreign_key_errors} foreign key violations");
    }

    transaction.execute_batch("PRAGMA user_version = 2;")?;
    transaction.commit()?;
    Ok(())
}

#[cfg(test)]
#[derive(Debug)]
struct V2Probe {
    user_version: i64,
    archived_outbox: i64,
    archived_outbox_states: i64,
    archived_unmanaged_files: i64,
    v2_outbox: i64,
    active_runs: i64,
    pending_events: i64,
    quarantined_entities: i64,
    actor_leases: i64,
    work_item_state: Option<String>,
    run_state: Option<String>,
    event_state: Option<String>,
    attempt_state: Option<String>,
    foreign_key_errors: usize,
}

#[cfg(test)]
mod tests {
    use std::{path::Path, path::PathBuf};

    use anyhow::{Result, anyhow};
    use tokio_rusqlite::{Connection, rusqlite::OptionalExtension};
    use uuid::Uuid;

    use super::{INITIAL_MIGRATION, SqliteRuntimeStore};

    fn temp_db_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!("codrik-{label}-{}.sqlite3", Uuid::new_v4()))
    }

    async fn seed_v1_runtime(path: &Path) -> Result<()> {
        let connection = Connection::open(path).await?;
        connection
            .call(|connection| -> Result<()> {
                connection.execute_batch(INITIAL_MIGRATION)?;
                connection.execute_batch("PRAGMA user_version = 1;")?;
                connection.execute(
                    "INSERT INTO actors (id, enabled, tools_json, created_at) VALUES ('actor-1', 1, '[]', 1)",
                    [],
                )?;
                connection.execute(
                    "INSERT INTO work_items (id, actor_id, kind, audience_kind, state, created_at, updated_at)
                     VALUES ('work-1', 'actor-1', 'interactive', 'actor_private', 'ready', 1, 1)",
                    [],
                )?;
                connection.execute(
                    "INSERT INTO events (id, actor_id, work_item_id, mailbox_sequence, gateway,
                        external_id, kind, audience_kind, payload_json, state, created_at, updated_at)
                     VALUES ('event-1', 'actor-1', 'work-1', 0, 'test', 'external-1',
                        'user_message', 'actor_private', '{}', 'pending', 1, 1)",
                    [],
                )?;
                connection.execute(
                    "INSERT INTO runs (id, actor_id, work_item_id, state, lease_generation,
                        observed_sequence, created_at, updated_at)
                     VALUES ('run-1', 'actor-1', 'work-1', 'active', 1, 0, 1, 1)",
                    [],
                )?;
                connection.execute(
                    "INSERT INTO tool_attempts (id, run_id, tool_call_id, tool_name,
                        arguments_json, capabilities_json, state, created_at, updated_at)
                     VALUES ('attempt-1', 'run-1', 'call-1', 'read_file', '{}', '[]',
                        'running', 1, 1)",
                    [],
                )?;
                connection.execute(
                    "INSERT INTO actor_leases (actor_id, generation, owner_id, expires_at)
                     VALUES ('actor-1', 1, 'owner-1', 999999)",
                    [],
                )?;

                for (index, state) in [
                    "pending",
                    "delivering",
                    "delivered",
                    "failed_retryable",
                    "failed_terminal",
                    "outcome_unknown",
                    "acknowledged_duplicate",
                ]
                .into_iter()
                .enumerate()
                {
                    let payload = if index == 0 {
                        r#"{"type":"file","path":"/tmp/unmanaged.txt"}"#
                    } else {
                        r#"{"type":"text","text":"legacy"}"#
                    };
                    connection.execute(
                        "INSERT INTO outbox (id, intent_key, actor_id, work_item_id, run_id,
                            intent_class, audience_kind, payload_json, state, attempt_count,
                            claim_owner, claim_expires_at, created_at, updated_at)
                         VALUES (?1, ?2, 'actor-1', 'work-1', 'run-1', 'response',
                            'actor_private', ?3, ?4, ?5, 'legacy-owner', 999999, 1, 1)",
                        (
                            format!("outbox-{index}"),
                            format!("intent-{index}"),
                            payload,
                            state,
                            index as i64,
                        ),
                    )?;
                }
                Ok(())
            })
            .await
            .map_err(|error| anyhow!(error.to_string()))?;
        connection.close().await?;
        Ok(())
    }

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

    #[tokio::test]
    async fn fresh_database_applies_v1_then_v2_with_foreign_key_integrity() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let (foreign_keys, tables) = store.schema_probe().await?;
        let probe = store.v2_probe().await?;

        assert!(foreign_keys);
        assert_eq!(probe.user_version, 2);
        assert_eq!(probe.foreign_key_errors, 0);
        for table in [
            "local_requests",
            "result_bundles",
            "artifacts",
            "outbox",
            "outbox_deliveries",
            "cancel_targets",
            "legacy_outbox_archive",
            "legacy_runtime_quarantine",
        ] {
            assert!(tables.contains(&table.to_string()), "missing {table}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn v1_migration_archives_outbox_and_quarantines_active_work() -> Result<()> {
        let path = temp_db_path("v1-quarantine");
        seed_v1_runtime(&path).await?;
        let store = SqliteRuntimeStore::open(&path).await?;
        let probe = store.v2_probe().await?;

        assert_eq!(probe.user_version, 2);
        assert_eq!(probe.archived_outbox, 7);
        assert_eq!(probe.archived_outbox_states, 7);
        assert_eq!(probe.archived_unmanaged_files, 1);
        assert_eq!(probe.v2_outbox, 0);
        assert_eq!(probe.active_runs, 0);
        assert_eq!(probe.pending_events, 0);
        assert_eq!(probe.quarantined_entities, 4);
        assert_eq!(probe.actor_leases, 0);
        assert_eq!(probe.work_item_state.as_deref(), Some("failed_terminal"));
        assert_eq!(probe.run_state.as_deref(), Some("failed_terminal"));
        assert_eq!(probe.event_state.as_deref(), Some("failed_terminal"));
        assert_eq!(probe.attempt_state.as_deref(), Some("outcome_unknown"));
        assert_eq!(probe.foreign_key_errors, 0);
        Ok(())
    }

    #[tokio::test]
    async fn v1_migration_rolls_back_every_schema_change_on_failure() -> Result<()> {
        let path = temp_db_path("v1-rollback");
        seed_v1_runtime(&path).await?;
        let connection = Connection::open(&path).await?;
        connection
            .call(|connection| {
                connection.execute_batch(
                    "CREATE TRIGGER reject_terminalization
                     BEFORE UPDATE ON work_items
                     BEGIN SELECT RAISE(ABORT, 'terminalization rejected'); END;",
                )
            })
            .await?;
        connection.close().await?;

        assert!(SqliteRuntimeStore::open(&path).await.is_err());
        let connection = Connection::open(&path).await?;
        let (version, old_outbox, archive_exists) = connection
            .call(|connection| {
                Ok::<_, tokio_rusqlite::rusqlite::Error>((
                    connection.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))?,
                    connection.query_row("SELECT count(*) FROM outbox", [], |row| {
                        row.get::<_, i64>(0)
                    })?,
                    connection
                        .query_row(
                            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'legacy_outbox_archive'",
                            [],
                            |row| row.get::<_, i64>(0),
                        )
                        .optional()?
                        .is_some(),
                ))
            })
            .await?;
        assert_eq!(version, 1);
        assert_eq!(old_outbox, 7);
        assert!(!archive_exists);
        Ok(())
    }
}
