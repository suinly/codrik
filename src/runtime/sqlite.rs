use std::path::Path;

use anyhow::{Context, Result, anyhow};
use tokio_rusqlite::Connection;
#[cfg(test)]
use tokio_rusqlite::rusqlite::OptionalExtension;
use tokio_rusqlite::rusqlite::TransactionBehavior;

mod actors;
mod artifacts;
mod bundles;
mod checkpoint;
mod dispatch;
mod failures;
mod identity_link;
mod ingress;
mod local_ingress;
mod outbox;
pub mod recovery;
mod retry;

const INITIAL_MIGRATION: &str = include_str!("migrations/0001_runtime.sql");
const SERVE_MIGRATION: &str = include_str!("migrations/0002_serve.sql");
const IDENTITY_LINKING_MIGRATION: &str = include_str!("migrations/0003_identity_linking.sql");

#[derive(Clone)]
pub struct SqliteRuntimeStore {
    connection: Connection,
    #[cfg(test)]
    fail_next_tool_start: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

fn map_call_error(error: tokio_rusqlite::Error<anyhow::Error>) -> anyhow::Error {
    match error {
        tokio_rusqlite::Error::Error(error) => error,
        other => anyhow!(other.to_string()),
    }
}

pub(crate) fn is_authority_failure(error: &anyhow::Error) -> bool {
    retry::is_authority_failure(error)
        || error
            .to_string()
            .to_ascii_lowercase()
            .contains("unsupported runtime schema")
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
                        migrate_to_v3(connection)?;
                    }
                    1 => {
                        migrate_to_v2(connection)?;
                        migrate_to_v3(connection)?;
                    }
                    2 => migrate_to_v3(connection)?,
                    3 => {}
                    other => anyhow::bail!("unsupported runtime schema version: {other}"),
                }
                connection.execute_batch("PRAGMA busy_timeout = 0;")?;
                Ok(())
            })
            .await
            .map_err(map_call_error)
            .context("failed to initialize runtime database")?;

        Ok(Self {
            connection,
            #[cfg(test)]
            fail_next_tool_start: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }

    #[cfg(test)]
    pub(crate) fn fail_next_tool_start_for_test(&self) {
        self.fail_next_tool_start
            .store(true, std::sync::atomic::Ordering::SeqCst);
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
                    archived_nonnull_errors: scalar(
                        "SELECT count(*) FROM legacy_outbox_archive WHERE last_error IS NOT NULL",
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

fn migrate_to_v3(connection: &mut tokio_rusqlite::rusqlite::Connection) -> Result<()> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(IDENTITY_LINKING_MIGRATION)?;
    let foreign_key_errors = {
        let mut statement = transaction.prepare("PRAGMA foreign_key_check")?;
        statement
            .query([])?
            .mapped(|row| row.get::<_, String>(0))
            .count()
    };
    if foreign_key_errors != 0 {
        anyhow::bail!("schema v3 migration left {foreign_key_errors} foreign key violations");
    }
    transaction.execute_batch("PRAGMA user_version = 3;")?;
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
    archived_nonnull_errors: i64,
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
    use std::{ffi::OsString, path::Path, path::PathBuf};

    use anyhow::{Result, anyhow};
    use tokio_rusqlite::{Connection, rusqlite::OptionalExtension};
    use uuid::Uuid;

    use super::{INITIAL_MIGRATION, SqliteRuntimeStore};

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new(label: &str) -> Self {
            Self {
                path: std::env::temp_dir()
                    .join(format!("codrik-{label}-{}.sqlite3", Uuid::new_v4())),
            }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let mut path: OsString = self.path.as_os_str().to_owned();
                path.push(suffix);
                let _ = std::fs::remove_file(path);
            }
        }
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
                for (id, state) in [
                    ("work-waiting", "waiting"),
                    ("work-unknown", "blocked_unknown_outcome"),
                    ("work-decision", "waiting_for_decision"),
                ] {
                    connection.execute(
                        "INSERT INTO work_items (id, actor_id, kind, audience_kind, state, created_at, updated_at)
                         VALUES (?1, 'actor-1', 'interactive', 'actor_private', ?2, 2, 3)",
                        (id, state),
                    )?;
                }
                for (index, id, state, payload) in [
                    (0, "event-1", "pending", r#"{"kind":"pending-evidence"}"#),
                    (1, "event-processing", "processing", r#"{"kind":"processing-evidence"}"#),
                    (2, "event-blocked", "blocked", r#"{"kind":"blocked-evidence"}"#),
                ] {
                    connection.execute(
                        "INSERT INTO events (id, actor_id, work_item_id, mailbox_sequence, gateway,
                            external_id, kind, audience_kind, payload_json, state, created_at, updated_at)
                         VALUES (?1, 'actor-1', 'work-1', ?2, 'test', ?3,
                            'user_message', 'actor_private', ?4, ?5, 4, 5)",
                        (id, index, format!("external-{index}"), payload, state),
                    )?;
                }
                connection.execute(
                    "INSERT INTO runs (id, actor_id, work_item_id, state, lease_generation,
                        observed_sequence, created_at, updated_at)
                     VALUES ('run-1', 'actor-1', 'work-1', 'active', 1, 0, 1, 1)",
                    [],
                )?;
                for (id, call, state, arguments, capabilities, outcome) in [
                    (
                        "attempt-prepared",
                        "call-prepared",
                        "prepared",
                        r#"{"path":"/prepared"}"#,
                        r#"["fs_read"]"#,
                        None,
                    ),
                    (
                        "attempt-1",
                        "call-running",
                        "running",
                        r#"{"path":"/running"}"#,
                        r#"["fs_read"]"#,
                        None,
                    ),
                    (
                        "attempt-unknown",
                        "call-unknown",
                        "outcome_unknown",
                        r#"{"path":"/ambiguous"}"#,
                        r#"["fs_read"]"#,
                        Some(r#"{"error":"unknown"}"#),
                    ),
                    (
                        "attempt-decision",
                        "call-decision",
                        "waiting_for_decision",
                        r#"{"path":"/decision"}"#,
                        r#"["fs_read","network"]"#,
                        Some(r#"{"decision":"required"}"#),
                    ),
                ] {
                    connection.execute(
                        "INSERT INTO tool_attempts (id, run_id, tool_call_id, tool_name,
                            arguments_json, capabilities_json, state, outcome_json, created_at, updated_at)
                         VALUES (?1, 'run-1', ?2, 'read_file', ?3, ?4, ?5, ?6, 6, 7)",
                        (id, call, arguments, capabilities, state, outcome),
                    )?;
                }
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
        assert_eq!(probe.user_version, 3);
        assert_eq!(probe.foreign_key_errors, 0);
        assert!(!tables.contains(&"runtime_metadata".to_string()));
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
        let outbox_columns = store
            .connection
            .call(|connection| {
                let mut statement = connection.prepare("PRAGMA table_info(outbox)")?;
                statement
                    .query_map([], |row| row.get::<_, String>(1))?
                    .collect::<std::result::Result<Vec<_>, _>>()
            })
            .await?;
        for lifecycle_column in [
            "state",
            "attempt_count",
            "claim_owner",
            "claim_expires_at",
            "last_error",
            "updated_at",
        ] {
            assert!(
                !outbox_columns
                    .iter()
                    .any(|column| column == lifecycle_column),
                "v2 outbox must not contain delivery lifecycle column {lifecycle_column}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn fresh_database_applies_identity_linking_schema_v3() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let (foreign_keys, tables) = store.schema_probe().await?;
        let version = store
            .connection
            .call(|connection| {
                connection.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
            })
            .await?;

        assert!(foreign_keys);
        assert_eq!(version, 3);
        assert!(tables.contains(&"identity_link_codes".to_string()));
        assert!(tables.contains(&"identity_link_attempts".to_string()));
        Ok(())
    }

    #[tokio::test]
    async fn v2_to_v3_preserves_actor_and_identity_rows() -> Result<()> {
        let db = TempDb::new("identity-link-v3");
        let connection = Connection::open(db.path()).await?;
        connection
            .call(|connection| -> Result<()> {
                connection.execute_batch(INITIAL_MIGRATION)?;
                connection.execute_batch("PRAGMA user_version = 1;")?;
                super::migrate_to_v2(connection)?;
                connection.execute(
                    "INSERT INTO actors(id, enabled, tools_json, created_at)
                     VALUES ('actor', 1, '[\"*\"]', 1)",
                    [],
                )?;
                connection.execute(
                    "INSERT INTO identities(provider, subject, actor_id, username)
                     VALUES ('telegram', '123', 'actor', 'owner')",
                    [],
                )?;
                Ok(())
            })
            .await
            .map_err(super::map_call_error)?;
        connection.close().await?;

        let store = SqliteRuntimeStore::open(db.path()).await?;
        let counts = store
            .connection
            .call(
                |connection| -> tokio_rusqlite::rusqlite::Result<(i64, i64, i64)> {
                    let actors =
                        connection
                            .query_row("SELECT COUNT(*) FROM actors", [], |row| row.get(0))?;
                    let identities =
                        connection
                            .query_row("SELECT COUNT(*) FROM identities", [], |row| row.get(0))?;
                    let version =
                        connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
                    Ok((actors, identities, version))
                },
            )
            .await?;
        assert_eq!(counts, (1_i64, 1_i64, 3_i64));
        Ok(())
    }

    #[tokio::test]
    async fn v1_migration_archives_outbox_and_quarantines_active_work() -> Result<()> {
        let db = TempDb::new("v1-quarantine");
        seed_v1_runtime(db.path()).await?;
        let store = SqliteRuntimeStore::open(db.path()).await?;
        let probe = store.v2_probe().await?;

        assert_eq!(probe.user_version, 3);
        assert_eq!(probe.archived_outbox, 7);
        assert_eq!(probe.archived_outbox_states, 7);
        assert_eq!(probe.archived_unmanaged_files, 1);
        assert_eq!(
            probe.archived_nonnull_errors, 0,
            "authoritative v1 had no outbox last_error source"
        );
        assert_eq!(probe.v2_outbox, 0);
        assert_eq!(probe.active_runs, 0);
        assert_eq!(probe.pending_events, 0);
        assert_eq!(probe.quarantined_entities, 12);
        assert_eq!(probe.actor_leases, 0);
        assert_eq!(probe.work_item_state.as_deref(), Some("failed_terminal"));
        assert_eq!(probe.run_state.as_deref(), Some("failed_terminal"));
        assert_eq!(probe.event_state.as_deref(), Some("failed_terminal"));
        assert_eq!(probe.attempt_state.as_deref(), Some("outcome_unknown"));
        assert_eq!(probe.foreign_key_errors, 0);
        let evidence = store
            .connection
            .call(|connection| {
                Ok::<_, tokio_rusqlite::rusqlite::Error>((
                    connection.query_row(
                        "SELECT count(*) FROM work_items WHERE state IN ('ready','waiting','blocked_unknown_outcome','waiting_for_decision')",
                        [],
                        |row| row.get::<_, i64>(0),
                    )?,
                    connection.query_row(
                        "SELECT state FROM events WHERE id = 'event-blocked'",
                        [],
                        |row| row.get::<_, String>(0),
                    )?,
                    connection.query_row(
                        "SELECT state FROM tool_attempts WHERE id = 'attempt-prepared'",
                        [],
                        |row| row.get::<_, String>(0),
                    )?,
                    connection.query_row(
                        "SELECT state FROM tool_attempts WHERE id = 'attempt-unknown'",
                        [],
                        |row| row.get::<_, String>(0),
                    )?,
                    connection.query_row(
                        "SELECT state FROM tool_attempts WHERE id = 'attempt-decision'",
                        [],
                        |row| row.get::<_, String>(0),
                    )?,
                    connection.query_row(
                        "SELECT count(*) FROM legacy_runtime_quarantine
                         WHERE entity_type = 'event' AND entity_id = 'event-blocked'
                           AND json_extract(snapshot_json, '$.payload_json') = '{\"kind\":\"blocked-evidence\"}'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )?,
                    connection.query_row(
                        "SELECT count(*) FROM legacy_runtime_quarantine
                         WHERE entity_type = 'tool_attempt' AND entity_id = 'attempt-unknown'
                           AND json_extract(snapshot_json, '$.tool_name') = 'read_file'
                           AND json_extract(snapshot_json, '$.arguments_json') = '{\"path\":\"/ambiguous\"}'
                           AND json_extract(snapshot_json, '$.capabilities_json') = '[\"fs_read\"]'
                           AND json_extract(snapshot_json, '$.outcome_json') = '{\"error\":\"unknown\"}'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )?,
                ))
            })
            .await?;
        assert_eq!(evidence.0, 0, "no parent work may remain dispatchable");
        assert_eq!(evidence.1, "failed_terminal");
        assert_eq!(evidence.2, "cancelled_known");
        assert_eq!(evidence.3, "outcome_unknown");
        assert_eq!(evidence.4, "waiting_for_decision");
        assert_eq!(evidence.5, 1, "blocked event payload must be preserved");
        assert_eq!(evidence.6, 1, "ambiguous tool evidence must be complete");
        Ok(())
    }

    #[tokio::test]
    async fn outbox_deliveries_reject_update_delete_and_parent_cascade() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        store
            .connection
            .call(|connection| -> tokio_rusqlite::rusqlite::Result<()> {
                let transaction = connection.transaction()?;
                transaction.execute_batch(
                    "INSERT INTO actors (id, enabled, tools_json, created_at) VALUES ('actor-1', 1, '[]', 1);
                     INSERT INTO work_items (id, actor_id, kind, audience_kind, state, created_at, updated_at)
                     VALUES ('work-1', 'actor-1', 'interactive', 'actor_private', 'completed', 1, 1);
                     INSERT INTO events (id, actor_id, work_item_id, mailbox_sequence, gateway, external_id,
                        kind, audience_kind, payload_json, state, created_at, updated_at)
                     VALUES ('event-1', 'actor-1', 'work-1', 0, 'test', 'external-1', 'user_message',
                        'actor_private', '{}', 'completed', 1, 1);
                     INSERT INTO runs (id, actor_id, work_item_id, state, lease_generation, observed_sequence,
                        created_at, updated_at)
                     VALUES ('run-1', 'actor-1', 'work-1', 'completed', 1, 0, 1, 1);
                     INSERT INTO local_requests (request_id, actor_id, event_id, work_item_id, prompt_sha256,
                        state, created_at, updated_at)
                     VALUES ('request-1', 'actor-1', 'event-1', 'work-1',
                        'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'active', 1, 1);
                     INSERT INTO result_bundles (id, request_id, delivery_count, manifest_sha256, state,
                        created_at, updated_at)
                     VALUES ('bundle-1', 'request-1', 1,
                        'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'pending', 1, 1);
                     UPDATE local_requests SET state = 'completed', result_bundle_id = 'bundle-1' WHERE request_id = 'request-1';
                     INSERT INTO outbox (id, intent_key, actor_id, work_item_id, run_id, intent_class,
                        audience_kind, payload_json, created_at)
                     VALUES ('outbox-1', 'intent-1', 'actor-1', 'work-1', 'run-1', 'response',
                        'actor_private', '{}', 1);
                     INSERT INTO outbox_deliveries (id, outbox_id, bundle_id, ordinal, transport, address, created_at)
                     VALUES ('delivery-1', 'outbox-1', 'bundle-1', 0, 'local_ipc', 'request-1', 1);",
                )?;
                transaction.commit()
            })
            .await?;

        for statement in [
            "UPDATE outbox_deliveries SET ordinal = 1 WHERE id = 'delivery-1'",
            "DELETE FROM outbox_deliveries WHERE id = 'delivery-1'",
            "DELETE FROM result_bundles WHERE id = 'bundle-1'",
        ] {
            let result = store
                .connection
                .call(move |connection| connection.execute(statement, []))
                .await;
            assert!(
                result.is_err(),
                "append-only membership allowed: {statement}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn v1_migration_rolls_back_every_schema_change_on_failure() -> Result<()> {
        let db = TempDb::new("v1-rollback");
        seed_v1_runtime(db.path()).await?;
        let connection = Connection::open(db.path()).await?;
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

        assert!(SqliteRuntimeStore::open(db.path()).await.is_err());
        let connection = Connection::open(db.path()).await?;
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
