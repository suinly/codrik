use std::future::Future;

use anyhow::Result;
use tokio_rusqlite::rusqlite::{Error as SqliteError, ErrorCode};

const BUSY_DELAYS_MS: [u64; 3] = [10, 25, 50];

pub(super) async fn call_with_busy_retry<T, F, Fut>(mut operation: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    for delay in BUSY_DELAYS_MS {
        match operation().await {
            Err(error) if is_busy_or_locked(&error) => {
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            }
            result => return result,
        }
    }
    operation().await
}

pub(super) async fn call_connection_with_busy_retry<T, F>(
    connection: &tokio_rusqlite::Connection,
    operation: F,
) -> Result<T>
where
    T: Send + 'static,
    F: Fn(&mut tokio_rusqlite::rusqlite::Connection) -> Result<T> + Clone + Send + Sync + 'static,
{
    call_with_busy_retry(|| {
        let operation = operation.clone();
        async move {
            connection
                .call(move |connection| operation(connection))
                .await
                .map_err(super::map_call_error)
        }
    })
    .await
}

fn is_busy_or_locked(error: &anyhow::Error) -> bool {
    sqlite_error_code(error)
        .is_some_and(|code| matches!(code, ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked))
}

pub(super) fn is_authority_failure(error: &anyhow::Error) -> bool {
    sqlite_error_code(error).is_some_and(|code| {
        matches!(
            code,
            ErrorCode::DatabaseBusy
                | ErrorCode::DatabaseLocked
                | ErrorCode::SystemIoFailure
                | ErrorCode::DatabaseCorrupt
                | ErrorCode::DiskFull
                | ErrorCode::CannotOpen
                | ErrorCode::ReadOnly
                | ErrorCode::FileLockingProtocolFailed
                | ErrorCode::NotADatabase
        )
    })
}

fn sqlite_error_code(error: &anyhow::Error) -> Option<ErrorCode> {
    error.chain().find_map(|cause| {
        cause
            .downcast_ref::<SqliteError>()
            .and_then(|error| match error {
                SqliteError::SqliteFailure(code, _) => Some(code.code),
                _ => None,
            })
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use anyhow::{Result, anyhow};
    use tokio_rusqlite::rusqlite::{Error, ErrorCode, ffi};

    use super::{call_connection_with_busy_retry, call_with_busy_retry, is_authority_failure};

    fn busy() -> anyhow::Error {
        anyhow!(Error::SqliteFailure(
            ffi::Error {
                code: ErrorCode::DatabaseBusy,
                extended_code: ffi::SQLITE_BUSY
            },
            None,
        ))
    }

    #[tokio::test(start_paused = true)]
    async fn busy_retries_use_exact_bounded_delays() -> Result<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let started = tokio::time::Instant::now();
        let result = call_with_busy_retry({
            let calls = calls.clone();
            move || {
                let calls = calls.clone();
                async move {
                    if calls.fetch_add(1, Ordering::SeqCst) < 3 {
                        Err(busy())
                    } else {
                        Ok(7)
                    }
                }
            }
        })
        .await?;
        assert_eq!(result, 7);
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        assert_eq!(started.elapsed(), std::time::Duration::from_millis(85));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn non_busy_errors_are_not_retried() {
        let calls = AtomicUsize::new(0);
        let error = call_with_busy_retry(|| async {
            calls.fetch_add(1, Ordering::SeqCst);
            Err::<(), _>(anyhow!("corrupt"))
        })
        .await
        .unwrap_err();
        assert_eq!(error.to_string(), "corrupt");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn connection_retry_exhausts_real_sqlite_write_contention() -> Result<()> {
        let path = std::env::temp_dir().join(format!(
            "codrik-busy-retry-{}.sqlite3",
            uuid::Uuid::new_v4()
        ));
        let locker = tokio_rusqlite::Connection::open(&path).await?;
        let contender = tokio_rusqlite::Connection::open(&path).await?;
        locker
            .call(|connection| -> Result<()> {
                connection.busy_timeout(std::time::Duration::ZERO)?;
                connection.execute_batch(
                    "CREATE TABLE values_for_test(value INTEGER); BEGIN IMMEDIATE;
                     INSERT INTO values_for_test VALUES (1);",
                )?;
                Ok(())
            })
            .await
            .map_err(super::super::map_call_error)?;
        contender
            .call(|connection| -> Result<()> {
                connection.busy_timeout(std::time::Duration::ZERO)?;
                Ok(())
            })
            .await
            .map_err(super::super::map_call_error)?;

        let calls = Arc::new(AtomicUsize::new(0));
        let started = tokio::time::Instant::now();
        let error = call_connection_with_busy_retry(&contender, {
            let calls = calls.clone();
            move |connection| -> Result<()> {
                calls.fetch_add(1, Ordering::SeqCst);
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                transaction.commit()?;
                Ok(())
            }
        })
        .await
        .unwrap_err();

        assert_eq!(calls.load(Ordering::SeqCst), 4);
        assert_eq!(started.elapsed(), std::time::Duration::from_millis(85));
        assert!(is_authority_failure(&error));
        locker
            .call(|connection| -> Result<()> {
                connection.execute_batch("ROLLBACK")?;
                Ok(())
            })
            .await
            .map_err(super::super::map_call_error)?;
        std::fs::remove_file(path)?;
        Ok(())
    }

    #[tokio::test]
    async fn connection_retry_propagates_non_busy_error_once() {
        let connection = tokio_rusqlite::Connection::open_in_memory().await.unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let error = call_connection_with_busy_retry(&connection, {
            let calls = calls.clone();
            move |_connection| -> Result<()> {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(anyhow!("invalid checkpoint transition"))
            }
        })
        .await
        .unwrap_err();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(error.to_string(), "invalid checkpoint transition");
    }

    #[test]
    fn sqlite_authority_codes_are_classified_without_string_matching() {
        let corrupt = anyhow!(Error::SqliteFailure(
            ffi::Error {
                code: ErrorCode::DatabaseCorrupt,
                extended_code: ffi::SQLITE_CORRUPT,
            },
            None,
        ));
        assert!(is_authority_failure(&corrupt));
        assert!(!is_authority_failure(&anyhow!("model unavailable")));
    }
}
