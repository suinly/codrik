use anyhow::{Result, bail};
use async_trait::async_trait;
use tokio_rusqlite::{params, rusqlite::OptionalExtension};

use crate::runtime::{
    gateway::{GatewayCommandKey, GatewayCommandOutcome},
    model::{ActorId, Timestamp},
    sqlite::{SqliteRuntimeStore, map_call_error},
    store::{
        IdentityLinkStore, LinkIdentity, StoreLinkCodeReplacement, StoreLinkCommandRedemption,
        StoreLinkRedemption,
    },
};

const ATTEMPT_WINDOW_MILLIS: i64 = 600_000;

#[async_trait]
impl IdentityLinkStore for SqliteRuntimeStore {
    async fn replace_link_code(
        &self,
        actor: &ActorId,
        code_hash: [u8; 32],
        created_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<StoreLinkCodeReplacement> {
        if expires_at.0 <= created_at.0 {
            bail!("identity link expiry must be after creation");
        }
        let actor = actor.clone();
        self.connection
            .call(move |connection| -> Result<StoreLinkCodeReplacement> {
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                let enabled = transaction
                    .query_row(
                        "SELECT enabled FROM actors WHERE id = ?1",
                        [actor.as_str()],
                        |row| row.get::<_, bool>(0),
                    )
                    .optional()?;
                match enabled {
                    Some(true) => {}
                    Some(false) => bail!("runtime actor {actor} is disabled"),
                    None => bail!("runtime actor {actor} does not exist"),
                }
                let collision = transaction
                    .query_row(
                        "SELECT 1 FROM identity_link_codes
                         WHERE code_hash = ?1 AND actor_id <> ?2",
                        params![code_hash.as_slice(), actor.as_str()],
                        |_| Ok(()),
                    )
                    .optional()?
                    .is_some();
                if collision {
                    return Ok(StoreLinkCodeReplacement::HashCollision);
                }
                transaction.execute(
                    "INSERT INTO identity_link_codes(actor_id, code_hash, created_at, expires_at)
                     VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT(actor_id) DO UPDATE SET
                         code_hash = excluded.code_hash,
                         created_at = excluded.created_at,
                         expires_at = excluded.expires_at",
                    params![
                        actor.as_str(),
                        code_hash.as_slice(),
                        created_at.0,
                        expires_at.0
                    ],
                )?;
                transaction.commit()?;
                Ok(StoreLinkCodeReplacement::Stored)
            })
            .await
            .map_err(map_call_error)
    }

    async fn redeem_link_code(
        &self,
        identity: LinkIdentity,
        code_hash: Option<[u8; 32]>,
        now: Timestamp,
    ) -> Result<StoreLinkRedemption> {
        if identity.provider.trim().is_empty() || identity.subject.trim().is_empty() {
            bail!("identity provider and subject must not be blank");
        }
        self.connection
            .call(move |connection| -> Result<StoreLinkRedemption> {
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                let outcome = redeem_in_transaction(&transaction, &identity, code_hash, now)?;
                transaction.commit()?;
                Ok(outcome)
            })
            .await
            .map_err(map_call_error)
    }

    async fn redeem_link_code_once(
        &self,
        key: GatewayCommandKey,
        identity: LinkIdentity,
        code_hash: Option<[u8; 32]>,
        now: Timestamp,
    ) -> Result<StoreLinkCommandRedemption> {
        if key.gateway.trim().is_empty() || key.external_id.trim().is_empty() {
            bail!("gateway command key must not be blank");
        }
        if identity.provider.trim().is_empty() || identity.subject.trim().is_empty() {
            bail!("identity provider and subject must not be blank");
        }
        self.connection
            .call(move |connection| -> Result<StoreLinkCommandRedemption> {
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                if let Some(stored) = transaction
                    .query_row(
                        "SELECT kind, outcome_json FROM gateway_commands
                         WHERE gateway = ?1 AND external_id = ?2",
                        params![key.gateway, key.external_id],
                        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                    )
                    .optional()?
                {
                    if stored.0 != "identity_link" {
                        bail!("gateway command key was reused for a different command kind");
                    }
                    let outcome: GatewayCommandOutcome = serde_json::from_str(&stored.1)?;
                    return command_outcome(outcome);
                }
                let outcome = redeem_in_transaction(&transaction, &identity, code_hash, now)?;
                let command = gateway_outcome(outcome);
                transaction.execute(
                    "INSERT INTO gateway_commands(
                        gateway, external_id, kind, outcome_json, created_at
                     ) VALUES (?1, ?2, 'identity_link', ?3, ?4)",
                    params![
                        key.gateway,
                        key.external_id,
                        serde_json::to_string(&command)?,
                        now.0
                    ],
                )?;
                transaction.commit()?;
                command_outcome(command)
            })
            .await
            .map_err(map_call_error)
    }

    async fn collect_expired_link_state(&self, now: Timestamp, limit: usize) -> Result<usize> {
        if limit == 0 {
            return Ok(0);
        }
        self.connection
            .call(move |connection| -> Result<usize> {
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                let codes = transaction.execute(
                    "DELETE FROM identity_link_codes WHERE actor_id IN (
                         SELECT actor_id FROM identity_link_codes
                         WHERE expires_at <= ?1 ORDER BY expires_at LIMIT ?2
                     )",
                    params![now.0, limit as i64],
                )?;
                let remaining = limit.saturating_sub(codes);
                let attempts = if remaining == 0 {
                    0
                } else {
                    transaction.execute(
                        "DELETE FROM identity_link_attempts WHERE (provider, subject) IN (
                             SELECT provider, subject FROM identity_link_attempts
                             WHERE (blocked_until IS NOT NULL AND blocked_until <= ?1)
                                OR (blocked_until IS NULL
                                    AND window_started_at + ?2 <= ?1)
                             ORDER BY window_started_at LIMIT ?3
                         )",
                        params![now.0, ATTEMPT_WINDOW_MILLIS, remaining as i64],
                    )?
                };
                transaction.commit()?;
                Ok(codes + attempts)
            })
            .await
            .map_err(map_call_error)
    }
}

fn redeem_in_transaction(
    transaction: &tokio_rusqlite::rusqlite::Transaction<'_>,
    identity: &LinkIdentity,
    code_hash: Option<[u8; 32]>,
    now: Timestamp,
) -> Result<StoreLinkRedemption> {
    let attempt = transaction
        .query_row(
            "SELECT window_started_at, failure_count, blocked_until
             FROM identity_link_attempts
             WHERE provider = ?1 AND subject = ?2",
            params![identity.provider, identity.subject],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                ))
            },
        )
        .optional()?;
    if let Some((_, _, Some(retry_at))) = attempt
        && now.0 < retry_at
    {
        return Ok(StoreLinkRedemption::RateLimited {
            retry_at: Timestamp(retry_at),
        });
    }
    let code_actor = match code_hash {
        Some(hash) => transaction
            .query_row(
                "SELECT identity_link_codes.actor_id
                 FROM identity_link_codes
                 JOIN actors ON actors.id = identity_link_codes.actor_id
                 WHERE code_hash = ?1 AND expires_at > ?2 AND actors.enabled = 1",
                params![hash.as_slice(), now.0],
                |row| row.get::<_, String>(0),
            )
            .optional()?,
        None => None,
    };
    let Some(code_actor) = code_actor else {
        record_failure(transaction, identity, attempt, now)?;
        return Ok(StoreLinkRedemption::InvalidOrExpired);
    };
    let existing = transaction
        .query_row(
            "SELECT actor_id FROM identities
             WHERE provider = ?1 AND subject = ?2",
            params![identity.provider, identity.subject],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    if let Some(existing_actor) = existing {
        if existing_actor != code_actor {
            return Ok(StoreLinkRedemption::IdentityConflict {
                actor_id: ActorId::from_string(existing_actor),
            });
        }
        if let Some(ref username) = identity.username {
            transaction.execute(
                "UPDATE identities SET username = ?3
                 WHERE provider = ?1 AND subject = ?2",
                params![identity.provider, identity.subject, username],
            )?;
        }
        consume_code_and_attempts(transaction, &code_actor, identity)?;
        return Ok(StoreLinkRedemption::AlreadyLinked {
            actor_id: ActorId::from_string(code_actor),
        });
    }
    transaction.execute(
        "INSERT INTO identities(provider, subject, actor_id, username)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            identity.provider,
            identity.subject,
            code_actor,
            identity.username
        ],
    )?;
    consume_code_and_attempts(transaction, &code_actor, identity)?;
    Ok(StoreLinkRedemption::Linked {
        actor_id: ActorId::from_string(code_actor),
    })
}

fn gateway_outcome(outcome: StoreLinkRedemption) -> GatewayCommandOutcome {
    match outcome {
        StoreLinkRedemption::Linked { actor_id } => GatewayCommandOutcome::Linked { actor_id },
        StoreLinkRedemption::AlreadyLinked { actor_id } => {
            GatewayCommandOutcome::AlreadyLinked { actor_id }
        }
        StoreLinkRedemption::InvalidOrExpired => GatewayCommandOutcome::InvalidOrExpired,
        StoreLinkRedemption::RateLimited { retry_at } => {
            GatewayCommandOutcome::RateLimited { retry_at }
        }
        StoreLinkRedemption::IdentityConflict { .. } => GatewayCommandOutcome::IdentityConflict,
    }
}

fn command_outcome(outcome: GatewayCommandOutcome) -> Result<StoreLinkCommandRedemption> {
    Ok(match outcome {
        GatewayCommandOutcome::Linked { actor_id } => {
            StoreLinkCommandRedemption::Linked { actor_id }
        }
        GatewayCommandOutcome::AlreadyLinked { actor_id } => {
            StoreLinkCommandRedemption::AlreadyLinked { actor_id }
        }
        GatewayCommandOutcome::InvalidOrExpired => StoreLinkCommandRedemption::InvalidOrExpired,
        GatewayCommandOutcome::RateLimited { retry_at } => {
            StoreLinkCommandRedemption::RateLimited { retry_at }
        }
        GatewayCommandOutcome::IdentityConflict => StoreLinkCommandRedemption::IdentityConflict,
    })
}

fn record_failure(
    transaction: &tokio_rusqlite::rusqlite::Transaction<'_>,
    identity: &LinkIdentity,
    attempt: Option<(i64, i64, Option<i64>)>,
    now: Timestamp,
) -> Result<()> {
    let (window_started_at, failure_count, blocked_until) = match attempt {
        Some((started, count, blocked))
            if blocked.is_some_and(|until| now.0 < until)
                || (blocked.is_none() && now.0 < started.saturating_add(ATTEMPT_WINDOW_MILLIS)) =>
        {
            let count = (count + 1).min(5);
            let blocked = (count == 5).then(|| now.0.saturating_add(ATTEMPT_WINDOW_MILLIS));
            (started, count, blocked)
        }
        _ => (now.0, 1, None),
    };
    transaction.execute(
        "INSERT INTO identity_link_attempts(
             provider, subject, window_started_at, failure_count, blocked_until
         ) VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(provider, subject) DO UPDATE SET
             window_started_at = excluded.window_started_at,
             failure_count = excluded.failure_count,
             blocked_until = excluded.blocked_until",
        params![
            identity.provider,
            identity.subject,
            window_started_at,
            failure_count,
            blocked_until
        ],
    )?;
    Ok(())
}

fn consume_code_and_attempts(
    transaction: &tokio_rusqlite::rusqlite::Transaction<'_>,
    actor: &str,
    identity: &LinkIdentity,
) -> Result<()> {
    transaction.execute(
        "DELETE FROM identity_link_codes WHERE actor_id = ?1",
        [actor],
    )?;
    transaction.execute(
        "DELETE FROM identity_link_attempts WHERE provider = ?1 AND subject = ?2",
        params![identity.provider, identity.subject],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use anyhow::{Result, anyhow};

    use crate::{
        runtime::{
            gateway::GatewayCommandKey,
            model::{ActorId, Timestamp},
            sqlite::SqliteRuntimeStore,
            store::{
                ActorStore, IdentityLinkStore, LinkIdentity, StoreLinkCodeReplacement,
                StoreLinkCommandRedemption, StoreLinkRedemption,
            },
        },
        test_fixtures::{ActorSeed, ActorSeedSet, IdentitySeed},
    };

    async fn enabled_actor_store() -> Result<(SqliteRuntimeStore, ActorId)> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let actor = ActorId::from_string("actor:owner");
        store
            .seed_actors_for_test(
                ActorSeedSet {
                    actors: vec![ActorSeed {
                        id: actor.to_string(),
                        enabled: true,
                        tools: vec!["*".into()],
                        identities: Vec::new(),
                    }],
                },
                Timestamp(1),
            )
            .await?;
        Ok((store, actor))
    }

    fn identity(subject: &str) -> LinkIdentity {
        LinkIdentity {
            provider: "telegram".into(),
            subject: subject.into(),
            username: Some("owner".into()),
        }
    }

    impl SqliteRuntimeStore {
        async fn link_code_count_for_test(&self) -> Result<i64> {
            self.connection
                .call(|connection| {
                    connection.query_row("SELECT COUNT(*) FROM identity_link_codes", [], |row| {
                        row.get(0)
                    })
                })
                .await
                .map_err(|error| anyhow!("failed to count link codes: {error}"))
        }

        async fn gateway_command_count_for_test(&self) -> Result<i64> {
            self.connection
                .call(|connection| {
                    connection.query_row("SELECT COUNT(*) FROM gateway_commands", [], |row| {
                        row.get(0)
                    })
                })
                .await
                .map_err(|error| anyhow!("failed to count gateway commands: {error}"))
        }
    }

    #[tokio::test]
    async fn replacement_revokes_previous_code_for_actor() -> Result<()> {
        let (store, actor) = enabled_actor_store().await?;
        assert_eq!(
            store
                .replace_link_code(&actor, [1; 32], Timestamp(10), Timestamp(610))
                .await?,
            StoreLinkCodeReplacement::Stored
        );
        assert_eq!(
            store
                .replace_link_code(&actor, [2; 32], Timestamp(20), Timestamp(620))
                .await?,
            StoreLinkCodeReplacement::Stored
        );
        assert_eq!(store.link_code_count_for_test().await?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn valid_code_links_identity_and_consumes_code() -> Result<()> {
        let (store, actor) = enabled_actor_store().await?;
        store
            .replace_link_code(&actor, [3; 32], Timestamp(10), Timestamp(610))
            .await?;
        assert_eq!(
            store
                .redeem_link_code(identity("123"), Some([3; 32]), Timestamp(11))
                .await?,
            StoreLinkRedemption::Linked {
                actor_id: actor.clone()
            }
        );
        assert_eq!(
            store.resolve_identity("telegram", "123").await?.unwrap().id,
            actor
        );
        assert_eq!(store.link_code_count_for_test().await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn repeated_gateway_link_update_returns_stored_outcome() -> Result<()> {
        let (store, actor) = enabled_actor_store().await?;
        store
            .replace_link_code(&actor, [7; 32], Timestamp(1), Timestamp(601))
            .await?;
        let key = GatewayCommandKey {
            gateway: "telegram:bot-1".into(),
            external_id: "42".into(),
        };
        let identity = identity("100");
        let first = store
            .redeem_link_code_once(key.clone(), identity.clone(), Some([7; 32]), Timestamp(2))
            .await?;
        let repeated = store
            .redeem_link_code_once(key, identity, None, Timestamp(900))
            .await?;

        assert_eq!(
            first,
            StoreLinkCommandRedemption::Linked { actor_id: actor }
        );
        assert_eq!(repeated, first);
        assert_eq!(store.gateway_command_count_for_test().await?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn fifth_failure_blocks_identity_for_ten_minutes() -> Result<()> {
        let (store, _) = enabled_actor_store().await?;
        for now in 0..5 {
            assert_eq!(
                store
                    .redeem_link_code(identity("attacker"), None, Timestamp(now))
                    .await?,
                StoreLinkRedemption::InvalidOrExpired
            );
        }
        assert_eq!(
            store
                .redeem_link_code(identity("attacker"), None, Timestamp(5))
                .await?,
            StoreLinkRedemption::RateLimited {
                retry_at: Timestamp(600_004)
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn code_is_expired_at_exact_expiry() -> Result<()> {
        let (store, actor) = enabled_actor_store().await?;
        store
            .replace_link_code(&actor, [4; 32], Timestamp(10), Timestamp(610))
            .await?;
        assert_eq!(
            store
                .redeem_link_code(identity("expired"), Some([4; 32]), Timestamp(610))
                .await?,
            StoreLinkRedemption::InvalidOrExpired
        );
        Ok(())
    }

    #[tokio::test]
    async fn different_actor_conflict_does_not_consume_code() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let code_actor = ActorId::from_string("actor:code");
        let identity_actor = ActorId::from_string("actor:identity");
        store
            .seed_actors_for_test(
                ActorSeedSet {
                    actors: vec![
                        ActorSeed {
                            id: code_actor.to_string(),
                            enabled: true,
                            tools: vec![],
                            identities: vec![],
                        },
                        ActorSeed {
                            id: identity_actor.to_string(),
                            enabled: true,
                            tools: vec![],
                            identities: vec![IdentitySeed {
                                provider: "telegram".into(),
                                subject: "123".into(),
                                username: Some("owner".into()),
                            }],
                        },
                    ],
                },
                Timestamp(1),
            )
            .await?;
        store
            .replace_link_code(&code_actor, [5; 32], Timestamp(10), Timestamp(610))
            .await?;
        assert_eq!(
            store
                .redeem_link_code(identity("123"), Some([5; 32]), Timestamp(11))
                .await?,
            StoreLinkRedemption::IdentityConflict {
                actor_id: identity_actor
            }
        );
        assert_eq!(store.link_code_count_for_test().await?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn cleanup_is_bounded_and_removes_expired_state() -> Result<()> {
        let (store, actor) = enabled_actor_store().await?;
        store
            .replace_link_code(&actor, [6; 32], Timestamp(1), Timestamp(2))
            .await?;
        store
            .redeem_link_code(identity("failed"), None, Timestamp(1))
            .await?;
        assert_eq!(
            store
                .collect_expired_link_state(Timestamp(600_001), 1)
                .await?,
            1
        );
        assert_eq!(
            store
                .collect_expired_link_state(Timestamp(600_001), 1)
                .await?,
            1
        );
        Ok(())
    }
}
