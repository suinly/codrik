use std::collections::HashSet;

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use tokio_rusqlite::params;

use crate::{
    agent::message::Message,
    runtime::{
        model::{AttemptId, AttemptState, Audience, EventId, Timestamp},
        sqlite::{SqliteRuntimeStore, dispatch::ensure_current_lease, map_call_error},
        store::{
            AttachedRun, AttemptOutcome, AttemptRecovery, CheckpointRun, CheckpointStore,
            ContextStore, FinalizeOutcome, FinalizeRun, NewOutboxIntent, NewToolAttempt,
            ToolAttempt, ToolAttemptStore,
        },
    },
};

#[async_trait]
impl CheckpointStore for SqliteRuntimeStore {
    async fn checkpoint_run(&self, command: CheckpointRun, now: Timestamp) -> Result<()> {
        self.connection
            .call(move |connection| -> Result<()> {
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                validate_run(&transaction, &command.run, now)?;
                incorporate_events(&transaction, &command.run, &command.incorporated_event_ids)?;
                checkpoint_attempts(
                    &transaction,
                    &command.run,
                    &command.checkpointed_attempt_ids,
                )?;
                insert_messages(&transaction, &command.run, &command.messages, now)?;
                transaction.commit()?;
                Ok(())
            })
            .await
            .map_err(map_call_error)
    }

    async fn finalize_run(&self, command: FinalizeRun, now: Timestamp) -> Result<FinalizeOutcome> {
        self.connection
            .call(move |connection| -> Result<FinalizeOutcome> {
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                validate_run(&transaction, &command.run, now)?;

                if let Some(newest_sequence) = newest_compatible_input(
                    &transaction,
                    &command.run,
                    command.run.observed_sequence,
                )? {
                    return Ok(FinalizeOutcome::Preempted { newest_sequence });
                }

                require_all_events_incorporated(
                    &transaction,
                    &command.run,
                    &command.incorporated_event_ids,
                )?;
                insert_messages(&transaction, &command.run, &command.final_messages, now)?;
                for intent in &command.outbox {
                    insert_outbox(&transaction, &command.run, intent, now)?;
                }
                transaction.execute(
                    "UPDATE events
                     SET state = 'completed', updated_at = ?2
                     WHERE id IN (
                        SELECT event_id FROM run_events
                        WHERE run_id = ?1 AND incorporated = 1
                     )",
                    params![command.run.run_id.as_str(), now.0],
                )?;
                transaction.execute(
                    "UPDATE runs SET state = 'completed', updated_at = ?2
                     WHERE id = ?1 AND state = 'active'",
                    params![command.run.run_id.as_str(), now.0],
                )?;
                transaction.execute(
                    "UPDATE work_items SET state = 'completed', updated_at = ?2 WHERE id = ?1",
                    params![command.run.work_item_id.as_str(), now.0],
                )?;
                transaction.commit()?;
                Ok(FinalizeOutcome::Completed)
            })
            .await
            .map_err(map_call_error)
    }

    async fn cancel_run(
        &self,
        run: &AttachedRun,
        control: &crate::runtime::store::ControlEvent,
        now: Timestamp,
    ) -> Result<()> {
        let run = run.clone();
        let control = control.clone();
        self.connection
            .call(move |connection| -> Result<()> {
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                validate_run(&transaction, &run, now)?;
                let changed = transaction.execute(
                    "UPDATE events SET state = 'cancelled', updated_at = ?3
                     WHERE id = ?1 AND actor_id = ?2 AND state = 'pending'
                       AND kind = 'cancel_requested'",
                    params![
                        control.event_id.as_str(),
                        run.lease.actor_id.as_str(),
                        now.0
                    ],
                )?;
                if changed != 1 {
                    bail!("cancellation event is no longer pending");
                }
                transaction.execute(
                    "UPDATE events SET state = 'cancelled', updated_at = ?2
                     WHERE id IN (SELECT event_id FROM run_events WHERE run_id = ?1)
                       AND state = 'processing'",
                    params![run.run_id.as_str(), now.0],
                )?;
                transaction.execute(
                    "UPDATE runs SET state = 'cancelled', updated_at = ?2 WHERE id = ?1",
                    params![run.run_id.as_str(), now.0],
                )?;
                transaction.execute(
                    "UPDATE work_items SET state = 'cancelled', updated_at = ?2 WHERE id = ?1",
                    params![run.work_item_id.as_str(), now.0],
                )?;
                transaction.commit()?;
                Ok(())
            })
            .await
            .map_err(map_call_error)
    }
}

#[async_trait]
impl ContextStore for SqliteRuntimeStore {
    async fn load_recent_context(
        &self,
        actor: &crate::runtime::model::ActorId,
        audience: &Audience,
        limit: usize,
    ) -> Result<Vec<Message>> {
        let actor = actor.to_string();
        let (audience_kind, audience_address) = encode_audience(audience)?;
        self.connection
            .call(move |connection| -> Result<Vec<Message>> {
                let predicate = match audience_kind.as_str() {
                    "actor_private" => "audience_kind IN ('actor_private', 'shareable')",
                    "shareable" => "audience_kind = 'shareable'",
                    "conversation_scoped" => {
                        "(audience_kind = 'shareable' OR (audience_kind = 'conversation_scoped' AND audience_address = ?3))"
                    }
                    _ => unreachable!(),
                };
                let sql = format!(
                    "SELECT message_json FROM recent_messages
                     WHERE actor_id = ?1 AND {predicate}
                     ORDER BY id DESC LIMIT ?2"
                );
                let mut statement = connection.prepare(&sql)?;
                let rows = if audience_kind == "conversation_scoped" {
                    statement
                        .query_map(params![actor, limit as i64, audience_address], |row| {
                            row.get::<_, String>(0)
                        })?
                        .collect::<std::result::Result<Vec<_>, _>>()?
                } else {
                    statement
                        .query_map(params![actor, limit as i64], |row| {
                            row.get::<_, String>(0)
                        })?
                        .collect::<std::result::Result<Vec<_>, _>>()?
                };
                rows.into_iter()
                    .rev()
                    .map(|json| Ok(serde_json::from_str(&json)?))
                    .collect()
            })
            .await
            .map_err(map_call_error)
    }
}

#[async_trait]
impl ToolAttemptStore for SqliteRuntimeStore {
    async fn prepare_attempt(
        &self,
        run: &AttachedRun,
        attempt: NewToolAttempt,
        now: Timestamp,
    ) -> Result<ToolAttempt> {
        let run = run.clone();
        self.connection
            .call(move |connection| -> Result<ToolAttempt> {
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                validate_run(&transaction, &run, now)?;
                let capabilities_json = serde_json::to_string(&attempt.capabilities)?;
                transaction.execute(
                    "INSERT INTO tool_attempts(
                        id, run_id, tool_call_id, tool_name, arguments_json,
                        capabilities_json, state, created_at, updated_at
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'prepared', ?7, ?7)
                     ON CONFLICT(run_id, tool_call_id) DO NOTHING",
                    params![
                        attempt.id.as_str(),
                        run.run_id.as_str(),
                        attempt.tool_call_id,
                        attempt.tool_name,
                        attempt.arguments_json,
                        capabilities_json,
                        now.0,
                    ],
                )?;
                let stored =
                    load_attempt_by_call(&transaction, run.run_id.as_str(), &attempt.tool_call_id)?;
                if stored.tool_name != attempt.tool_name
                    || stored.arguments_json != attempt.arguments_json
                    || stored.capabilities != attempt.capabilities
                {
                    bail!("tool call id was reused with different attempt data");
                }
                transaction.commit()?;
                Ok(stored)
            })
            .await
            .map_err(map_call_error)
    }

    async fn mark_attempt_running(
        &self,
        run: &AttachedRun,
        id: &AttemptId,
        now: Timestamp,
    ) -> Result<()> {
        transition_attempt(self, run, id, "prepared", "running", None, now).await
    }

    async fn finish_attempt(
        &self,
        run: &AttachedRun,
        id: &AttemptId,
        outcome: AttemptOutcome,
        now: Timestamp,
    ) -> Result<()> {
        let next_state = match &outcome {
            AttemptOutcome::Succeeded { .. } => "succeeded",
            AttemptOutcome::FailedKnown { .. } => "failed_known",
            AttemptOutcome::CancelledKnown => "cancelled_known",
        };
        let outcome_json = serde_json::to_string(&outcome)?;
        transition_attempt(
            self,
            run,
            id,
            "running",
            next_state,
            Some(outcome_json),
            now,
        )
        .await
    }

    async fn recover_attempt(&self, id: &AttemptId) -> Result<AttemptRecovery> {
        let id = id.to_string();
        self.connection
            .call(move |connection| -> Result<AttemptRecovery> {
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                let (state, outcome_json) = transaction.query_row(
                    "SELECT state, outcome_json FROM tool_attempts WHERE id = ?1",
                    [id.as_str()],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
                )?;
                let recovery = match state.as_str() {
                    "prepared" => AttemptRecovery::MayInvoke,
                    "running" => {
                        let changed = transaction.execute(
                            "UPDATE tool_attempts SET state = 'outcome_unknown', updated_at = updated_at
                             WHERE id = ?1 AND state = 'running'",
                            [id.as_str()],
                        )?;
                        if changed != 1 {
                            bail!("attempt changed during recovery");
                        }
                        AttemptRecovery::OutcomeUnknown
                    }
                    "outcome_unknown" | "waiting_for_decision" => {
                        AttemptRecovery::OutcomeUnknown
                    }
                    "succeeded" | "failed_known" | "cancelled_known" => {
                        let outcome_json = outcome_json
                            .ok_or_else(|| anyhow!("terminal attempt is missing its outcome"))?;
                        AttemptRecovery::Terminal(serde_json::from_str(&outcome_json)?)
                    }
                    other => bail!("invalid stored attempt state: {other}"),
                };
                transaction.commit()?;
                Ok(recovery)
            })
            .await
            .map_err(map_call_error)
    }

    async fn block_unknown_attempt(
        &self,
        run: &AttachedRun,
        id: &AttemptId,
        now: Timestamp,
    ) -> Result<()> {
        let run = run.clone();
        let id = id.to_string();
        self.connection
            .call(move |connection| -> Result<()> {
                let transaction = connection.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                validate_run(&transaction, &run, now)?;
                let changed = transaction.execute(
                    "UPDATE tool_attempts SET state = 'waiting_for_decision', updated_at = ?3
                     WHERE id = ?1 AND run_id = ?2 AND state = 'outcome_unknown'",
                    params![id, run.run_id.as_str(), now.0],
                )?;
                if changed != 1 {
                    bail!("attempt is not outcome_unknown");
                }
                transaction.execute(
                    "UPDATE work_items SET state = 'waiting_for_decision', updated_at = ?2
                     WHERE id = ?1",
                    params![run.work_item_id.as_str(), now.0],
                )?;
                transaction.commit()?;
                Ok(())
            })
            .await
            .map_err(map_call_error)
    }

    async fn unresolved_attempts(&self, run: &AttachedRun) -> Result<Vec<ToolAttempt>> {
        let run = run.clone();
        self.connection
            .call(move |connection| -> Result<Vec<ToolAttempt>> {
                ensure_matching_lease(connection, &run)?;
                let mut statement = connection.prepare(
                    "SELECT id, tool_call_id, tool_name, arguments_json, capabilities_json, state
                     FROM tool_attempts
                     WHERE run_id = ?1
                       AND (state IN ('prepared', 'running', 'outcome_unknown', 'waiting_for_decision')
                            OR observation_checkpointed = 0)
                     ORDER BY created_at, id",
                )?;
                statement
                    .query_map([run.run_id.as_str()], attempt_row)?
                    .map(|row| decode_attempt(row?))
                    .collect()
            })
            .await
            .map_err(map_call_error)
    }
}

async fn transition_attempt(
    store: &SqliteRuntimeStore,
    run: &AttachedRun,
    id: &AttemptId,
    expected_state: &'static str,
    next_state: &'static str,
    outcome_json: Option<String>,
    now: Timestamp,
) -> Result<()> {
    let run = run.clone();
    let id = id.to_string();
    store
        .connection
        .call(move |connection| -> Result<()> {
            let transaction = connection.transaction_with_behavior(
                tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
            )?;
            validate_run(&transaction, &run, now)?;
            let changed = transaction.execute(
                "UPDATE tool_attempts SET state = ?4, outcome_json = ?5, updated_at = ?6
                 WHERE id = ?1 AND run_id = ?2 AND state = ?3",
                params![
                    id,
                    run.run_id.as_str(),
                    expected_state,
                    next_state,
                    outcome_json,
                    now.0,
                ],
            )?;
            if changed != 1 {
                bail!("attempt is not in expected state {expected_state}");
            }
            transaction.commit()?;
            Ok(())
        })
        .await
        .map_err(map_call_error)
}

fn load_attempt_by_call(
    transaction: &tokio_rusqlite::rusqlite::Transaction<'_>,
    run_id: &str,
    tool_call_id: &str,
) -> Result<ToolAttempt> {
    let row = transaction.query_row(
        "SELECT id, tool_call_id, tool_name, arguments_json, capabilities_json, state
         FROM tool_attempts WHERE run_id = ?1 AND tool_call_id = ?2",
        params![run_id, tool_call_id],
        attempt_row,
    )?;
    decode_attempt(row)
}

fn attempt_row(
    row: &tokio_rusqlite::rusqlite::Row<'_>,
) -> tokio_rusqlite::rusqlite::Result<(String, String, String, String, String, String)> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
    ))
}

fn decode_attempt(row: (String, String, String, String, String, String)) -> Result<ToolAttempt> {
    Ok(ToolAttempt {
        id: AttemptId::from_string(row.0),
        tool_call_id: row.1,
        tool_name: row.2,
        arguments_json: row.3,
        capabilities: serde_json::from_str(&row.4)?,
        state: decode_attempt_state(&row.5)?,
    })
}

fn decode_attempt_state(state: &str) -> Result<AttemptState> {
    match state {
        "prepared" => Ok(AttemptState::Prepared),
        "running" => Ok(AttemptState::Running),
        "succeeded" => Ok(AttemptState::Succeeded),
        "failed_known" => Ok(AttemptState::FailedKnown),
        "outcome_unknown" => Ok(AttemptState::OutcomeUnknown),
        "cancelled_known" => Ok(AttemptState::CancelledKnown),
        "waiting_for_decision" => Ok(AttemptState::WaitingForDecision),
        _ => bail!("invalid stored attempt state: {state}"),
    }
}

fn ensure_matching_lease(
    connection: &tokio_rusqlite::rusqlite::Connection,
    run: &AttachedRun,
) -> Result<()> {
    let matches = connection.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM actor_leases
            JOIN runs ON runs.actor_id = actor_leases.actor_id
            WHERE runs.id = ?1 AND runs.state = 'active'
              AND actor_leases.actor_id = ?2 AND actor_leases.owner_id = ?3
              AND actor_leases.generation = ?4 AND runs.lease_generation = ?4
         )",
        params![
            run.run_id.as_str(),
            run.lease.actor_id.as_str(),
            run.lease.owner_id,
            run.lease.generation,
        ],
        |row| row.get::<_, bool>(0),
    )?;
    if !matches {
        return Err(crate::runtime::store::StaleLease.into());
    }
    Ok(())
}

fn validate_run(
    transaction: &tokio_rusqlite::rusqlite::Transaction<'_>,
    run: &AttachedRun,
    now: Timestamp,
) -> Result<()> {
    ensure_current_lease(transaction, &run.lease, now)?;
    let (actor_id, work_item_id, lease_generation, audience_kind, audience_address) = transaction
        .query_row(
            "SELECT runs.actor_id, runs.work_item_id, runs.lease_generation,
                    work_items.audience_kind, work_items.audience_address
             FROM runs
             JOIN work_items ON work_items.id = runs.work_item_id
             WHERE runs.id = ?1 AND runs.state = 'active'",
            [run.run_id.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            },
        )
        .map_err(|_| anyhow!("run is not active"))?;
    let expected_audience = encode_audience(&run.audience)?;
    if actor_id != run.lease.actor_id.as_str()
        || work_item_id != run.work_item_id.as_str()
        || lease_generation != run.lease.generation
        || (audience_kind, audience_address) != expected_audience
    {
        bail!("attached run does not match durable run state");
    }
    Ok(())
}

fn incorporate_events(
    transaction: &tokio_rusqlite::rusqlite::Transaction<'_>,
    run: &AttachedRun,
    event_ids: &[EventId],
) -> Result<()> {
    require_unique_ids(event_ids)?;
    for event_id in event_ids {
        let changed = transaction.execute(
            "UPDATE run_events SET incorporated = 1 WHERE run_id = ?1 AND event_id = ?2",
            params![run.run_id.as_str(), event_id.as_str()],
        )?;
        if changed != 1 {
            bail!("event is not attached to run: {event_id}");
        }
    }
    Ok(())
}

fn checkpoint_attempts(
    transaction: &tokio_rusqlite::rusqlite::Transaction<'_>,
    run: &AttachedRun,
    attempt_ids: &[crate::runtime::model::AttemptId],
) -> Result<()> {
    for attempt_id in attempt_ids {
        let changed = transaction.execute(
            "UPDATE tool_attempts SET observation_checkpointed = 1
             WHERE id = ?1 AND run_id = ?2
               AND state IN ('succeeded', 'failed_known', 'cancelled_known')",
            params![attempt_id.as_str(), run.run_id.as_str()],
        )?;
        if changed != 1 {
            bail!("attempt is not attached to run: {attempt_id}");
        }
    }
    Ok(())
}

fn insert_messages(
    transaction: &tokio_rusqlite::rusqlite::Transaction<'_>,
    run: &AttachedRun,
    messages: &[Message],
    now: Timestamp,
) -> Result<()> {
    let (audience_kind, audience_address) = encode_audience(&run.audience)?;
    for message in messages {
        let message_json = serde_json::to_string(message)?;
        transaction.execute(
            "INSERT INTO recent_messages(
                actor_id, work_item_id, run_id, audience_kind, audience_address,
                message_json, created_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                run.lease.actor_id.as_str(),
                run.work_item_id.as_str(),
                run.run_id.as_str(),
                audience_kind,
                audience_address,
                message_json,
                now.0,
            ],
        )?;
    }
    Ok(())
}

fn newest_compatible_input(
    transaction: &tokio_rusqlite::rusqlite::Transaction<'_>,
    run: &AttachedRun,
    observed_sequence: i64,
) -> Result<Option<i64>> {
    let (audience_kind, audience_address) = encode_audience(&run.audience)?;
    Ok(transaction.query_row(
        "SELECT MAX(mailbox_sequence) FROM events
         WHERE actor_id = ?1 AND state = 'pending'
           AND mailbox_sequence > ?2
           AND kind IN ('cancel_requested', 'user_message')
           AND audience_kind = ?3 AND audience_address IS ?4",
        params![
            run.lease.actor_id.as_str(),
            observed_sequence,
            audience_kind,
            audience_address,
        ],
        |row| row.get(0),
    )?)
}

fn require_all_events_incorporated(
    transaction: &tokio_rusqlite::rusqlite::Transaction<'_>,
    run: &AttachedRun,
    event_ids: &[EventId],
) -> Result<()> {
    require_unique_ids(event_ids)?;
    let durable = {
        let mut statement = transaction
            .prepare("SELECT event_id, incorporated FROM run_events WHERE run_id = ?1")?;
        statement
            .query_map([run.run_id.as_str()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    let requested = event_ids
        .iter()
        .map(|id| id.as_str())
        .collect::<HashSet<_>>();
    if durable.len() != requested.len()
        || durable
            .iter()
            .any(|(id, incorporated)| !incorporated || !requested.contains(id.as_str()))
    {
        bail!("all attached source events must be incorporated before finalization");
    }
    Ok(())
}

fn insert_outbox(
    transaction: &tokio_rusqlite::rusqlite::Transaction<'_>,
    run: &AttachedRun,
    intent: &NewOutboxIntent,
    now: Timestamp,
) -> Result<()> {
    if intent.audience != run.audience {
        bail!("outbox audience must match the finalized run");
    }
    let (audience_kind, audience_address) = encode_audience(&intent.audience)?;
    let payload_json = serde_json::to_string(&intent.payload)?;
    transaction.execute(
        "INSERT INTO outbox(
            id, intent_key, actor_id, work_item_id, run_id, intent_class,
            audience_kind, audience_address, payload_json, created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
         ON CONFLICT(intent_key) DO NOTHING",
        params![
            intent.id.as_str(),
            intent.intent_key,
            run.lease.actor_id.as_str(),
            run.work_item_id.as_str(),
            run.run_id.as_str(),
            intent.intent_class,
            audience_kind,
            audience_address,
            payload_json,
            now.0,
        ],
    )?;
    Ok(())
}

fn require_unique_ids(event_ids: &[EventId]) -> Result<()> {
    let mut seen = HashSet::new();
    if event_ids.iter().any(|id| !seen.insert(id.as_str())) {
        bail!("event ids must be unique");
    }
    Ok(())
}

fn encode_audience(audience: &Audience) -> Result<(String, Option<String>)> {
    match audience {
        Audience::ActorPrivate => Ok(("actor_private".into(), None)),
        Audience::Shareable => Ok(("shareable".into(), None)),
        Audience::ConversationScoped { address } if !address.trim().is_empty() => {
            Ok(("conversation_scoped".into(), Some(address.clone())))
        }
        Audience::ConversationScoped { .. } => {
            bail!("conversation-scoped audience requires an address")
        }
    }
}

#[cfg(test)]
impl SqliteRuntimeStore {
    async fn source_events_completed(&self, run: &AttachedRun) -> Result<bool> {
        let run_id = run.run_id.to_string();
        self.connection
            .call(
                move |connection| -> tokio_rusqlite::rusqlite::Result<bool> {
                    connection.query_row(
                        "SELECT COUNT(*) = 0 FROM events
                     JOIN run_events ON run_events.event_id = events.id
                     WHERE run_events.run_id = ?1 AND events.state != 'completed'",
                        [run_id],
                        |row| row.get(0),
                    )
                },
            )
            .await
            .map_err(|error| anyhow!("failed to inspect source events: {error}"))
    }

    async fn pending_event_count(&self) -> Result<i64> {
        self.connection
            .call(|connection| {
                connection.query_row(
                    "SELECT COUNT(*) FROM events WHERE state = 'pending'",
                    [],
                    |row| row.get(0),
                )
            })
            .await
            .map_err(|error| anyhow!("failed to count pending events: {error}"))
    }

    async fn work_item_state(&self, run: &AttachedRun) -> Result<String> {
        let work_item_id = run.work_item_id.to_string();
        self.connection
            .call(move |connection| {
                connection.query_row(
                    "SELECT state FROM work_items WHERE id = ?1",
                    [work_item_id],
                    |row| row.get(0),
                )
            })
            .await
            .map_err(|error| anyhow!("failed to inspect work item: {error}"))
    }

    async fn seed_context_message(
        &self,
        run: &AttachedRun,
        audience_kind: &str,
        audience_address: Option<&str>,
        text: &str,
    ) -> Result<()> {
        let actor_id = run.lease.actor_id.to_string();
        let work_item_id = run.work_item_id.to_string();
        let run_id = run.run_id.to_string();
        let audience_kind = audience_kind.to_string();
        let audience_address = audience_address.map(str::to_string);
        let message_json = serde_json::to_string(&Message::assistant(text))?;
        self.connection
            .call(move |connection| -> tokio_rusqlite::rusqlite::Result<()> {
                connection.execute(
                    "INSERT INTO recent_messages(
                        actor_id, work_item_id, run_id, audience_kind, audience_address,
                        message_json, created_at
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1)",
                    params![
                        actor_id,
                        work_item_id,
                        run_id,
                        audience_kind,
                        audience_address,
                        message_json,
                    ],
                )?;
                Ok(())
            })
            .await
            .map_err(|error| anyhow!("failed to seed context: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        agent::{message::Message, tool::ToolExecution},
        auth::{LegacyActor, LegacyAuthorizationSnapshot, LegacyIdentity},
        llm::client::LlmToolCall,
        runtime::{
            model::{Audience, OutboxId, Timestamp},
            sqlite::SqliteRuntimeStore,
            store::{
                AttemptOutcome, AttemptRecovery, CheckpointRun, CheckpointStore, ContextStore,
                DispatchStore, FinalizeOutcome, FinalizeRun, IngressStore, NewInboundEvent,
                NewOutboxIntent, NewToolAttempt, OutboxPayload, RuntimeAuthorizationStore,
                StaleLease, ToolAttemptStore,
            },
        },
    };

    async fn store_with_run() -> (SqliteRuntimeStore, crate::runtime::store::AttachedRun) {
        let store = SqliteRuntimeStore::open_in_memory().await.unwrap();
        store
            .import_legacy_authorization(
                LegacyAuthorizationSnapshot {
                    version: 1,
                    actors: vec![LegacyActor {
                        id: "actor:telegram:123".into(),
                        enabled: true,
                        tools: vec!["*".into()],
                        identities: vec![LegacyIdentity {
                            provider: "telegram".into(),
                            subject: "123".into(),
                            username: None,
                        }],
                    }],
                },
                Timestamp(1),
            )
            .await
            .unwrap();
        store
            .ingest(
                NewInboundEvent::text(
                    "local",
                    "event-1",
                    "telegram",
                    "123",
                    Audience::ActorPrivate,
                    "hello",
                )
                .unwrap(),
                Timestamp(2),
            )
            .await
            .unwrap();
        let lease = store
            .acquire_ready_actor("worker", Timestamp(100), Timestamp(500))
            .await
            .unwrap()
            .unwrap();
        let run = store
            .attach_next_run(&lease, 8, Timestamp(101))
            .await
            .unwrap()
            .unwrap();
        (store, run)
    }

    #[tokio::test]
    async fn finalization_preempts_for_newer_compatible_input_then_completes_to_outbox() {
        let (store, run) = store_with_run().await;
        store
            .checkpoint_run(
                CheckpointRun {
                    run: run.clone(),
                    incorporated_event_ids: run.source_event_ids.clone(),
                    checkpointed_attempt_ids: Vec::new(),
                    messages: vec![
                        Message::user("hello"),
                        Message::assistant_tool_calls(
                            "thinking",
                            vec![LlmToolCall {
                                id: "call-1".into(),
                                name: "datetime".into(),
                                arguments: "{}".into(),
                            }],
                        ),
                    ],
                },
                Timestamp(150),
            )
            .await
            .unwrap();
        store
            .ingest(
                NewInboundEvent::text(
                    "local",
                    "event-2",
                    "telegram",
                    "123",
                    Audience::ActorPrivate,
                    "more context",
                )
                .unwrap(),
                Timestamp(151),
            )
            .await
            .unwrap();

        assert_eq!(
            store
                .finalize_run(finalize(&run, "intent-1"), Timestamp(200))
                .await
                .unwrap(),
            FinalizeOutcome::Preempted { newest_sequence: 2 }
        );
        assert!(store.outbox_intents().await.unwrap().is_empty());

        let resumed = store
            .attach_next_run(&run.lease, 8, Timestamp(201))
            .await
            .unwrap()
            .unwrap();
        store
            .ingest(
                NewInboundEvent::text(
                    "local",
                    "event-group",
                    "telegram",
                    "123",
                    Audience::ConversationScoped {
                        address: "telegram-group:7".into(),
                    },
                    "unrelated group input",
                )
                .unwrap(),
                Timestamp(201),
            )
            .await
            .unwrap();
        store
            .checkpoint_run(
                CheckpointRun {
                    run: resumed.clone(),
                    incorporated_event_ids: resumed.source_event_ids.clone(),
                    checkpointed_attempt_ids: Vec::new(),
                    messages: Vec::new(),
                },
                Timestamp(202),
            )
            .await
            .unwrap();
        let mut command = finalize(&resumed, "intent-1");
        let mut duplicate = command.outbox[0].clone();
        duplicate.id = OutboxId::new();
        command.outbox.push(duplicate);
        assert_eq!(
            store.finalize_run(command, Timestamp(203)).await.unwrap(),
            FinalizeOutcome::Completed
        );
        let outbox = store.outbox_intents().await.unwrap();
        assert_eq!(outbox.len(), 1);
        assert_eq!(outbox[0].intent_key, "intent-1");
        assert!(store.source_events_completed(&resumed).await.unwrap());
        assert_eq!(store.pending_event_count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn stale_lease_finalization_leaves_rows_unchanged() {
        let (store, run) = store_with_run().await;
        let replacement = store
            .acquire_ready_actor("replacement", Timestamp(501), Timestamp(600))
            .await
            .unwrap()
            .unwrap();

        let error = store
            .finalize_run(finalize(&run, "stale-intent"), Timestamp(502))
            .await
            .unwrap_err();

        assert!(error.downcast_ref::<StaleLease>().is_some());
        assert!(store.outbox_intents().await.unwrap().is_empty());
        assert!(!store.source_events_completed(&run).await.unwrap());
        store.release_lease(&replacement).await.unwrap();
    }

    #[tokio::test]
    async fn recent_context_respects_audience_visibility() {
        let (store, run) = store_with_run().await;
        store
            .seed_context_message(&run, "actor_private", None, "private")
            .await
            .unwrap();
        store
            .seed_context_message(&run, "shareable", None, "shared")
            .await
            .unwrap();
        store
            .seed_context_message(
                &run,
                "conversation_scoped",
                Some("telegram-group:7"),
                "group",
            )
            .await
            .unwrap();

        let private = store
            .load_recent_context(&run.lease.actor_id, &Audience::ActorPrivate, 10)
            .await
            .unwrap();
        assert_eq!(
            private.iter().map(Message::text).collect::<Vec<_>>(),
            ["private", "shared"]
        );
        let group = store
            .load_recent_context(
                &run.lease.actor_id,
                &Audience::ConversationScoped {
                    address: "telegram-group:7".into(),
                },
                10,
            )
            .await
            .unwrap();
        assert_eq!(
            group.iter().map(Message::text).collect::<Vec<_>>(),
            ["shared", "group"]
        );
    }

    #[tokio::test]
    async fn attempt_running_at_recovery_becomes_outcome_unknown() {
        let (store, run) = store_with_run().await;
        let attempt = store
            .prepare_attempt(&run, new_attempt("call-1"), Timestamp(110))
            .await
            .unwrap();
        assert_eq!(attempt.state, crate::runtime::model::AttemptState::Prepared);

        store
            .mark_attempt_running(&run, &attempt.id, Timestamp(111))
            .await
            .unwrap();

        assert_eq!(
            store.recover_attempt(&attempt.id).await.unwrap(),
            AttemptRecovery::OutcomeUnknown
        );
    }

    #[tokio::test]
    async fn attempt_prepared_at_recovery_may_invoke() {
        let (store, run) = store_with_run().await;
        let attempt = store
            .prepare_attempt(&run, new_attempt("call-1"), Timestamp(110))
            .await
            .unwrap();

        assert_eq!(
            store.recover_attempt(&attempt.id).await.unwrap(),
            AttemptRecovery::MayInvoke
        );
    }

    #[tokio::test]
    async fn attempt_prepare_is_idempotent_for_run_tool_call_id() {
        let (store, run) = store_with_run().await;
        let first = store
            .prepare_attempt(&run, new_attempt("call-1"), Timestamp(110))
            .await
            .unwrap();
        let second = store
            .prepare_attempt(&run, new_attempt("call-1"), Timestamp(111))
            .await
            .unwrap();

        assert_eq!(second, first);
    }

    #[tokio::test]
    async fn attempt_terminal_outcome_remains_unresolved_until_observation_checkpoint() {
        let (store, run) = store_with_run().await;
        let attempt = store
            .prepare_attempt(&run, new_attempt("call-1"), Timestamp(110))
            .await
            .unwrap();
        store
            .mark_attempt_running(&run, &attempt.id, Timestamp(111))
            .await
            .unwrap();
        let outcome = AttemptOutcome::Succeeded {
            execution: ToolExecution::text("2026-07-14"),
        };
        store
            .finish_attempt(&run, &attempt.id, outcome.clone(), Timestamp(112))
            .await
            .unwrap();

        assert_eq!(
            store.recover_attempt(&attempt.id).await.unwrap(),
            AttemptRecovery::Terminal(outcome)
        );
        assert_eq!(store.unresolved_attempts(&run).await.unwrap().len(), 1);

        store
            .checkpoint_run(
                CheckpointRun {
                    run: run.clone(),
                    incorporated_event_ids: run.source_event_ids.clone(),
                    checkpointed_attempt_ids: vec![attempt.id],
                    messages: vec![Message::tool_result("call-1", "2026-07-14")],
                },
                Timestamp(113),
            )
            .await
            .unwrap();

        assert!(store.unresolved_attempts(&run).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn attempt_unknown_outcome_blocks_work_for_decision() {
        let (store, run) = store_with_run().await;
        let attempt = store
            .prepare_attempt(&run, new_attempt("call-1"), Timestamp(110))
            .await
            .unwrap();
        store
            .mark_attempt_running(&run, &attempt.id, Timestamp(111))
            .await
            .unwrap();
        assert_eq!(
            store.recover_attempt(&attempt.id).await.unwrap(),
            AttemptRecovery::OutcomeUnknown
        );

        store
            .block_unknown_attempt(&run, &attempt.id, Timestamp(112))
            .await
            .unwrap();

        assert_eq!(
            store.work_item_state(&run).await.unwrap(),
            "waiting_for_decision"
        );
        assert_eq!(
            store.recover_attempt(&attempt.id).await.unwrap(),
            AttemptRecovery::OutcomeUnknown
        );
    }

    fn new_attempt(tool_call_id: &str) -> NewToolAttempt {
        NewToolAttempt {
            id: crate::runtime::model::AttemptId::new(),
            tool_call_id: tool_call_id.into(),
            tool_name: "datetime".into(),
            arguments_json: "{}".into(),
            capabilities: crate::agent::tool::ToolCapabilities::read_only(),
        }
    }

    fn finalize(run: &crate::runtime::store::AttachedRun, intent_key: &str) -> FinalizeRun {
        FinalizeRun {
            run: run.clone(),
            incorporated_event_ids: run.source_event_ids.clone(),
            final_messages: vec![Message::assistant("done")],
            outbox: vec![NewOutboxIntent {
                id: OutboxId::new(),
                intent_key: intent_key.into(),
                intent_class: "reply".into(),
                audience: run.audience.clone(),
                payload: OutboxPayload::Text {
                    text: "done".into(),
                },
            }],
        }
    }
}
