use std::{collections::BTreeSet, path::PathBuf};

use anyhow::{Result, bail};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::runtime::{
    artifacts::remove_deleted_artifacts,
    model::{ActorId, Clock},
    signals::ActorDirectorySignals,
    store::{
        ActorAdminStore, ActorCreateOutcome, ActorDeleteMode, ActorDeleteOutcome, ActorDetails,
        RuntimeActor,
    },
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ActorAdminCommand {
    List,
    Show { actor_id: ActorId },
    Create { actor_id: ActorId },
    Enable { actor_id: ActorId },
    Disable { actor_id: ActorId },
    Delete { actor_id: ActorId, force: bool },
    ToolsList { actor_id: ActorId },
    ToolsGrant { actor_id: ActorId, tool: String },
    ToolsRevoke { actor_id: ActorId, tool: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum ActorAdminResult {
    Actors {
        actors: Vec<RuntimeActor>,
    },
    Actor {
        details: ActorDetails,
        changed: bool,
    },
    Tools {
        actor_id: ActorId,
        tools: Vec<String>,
    },
    Deleted {
        actor_id: ActorId,
    },
}

#[async_trait]
pub trait ActorAdministrator: Send + Sync {
    async fn execute(&self, command: ActorAdminCommand) -> Result<ActorAdminResult>;
}

pub struct ActorAdministration<S, C> {
    store: S,
    default_actor: ActorId,
    known_tools: BTreeSet<String>,
    signals: ActorDirectorySignals,
    clock: C,
    artifact_root: PathBuf,
}

impl<S, C> ActorAdministration<S, C> {
    pub fn new(
        store: S,
        default_actor: ActorId,
        mut known_tools: BTreeSet<String>,
        signals: ActorDirectorySignals,
        clock: C,
        artifact_root: impl Into<PathBuf>,
    ) -> Self {
        known_tools.insert("*".into());
        Self {
            store,
            default_actor,
            known_tools,
            signals,
            clock,
            artifact_root: artifact_root.into(),
        }
    }
}

impl<S, C> ActorAdministration<S, C>
where
    S: ActorAdminStore,
    C: Clock,
{
    async fn details(&self, actor: &ActorId) -> Result<ActorDetails> {
        self.store
            .actor_details(actor)
            .await?
            .ok_or_else(|| anyhow::anyhow!("actor {actor} does not exist"))
    }

    fn validate_tool(&self, tool: &str) -> Result<()> {
        if !self.known_tools.contains(tool) {
            bail!("unknown tool: {tool}")
        }
        Ok(())
    }

    async fn set_enabled(&self, actor: ActorId, enabled: bool) -> Result<ActorAdminResult> {
        if !enabled && actor == self.default_actor {
            bail!("default actor cannot be disabled")
        }
        let outcome = self
            .store
            .set_actor_enabled(&actor, enabled)
            .await?
            .ok_or_else(|| anyhow::anyhow!("actor {actor} does not exist"))?;
        if outcome.changed {
            self.signals.notify();
        }
        Ok(ActorAdminResult::Actor {
            details: self.details(&actor).await?,
            changed: outcome.changed,
        })
    }

    async fn mutate_tool(
        &self,
        actor: ActorId,
        tool: String,
        grant: bool,
    ) -> Result<ActorAdminResult> {
        self.validate_tool(&tool)?;
        let outcome = if grant {
            self.store.grant_actor_tool(&actor, &tool).await?
        } else {
            self.store.revoke_actor_tool(&actor, &tool).await?
        }
        .ok_or_else(|| anyhow::anyhow!("actor {actor} does not exist"))?;
        if outcome.changed {
            self.signals.notify();
        }
        Ok(ActorAdminResult::Actor {
            details: self.details(&actor).await?,
            changed: outcome.changed,
        })
    }

    async fn delete(&self, actor: ActorId, force: bool) -> Result<ActorAdminResult> {
        if actor == self.default_actor {
            bail!("default actor cannot be deleted")
        }
        let mode = if force {
            ActorDeleteMode::Force
        } else {
            ActorDeleteMode::EmptyOnly
        };
        match self
            .store
            .delete_actor(&actor, mode, self.clock.now())
            .await?
        {
            ActorDeleteOutcome::Deleted { artifact_paths } => {
                remove_deleted_artifacts(&self.artifact_root, &artifact_paths).await;
                self.signals.notify();
                Ok(ActorAdminResult::Deleted { actor_id: actor })
            }
            ActorDeleteOutcome::NotFound => bail!("actor {actor} does not exist"),
            ActorDeleteOutcome::Nonempty => {
                bail!("actor {actor} is not empty; disable it and use --force")
            }
            ActorDeleteOutcome::Busy => bail!("actor {actor} is busy"),
            ActorDeleteOutcome::UnresolvedDelivery => {
                bail!("actor {actor} has unresolved delivery")
            }
        }
    }
}

#[async_trait]
impl<S, C> ActorAdministrator for ActorAdministration<S, C>
where
    S: ActorAdminStore,
    C: Clock,
{
    async fn execute(&self, command: ActorAdminCommand) -> Result<ActorAdminResult> {
        match command {
            ActorAdminCommand::List => Ok(ActorAdminResult::Actors {
                actors: self.store.list_actors().await?,
            }),
            ActorAdminCommand::Show { actor_id } => Ok(ActorAdminResult::Actor {
                details: self.details(&actor_id).await?,
                changed: false,
            }),
            ActorAdminCommand::Create { actor_id } => {
                let outcome = self.store.create_actor(&actor_id, self.clock.now()).await?;
                let changed = matches!(outcome, ActorCreateOutcome::Created(_));
                if changed {
                    self.signals.notify();
                }
                Ok(ActorAdminResult::Actor {
                    details: self.details(&actor_id).await?,
                    changed,
                })
            }
            ActorAdminCommand::Enable { actor_id } => self.set_enabled(actor_id, true).await,
            ActorAdminCommand::Disable { actor_id } => self.set_enabled(actor_id, false).await,
            ActorAdminCommand::Delete { actor_id, force } => self.delete(actor_id, force).await,
            ActorAdminCommand::ToolsList { actor_id } => {
                let actor = self.details(&actor_id).await?.actor;
                Ok(ActorAdminResult::Tools {
                    actor_id,
                    tools: actor.tools,
                })
            }
            ActorAdminCommand::ToolsGrant { actor_id, tool } => {
                self.mutate_tool(actor_id, tool, true).await
            }
            ActorAdminCommand::ToolsRevoke { actor_id, tool } => {
                self.mutate_tool(actor_id, tool, false).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use anyhow::Result;

    use crate::runtime::{
        actor_admin::{ActorAdminCommand, ActorAdministration, ActorAdministrator},
        model::{ActorId, ManualClock, Timestamp},
        signals::ActorDirectorySignals,
        sqlite::SqliteRuntimeStore,
        store::{ActorStore, RuntimeActor},
    };

    async fn administration(
        signals: ActorDirectorySignals,
    ) -> Result<ActorAdministration<SqliteRuntimeStore, ManualClock>> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let default = ActorId::parse_workspace_safe("owner")?;
        store
            .ensure_initial_actor(&default, &["*".into()], Timestamp(1))
            .await?;
        Ok(ActorAdministration::new(
            store,
            default,
            BTreeSet::from(["*".into(), "bash".into(), "datetime".into()]),
            signals,
            ManualClock::new(2),
            std::env::temp_dir().join("codrik-actor-admin-artifacts"),
        ))
    }

    #[tokio::test]
    async fn protects_default_actor_and_rejects_unknown_tools() -> Result<()> {
        let admin = administration(ActorDirectorySignals::default()).await?;
        let owner = ActorId::parse_workspace_safe("owner")?;

        assert!(
            admin
                .execute(ActorAdminCommand::Disable {
                    actor_id: owner.clone(),
                })
                .await
                .unwrap_err()
                .to_string()
                .contains("default actor")
        );
        assert!(
            admin
                .execute(ActorAdminCommand::ToolsGrant {
                    actor_id: owner,
                    tool: "missing".into(),
                })
                .await
                .unwrap_err()
                .to_string()
                .contains("unknown tool")
        );
        Ok(())
    }

    #[tokio::test]
    async fn committed_change_notifies_but_idempotent_create_does_not() -> Result<()> {
        let signals = ActorDirectorySignals::default();
        let mut changed = signals.subscribe();
        let admin = administration(signals).await?;
        let alice = ActorId::parse_workspace_safe("alice")?;

        admin
            .execute(ActorAdminCommand::Create {
                actor_id: alice.clone(),
            })
            .await?;
        changed.changed().await?;
        let observed = *changed.borrow_and_update();

        admin
            .execute(ActorAdminCommand::Create { actor_id: alice })
            .await?;
        assert_eq!(*changed.borrow(), observed);
        Ok(())
    }

    #[tokio::test]
    async fn create_returns_enabled_actor_without_tools() -> Result<()> {
        let admin = administration(ActorDirectorySignals::default()).await?;
        let result = admin
            .execute(ActorAdminCommand::Create {
                actor_id: ActorId::parse_workspace_safe("alice")?,
            })
            .await?;

        assert!(matches!(
            result,
            crate::runtime::actor_admin::ActorAdminResult::Actor {
                details: crate::runtime::store::ActorDetails {
                    actor: RuntimeActor {
                        enabled: true,
                        ref tools,
                        ..
                    },
                    ..
                },
                changed: true,
            } if tools.is_empty()
        ));
        Ok(())
    }

    #[tokio::test]
    async fn wildcard_is_known_without_being_a_registered_handler() -> Result<()> {
        let store = SqliteRuntimeStore::open_in_memory().await?;
        let owner = ActorId::parse_workspace_safe("owner")?;
        store
            .ensure_initial_actor(&owner, &[], Timestamp(1))
            .await?;
        let admin = ActorAdministration::new(
            store,
            owner.clone(),
            BTreeSet::from(["datetime".into()]),
            ActorDirectorySignals::default(),
            ManualClock::new(2),
            std::env::temp_dir().join("codrik-actor-admin-artifacts"),
        );

        admin
            .execute(ActorAdminCommand::ToolsGrant {
                actor_id: owner,
                tool: "*".into(),
            })
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn default_actor_cannot_be_deleted() -> Result<()> {
        let admin = administration(ActorDirectorySignals::default()).await?;

        let error = admin
            .execute(ActorAdminCommand::Delete {
                actor_id: ActorId::parse_workspace_safe("owner")?,
                force: false,
            })
            .await
            .unwrap_err();

        assert!(error.to_string().contains("default actor"));
        Ok(())
    }

    #[tokio::test]
    async fn deleting_empty_actor_notifies_directory_subscribers() -> Result<()> {
        let signals = ActorDirectorySignals::default();
        let mut changed = signals.subscribe();
        let admin = administration(signals).await?;
        let alice = ActorId::parse_workspace_safe("alice")?;
        admin
            .execute(ActorAdminCommand::Create {
                actor_id: alice.clone(),
            })
            .await?;
        changed.changed().await?;
        changed.borrow_and_update();

        let result = admin
            .execute(ActorAdminCommand::Delete {
                actor_id: alice.clone(),
                force: false,
            })
            .await?;

        assert_eq!(
            result,
            crate::runtime::actor_admin::ActorAdminResult::Deleted { actor_id: alice }
        );
        changed.changed().await?;
        Ok(())
    }

    #[tokio::test]
    async fn force_delete_requires_disabled_actor() -> Result<()> {
        let admin = administration(ActorDirectorySignals::default()).await?;
        let alice = ActorId::parse_workspace_safe("alice")?;
        admin
            .execute(ActorAdminCommand::Create {
                actor_id: alice.clone(),
            })
            .await?;

        let error = admin
            .execute(ActorAdminCommand::Delete {
                actor_id: alice,
                force: true,
            })
            .await
            .unwrap_err();

        assert!(error.to_string().contains("busy"));
        Ok(())
    }
}
