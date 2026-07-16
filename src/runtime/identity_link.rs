use anyhow::{Result, bail};
use async_trait::async_trait;
use sha2::{Digest, Sha256};

use crate::runtime::{
    gateway::GatewayCommandKey,
    model::{ActorId, Clock, Timestamp},
    store::{
        IdentityLinkStore, LinkIdentity, StoreLinkCodeReplacement, StoreLinkCommandRedemption,
        StoreLinkRedemption,
    },
};

pub const LINK_CODE_ALPHABET: &[u8] = b"23456789ABCDEFGHJKMNPQRSTUVWXYZ";
pub const LINK_CODE_TTL_MILLIS: i64 = 600_000;
const LINK_CODE_HASH_DOMAIN: &[u8] = b"codrik-identity-link-v1\0";

pub(crate) fn normalize_link_code(raw: &str) -> Option<String> {
    let normalized = raw
        .bytes()
        .filter(|byte| !byte.is_ascii_whitespace() && *byte != b'-')
        .map(|byte| byte.to_ascii_uppercase())
        .collect::<Vec<_>>();
    if normalized.len() != 8
        || normalized
            .iter()
            .any(|byte| !LINK_CODE_ALPHABET.contains(byte))
    {
        return None;
    }
    String::from_utf8(normalized).ok()
}

pub(crate) fn group_link_code(normalized: &str) -> String {
    format!("{}-{}", &normalized[..4], &normalized[4..])
}

pub(crate) fn hash_link_code(normalized: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(LINK_CODE_HASH_DOMAIN);
    hasher.update(normalized.as_bytes());
    hasher.finalize().into()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IssuedLinkCode {
    pub code: String,
    pub expires_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LinkRedemption {
    Linked { actor_id: ActorId },
    AlreadyLinked { actor_id: ActorId },
    InvalidOrExpired,
    RateLimited { retry_at: Timestamp },
    IdentityConflict,
}

pub trait LinkCodeGenerator: Send + Sync {
    fn generate(&self) -> Result<String>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemLinkCodeGenerator;

impl LinkCodeGenerator for SystemLinkCodeGenerator {
    fn generate(&self) -> Result<String> {
        let mut code = Vec::with_capacity(8);
        let mut random = [0_u8; 16];
        while code.len() < 8 {
            getrandom::fill(&mut random)?;
            for byte in random {
                if byte < 248 {
                    code.push(LINK_CODE_ALPHABET[(byte % 31) as usize]);
                    if code.len() == 8 {
                        break;
                    }
                }
            }
        }
        Ok(String::from_utf8(code)?)
    }
}

#[async_trait]
pub trait IdentityLinkManager: Send + Sync {
    async fn issue_code(&self, actor: &ActorId) -> Result<IssuedLinkCode>;
    async fn redeem_code(&self, identity: LinkIdentity, code: &str) -> Result<LinkRedemption>;
    async fn redeem_code_once(
        &self,
        key: GatewayCommandKey,
        identity: LinkIdentity,
        code: &str,
    ) -> Result<LinkRedemption>;
    async fn collect_expired(&self, limit: usize) -> Result<usize>;
}

#[derive(Clone)]
pub struct IdentityLinkService<S, C, G> {
    store: S,
    clock: C,
    generator: G,
}

impl<S, C, G> IdentityLinkService<S, C, G> {
    pub fn new(store: S, clock: C, generator: G) -> Self {
        Self {
            store,
            clock,
            generator,
        }
    }
}

#[async_trait]
impl<S, C, G> IdentityLinkManager for IdentityLinkService<S, C, G>
where
    S: IdentityLinkStore + Send + Sync,
    C: Clock,
    G: LinkCodeGenerator,
{
    async fn issue_code(&self, actor: &ActorId) -> Result<IssuedLinkCode> {
        let created_at = self.clock.now();
        let expires_at = created_at.plus_millis(LINK_CODE_TTL_MILLIS);
        for _ in 0..5 {
            let generated = self.generator.generate()?;
            let normalized = normalize_link_code(&generated)
                .ok_or_else(|| anyhow::anyhow!("link code generator returned an invalid code"))?;
            let hash = hash_link_code(&normalized);
            match self
                .store
                .replace_link_code(actor, hash, created_at, expires_at)
                .await?
            {
                StoreLinkCodeReplacement::Stored => {
                    return Ok(IssuedLinkCode {
                        code: group_link_code(&normalized),
                        expires_at,
                    });
                }
                StoreLinkCodeReplacement::HashCollision => {}
            }
        }
        bail!("failed to generate a unique identity link code after 5 attempts")
    }

    async fn redeem_code(&self, identity: LinkIdentity, code: &str) -> Result<LinkRedemption> {
        let hash = normalize_link_code(code).map(|normalized| hash_link_code(&normalized));
        Ok(
            match self
                .store
                .redeem_link_code(identity, hash, self.clock.now())
                .await?
            {
                StoreLinkRedemption::Linked { actor_id } => LinkRedemption::Linked { actor_id },
                StoreLinkRedemption::AlreadyLinked { actor_id } => {
                    LinkRedemption::AlreadyLinked { actor_id }
                }
                StoreLinkRedemption::InvalidOrExpired => LinkRedemption::InvalidOrExpired,
                StoreLinkRedemption::RateLimited { retry_at } => {
                    LinkRedemption::RateLimited { retry_at }
                }
                StoreLinkRedemption::IdentityConflict { .. } => LinkRedemption::IdentityConflict,
            },
        )
    }

    async fn redeem_code_once(
        &self,
        key: GatewayCommandKey,
        identity: LinkIdentity,
        code: &str,
    ) -> Result<LinkRedemption> {
        let hash = normalize_link_code(code).map(|normalized| hash_link_code(&normalized));
        Ok(
            match self
                .store
                .redeem_link_code_once(key, identity, hash, self.clock.now())
                .await?
            {
                StoreLinkCommandRedemption::Linked { actor_id } => {
                    LinkRedemption::Linked { actor_id }
                }
                StoreLinkCommandRedemption::AlreadyLinked { actor_id } => {
                    LinkRedemption::AlreadyLinked { actor_id }
                }
                StoreLinkCommandRedemption::InvalidOrExpired => LinkRedemption::InvalidOrExpired,
                StoreLinkCommandRedemption::RateLimited { retry_at } => {
                    LinkRedemption::RateLimited { retry_at }
                }
                StoreLinkCommandRedemption::IdentityConflict => LinkRedemption::IdentityConflict,
            },
        )
    }

    async fn collect_expired(&self, limit: usize) -> Result<usize> {
        self.store
            .collect_expired_link_state(self.clock.now(), limit)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    use anyhow::Result;
    use async_trait::async_trait;
    use sha2::{Digest, Sha256};

    use super::{
        IdentityLinkManager, IdentityLinkService, IssuedLinkCode, LINK_CODE_ALPHABET,
        LinkCodeGenerator, LinkRedemption, SystemLinkCodeGenerator, group_link_code,
        hash_link_code, normalize_link_code,
    };
    use crate::runtime::{
        gateway::GatewayCommandKey,
        model::{ActorId, ManualClock, Timestamp},
        store::{
            IdentityLinkStore, LinkIdentity, StoreLinkCodeReplacement, StoreLinkCommandRedemption,
            StoreLinkRedemption,
        },
    };

    #[derive(Clone)]
    struct SequenceGenerator(Arc<Mutex<VecDeque<String>>>);

    impl SequenceGenerator {
        fn new(values: impl IntoIterator<Item = &'static str>) -> Self {
            Self(Arc::new(Mutex::new(
                values.into_iter().map(str::to_string).collect(),
            )))
        }
    }

    impl LinkCodeGenerator for SequenceGenerator {
        fn generate(&self) -> Result<String> {
            Ok(self.0.lock().unwrap().pop_front().unwrap())
        }
    }

    #[derive(Clone)]
    struct ScriptedStore {
        replacements: Arc<Mutex<VecDeque<StoreLinkCodeReplacement>>>,
        last_hash: Arc<Mutex<Option<Option<[u8; 32]>>>>,
    }

    #[async_trait]
    impl IdentityLinkStore for ScriptedStore {
        async fn replace_link_code(
            &self,
            _actor: &ActorId,
            _code_hash: [u8; 32],
            _created_at: Timestamp,
            _expires_at: Timestamp,
        ) -> Result<StoreLinkCodeReplacement> {
            Ok(self.replacements.lock().unwrap().pop_front().unwrap())
        }

        async fn redeem_link_code(
            &self,
            _identity: LinkIdentity,
            code_hash: Option<[u8; 32]>,
            _now: Timestamp,
        ) -> Result<StoreLinkRedemption> {
            *self.last_hash.lock().unwrap() = Some(code_hash);
            Ok(StoreLinkRedemption::InvalidOrExpired)
        }

        async fn redeem_link_code_once(
            &self,
            _key: GatewayCommandKey,
            _identity: LinkIdentity,
            code_hash: Option<[u8; 32]>,
            _now: Timestamp,
        ) -> Result<StoreLinkCommandRedemption> {
            *self.last_hash.lock().unwrap() = Some(code_hash);
            Ok(StoreLinkCommandRedemption::InvalidOrExpired)
        }

        async fn collect_expired_link_state(
            &self,
            _now: Timestamp,
            _limit: usize,
        ) -> Result<usize> {
            Ok(0)
        }
    }

    #[test]
    fn normalization_accepts_supported_forms() {
        for raw in ["ABCD-EFGH", "abcdefgh", "  ABCD EFGH\n"] {
            assert_eq!(normalize_link_code(raw).as_deref(), Some("ABCDEFGH"));
        }
    }

    #[test]
    fn normalization_rejects_length_and_alphabet_errors() {
        for raw in ["ABC", "ABCDEFGHI", "ABCD0FGH", "ABCD/FGH"] {
            assert!(normalize_link_code(raw).is_none(), "{raw}");
        }
    }

    #[test]
    fn grouped_code_and_domain_separated_hash_are_stable() {
        assert_eq!(group_link_code("ABCDEFGH"), "ABCD-EFGH");
        let expected: [u8; 32] = Sha256::digest(b"codrik-identity-link-v1\0ABCDEFGH").into();
        assert_eq!(hash_link_code("ABCDEFGH"), expected);
    }

    #[test]
    fn system_generator_returns_eight_allowed_symbols() -> Result<()> {
        for _ in 0..64 {
            let code = SystemLinkCodeGenerator.generate()?;
            assert_eq!(code.len(), 8);
            assert!(code.bytes().all(|byte| LINK_CODE_ALPHABET.contains(&byte)));
        }
        Ok(())
    }

    #[tokio::test]
    async fn issue_retries_collision_and_returns_grouped_code() -> Result<()> {
        let store = ScriptedStore {
            replacements: Arc::new(Mutex::new(VecDeque::from([
                StoreLinkCodeReplacement::HashCollision,
                StoreLinkCodeReplacement::Stored,
            ]))),
            last_hash: Arc::new(Mutex::new(None)),
        };
        let service = IdentityLinkService::new(
            store,
            ManualClock::new(100),
            SequenceGenerator::new(["ABCDEFGH", "MNPQRSTU"]),
        );
        assert_eq!(
            service.issue_code(&ActorId::from_string("actor")).await?,
            IssuedLinkCode {
                code: "MNPQ-RSTU".into(),
                expires_at: Timestamp(600_100),
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn invalid_syntax_redeems_without_hash() -> Result<()> {
        let store = ScriptedStore {
            replacements: Arc::new(Mutex::new(VecDeque::new())),
            last_hash: Arc::new(Mutex::new(None)),
        };
        let observed = store.last_hash.clone();
        let service =
            IdentityLinkService::new(store, ManualClock::new(100), SequenceGenerator::new([]));
        assert_eq!(
            service
                .redeem_code(
                    LinkIdentity {
                        provider: "telegram".into(),
                        subject: "123".into(),
                        username: None,
                    },
                    "bad/code",
                )
                .await?,
            LinkRedemption::InvalidOrExpired
        );
        assert_eq!(*observed.lock().unwrap(), Some(None));
        Ok(())
    }

    #[tokio::test]
    async fn gateway_redemption_normalizes_before_idempotent_store_call() -> Result<()> {
        let store = ScriptedStore {
            replacements: Arc::new(Mutex::new(VecDeque::new())),
            last_hash: Arc::new(Mutex::new(None)),
        };
        let observed = store.last_hash.clone();
        let service =
            IdentityLinkService::new(store, ManualClock::new(100), SequenceGenerator::new([]));
        assert_eq!(
            service
                .redeem_code_once(
                    GatewayCommandKey {
                        gateway: "telegram:bot-1".into(),
                        external_id: "42".into(),
                    },
                    LinkIdentity {
                        provider: "telegram:bot-1".into(),
                        subject: "123".into(),
                        username: None,
                    },
                    "abcd-efgh",
                )
                .await?,
            LinkRedemption::InvalidOrExpired
        );
        assert_eq!(
            *observed.lock().unwrap(),
            Some(Some(hash_link_code("ABCDEFGH")))
        );
        Ok(())
    }
}
