#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ActorSeedSet {
    pub actors: Vec<ActorSeed>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ActorSeed {
    pub id: String,
    pub enabled: bool,
    pub tools: Vec<String>,
    pub identities: Vec<IdentitySeed>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IdentitySeed {
    pub provider: String,
    pub subject: String,
    pub username: Option<String>,
}
