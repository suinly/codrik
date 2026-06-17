use std::{fs, path::Path};

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub api_key: String,
    pub base_url: String,
    pub model: String,
}

impl AppConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();

        let content = fs::read_to_string(path)
            .with_context(|| format!("failed to read config file: {}", path.display()))?;

        let config = serde_yaml::from_str(&content)
            .with_context(|| format!("failed to parse config file: {}", path.display()))?;

        Ok(config)
    }
}
