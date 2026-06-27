use std::{
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub api_key: String,
    pub base_url: String,
    pub model: String,
    pub telegram: Option<TelegramConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TelegramConfig {
    pub token: String,
}

impl AppConfig {
    pub fn load_default() -> Result<Self> {
        Self::load(default_config_path()?)
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();

        let content = fs::read_to_string(path)
            .with_context(|| format!("failed to read config file: {}", path.display()))?;

        let config = yaml_serde::from_str(&content)
            .with_context(|| format!("failed to parse config file: {}", path.display()))?;

        Ok(config)
    }
}

fn default_config_path() -> Result<PathBuf> {
    if let Ok(path) = env::var("CODRIK_CONFIG") {
        return Ok(PathBuf::from(path));
    }

    let cwd_config = PathBuf::from("codrik.config.yml");
    if cwd_config.exists() {
        return Ok(cwd_config);
    }

    let home = env::var("HOME").context("HOME is not set; set CODRIK_CONFIG explicitly")?;
    Ok(PathBuf::from(home)
        .join(".codrik")
        .join("codrik.config.yml"))
}

pub fn codrik_dir() -> Result<PathBuf> {
    if let Ok(path) = env::var("CODRIK_HOME") {
        return Ok(PathBuf::from(path));
    }

    let home = env::var("HOME").context("HOME is not set; set CODRIK_HOME explicitly")?;
    Ok(PathBuf::from(home).join(".codrik"))
}
