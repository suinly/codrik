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
    #[serde(default)]
    pub attachments: AttachmentConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TelegramConfig {
    pub token: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct AttachmentConfig {
    #[serde(default = "default_max_file_size_mb")]
    pub max_file_size_mb: u64,
    #[serde(default)]
    pub image_detail: ImageDetailConfig,
}

impl Default for AttachmentConfig {
    fn default() -> Self {
        Self {
            max_file_size_mb: default_max_file_size_mb(),
            image_detail: ImageDetailConfig::Auto,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImageDetailConfig {
    #[default]
    Auto,
    Low,
    High,
}

fn default_max_file_size_mb() -> u64 {
    20
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

    let cwd_config = PathBuf::from("config.yml");
    if cwd_config.exists() {
        return Ok(cwd_config);
    }

    let home = env::var("HOME").context("HOME is not set; set CODRIK_CONFIG explicitly")?;
    Ok(PathBuf::from(home).join(".codrik").join("config.yml"))
}

pub fn codrik_dir() -> Result<PathBuf> {
    if let Ok(path) = env::var("CODRIK_HOME") {
        return Ok(PathBuf::from(path));
    }

    let home = env::var("HOME").context("HOME is not set; set CODRIK_HOME explicitly")?;
    Ok(PathBuf::from(home).join(".codrik"))
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::{AppConfig, ImageDetailConfig};

    #[test]
    fn attachment_config_defaults_when_omitted() -> Result<()> {
        let config: AppConfig =
            yaml_serde::from_str("api_key: key\nbase_url: https://example.test/v1\nmodel: test\n")?;

        assert_eq!(config.attachments.max_file_size_mb, 20);
        assert_eq!(config.attachments.image_detail, ImageDetailConfig::Auto);
        Ok(())
    }

    #[test]
    fn attachment_config_accepts_explicit_values() -> Result<()> {
        let config: AppConfig = yaml_serde::from_str(
            "api_key: key\nbase_url: https://example.test/v1\nmodel: test\nattachments:\n  max_file_size_mb: 32\n  image_detail: high\n",
        )?;

        assert_eq!(config.attachments.max_file_size_mb, 32);
        assert_eq!(config.attachments.image_detail, ImageDetailConfig::High);
        Ok(())
    }
}
