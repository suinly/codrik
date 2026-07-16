use std::{
    env, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    pub api_key: String,
    pub base_url: String,
    pub model: String,
    #[serde(default)]
    pub attachments: AttachmentConfig,
    #[serde(default)]
    pub runtime: Option<RuntimeConfig>,
    #[serde(default)]
    pub telegram: Option<TelegramConfig>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelegramConfig {
    pub token: String,
    pub public_url: String,
    #[serde(default = "default_telegram_listen")]
    pub listen: String,
    pub webhook_secret: String,
}

#[derive(Clone)]
pub struct ValidatedTelegramConfig {
    pub token: String,
    pub public_url: url::Url,
    pub listen: SocketAddr,
    pub webhook_secret: String,
}

impl std::fmt::Debug for TelegramConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TelegramConfig")
            .field("token", &"[REDACTED]")
            .field("public_url", &self.public_url)
            .field("listen", &self.listen)
            .field("webhook_secret", &"[REDACTED]")
            .finish()
    }
}

impl std::fmt::Debug for ValidatedTelegramConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ValidatedTelegramConfig")
            .field("token", &"[REDACTED]")
            .field("public_url", &self.public_url)
            .field("listen", &self.listen)
            .field("webhook_secret", &"[REDACTED]")
            .finish()
    }
}

impl TelegramConfig {
    pub fn validate(&self) -> Result<ValidatedTelegramConfig> {
        let public_url =
            url::Url::parse(&self.public_url).context("telegram.public_url is not a valid URL")?;
        if public_url.scheme() != "https"
            || public_url.host_str().is_none()
            || public_url.query().is_some()
            || public_url.fragment().is_some()
        {
            bail!("telegram.public_url must be an HTTPS URL without query or fragment");
        }
        let listen = self
            .listen
            .parse::<SocketAddr>()
            .context("telegram.listen must be a socket address")?;
        if self.token.trim().is_empty() {
            bail!("telegram.token must not be blank");
        }
        if self.webhook_secret.is_empty()
            || self.webhook_secret.len() > 256
            || !self
                .webhook_secret
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
        {
            bail!("telegram.webhook_secret has invalid length or characters");
        }
        Ok(ValidatedTelegramConfig {
            token: self.token.clone(),
            public_url,
            listen,
            webhook_secret: self.webhook_secret.clone(),
        })
    }
}

fn default_telegram_listen() -> String {
    "127.0.0.1:8080".into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct RuntimeConfig {
    #[serde(deserialize_with = "deserialize_strict_string")]
    pub actor_id: String,
    #[serde(default)]
    pub database_path: Option<PathBuf>,
    #[serde(default)]
    pub socket_path: Option<PathBuf>,
    #[serde(default)]
    pub lock_path: Option<PathBuf>,
    #[serde(default)]
    pub artifact_path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimePaths {
    pub database: PathBuf,
    pub socket: PathBuf,
    pub lock: PathBuf,
    pub artifacts: PathBuf,
    pub client_requests: PathBuf,
}

fn deserialize_strict_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct StrictStringVisitor;

    impl serde::de::Visitor<'_> for StrictStringVisitor {
        type Value = String;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a string")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(value.to_string())
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(value)
        }
    }

    deserializer.deserialize_any(StrictStringVisitor)
}

impl RuntimeConfig {
    pub fn resolve_paths(&self, codrik_home: &Path) -> Result<RuntimePaths> {
        if self.actor_id.trim().is_empty() {
            bail!("runtime.actor_id must not be blank");
        }

        Ok(RuntimePaths {
            database: resolve_runtime_path(
                self.database_path.as_deref(),
                codrik_home,
                "runtime.sqlite",
            ),
            socket: resolve_runtime_path(self.socket_path.as_deref(), codrik_home, "codrik.sock"),
            lock: resolve_runtime_path(self.lock_path.as_deref(), codrik_home, "runtime.lock"),
            artifacts: resolve_runtime_path(
                self.artifact_path.as_deref(),
                codrik_home,
                "artifacts",
            ),
            client_requests: codrik_home.join("client").join("requests"),
        })
    }
}

fn resolve_runtime_path(configured: Option<&Path>, codrik_home: &Path, default: &str) -> PathBuf {
    let Some(path) = configured else {
        return codrik_home.join(default);
    };

    match path.strip_prefix("~/") {
        Ok(relative) => codrik_home.join(relative),
        Err(_) => path.to_path_buf(),
    }
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

    pub fn required_runtime(&self) -> Result<&RuntimeConfig> {
        let runtime = self
            .runtime
            .as_ref()
            .context("runtime configuration is required; add a runtime section to config.yml")?;

        if runtime.actor_id.trim().is_empty() {
            bail!("runtime.actor_id must not be blank");
        }

        Ok(runtime)
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
    use std::path::{Path, PathBuf};

    use anyhow::Result;

    use super::{AppConfig, ImageDetailConfig};

    #[test]
    fn telegram_config_defaults_and_validates() -> Result<()> {
        let config: AppConfig = yaml_serde::from_str(
            r#"api_key: key
base_url: https://example.test/v1
model: test
telegram:
  token: bot-token
  public_url: https://agent.example/webhooks/telegram
  webhook_secret: abc_DEF-123
"#,
        )?;

        let config_debug = format!("{config:?}");
        assert!(!config_debug.contains("bot-token"));
        assert!(!config_debug.contains("abc_DEF-123"));
        let telegram = config.telegram.as_ref().unwrap().validate()?;

        assert_eq!(telegram.listen, "127.0.0.1:8080".parse()?);
        assert_eq!(
            telegram.public_url.as_str(),
            "https://agent.example/webhooks/telegram"
        );
        assert!(!format!("{telegram:?}").contains("bot-token"));
        assert!(!format!("{telegram:?}").contains("abc_DEF-123"));
        Ok(())
    }

    #[test]
    fn telegram_config_rejects_insecure_url_bad_secret_and_unknown_fields() {
        for yaml in [
            "telegram:\n  token: t\n  public_url: http://agent.example/hook\n  webhook_secret: valid",
            "telegram:\n  token: t\n  public_url: https://agent.example/hook\n  webhook_secret: 'bad secret'",
            "telegram:\n  token: t\n  public_url: https://agent.example/hook\n  webhook_secret: valid\n  extra: true",
        ] {
            let document =
                format!("api_key: key\nbase_url: https://example.test/v1\nmodel: test\n{yaml}\n");
            let invalid = match yaml_serde::from_str::<AppConfig>(&document) {
                Ok(config) => config.telegram.unwrap().validate().is_err(),
                Err(_) => true,
            };
            assert!(invalid, "accepted invalid Telegram config:\n{document}");
        }
    }

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

    #[test]
    fn obsolete_telegram_secret_config_is_rejected() {
        let result = yaml_serde::from_str::<AppConfig>(
            "api_key: key\nbase_url: https://example.test/v1\nmodel: test\ntelegram:\n  token: secret\n",
        );
        assert!(result.is_err());
    }

    #[test]
    fn runtime_config_defaults_under_codrik_home() -> Result<()> {
        let config: AppConfig = yaml_serde::from_str(
            "api_key: key\nbase_url: https://example.test/v1\nmodel: test\nruntime:\n  actor_id: actor:local:owner\n",
        )?;

        let paths = config
            .required_runtime()?
            .resolve_paths(Path::new("/tmp/codrik-home"))?;

        assert_eq!(
            paths.database,
            PathBuf::from("/tmp/codrik-home/runtime.sqlite")
        );
        assert_eq!(paths.socket, PathBuf::from("/tmp/codrik-home/codrik.sock"));
        assert_eq!(paths.lock, PathBuf::from("/tmp/codrik-home/runtime.lock"));
        assert_eq!(paths.artifacts, PathBuf::from("/tmp/codrik-home/artifacts"));
        assert_eq!(
            paths.client_requests,
            PathBuf::from("/tmp/codrik-home/client/requests")
        );
        Ok(())
    }

    #[test]
    fn runtime_config_expands_only_a_leading_home_prefix() -> Result<()> {
        let config: AppConfig = yaml_serde::from_str(
            "api_key: key\nbase_url: https://example.test/v1\nmodel: test\nruntime:\n  actor_id: actor:local:owner\n  database_path: ~/data/runtime.sqlite\n  socket_path: $HOME/codrik.sock\n  lock_path: data/~/runtime.lock\n",
        )?;

        let paths = config
            .required_runtime()?
            .resolve_paths(Path::new("/tmp/codrik-home"))?;

        assert_eq!(
            paths.database,
            PathBuf::from("/tmp/codrik-home/data/runtime.sqlite")
        );
        assert_eq!(paths.socket, PathBuf::from("$HOME/codrik.sock"));
        assert_eq!(paths.lock, PathBuf::from("data/~/runtime.lock"));
        Ok(())
    }

    #[test]
    fn runtime_config_rejects_blank_actor_id() -> Result<()> {
        let config: AppConfig = yaml_serde::from_str(
            "api_key: key\nbase_url: https://example.test/v1\nmodel: test\nruntime:\n  actor_id: '   '\n",
        )?;

        assert!(config.required_runtime().is_err());
        Ok(())
    }

    #[test]
    fn runtime_config_rejects_non_string_actor_id() {
        for actor_id in ["true", "7", "null"] {
            let yaml = format!(
                "api_key: key\nbase_url: https://example.test/v1\nmodel: test\nruntime:\n  actor_id: {actor_id}\n"
            );
            assert!(
                yaml_serde::from_str::<AppConfig>(&yaml).is_err(),
                "accepted actor_id: {actor_id}"
            );
        }
    }

    #[test]
    fn runtime_config_may_be_omitted_while_parsing() -> Result<()> {
        let config: AppConfig =
            yaml_serde::from_str("api_key: key\nbase_url: https://example.test/v1\nmodel: test\n")?;

        assert!(config.required_runtime().is_err());
        Ok(())
    }
}
