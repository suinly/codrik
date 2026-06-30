use std::{
    env, fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, Result, bail};
use reqwest::Client;
use serde::Deserialize;
use sha2::{Digest, Sha256};

const REPO_API_URL: &str = "https://api.github.com/repos/suinly/codrik";
const BIN_NAME: &str = "codrik";
const TELEGRAM_SERVICE: &str = "codrik-telegram.service";
const TELEGRAM_LAUNCHD_LABEL: &str = "com.suinly.codrik.telegram";

#[derive(Debug, Deserialize)]
struct GitHubRelease {
    tag_name: String,
    assets: Vec<GitHubAsset>,
}

#[derive(Debug, Deserialize)]
struct GitHubAsset {
    name: String,
    browser_download_url: String,
}

pub async fn update() -> Result<()> {
    let current_version = env!("CARGO_PKG_VERSION");
    let target = current_platform_asset_target()?;
    let client = Client::builder().user_agent(user_agent()).build()?;
    let latest = latest_release(&client).await?;
    let latest_version = latest.tag_name.trim_start_matches('v');

    if latest_version == current_version {
        println!("codrik is already up to date ({current_version})");
        return Ok(());
    }

    let asset_name = format!("{BIN_NAME}-{}-{target}", latest.tag_name);
    let checksum_name = format!("{asset_name}.sha256");
    let asset = find_asset(&latest, &asset_name)?;
    let checksum_asset = find_asset(&latest, &checksum_name)?;

    println!("Updating codrik {current_version} -> {latest_version}");
    println!("Downloading {asset_name}");

    let binary = download_bytes(&client, &asset.browser_download_url).await?;
    let checksum = download_text(&client, &checksum_asset.browser_download_url).await?;
    verify_sha256(&binary, &checksum, &asset_name)?;

    let exe_path = env::current_exe().context("failed to determine current executable path")?;
    replace_current_binary(&exe_path, &binary)?;
    println!(
        "Installed codrik {} to {}",
        latest.tag_name,
        exe_path.display()
    );

    restart_gateway_service_if_running()?;

    Ok(())
}

async fn latest_release(client: &Client) -> Result<GitHubRelease> {
    client
        .get(format!("{REPO_API_URL}/releases/latest"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
        .context("failed to read latest GitHub release")
}

fn find_asset<'a>(release: &'a GitHubRelease, name: &str) -> Result<&'a GitHubAsset> {
    release
        .assets
        .iter()
        .find(|asset| asset.name == name)
        .with_context(|| format!("release {} does not contain asset {name}", release.tag_name))
}

async fn download_bytes(client: &Client, url: &str) -> Result<Vec<u8>> {
    Ok(client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?
        .to_vec())
}

async fn download_text(client: &Client, url: &str) -> Result<String> {
    client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await
        .context("failed to download checksum")
}

fn verify_sha256(binary: &[u8], checksum: &str, asset_name: &str) -> Result<()> {
    let expected = checksum
        .split_whitespace()
        .next()
        .context("checksum file is empty")?;
    let actual = format!("{:x}", Sha256::digest(binary));

    if actual != expected {
        bail!("checksum mismatch for {asset_name}: expected {expected}, got {actual}");
    }

    Ok(())
}

fn replace_current_binary(exe_path: &Path, binary: &[u8]) -> Result<()> {
    let tmp_path = temp_binary_path(exe_path)?;

    fs::write(&tmp_path, binary)
        .with_context(|| format!("failed to write {}", tmp_path.display()))?;
    fs::set_permissions(&tmp_path, fs::Permissions::from_mode(0o755))
        .with_context(|| format!("failed to chmod {}", tmp_path.display()))?;
    fs::rename(&tmp_path, exe_path)
        .with_context(|| format!("failed to replace {}", exe_path.display()))?;

    Ok(())
}

fn temp_binary_path(exe_path: &Path) -> Result<PathBuf> {
    let file_name = exe_path
        .file_name()
        .and_then(|name| name.to_str())
        .context("current executable path has no file name")?;
    let tmp_name = format!(".{file_name}.update-{}", std::process::id());
    Ok(exe_path.with_file_name(tmp_name))
}

fn restart_gateway_service_if_running() -> Result<()> {
    match env::consts::OS {
        "linux" => restart_systemd_user_service_if_running(),
        "macos" => restart_launchd_service_if_running(),
        _ => Ok(()),
    }
}

fn restart_systemd_user_service_if_running() -> Result<()> {
    let active = Command::new("systemctl")
        .args(["--user", "is-active", "--quiet", TELEGRAM_SERVICE])
        .status();

    if !matches!(active, Ok(status) if status.success()) {
        return Ok(());
    }

    let status = Command::new("systemctl")
        .args(["--user", "restart", TELEGRAM_SERVICE])
        .status()
        .context("failed to restart telegram user service")?;

    if !status.success() {
        bail!("failed to restart {TELEGRAM_SERVICE}");
    }

    println!("Restarted {TELEGRAM_SERVICE}");
    Ok(())
}

fn restart_launchd_service_if_running() -> Result<()> {
    let uid = current_uid()?;
    let service = format!("gui/{uid}/{TELEGRAM_LAUNCHD_LABEL}");
    let active = Command::new("launchctl").args(["print", &service]).status();

    if !matches!(active, Ok(status) if status.success()) {
        return Ok(());
    }

    let status = Command::new("launchctl")
        .args(["kickstart", "-k", &service])
        .status()
        .context("failed to restart telegram LaunchAgent")?;

    if !status.success() {
        bail!("failed to restart {TELEGRAM_LAUNCHD_LABEL}");
    }

    println!("Restarted {TELEGRAM_LAUNCHD_LABEL}");
    Ok(())
}

fn current_uid() -> Result<String> {
    let output = Command::new("id")
        .arg("-u")
        .output()
        .context("failed to read current uid")?;

    if !output.status.success() {
        bail!("failed to read current uid");
    }

    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}

fn current_platform_asset_target() -> Result<&'static str> {
    asset_target_for(env::consts::OS, env::consts::ARCH)
}

fn asset_target_for(os: &str, arch: &str) -> Result<&'static str> {
    match (os, arch) {
        ("macos", "aarch64") => Ok("aarch64-apple-darwin"),
        ("macos", "x86_64") => Ok("x86_64-apple-darwin"),
        ("linux", "aarch64") => Ok("raspberry-pi-5-aarch64-unknown-linux-gnu"),
        ("linux", "x86_64") => Ok("x86_64-unknown-linux-gnu"),
        _ => bail!("unsupported platform: {os} {arch}"),
    }
}

fn user_agent() -> String {
    format!("{BIN_NAME}/{}", env!("CARGO_PKG_VERSION"))
}

#[cfg(test)]
mod tests {
    use super::{asset_target_for, verify_sha256};

    #[test]
    fn maps_supported_platforms_to_release_assets() {
        assert_eq!(
            asset_target_for("macos", "aarch64").unwrap(),
            "aarch64-apple-darwin"
        );
        assert_eq!(
            asset_target_for("macos", "x86_64").unwrap(),
            "x86_64-apple-darwin"
        );
        assert_eq!(
            asset_target_for("linux", "aarch64").unwrap(),
            "raspberry-pi-5-aarch64-unknown-linux-gnu"
        );
        assert_eq!(
            asset_target_for("linux", "x86_64").unwrap(),
            "x86_64-unknown-linux-gnu"
        );
    }

    #[test]
    fn verifies_sha256_checksum() {
        let checksum = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824  codrik";

        verify_sha256(b"hello", checksum, "codrik").unwrap();
    }
}
