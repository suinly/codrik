const INSTALLER: &str = include_str!("../scripts/install.sh");

use std::{fs, path::Path, process::Command};

fn temp_dir() -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!(
        "codrik-install-test-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    fs::create_dir(&path).unwrap();
    path
}

fn run_library(script: &str, args: &[&Path]) -> std::process::Output {
    let installer = Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/install.sh");
    let mut command = Command::new("sh");
    command
        .arg("-c")
        .arg(format!(". \"$1\"\n{script}"))
        .arg("sh")
        .arg(installer)
        .env("CODRIK_INSTALL_LIBRARY_ONLY", "1")
        .env("CODRIK_VALIDATOR_BIN", env!("CARGO_BIN_EXE_codrik"));
    for arg in args {
        command.arg(arg);
    }
    command.output().unwrap()
}

fn run_validator(config: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_codrik"))
        .arg("__installer_validate_config")
        .arg(config)
        .output()
        .unwrap()
}

#[test]
fn installer_validator_uses_production_config_parser() {
    let root = temp_dir();
    let config = root.join("config.yml");
    for yaml in [
        "api_key: k\nbase_url: https://example.test/v1\nmodel: m\nruntime:\n  actor_id: actor:existing:7\n",
        "api_key: k\nbase_url: https://example.test/v1\nmodel: m\nruntime:\n  actor_id: \" actor:existing:7 \"\n",
        "api_key: k\nbase_url: https://example.test/v1\nmodel: m\nruntime:\n  actor_id: 'actor:existing:7'\n",
    ] {
        fs::write(&config, yaml).unwrap();
        let output = run_validator(&config);
        assert!(
            output.status.success(),
            "{yaml}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            String::from_utf8(output.stdout).unwrap(),
            "actor:existing:7\n"
        );
    }

    for yaml in [
        "api_key: k\nbase_url: https://example.test/v1\nmodel: m\nruntime:\n  actor_id: true\n",
        "api_key: k\nbase_url: https://example.test/v1\nmodel: m\nruntime:\n  actor_id: 7\n",
        "api_key: k\nbase_url: https://example.test/v1\nmodel: m\nruntime:\n  actor_id: null\n",
        "api_key: k\nbase_url: https://example.test/v1\nmodel: m\nruntime:\n  actor_id: '   '\n",
        "api_key: k\nbase_url: https://example.test/v1\nmodel: m\nruntime:\n  actor_id: '../owner'\n",
        "api_key: k\nbase_url: https://example.test/v1\nmodel: m\nruntime:\n  actor_id: first\n  actor_id: second\n",
    ] {
        fs::write(&config, yaml).unwrap();
        assert!(
            !run_validator(&config).status.success(),
            "accepted invalid YAML: {yaml}"
        );
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn generated_services_run_only_the_foreground_serve_command() {
    assert!(INSTALLER.contains("ExecStart=$bin_path serve"));
    assert!(INSTALLER.contains("<string>serve</string>"));
    assert!(!INSTALLER.contains("ExecStart=$bin_path gateway"));
    assert!(!INSTALLER.contains("<string>gateway</string>"));
}

#[test]
fn service_names_replace_the_polling_gateway_definitions() {
    assert!(INSTALLER.contains("codrik.service"));
    assert!(INSTALLER.contains("com.suinly.codrik.plist"));
    assert!(INSTALLER.contains("<string>com.suinly.codrik</string>"));
    assert!(!INSTALLER.contains("codrik-$gateway.service"));
    assert!(!INSTALLER.contains("com.suinly.codrik.$gateway"));
}

#[test]
fn generated_config_uses_the_local_owner_actor() {
    assert!(INSTALLER.contains("actor_id=\"actor:local:owner\""));
    assert!(INSTALLER.contains("actor_id: actor:local:owner"));
}

#[test]
fn old_config_without_actor_prints_exact_yaml_and_blocks_service_start() {
    assert!(INSTALLER.contains("Existing config is missing runtime.actor_id. Add exactly:"));
    assert!(INSTALLER.contains("runtime:\n  actor_id: <existing-actor-id>"));
    assert!(INSTALLER.contains("CONFIGURED_RUNTIME_READY=0"));
}

#[test]
fn polling_gateway_installation_is_removed() {
    for removed in [
        "Configure a gateway?",
        "Gateway service to run",
        "Telegram bot token",
        "install_gateway_service",
        "gateway telegram",
    ] {
        assert!(
            !INSTALLER.contains(removed),
            "legacy polling installer text remains: {removed}"
        );
    }
}

#[test]
fn installer_source_contains_no_users_json_or_authorization_helpers() {
    for removed in [
        "users.json",
        "authorization_has_actors",
        "bootstrap_or_select_actor",
        "__installer_has_actors",
        "__installer_validate_actor",
    ] {
        assert!(
            !INSTALLER.contains(removed),
            "legacy installer text remains: {removed}"
        );
    }
}

#[test]
fn generated_service_files_match_the_foreground_goldens() {
    let root = temp_dir();
    let systemd = root.join("codrik.service");
    let launchd = root.join("com.suinly.codrik.plist");
    let output = run_library(
        "write_systemd_user_service \"$2\" /opt/codrik /cfg/config.yml /runtime\nwrite_launchd_service \"$3\" /opt/codrik /cfg/config.yml /runtime",
        &[&systemd, &launchd],
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let systemd_text = fs::read_to_string(systemd).unwrap();
    assert!(systemd_text.contains("ExecStart=/opt/codrik serve\n"));
    assert!(!systemd_text.contains("gateway"));
    let launchd_text = fs::read_to_string(launchd).unwrap();
    assert_eq!(launchd_text.matches("<string>serve</string>").count(), 1);
    assert!(!launchd_text.contains("gateway"));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn clean_install_writes_owner_config_without_users_json() {
    let root = temp_dir();
    let config_dir = root.join("config");
    let runtime_dir = root.join("runtime");
    let service = root.join("service-started");
    let output = run_library(
        r#"
is_interactive() { return 0; }
ask_yes_no() { return 0; }
ask_secret() { printf '%s\n' test-key; }
ask() { printf '%s\n' "$2"; }
SERVICE_MARKER="$4"
install_serve_service() { touch "$SERVICE_MARKER"; }
capture_install_state "$2" "$3"
configure_codrik "$2"
maybe_install_serve_service /opt/codrik
"#,
        &[&config_dir, &runtime_dir, &service],
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let config = fs::read_to_string(config_dir.join("config.yml")).unwrap();
    assert!(config.contains("actor_id: actor:local:owner"));
    assert!(!runtime_dir.join("users.json").exists());
    assert!(service.exists());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn kept_valid_config_allows_service_without_authorization_file() {
    let root = temp_dir();
    let config_dir = root.join("config");
    let runtime_dir = root.join("runtime");
    let service = root.join("service-started");
    fs::create_dir_all(&config_dir).unwrap();
    fs::create_dir_all(&runtime_dir).unwrap();
    let original = b"api_key: old\nbase_url: https://example.test/v1\nmodel: old\nruntime:\n  actor_id: 'actor:existing:7'\n";
    fs::write(config_dir.join("config.yml"), original).unwrap();
    let output = run_library(
        r#"
is_interactive() { return 0; }
ask_yes_no() { case "$1" in *Overwrite*) return 1 ;; *) return 0 ;; esac; }
SERVICE_MARKER="$4"
install_serve_service() { touch "$SERVICE_MARKER"; }
capture_install_state "$2" "$3"
configure_codrik "$2"
maybe_install_serve_service /opt/codrik
"#,
        &[&config_dir, &runtime_dir, &service],
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(fs::read(config_dir.join("config.yml")).unwrap(), original);
    assert!(service.exists());
    assert!(!runtime_dir.join("users.json").exists());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn kept_invalid_config_blocks_service() {
    let root = temp_dir();
    let config_dir = root.join("config");
    let runtime_dir = root.join("runtime");
    let service = root.join("service-started");
    fs::create_dir_all(&config_dir).unwrap();
    fs::create_dir_all(&runtime_dir).unwrap();
    let original = b"api_key: old\nruntime:\n  actor_id: '   '\n";
    fs::write(config_dir.join("config.yml"), original).unwrap();
    let output = run_library(
        r#"
is_interactive() { return 0; }
ask_yes_no() { case "$1" in *Overwrite*) return 1 ;; *) return 0 ;; esac; }
SERVICE_MARKER="$4"
install_serve_service() { touch "$SERVICE_MARKER"; }
capture_install_state "$2" "$3"
configure_codrik "$2"
maybe_install_serve_service /opt/codrik
"#,
        &[&config_dir, &runtime_dir, &service],
    );
    assert!(output.status.success());
    assert_eq!(fs::read(config_dir.join("config.yml")).unwrap(), original);
    assert!(!service.exists());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn missing_runtime_actor_message_is_exact() {
    let output = run_library("print_missing_runtime_actor", &[]);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stderr).unwrap(),
        "Existing config is missing runtime.actor_id. Add exactly:\nruntime:\n  actor_id: <existing-actor-id>\nCodrik service was not started.\n"
    );
}
