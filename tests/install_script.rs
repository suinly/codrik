const INSTALLER: &str = include_str!("../scripts/install.sh");

use std::{fs, os::unix::fs::PermissionsExt, path::Path, process::Command};

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
        .env("CODRIK_INSTALL_LIBRARY_ONLY", "1");
    for arg in args {
        command.arg(arg);
    }
    command.output().unwrap()
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
fn clean_authorization_bootstraps_only_the_local_owner() {
    assert!(INSTALLER.contains(r#""actor:local:owner""#));
    assert!(INSTALLER.contains(r#""tools": ["*"]"#));
    assert!(INSTALLER.contains("chmod 700 \"$runtime_dir\""));
    assert!(INSTALLER.contains("chmod 600 \"$users_file\""));
    assert!(INSTALLER.contains("actor_id: actor:local:owner"));
}

#[test]
fn existing_authorization_is_preserved_and_requires_explicit_actor_selection() {
    assert!(INSTALLER.contains("Existing authorization actor ID"));
    assert!(INSTALLER.contains("users.json is user-owned and will not be modified"));
    let existing_branch = INSTALLER
        .find("if authorization_has_actors \"$users_file\"; then")
        .expect("existing authorization branch");
    let early_return = INSTALLER[existing_branch..]
        .find("return")
        .map(|offset| existing_branch + offset)
        .expect("existing authorization branch should return");
    let bootstrap_write = INSTALLER
        .find("cat >\"$users_file\"")
        .expect("clean authorization bootstrap");
    assert!(existing_branch < early_return && early_return < bootstrap_write);
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
fn clean_and_empty_authorization_create_private_local_owner() {
    for initially_empty in [false, true] {
        let root = temp_dir();
        if initially_empty {
            fs::write(root.join("users.json"), b"").unwrap();
        }
        let selected = root.join("selected");
        let output = run_library(
            "bootstrap_or_select_actor \"$2\" >\"$3\"",
            &[&root, &selected],
        );
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(fs::read_to_string(selected).unwrap(), "actor:local:owner\n");
        assert_eq!(
            fs::metadata(&root).unwrap().permissions().mode() & 0o777,
            0o700
        );
        let users = root.join("users.json");
        assert_eq!(
            fs::metadata(&users).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let value: serde_json::Value = serde_json::from_slice(&fs::read(users).unwrap()).unwrap();
        assert_eq!(value["actors"].as_object().unwrap().len(), 1);
        assert_eq!(
            value["actors"]["actor:local:owner"]["tools"],
            serde_json::json!(["*"])
        );
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn existing_authorization_remains_byte_for_byte_unchanged() {
    let root = temp_dir();
    let users = root.join("users.json");
    let original =
        b"{ \"version\": 1, \"actors\": { \"actor:existing:7\": {\"enabled\":true} } }\n";
    fs::write(&users, original).unwrap();
    let selected = root.join("selected");
    let output = run_library(
        "ask() { printf '%s\\n' actor:existing:7; }\nbootstrap_or_select_actor \"$2\" >\"$3\"",
        &[&root, &selected],
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(fs::read(&users).unwrap(), original);
    assert_eq!(fs::read_to_string(selected).unwrap(), "actor:existing:7\n");
    assert!(String::from_utf8_lossy(&output.stderr).contains("will not be modified"));
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
