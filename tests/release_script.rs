#[test]
fn release_script_raises_file_descriptor_limit_before_builds() {
    let script = include_str!("../scripts/release.sh");
    let raise_limit = script
        .find("raise_file_descriptor_limit")
        .expect("release script should raise the file descriptor limit");
    let first_build = script
        .find("cargo build --release")
        .expect("release script should build release artifacts");

    assert!(
        raise_limit < first_build,
        "release script should raise the file descriptor limit before build commands"
    );
}
