use anyhow::Error;
use serde::Serialize;

#[derive(Serialize)]
struct SuccessObservation {
    ok: bool,
    result: String,
}

#[derive(Serialize)]
struct FailureObservation {
    ok: bool,
    error: String,
}

pub(crate) fn success(result: impl Into<String>) -> String {
    serde_json::to_string(&SuccessObservation {
        ok: true,
        result: result.into(),
    })
    .expect("tool success observation should serialize")
}

pub(crate) fn failure(error: &Error) -> String {
    serde_json::to_string(&FailureObservation {
        ok: false,
        error: error.to_string(),
    })
    .expect("tool failure observation should serialize")
}

#[cfg(test)]
mod tests {
    use anyhow::Context;

    use super::{failure, success};

    #[test]
    fn success_serializes_as_json_observation() {
        assert_eq!(
            success("2026-06-26"),
            r#"{"ok":true,"result":"2026-06-26"}"#
        );
    }

    #[test]
    fn failure_serializes_as_json_observation() {
        let error = anyhow::anyhow!("date command failed");

        assert_eq!(
            failure(&error),
            r#"{"ok":false,"error":"date command failed"}"#
        );
    }

    #[test]
    fn failure_uses_top_level_error_message_only() {
        let error = std::fs::read_to_string("/definitely/missing")
            .context("failed to read tool input")
            .expect_err("path should not exist");

        assert_eq!(
            failure(&error),
            r#"{"ok":false,"error":"failed to read tool input"}"#
        );
    }
}
