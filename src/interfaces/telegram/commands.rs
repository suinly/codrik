use teloxide::types::ChatId;

use crate::{
    app::{self, SessionDeletionOutcome},
    config::AppConfig,
    memory::telegram_sessions::{TelegramSession, TelegramSessionStore},
};

pub(super) async fn answer_session_command(
    session_store: &TelegramSessionStore,
    chat_id: ChatId,
    text: &str,
    bot_username: Option<&str>,
    config: &AppConfig,
) -> Option<String> {
    let command = TelegramCommand::parse(text, bot_username)?;
    let result = match command {
        TelegramCommand::New => session_store
            .create_session(chat_id.0)
            .await
            .map(|session_id| format!("Создал новую сессию: {session_id}")),
        TelegramCommand::Sessions => session_store
            .list_sessions(chat_id.0)
            .await
            .map(format_sessions),
        TelegramCommand::SwitchSession(session_id) => {
            match session_store.switch_session(chat_id.0, &session_id).await {
                Ok(true) => Ok(format!("Переключился на сессию: {session_id}")),
                Ok(false) => Ok(format!("Сессия не найдена: {session_id}")),
                Err(error) => Err(error),
            }
        }
        TelegramCommand::DeleteSession(session_id) => {
            app::delete_inactive_session(config.clone(), session_store, chat_id.0, &session_id)
                .await
                .map(|outcome| match outcome {
                    SessionDeletionOutcome::NotFound => format!("Сессия не найдена: {session_id}"),
                    SessionDeletionOutcome::Active => {
                        "Нельзя удалить активную сессию. Сначала переключись на другую.".to_string()
                    }
                    SessionDeletionOutcome::Deleted { failed_remote_deletions: 0 } => {
                        format!("Удалил сессию: {session_id}")
                    }
                    SessionDeletionOutcome::Deleted { failed_remote_deletions } => format!(
                        "Удалил сессию локально; не удалось удалить provider-файлы: {failed_remote_deletions}"
                    ),
                })
        }
        TelegramCommand::Start | TelegramCommand::Stop => return None,
    };

    Some(match result {
        Ok(answer) => answer,
        Err(error) => {
            eprintln!("Telegram session command failed for chat {chat_id}: {error:#}");
            format!("Gateway error: {error:#}")
        }
    })
}

pub(super) fn is_start_command(text: &str, bot_username: Option<&str>) -> bool {
    matches!(
        TelegramCommand::parse(text, bot_username),
        Some(TelegramCommand::Start)
    )
}

pub(super) fn is_stop_command(text: &str, bot_username: Option<&str>) -> bool {
    matches!(
        TelegramCommand::parse(text, bot_username),
        Some(TelegramCommand::Stop)
    )
}

pub(super) fn is_command_addressed_to_other_bot(text: &str, bot_username: Option<&str>) -> bool {
    let Some(command) = text.split_whitespace().next() else {
        return false;
    };
    let Some((name, addressed_bot)) = command.split_once('@') else {
        return false;
    };

    name.starts_with('/')
        && bot_username.is_none_or(|bot_username| !addressed_bot.eq_ignore_ascii_case(bot_username))
}

fn format_sessions(sessions: Vec<TelegramSession>) -> String {
    let mut lines = vec!["Сессии:".to_string()];

    for session in sessions.iter().rev().take(10) {
        let marker = if session.is_active { "*" } else { " " };
        lines.push(format!("{marker} {}", session.id));
    }

    lines.push("Чтобы переключиться: /sessions <id>".to_string());
    lines.join("\n")
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TelegramCommand {
    Start,
    Stop,
    New,
    Sessions,
    SwitchSession(String),
    DeleteSession(String),
}

impl TelegramCommand {
    fn parse(text: &str, bot_username: Option<&str>) -> Option<Self> {
        let mut parts = text.split_whitespace();
        let command = normalize_command(parts.next()?, bot_username)?;

        match command.as_str() {
            "/start" => Some(Self::Start),
            "/stop" => Some(Self::Stop),
            "/new" => Some(Self::New),
            "/sessions" => match parts.next() {
                Some("delete") => parts.next().map(|id| Self::DeleteSession(id.to_string())),
                Some(session_id) => Some(Self::SwitchSession(session_id.to_string())),
                None => Some(Self::Sessions),
            },
            _ => None,
        }
    }
}

fn normalize_command(command: &str, bot_username: Option<&str>) -> Option<String> {
    let Some((name, addressed_bot)) = command.split_once('@') else {
        return Some(command.to_string());
    };

    let bot_username = bot_username?;
    if addressed_bot.eq_ignore_ascii_case(bot_username) {
        Some(name.to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::memory::telegram_sessions::TelegramSession;

    use super::{
        TelegramCommand, format_sessions, is_command_addressed_to_other_bot, is_start_command,
        is_stop_command,
    };

    #[test]
    fn recognizes_plain_and_addressed_start_commands() {
        assert!(is_start_command("/start", Some("CodrikBot")));
        assert!(is_start_command("/start extra", Some("CodrikBot")));
        assert!(is_start_command("/start@CodrikBot", Some("CodrikBot")));
        assert!(is_start_command("/start@codrikbot", Some("CodrikBot")));
        assert!(!is_start_command("/start@OtherBot", Some("CodrikBot")));
        assert!(!is_start_command("/restart", Some("CodrikBot")));
    }

    #[test]
    fn recognizes_plain_and_addressed_stop_commands() {
        assert!(is_stop_command("/stop", Some("CodrikBot")));
        assert!(is_stop_command("/stop@CodrikBot", Some("CodrikBot")));
        assert!(!is_stop_command("/stop@OtherBot", Some("CodrikBot")));
        assert!(!is_stop_command("/stopping", Some("CodrikBot")));
    }

    #[test]
    fn parses_session_commands() {
        assert_eq!(
            TelegramCommand::parse("/new", Some("CodrikBot")),
            Some(TelegramCommand::New)
        );
        assert_eq!(
            TelegramCommand::parse("/new@CodrikBot", Some("CodrikBot")),
            Some(TelegramCommand::New)
        );
        assert_eq!(
            TelegramCommand::parse("/sessions", Some("CodrikBot")),
            Some(TelegramCommand::Sessions)
        );
        assert_eq!(
            TelegramCommand::parse("/sessions telegram-chat-123", Some("CodrikBot")),
            Some(TelegramCommand::SwitchSession(
                "telegram-chat-123".to_string()
            ))
        );
        assert_eq!(
            TelegramCommand::parse("/sessions delete old", Some("CodrikBot")),
            Some(TelegramCommand::DeleteSession("old".to_string()))
        );
        assert_eq!(
            TelegramCommand::parse("/newsletter", Some("CodrikBot")),
            None
        );
    }

    #[test]
    fn ignores_session_commands_addressed_to_other_bots() {
        assert_eq!(
            TelegramCommand::parse("/new@OtherBot", Some("CodrikBot")),
            None
        );
        assert_eq!(
            TelegramCommand::parse("/sessions@OtherBot", Some("CodrikBot")),
            None
        );
    }

    #[test]
    fn detects_commands_addressed_to_other_bots() {
        assert!(is_command_addressed_to_other_bot(
            "/new@OtherBot",
            Some("CodrikBot")
        ));
        assert!(!is_command_addressed_to_other_bot(
            "/new@CodrikBot",
            Some("CodrikBot")
        ));
        assert!(!is_command_addressed_to_other_bot(
            "/new",
            Some("CodrikBot")
        ));
    }

    #[test]
    fn formats_sessions_with_active_marker_and_switch_hint() {
        let message = format_sessions(vec![
            TelegramSession {
                id: "telegram-chat-123".to_string(),
                is_active: false,
                created_at: 1,
                last_used_at: 1,
            },
            TelegramSession {
                id: "telegram-chat-123-2".to_string(),
                is_active: true,
                created_at: 2,
                last_used_at: 2,
            },
        ]);

        assert!(message.contains("Сессии:"));
        assert!(message.contains("* telegram-chat-123-2"));
        assert!(message.contains("  telegram-chat-123"));
        assert!(message.contains("/sessions <id>"));
    }
}
