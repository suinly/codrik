use std::env;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::{agent::message::Message, llm::client::LlmClient};

pub struct OpenAiClient {
    api_key: String,
    base_url: String,
    model: String,
    http: Client,
}

impl OpenAiClient {
    pub fn new() -> Self {
        let api_key = env::var("OPENAI_API_KEY").expect("OPENAI_API_KEY is not set");

        Self {
            api_key,
            base_url: "https://api.openai.com/v1".into(),
            model: "gpt-5.5".into(),
            http: Client::new(),
        }
    }

    pub fn set_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = api_key.into();
        self
    }

    pub fn set_model(mut self, model: impl Into<String>) -> Self {
        self.model = model.into();
        self
    }

    pub fn set_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    async fn request(&self, messages: Vec<Message>) -> Result<String> {
        let url = format!("{}/chat/completions", &self.base_url.trim_end_matches('/'));

        let response = self
            .http
            .post(url)
            .bearer_auth(&self.api_key)
            .json(&OpenAiRequest {
                model: self.model.clone(),
                messages,
                stream: false,
            })
            .send()
            .await?;

        if !response.status().is_success() {
            bail!("request failed!");
        }

        let response = response.json::<ChatCompletionResponse>().await?;

        extract_answer(response)
    }
}

#[async_trait]
impl LlmClient for OpenAiClient {
    async fn generate(&self, messages: Vec<Message>) -> Result<String> {
        let response = self.request(messages).await?;

        Ok(response)
    }
}

#[derive(Debug, Serialize)]
struct OpenAiRequest {
    model: String,
    messages: Vec<Message>,
    stream: bool,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionResponse {
    choices: Vec<ChatChoice>,
}

#[derive(Debug, Deserialize)]
struct ChatChoice {
    message: ChatMessage,
}

#[derive(Debug, Deserialize)]
struct ChatMessage {
    content: String,
}

fn extract_answer(response: ChatCompletionResponse) -> Result<String> {
    response
        .choices
        .into_iter()
        .next()
        .context("chat completion response has no choices")
        .map(|choice| choice.message.content)
}

#[cfg(test)]
mod tests {
    use crate::{agent::message::Message, llm::openai::OpenAiRequest};

    use super::{ChatCompletionResponse, extract_answer};
    use anyhow::Result;

    #[test]
    fn extracts_answer_from_chat_completion_response() -> Result<()> {
        let response: ChatCompletionResponse = serde_json::from_str(
            r#"
                {
                    "choices": [
                        {
                            "message": {
                                "content": "Hi!"
                            }
                        }
                    ]
                }
            "#,
        )?;

        let answer = extract_answer(response)?;

        assert_eq!(answer, "Hi!");

        Ok(())
    }

    #[test]
    fn returns_error_when_response_has_no_choices() {
        let response = ChatCompletionResponse { choices: vec![] };

        let result = extract_answer(response);

        assert!(result.is_err());
    }

    #[test]
    fn serializes_chat_completion_request() -> Result<()> {
        let request = OpenAiRequest {
            model: "test-model".into(),
            messages: vec![Message::user("Hi!")],
            stream: false,
        };

        let value = serde_json::to_value(request)?;

        assert_eq!(value["model"], "test-model");
        assert_eq!(value["stream"], false);
        assert_eq!(value["messages"][0]["role"], "user");
        assert_eq!(value["messages"][0]["content"], "Hi!");

        Ok(())
    }
}
