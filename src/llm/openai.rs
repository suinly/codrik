use std::{collections::BTreeMap, env};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::{
    agent::{
        message::{Message, Role},
        tool::{Tool, ToolParameter, ToolParameterKind},
    },
    llm::client::{LlmClient, LlmRequest, LlmResponse},
};

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

    async fn request(&self, request: LlmRequest) -> Result<LlmResponse> {
        let url = format!("{}/chat/completions", &self.base_url.trim_end_matches('/'));

        let openai_request = self.to_openai_request(request);

        let response = self
            .http
            .post(url)
            .bearer_auth(&self.api_key)
            .json(&openai_request)
            .send()
            .await?;

        if !response.status().is_success() {
            bail!("request failed!");
        }

        let response = response.json::<ChatCompletionResponse>().await?;

        extract_answer(response)
    }

    fn to_openai_request(&self, request: LlmRequest) -> OpenAiRequest {
        OpenAiRequest {
            model: self.model.clone(),
            messages: request
                .messages
                .into_iter()
                .map(OpenAiMessage::from)
                .collect(),
            tools: request.tools.into_iter().map(OpenAiTool::from).collect(),
            stream: false,
        }
    }
}

#[async_trait]
impl LlmClient for OpenAiClient {
    async fn generate(&self, request: LlmRequest) -> Result<LlmResponse> {
        let response = self.request(request).await?;

        Ok(response)
    }
}

#[derive(Debug, Serialize)]
struct OpenAiRequest {
    model: String,
    messages: Vec<OpenAiMessage>,
    tools: Vec<OpenAiTool>,
    stream: bool,
}

#[derive(Debug, Serialize)]
struct OpenAiMessage {
    content: String,
    role: OpenAiRole,
}

impl From<Message> for OpenAiMessage {
    fn from(message: Message) -> Self {
        Self {
            content: message.content,
            role: match message.role {
                Role::User => OpenAiRole::User,
                Role::Assistant => OpenAiRole::Assistant,
                Role::System => OpenAiRole::System,
            },
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "lowercase")]
enum OpenAiRole {
    User,
    Assistant,
    System,
}

#[derive(Debug, Serialize)]
struct OpenAiTool {
    #[serde(rename = "type")]
    kind: String,
    function: OpenAiFunction,
}

impl From<Tool> for OpenAiTool {
    fn from(tool: Tool) -> Self {
        Self {
            kind: "function".into(),
            function: OpenAiFunction {
                name: tool.name,
                description: tool.description,
                parameters: OpenAiParameters {
                    kind: "object".into(),
                    properties: tool
                        .parameters
                        .properties
                        .into_iter()
                        .map(|(name, parameter)| (name, OpenAiProperty::from(parameter)))
                        .collect(),
                    required: tool.parameters.required,
                },
            },
        }
    }
}

#[derive(Debug, Serialize)]
struct OpenAiFunction {
    name: String,
    description: String,
    parameters: OpenAiParameters,
}

#[derive(Debug, Serialize)]
struct OpenAiParameters {
    #[serde(rename = "type")]
    kind: String,
    properties: BTreeMap<String, OpenAiProperty>,
    required: Vec<String>,
}

#[derive(Debug, Serialize)]
struct OpenAiProperty {
    #[serde(rename = "type")]
    kind: OpenAiPropertyKind,
    description: String,

    #[serde(rename = "enum", skip_serializing_if = "Vec::is_empty")]
    allowed_values: Vec<String>,
}

impl From<ToolParameter> for OpenAiProperty {
    fn from(parameter: ToolParameter) -> Self {
        Self {
            kind: OpenAiPropertyKind::from(parameter.kind),
            description: parameter.description,
            allowed_values: parameter.allowed_values,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum OpenAiPropertyKind {
    String,
    Number,
    Boolean,
}

impl From<ToolParameterKind> for OpenAiPropertyKind {
    fn from(kind: ToolParameterKind) -> Self {
        match kind {
            ToolParameterKind::String => OpenAiPropertyKind::String,
            ToolParameterKind::Number => OpenAiPropertyKind::Number,
            ToolParameterKind::Boolean => OpenAiPropertyKind::Boolean,
        }
    }
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

fn extract_answer(response: ChatCompletionResponse) -> Result<LlmResponse> {
    response
        .choices
        .into_iter()
        .next()
        .context("chat completion response has no choices")
        .map(|choice| LlmResponse {
            content: choice.message.content,
        })
}
