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

    async fn make_request(&self, request: ChatCompletionRequest) -> Result<ChatCompletionResponse> {
        let url = format!("{}/chat/completions", &self.base_url.trim_end_matches('/'));

        let response = self
            .http
            .post(url)
            .bearer_auth(&self.api_key)
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            bail!("request failed!");
        }

        Ok(response.json::<ChatCompletionResponse>().await?)
    }

    fn to_chat_completion_request(&self, request: LlmRequest) -> ChatCompletionRequest {
        ChatCompletionRequest {
            model: self.model.clone(),
            messages: request
                .messages
                .into_iter()
                .map(ChatCompletionMessage::from)
                .collect(),
            tools: request
                .tools
                .into_iter()
                .map(ChatCompletionTool::from)
                .collect(),
            stream: false,
        }
    }

    fn to_llm_response(&self, response: ChatCompletionResponse) -> Result<LlmResponse> {
        response
            .choices
            .into_iter()
            .next()
            .context("chat completion response has no choices")
            .map(|choice| LlmResponse {
                content: choice.message.content,
            })
    }
}

#[async_trait]
impl LlmClient for OpenAiClient {
    async fn generate(&self, request: LlmRequest) -> Result<LlmResponse> {
        let chat_completion_request = self.to_chat_completion_request(request);

        let chat_completion_response = self.make_request(chat_completion_request).await?;

        let response = self.to_llm_response(chat_completion_response)?;

        Ok(response)
    }
}

#[derive(Debug, Serialize)]
struct ChatCompletionRequest {
    model: String,
    messages: Vec<ChatCompletionMessage>,
    tools: Vec<ChatCompletionTool>,
    stream: bool,
}

#[derive(Debug, Serialize)]
struct ChatCompletionTool {
    #[serde(rename = "type")]
    kind: String,
    function: FunctionDefinition,
}

impl From<Tool> for ChatCompletionTool {
    fn from(tool: Tool) -> Self {
        Self {
            kind: "function".into(),
            function: FunctionDefinition {
                name: tool.name,
                description: tool.description,
                parameters: FunctionParameters {
                    kind: "object".into(),
                    properties: tool
                        .parameters
                        .properties
                        .into_iter()
                        .map(|(name, parameter)| (name, FunctionProperty::from(parameter)))
                        .collect(),
                    required: tool.parameters.required,
                },
            },
        }
    }
}

#[derive(Debug, Serialize)]
struct FunctionDefinition {
    name: String,
    description: String,
    parameters: FunctionParameters,
}

#[derive(Debug, Serialize)]
struct FunctionParameters {
    #[serde(rename = "type")]
    kind: String,
    properties: BTreeMap<String, FunctionProperty>,
    required: Vec<String>,
}

#[derive(Debug, Serialize)]
struct FunctionProperty {
    #[serde(rename = "type")]
    kind: FunctionPropertyKind,
    description: String,

    #[serde(rename = "enum", skip_serializing_if = "Vec::is_empty")]
    allowed_values: Vec<String>,
}

impl From<ToolParameter> for FunctionProperty {
    fn from(parameter: ToolParameter) -> Self {
        Self {
            kind: FunctionPropertyKind::from(parameter.kind),
            description: parameter.description,
            allowed_values: parameter.allowed_values,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum FunctionPropertyKind {
    String,
    Number,
    Boolean,
}

impl From<ToolParameterKind> for FunctionPropertyKind {
    fn from(kind: ToolParameterKind) -> Self {
        match kind {
            ToolParameterKind::String => FunctionPropertyKind::String,
            ToolParameterKind::Number => FunctionPropertyKind::Number,
            ToolParameterKind::Boolean => FunctionPropertyKind::Boolean,
        }
    }
}

#[derive(Debug, Deserialize)]
struct ChatCompletionResponse {
    choices: Vec<ChatCompletionChoice>,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionChoice {
    message: ChatCompletionMessage,
}

#[derive(Debug, Serialize, Deserialize)]
struct ChatCompletionMessage {
    content: String,
    role: ChatCompoletionRole,
}

impl From<Message> for ChatCompletionMessage {
    fn from(message: Message) -> Self {
        Self {
            content: message.content,
            role: match message.role {
                Role::User => ChatCompoletionRole::User,
                Role::Assistant => ChatCompoletionRole::Assistant,
                Role::System => ChatCompoletionRole::System,
            },
        }
    }
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum ChatCompoletionRole {
    User,
    Assistant,
    System,
}
