use crate::{
    agent::{
        message::Role,
        tool::{Tool, ToolParameter, ToolParameterKind, ToolParameters},
    },
    llm::client::{LlmClient, LlmRequest, LlmResponse, LlmToolCall},
};
use anyhow::{Context, Result};
use async_openai::{
    Client,
    config::OpenAIConfig,
    types::chat::{
        ChatCompletionMessageToolCall, ChatCompletionMessageToolCalls,
        ChatCompletionRequestAssistantMessage, ChatCompletionRequestAssistantMessageContent,
        ChatCompletionRequestMessage, ChatCompletionRequestSystemMessage,
        ChatCompletionRequestToolMessage, ChatCompletionRequestUserMessage, ChatCompletionTool,
        ChatCompletionTools, CreateChatCompletionRequest, CreateChatCompletionRequestArgs,
        CreateChatCompletionResponse, FunctionCall, FunctionObject,
    },
};
use serde_json::{Value, json};

pub struct OpenAiClient {
    model: String,
    client: Client<OpenAIConfig>,
}

impl OpenAiClient {
    pub fn new(
        model: impl Into<String>,
        api_key: impl Into<String>,
        base_url: impl Into<String>,
    ) -> Self {
        let config = OpenAIConfig::new()
            .with_api_key(api_key.into())
            .with_api_base(base_url.into());

        let client = Client::with_config(config);

        Self {
            model: model.into(),
            client,
        }
    }

    fn to_openai_request(&self, llm_request: LlmRequest) -> Result<CreateChatCompletionRequest> {
        let messages = llm_request
            .messages
            .into_iter()
            .map(|message| -> Result<ChatCompletionRequestMessage> {
                Ok(match message.role {
                    Role::User => ChatCompletionRequestUserMessage::from(message.content).into(),
                    Role::Assistant => ChatCompletionRequestAssistantMessage {
                        content: if message.content.is_empty() && !message.tool_calls.is_empty() {
                            None
                        } else {
                            Some(ChatCompletionRequestAssistantMessageContent::Text(
                                message.content,
                            ))
                        },
                        tool_calls: if message.tool_calls.is_empty() {
                            None
                        } else {
                            Some(
                                message
                                    .tool_calls
                                    .into_iter()
                                    .map(Self::to_openai_tool_call)
                                    .collect(),
                            )
                        },
                        ..Default::default()
                    }
                    .into(),
                    Role::System => {
                        ChatCompletionRequestSystemMessage::from(message.content).into()
                    }
                    Role::Tool => {
                        let tool_call_id = message
                            .tool_call_id
                            .context("tool message is missing tool_call_id")?;

                        ChatCompletionRequestToolMessage {
                            content: message.content.into(),
                            tool_call_id,
                        }
                        .into()
                    }
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let tools = llm_request
            .tools
            .into_iter()
            .map(Self::to_openai_tool)
            .collect::<Vec<_>>();

        let mut request = CreateChatCompletionRequestArgs::default();
        request.model(self.model.clone()).messages(messages);

        if !tools.is_empty() {
            request.tools(tools);
        }

        let request = request.build()?;

        Ok(request)
    }

    fn to_llm_response(response: CreateChatCompletionResponse) -> Result<LlmResponse> {
        let choice = response
            .choices
            .into_iter()
            .next()
            .context("chat completion response has no choices")?;

        let content = choice.message.content.unwrap_or_default();

        let tool_calls = choice
            .message
            .tool_calls
            .unwrap_or_default()
            .into_iter()
            .filter_map(|tool_call| match tool_call {
                ChatCompletionMessageToolCalls::Function(function_call) => Some(LlmToolCall {
                    id: function_call.id,
                    name: function_call.function.name,
                    arguments: function_call.function.arguments,
                }),
                ChatCompletionMessageToolCalls::Custom(_) => None,
            })
            .collect();

        Ok(LlmResponse {
            content,
            tool_calls,
        })
    }

    fn to_openai_tool(tool: Tool) -> ChatCompletionTools {
        ChatCompletionTools::Function(ChatCompletionTool {
            function: FunctionObject {
                name: tool.name,
                description: Some(tool.description),
                parameters: Some(Self::to_openai_parameters(tool.parameters)),
                strict: None,
            },
        })
    }

    fn to_openai_tool_call(tool_call: LlmToolCall) -> ChatCompletionMessageToolCalls {
        ChatCompletionMessageToolCalls::Function(ChatCompletionMessageToolCall {
            id: tool_call.id,
            function: FunctionCall {
                name: tool_call.name,
                arguments: tool_call.arguments,
            },
        })
    }

    fn to_openai_parameters(parameters: ToolParameters) -> Value {
        let properties = parameters
            .properties
            .into_iter()
            .map(|(name, parameter)| (name, Self::to_openai_parameter(parameter)))
            .collect::<serde_json::Map<_, _>>();

        json!({
            "type": "object",
            "properties": properties,
            "required": parameters.required,
        })
    }

    fn to_openai_parameter(parameter: ToolParameter) -> Value {
        let kind = match parameter.kind {
            ToolParameterKind::String => "string",
            ToolParameterKind::Number => "number",
            ToolParameterKind::Boolean => "boolean",
        };

        let mut value = json!({
            "type": kind,
            "description": parameter.description,
        });

        if !parameter.allowed_values.is_empty() {
            value["enum"] = json!(parameter.allowed_values);
        }

        value
    }
}

#[async_trait::async_trait]
impl LlmClient for OpenAiClient {
    async fn generate(&self, llm_request: LlmRequest) -> Result<LlmResponse> {
        let request = self.to_openai_request(llm_request)?;
        let response = self.client.chat().create(request).await?;

        Self::to_llm_response(response)
    }
}
