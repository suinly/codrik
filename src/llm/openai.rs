use crate::{
    agent::{
        message::Role,
        tool::{Tool, ToolParameter, ToolParameterKind, ToolParameters},
    },
    llm::client::{
        LlmClient, LlmRequest, LlmResponse, LlmStreamClient, LlmStreamEvent, LlmStreamSink,
        LlmToolCall, LlmToolCallDelta, RUN_CANCELLED, RunContext,
    },
};
use anyhow::{Context, Result, bail};
use async_openai::{
    Client,
    config::OpenAIConfig,
    types::chat::{
        ChatCompletionMessageToolCall, ChatCompletionMessageToolCalls,
        ChatCompletionRequestAssistantMessage, ChatCompletionRequestAssistantMessageContent,
        ChatCompletionRequestMessage, ChatCompletionRequestSystemMessage,
        ChatCompletionRequestToolMessage, ChatCompletionRequestUserMessage, ChatCompletionTool,
        ChatCompletionTools, CreateChatCompletionRequest, CreateChatCompletionRequestArgs,
        CreateChatCompletionResponse, CreateChatCompletionStreamResponse, FunctionCall,
        FunctionCallStream, FunctionObject,
    },
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::collections::BTreeMap;

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

    async fn collect_stream_response(
        &self,
        mut request: CreateChatCompletionRequest,
        sink: &mut dyn LlmStreamSink,
        context: &RunContext,
    ) -> Result<LlmResponse> {
        request.stream = Some(true);

        let chat = self.client.chat();
        let mut stream = tokio::select! {
            stream = chat.create_stream(request) => stream?,
            _ = context.cancelled() => bail!(RUN_CANCELLED),
        };
        let mut accumulator = StreamAccumulator::default();

        loop {
            let Some(chunk) = (tokio::select! {
                chunk = stream.next() => chunk,
                _ = context.cancelled() => bail!(RUN_CANCELLED),
            }) else {
                break;
            };

            accumulator.push(chunk?, sink).await?;
        }

        accumulator.into_response()
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
    async fn generate(&self, llm_request: LlmRequest, context: &RunContext) -> Result<LlmResponse> {
        let mut request = self.to_openai_request(llm_request)?;
        request.stream = Some(false);
        let chat = self.client.chat();
        let response = tokio::select! {
            response = chat.create(request) => response?,
            _ = context.cancelled() => bail!(RUN_CANCELLED),
        };

        Self::to_llm_response(response)
    }
}

#[async_trait::async_trait]
impl LlmStreamClient for OpenAiClient {
    async fn stream(
        &self,
        llm_request: LlmRequest,
        sink: &mut dyn LlmStreamSink,
        context: &RunContext,
    ) -> Result<LlmResponse> {
        let request = self.to_openai_request(llm_request)?;

        self.collect_stream_response(request, sink, context).await
    }
}

#[derive(Default)]
struct StreamAccumulator {
    content: String,
    tool_calls: BTreeMap<u32, PartialToolCall>,
}

#[derive(Default)]
struct PartialToolCall {
    id: Option<String>,
    name: Option<String>,
    arguments: String,
}

impl StreamAccumulator {
    async fn push(
        &mut self,
        chunk: CreateChatCompletionStreamResponse,
        sink: &mut dyn LlmStreamSink,
    ) -> Result<()> {
        for choice in chunk.choices {
            if let Some(content) = choice.delta.content
                && !content.is_empty()
            {
                self.content.push_str(&content);
                sink.on_event(LlmStreamEvent::TextDelta(content)).await?;
            }

            for tool_call in choice.delta.tool_calls.unwrap_or_default() {
                let entry = self.tool_calls.entry(tool_call.index).or_default();

                if let Some(id) = tool_call.id {
                    sink.on_event(LlmStreamEvent::ToolCallDelta(
                        entry.record_id(tool_call.index, id),
                    ))
                    .await?;
                }

                if let Some(function) = tool_call.function
                    && let Some(delta) = entry.record_function_delta(tool_call.index, function)
                {
                    sink.on_event(LlmStreamEvent::ToolCallDelta(delta)).await?;
                }
            }
        }

        Ok(())
    }

    fn into_response(self) -> Result<LlmResponse> {
        let tool_calls = self
            .tool_calls
            .into_iter()
            .map(|(index, tool_call)| -> Result<LlmToolCall> {
                Ok(LlmToolCall {
                    id: tool_call
                        .id
                        .with_context(|| format!("streamed tool call {index} is missing id"))?,
                    name: tool_call
                        .name
                        .with_context(|| format!("streamed tool call {index} is missing name"))?,
                    arguments: tool_call.arguments,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(LlmResponse {
            content: self.content,
            tool_calls,
        })
    }
}

impl PartialToolCall {
    fn record_id(&mut self, index: u32, id: String) -> LlmToolCallDelta {
        self.id = Some(id.clone());

        LlmToolCallDelta {
            index,
            id: Some(id),
            name: None,
            arguments: None,
        }
    }

    fn record_function_delta(
        &mut self,
        index: u32,
        function: FunctionCallStream,
    ) -> Option<LlmToolCallDelta> {
        let mut name_delta = None;
        if let Some(name) = function.name {
            self.name = Some(match self.name.take() {
                Some(existing) => existing + &name,
                None => name.clone(),
            });
            name_delta = Some(name);
        }

        let mut arguments_delta = None;
        if let Some(arguments) = function.arguments {
            self.arguments.push_str(&arguments);
            arguments_delta = Some(arguments);
        }

        if name_delta.is_none() && arguments_delta.is_none() {
            return None;
        }

        Some(LlmToolCallDelta {
            index,
            id: None,
            name: name_delta,
            arguments: arguments_delta,
        })
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use async_openai::types::chat::{
        ChatChoiceStream, ChatCompletionMessageToolCallChunk, ChatCompletionStreamResponseDelta,
        FunctionCallStream, FunctionType,
    };

    use super::*;

    #[derive(Default)]
    struct RecordingSink {
        events: Vec<LlmStreamEvent>,
    }

    #[async_trait::async_trait]
    impl LlmStreamSink for RecordingSink {
        async fn on_event(&mut self, event: LlmStreamEvent) -> Result<()> {
            self.events.push(event);

            Ok(())
        }
    }

    #[tokio::test]
    async fn stream_accumulator_collects_text_deltas() -> Result<()> {
        let mut accumulator = StreamAccumulator::default();
        let mut sink = RecordingSink::default();

        accumulator
            .push(stream_chunk(delta_with_content("hello "), None), &mut sink)
            .await?;
        accumulator
            .push(stream_chunk(delta_with_content("world"), None), &mut sink)
            .await?;

        let response = accumulator.into_response()?;

        assert_eq!(response.content, "hello world");
        assert!(response.tool_calls.is_empty());
        assert_eq!(
            sink.events,
            vec![
                LlmStreamEvent::TextDelta("hello ".to_string()),
                LlmStreamEvent::TextDelta("world".to_string()),
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn stream_accumulator_collects_chunked_tool_calls_by_index() -> Result<()> {
        let mut accumulator = StreamAccumulator::default();
        let mut sink = RecordingSink::default();

        accumulator
            .push(
                stream_chunk(
                    empty_delta(),
                    Some(vec![tool_call_delta(
                        0,
                        Some("call_1"),
                        Some("demo"),
                        Some("{\"city\""),
                    )]),
                ),
                &mut sink,
            )
            .await?;
        accumulator
            .push(
                stream_chunk(
                    empty_delta(),
                    Some(vec![tool_call_delta(0, None, None, Some(":\"Paris\"}"))]),
                ),
                &mut sink,
            )
            .await?;

        let response = accumulator.into_response()?;

        assert_eq!(
            response.tool_calls,
            vec![LlmToolCall {
                id: "call_1".to_string(),
                name: "demo".to_string(),
                arguments: "{\"city\":\"Paris\"}".to_string(),
            }]
        );
        assert_eq!(
            sink.events,
            vec![
                LlmStreamEvent::ToolCallDelta(LlmToolCallDelta {
                    index: 0,
                    id: Some("call_1".to_string()),
                    name: None,
                    arguments: None,
                }),
                LlmStreamEvent::ToolCallDelta(LlmToolCallDelta {
                    index: 0,
                    id: None,
                    name: Some("demo".to_string()),
                    arguments: Some("{\"city\"".to_string()),
                }),
                LlmStreamEvent::ToolCallDelta(LlmToolCallDelta {
                    index: 0,
                    id: None,
                    name: None,
                    arguments: Some(":\"Paris\"}".to_string()),
                }),
            ]
        );

        Ok(())
    }

    fn stream_chunk(
        delta: ChatCompletionStreamResponseDelta,
        tool_calls: Option<Vec<ChatCompletionMessageToolCallChunk>>,
    ) -> CreateChatCompletionStreamResponse {
        let mut delta = delta;
        delta.tool_calls = tool_calls;

        #[allow(deprecated)]
        CreateChatCompletionStreamResponse {
            id: "chatcmpl_test".to_string(),
            choices: vec![ChatChoiceStream {
                index: 0,
                delta,
                finish_reason: None,
                logprobs: None,
            }],
            created: 0,
            model: "test-model".to_string(),
            service_tier: None,
            system_fingerprint: None,
            object: "chat.completion.chunk".to_string(),
            usage: None,
        }
    }

    fn delta_with_content(content: impl Into<String>) -> ChatCompletionStreamResponseDelta {
        #[allow(deprecated)]
        ChatCompletionStreamResponseDelta {
            content: Some(content.into()),
            function_call: None,
            tool_calls: None,
            role: None,
            refusal: None,
        }
    }

    fn empty_delta() -> ChatCompletionStreamResponseDelta {
        #[allow(deprecated)]
        ChatCompletionStreamResponseDelta {
            content: None,
            function_call: None,
            tool_calls: None,
            role: None,
            refusal: None,
        }
    }

    fn tool_call_delta(
        index: u32,
        id: Option<&str>,
        name: Option<&str>,
        arguments: Option<&str>,
    ) -> ChatCompletionMessageToolCallChunk {
        ChatCompletionMessageToolCallChunk {
            index,
            id: id.map(ToString::to_string),
            r#type: Some(FunctionType::Function),
            function: Some(FunctionCallStream {
                name: name.map(ToString::to_string),
                arguments: arguments.map(ToString::to_string),
            }),
        }
    }
}
