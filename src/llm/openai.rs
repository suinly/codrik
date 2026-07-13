mod attachments;

pub use attachments::OpenAiAttachmentContext;

use crate::{
    agent::{
        message::{Message, MessagePart, Role},
        tool::{Tool as AgentTool, ToolParameter, ToolParameterKind, ToolParameters},
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
    types::responses::{
        CreateResponse, CreateResponseArgs, EasyInputContent, EasyInputMessage, FunctionCallOutput,
        FunctionCallOutputItemParam, FunctionTool, FunctionToolCall, InputContent, InputItem,
        InputParam, InputTextContent, Item, OutputItem, OutputMessageContent, Response,
        ResponseStreamEvent, Role as ResponseRole, Status, Tool as ResponseTool,
    },
};
use futures_util::StreamExt;
use serde_json::{Value, json};

pub struct OpenAiClient {
    model: String,
    client: Client<OpenAIConfig>,
    attachment_context: Option<OpenAiAttachmentContext>,
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
            attachment_context: None,
        }
    }

    async fn to_openai_request(
        &self,
        llm_request: LlmRequest,
    ) -> Result<(
        CreateResponse,
        Vec<crate::memory::provider_files::ProviderFileKey>,
    )> {
        let instructions = llm_request
            .messages
            .iter()
            .filter(|message| message.role == Role::System)
            .map(Message::text)
            .filter(|text| !text.is_empty())
            .collect::<Vec<_>>()
            .join("\n");
        let parts = llm_request
            .messages
            .iter()
            .flat_map(|message| message.content.iter())
            .collect::<Vec<_>>();
        let selected_documents = attachments::selected_document_ids(parts.into_iter());
        let mut items = Vec::new();
        let mut reused_keys = Vec::new();
        for message in llm_request.messages {
            let (message_items, message_reused_keys) =
                self.to_response_items(message, &selected_documents).await?;
            items.extend(message_items);
            reused_keys.extend(message_reused_keys);
        }
        let tools = llm_request
            .tools
            .into_iter()
            .map(Self::to_openai_tool)
            .collect::<Vec<_>>();

        let mut request = CreateResponseArgs::default();
        request
            .model(self.model.clone())
            .input(InputParam::Items(items))
            .store(false);
        if !instructions.is_empty() {
            request.instructions(instructions);
        }
        if !tools.is_empty() {
            request.tools(tools);
        }

        Ok((request.build()?, reused_keys))
    }

    async fn to_response_items(
        &self,
        message: Message,
        selected_documents: &std::collections::HashSet<String>,
    ) -> Result<(
        Vec<InputItem>,
        Vec<crate::memory::provider_files::ProviderFileKey>,
    )> {
        let mut items = Vec::new();
        let mut reused_keys = Vec::new();
        let role = message.role.clone();

        match role {
            Role::System => {}
            Role::User | Role::Assistant => {
                let mut content = Vec::new();
                for part in message.content {
                    match part {
                        MessagePart::Text(text) if !text.is_empty() => {
                            content.push(InputContent::InputText(InputTextContent { text }));
                        }
                        MessagePart::Text(_) => {}
                        MessagePart::Attachment(file) => {
                            let resolved =
                                self.resolve_attachment(&file, selected_documents).await?;
                            if resolved.reused_cache
                                && let Some(key) = resolved.cache_key
                            {
                                reused_keys.push(key);
                            }
                            content.push(resolved.content);
                        }
                    }
                }
                if !content.is_empty() {
                    items.push(InputItem::EasyMessage(EasyInputMessage {
                        role: match message.role {
                            Role::User => ResponseRole::User,
                            Role::Assistant => ResponseRole::Assistant,
                            _ => unreachable!(),
                        },
                        content: EasyInputContent::ContentList(content),
                        ..Default::default()
                    }));
                }

                if message.role == Role::Assistant {
                    items.extend(message.tool_calls.into_iter().map(|tool_call| {
                        InputItem::Item(Item::FunctionCall(FunctionToolCall {
                            arguments: tool_call.arguments,
                            call_id: tool_call.id,
                            namespace: None,
                            name: tool_call.name,
                            id: None,
                            status: None,
                        }))
                    }));
                }
            }
            Role::Tool => {
                let content = message.text();
                let call_id = message
                    .tool_call_id
                    .context("tool message is missing tool_call_id")?;
                items.push(InputItem::Item(Item::FunctionCallOutput(
                    FunctionCallOutputItemParam {
                        call_id,
                        output: FunctionCallOutput::Text(content),
                        id: None,
                        status: None,
                    },
                )));
            }
        }

        Ok((items, reused_keys))
    }

    fn to_llm_response(response: Response) -> Result<LlmResponse> {
        if response.status != Status::Completed {
            bail!("OpenAI response ended with status {:?}", response.status);
        }

        let mut content = String::new();
        let mut tool_calls = Vec::new();
        for item in response.output {
            match item {
                OutputItem::Message(message) => {
                    for part in message.content {
                        if let OutputMessageContent::OutputText(text) = part {
                            content.push_str(&text.text);
                        }
                    }
                }
                OutputItem::FunctionCall(function_call) => tool_calls.push(LlmToolCall {
                    id: function_call.call_id,
                    name: function_call.name,
                    arguments: function_call.arguments,
                }),
                _ => {}
            }
        }

        Ok(LlmResponse {
            content,
            tool_calls,
        })
    }

    async fn collect_stream_response(
        &self,
        request: CreateResponse,
        sink: &mut dyn LlmStreamSink,
        context: &RunContext,
    ) -> Result<LlmResponse> {
        let responses = self.client.responses();
        let mut stream = tokio::select! {
            stream = responses.create_stream(request) => stream?,
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

    fn to_openai_tool(tool: AgentTool) -> ResponseTool {
        ResponseTool::Function(FunctionTool {
            name: tool.name,
            description: Some(tool.description),
            parameters: Some(Self::to_openai_parameters(tool.parameters)),
            strict: None,
            defer_loading: None,
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
        let original_request = llm_request.clone();
        let (request, reused_keys) = self.to_openai_request(llm_request).await?;
        let responses = self.client.responses();
        let response = tokio::select! {
            response = responses.create(request) => response,
            _ = context.cancelled() => bail!(RUN_CANCELLED),
        };

        let response = match response {
            Ok(response) => response,
            Err(error)
                if !reused_keys.is_empty() && is_stale_provider_file_error(&error.to_string()) =>
            {
                self.evict_provider_files(&reused_keys).await?;
                let (request, _) = self.to_openai_request(original_request).await?;
                tokio::select! {
                    response = responses.create(request) => response?,
                    _ = context.cancelled() => bail!(RUN_CANCELLED),
                }
            }
            Err(error) => return Err(error.into()),
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
        let original_request = llm_request.clone();
        let (request, reused_keys) = self.to_openai_request(llm_request).await?;

        match self.collect_stream_response(request, sink, context).await {
            Ok(response) => Ok(response),
            Err(error)
                if !reused_keys.is_empty() && is_stale_provider_file_error(&error.to_string()) =>
            {
                self.evict_provider_files(&reused_keys).await?;
                let (request, _) = self.to_openai_request(original_request).await?;
                self.collect_stream_response(request, sink, context).await
            }
            Err(error) => Err(error),
        }
    }
}

fn is_stale_provider_file_error(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("file")
        && (message.contains("not found")
            || message.contains("inaccessible")
            || message.contains("does not exist"))
}

#[derive(Default)]
struct StreamAccumulator {
    completed: Option<Response>,
}

impl StreamAccumulator {
    async fn push(
        &mut self,
        event: ResponseStreamEvent,
        sink: &mut dyn LlmStreamSink,
    ) -> Result<()> {
        match event {
            ResponseStreamEvent::ResponseOutputTextDelta(event) if !event.delta.is_empty() => {
                sink.on_event(LlmStreamEvent::TextDelta(event.delta))
                    .await?;
            }
            ResponseStreamEvent::ResponseOutputItemAdded(event) => {
                if let OutputItem::FunctionCall(function_call) = event.item {
                    sink.on_event(LlmStreamEvent::ToolCallDelta(LlmToolCallDelta {
                        index: event.output_index,
                        id: Some(function_call.call_id),
                        name: Some(function_call.name),
                        arguments: None,
                    }))
                    .await?;
                }
            }
            ResponseStreamEvent::ResponseFunctionCallArgumentsDelta(event)
                if !event.delta.is_empty() =>
            {
                sink.on_event(LlmStreamEvent::ToolCallDelta(LlmToolCallDelta {
                    index: event.output_index,
                    id: None,
                    name: None,
                    arguments: Some(event.delta),
                }))
                .await?;
            }
            ResponseStreamEvent::ResponseCompleted(event) => {
                self.completed = Some(event.response);
            }
            ResponseStreamEvent::ResponseFailed(event) => {
                bail!("OpenAI Responses stream failed: {:?}", event.response.error);
            }
            ResponseStreamEvent::ResponseIncomplete(event) => {
                bail!(
                    "OpenAI Responses stream was incomplete: {:?}",
                    event.response.incomplete_details
                );
            }
            ResponseStreamEvent::ResponseError(event) => {
                bail!("OpenAI Responses stream error: {}", event.message);
            }
            _ => {}
        }

        Ok(())
    }

    fn into_response(self) -> Result<LlmResponse> {
        OpenAiClient::to_llm_response(
            self.completed
                .context("OpenAI Responses stream ended before response.completed")?,
        )
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use async_openai::types::responses::{InputItem, Response, ResponseStreamEvent};
    use serde_json::json;

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
    async fn tool_result_maps_to_function_call_output_by_call_id() -> Result<()> {
        let client = OpenAiClient::new("test", "key", "http://localhost");
        let (items, _) = client
            .to_response_items(Message::tool_result("call_1", "sunny"), &Default::default())
            .await?;

        assert_eq!(items.len(), 1);
        assert_eq!(
            serde_json::to_value(&items[0])?,
            json!({
                "type": "function_call_output",
                "call_id": "call_1",
                "output": "sunny"
            })
        );

        Ok(())
    }

    #[test]
    fn response_function_call_uses_call_id() -> Result<()> {
        let response = response_with_output(json!([{
            "type": "function_call",
            "id": "item_1",
            "call_id": "call_1",
            "name": "weather",
            "arguments": "{}",
            "status": "completed"
        }]))?;

        assert_eq!(
            OpenAiClient::to_llm_response(response)?.tool_calls,
            vec![LlmToolCall {
                id: "call_1".to_string(),
                name: "weather".to_string(),
                arguments: "{}".to_string(),
            }]
        );

        Ok(())
    }

    #[tokio::test]
    async fn request_keeps_system_text_in_instructions_not_input() -> Result<()> {
        let client = OpenAiClient::new("test", "key", "http://localhost");
        let (request, _) = client
            .to_openai_request(LlmRequest {
                messages: vec![Message::system("be concise"), Message::user("hello")],
                tools: Vec::new(),
            })
            .await?;

        assert_eq!(request.instructions.as_deref(), Some("be concise"));
        let InputParam::Items(items) = request.input else {
            panic!("request input should contain typed items");
        };
        assert_eq!(items.len(), 1);
        assert!(matches!(items[0], InputItem::EasyMessage(_)));
        assert_eq!(request.store, Some(false));

        Ok(())
    }

    #[tokio::test]
    async fn stream_accumulator_emits_deltas_and_uses_completed_response() -> Result<()> {
        let mut accumulator = StreamAccumulator::default();
        let mut sink = RecordingSink::default();

        accumulator
            .push(
                stream_event(json!({
                    "type": "response.output_text.delta",
                    "sequence_number": 1,
                    "item_id": "msg_1",
                    "output_index": 0,
                    "content_index": 0,
                    "delta": "hello "
                }))?,
                &mut sink,
            )
            .await?;
        accumulator
            .push(
                stream_event(json!({
                    "type": "response.output_item.added",
                    "sequence_number": 2,
                    "output_index": 1,
                    "item": {
                        "type": "function_call",
                        "id": "item_1",
                        "call_id": "call_1",
                        "name": "weather",
                        "arguments": "",
                        "status": "in_progress"
                    }
                }))?,
                &mut sink,
            )
            .await?;
        accumulator
            .push(
                stream_event(json!({
                    "type": "response.function_call_arguments.delta",
                    "sequence_number": 3,
                    "item_id": "item_1",
                    "output_index": 1,
                    "delta": "{\"city\":\"Paris\"}"
                }))?,
                &mut sink,
            )
            .await?;
        accumulator
            .push(
                stream_event(json!({
                    "type": "response.completed",
                    "sequence_number": 4,
                    "response": {
                        "id": "resp_1",
                        "object": "response",
                        "created_at": 1,
                        "model": "test",
                        "status": "completed",
                        "output": [
                            {
                                "type": "message",
                                "id": "msg_1",
                                "role": "assistant",
                                "status": "completed",
                                "content": [{"type": "output_text", "text": "hello world", "annotations": []}]
                            },
                            {
                                "type": "function_call",
                                "id": "item_1",
                                "call_id": "call_1",
                                "name": "weather",
                                "arguments": "{\"city\":\"Paris\"}",
                                "status": "completed"
                            }
                        ]
                    }
                }))?,
                &mut sink,
            )
            .await?;

        let response = accumulator.into_response()?;

        assert_eq!(response.content, "hello world");
        assert_eq!(
            response.tool_calls,
            vec![LlmToolCall {
                id: "call_1".to_string(),
                name: "weather".to_string(),
                arguments: "{\"city\":\"Paris\"}".to_string(),
            }]
        );
        assert_eq!(
            sink.events,
            vec![
                LlmStreamEvent::TextDelta("hello ".to_string()),
                LlmStreamEvent::ToolCallDelta(LlmToolCallDelta {
                    index: 1,
                    id: Some("call_1".to_string()),
                    name: Some("weather".to_string()),
                    arguments: None,
                }),
                LlmStreamEvent::ToolCallDelta(LlmToolCallDelta {
                    index: 1,
                    id: None,
                    name: None,
                    arguments: Some("{\"city\":\"Paris\"}".to_string()),
                }),
            ]
        );

        Ok(())
    }

    fn response_with_output(output: serde_json::Value) -> Result<Response> {
        Ok(serde_json::from_value(json!({
            "id": "resp_1",
            "object": "response",
            "created_at": 1,
            "model": "test",
            "status": "completed",
            "output": output
        }))?)
    }

    fn stream_event(value: serde_json::Value) -> Result<ResponseStreamEvent> {
        Ok(serde_json::from_value(value)?)
    }
}
