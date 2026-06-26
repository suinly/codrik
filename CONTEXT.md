# codrik-rs Context

codrik-rs is a small agent runtime that runs user turns through an LLM, optional tools, and session memory.

## Language

**Agent turn**:
A single user request handled by the agent runtime from user message through final answer or terminal failure.
_Avoid_: request handler, execution cycle

**Tool observation**:
The model-facing JSON envelope that records a tool call result. Success and failure observations share one shape so the model can recover from retryable tool failures.
_Avoid_: raw tool output, tool response string
