use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct Tool {
    name: String,
    description: String,
    parameters: String,
}
