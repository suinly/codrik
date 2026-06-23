mod agent;
mod app;
mod config;
mod interfaces;
mod llm;
mod memory;
mod tools;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    interfaces::cli::run().await
}
