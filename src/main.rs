mod agent;
mod app;
mod config;
mod interfaces;
mod llm;
mod memory;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    interfaces::cli::run().await
}
