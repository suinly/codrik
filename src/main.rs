mod agent;
mod app;
mod auth;
mod config;
mod interfaces;
mod llm;
mod memory;
mod skills;
mod tools;
mod updater;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    interfaces::cli::run().await
}
