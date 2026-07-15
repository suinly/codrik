#[tokio::main]
async fn main() -> anyhow::Result<()> {
    codrik::interfaces::cli::run().await
}
