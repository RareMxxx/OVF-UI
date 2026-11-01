#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!(
        "Broadcom OVF Tool: {}",
        ovf_dock::updates::check_latest().await?
    );
    Ok(())
}
