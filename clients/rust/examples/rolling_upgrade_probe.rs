//! Persistent SDK client driven by scripts/rolling_upgrade_e2e.py.
use std::io::{BufRead, Write};
use talon_rust_client::{parse_uri, ClientBuilder, ObjectStat};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::args()
        .nth(1)
        .ok_or("coordinator address required")?;
    let client = ClientBuilder::default()
        .with_coordinator(address.clone())
        .with_block_size(65536)
        .build()?;
    let object = parse_uri("az://container/bench")?;
    let stat = ObjectStat {
        size: 1 << 20,
        version: "v1".into(),
    };
    for line in std::io::stdin().lock().lines() {
        match line?.as_str() {
            "read" => match client.read(&object, 0, Some(8192), Some(&stat)).await {
                Ok(bytes) => {
                    assert_eq!(
                        bytes,
                        (0..8192).map(|i| (i % 251) as u8).collect::<Vec<_>>()
                    );
                    println!("OK {}", bytes.len());
                }
                Err(error) => println!("ERR {:?}", error.kind()),
            },
            "stat" => match client.stat(&object).await {
                Ok(_) => println!("OK stat"),
                Err(error) => println!("ERR {:?}", error.kind()),
            },
            "membership" => {
                talon_cache_client::CoordinatorClient::new(&address)
                    .discovery()
                    .await?;
                println!("MEMBERSHIP_OK");
            }
            "quit" => break,
            _ => return Err("unknown probe command".into()),
        }
        std::io::stdout().flush()?;
    }
    Ok(())
}
