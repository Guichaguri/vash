//! Drives a running server over a real socket.
//!
//! ```text
//! cargo run --bin kached -- --listen 127.0.0.1:11311 --data ./data --ephemeral
//! cargo run -p cache-client --example smoke -- 127.0.0.1:11311
//! ```

use cache_client::Client;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:11311".to_string());

    let mut client = Client::connect(addr.as_str()).await?;
    let info = client.server_info();
    println!(
        "connected to {addr}: protocol v{}, {} shard(s), max key {}B, max value {}B, capabilities {:#04b}",
        info.protocol_version, info.shards, info.max_key_len, info.max_value_len, info.capabilities
    );

    client.ping().await?;
    println!("ping    -> ok");

    let cas = client.set(b"smoke:key", b"smoke value", 0).await?;
    println!("set     -> cas {cas}");

    match client.get(b"smoke:key").await? {
        Some(v) => println!(
            "get     -> {:?} (cas {}, flags {})",
            String::from_utf8_lossy(&v.data),
            v.cas,
            v.mc_flags
        ),
        None => println!("get     -> MISS (unexpected)"),
    }

    println!("delete  -> {}", client.delete(b"smoke:key").await?);
    println!("get     -> {:?}", client.get(b"smoke:key").await?.is_some());

    let cas = client.set(b"smoke:ttl", b"expires", 1).await?;
    println!(
        "set ttl -> cas {cas}, live now: {}",
        client.get(b"smoke:ttl").await?.is_some()
    );
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    println!(
        "after 1.1s -> live: {}",
        client.get(b"smoke:ttl").await?.is_some()
    );

    Ok(())
}
