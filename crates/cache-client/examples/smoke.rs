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

    let items: Vec<(&[u8], &[u8], u32)> = vec![
        (b"smoke:b1", b"batch one", 0),
        (b"smoke:b2", b"batch two", 0),
        (b"smoke:b3", b"batch three", 0),
    ];
    println!("set_many-> cas {:?}", client.set_many(&items).await?);

    let got = client
        .get_many(&[b"smoke:b1".as_slice(), b"nope", b"smoke:b3"])
        .await?;
    let rendered: Vec<_> = got
        .iter()
        .map(|v| match v {
            Some(v) => String::from_utf8_lossy(&v.data).into_owned(),
            None => "<miss>".to_string(),
        })
        .collect();
    println!("get_many-> {rendered:?}");
    println!(
        "del_many-> {:?}",
        client
            .delete_many(&[b"smoke:b1".as_slice(), b"nope", b"smoke:b3"])
            .await?
    );

    client
        .set_tagged(b"smoke:t1", b"tagged one", 0, &[b"demo"])
        .await?;
    client
        .set_tagged(b"smoke:t2", b"tagged two", 0, &[b"demo", b"other"])
        .await?;
    client.set(b"smoke:plain", b"untagged", 0).await?;
    println!(
        "tagged  -> t1={} t2={} plain={}",
        client.get(b"smoke:t1").await?.is_some(),
        client.get(b"smoke:t2").await?.is_some(),
        client.get(b"smoke:plain").await?.is_some()
    );

    println!("del_tag -> {}", client.delete_by_tag(b"demo").await?);
    println!(
        "after   -> t1={} t2={} plain={} (untagged untouched)",
        client.get(b"smoke:t1").await?.is_some(),
        client.get(b"smoke:t2").await?.is_some(),
        client.get(b"smoke:plain").await?.is_some()
    );
    client.delete(b"smoke:plain").await?;

    let cas = client.set(b"smoke:ttl", b"expires", 1).await?;
    println!(
        "set ttl -> cas {cas}, live now: {}",
        client.get(b"smoke:ttl").await?.is_some()
    );
    println!("touch   -> {}", client.touch(b"smoke:ttl", 3600).await?);
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    println!(
        "after 1.1s -> live: {} (touch extended it past the original ttl)",
        client.get(b"smoke:ttl").await?.is_some()
    );

    Ok(())
}
