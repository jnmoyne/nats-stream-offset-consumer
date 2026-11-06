//! Throughput benchmark under different commit scenarios. Consumes `--msgs`
//! messages from `--stream` with a fresh consumer name and prints one JSON line.
//!
//! cargo run --release --example bench -- --scenario sync-100 --msgs 2000000

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_nats::jetstream;
use offset_consumer::{ConsumerConfig, OffsetConsumer};
use tokio::task::JoinHandle;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut server = "nats://127.0.0.1:4222".to_string();
    let mut stream = "BENCH".to_string();
    let mut msgs: u64 = 2_000_000;
    let mut scenario = "none".to_string();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i + 1 < args.len() {
        match args[i].as_str() {
            "--server" => server = args[i + 1].clone(),
            "--stream" => stream = args[i + 1].clone(),
            "--msgs" => msgs = args[i + 1].parse()?,
            "--scenario" => scenario = args[i + 1].clone(),
            other => return Err(format!("unknown flag {other}").into()),
        }
        i += 2;
    }

    let (mode, every): (&str, u64) = if scenario == "none" {
        ("none", 0)
    } else if scenario.starts_with("auto") {
        ("auto", 0)
    } else if let Some(n) = scenario.strip_prefix("async-") {
        ("async", n.parse()?)
    } else if let Some(n) = scenario.strip_prefix("sync-") {
        ("sync", n.parse()?)
    } else {
        return Err(format!("unknown scenario {scenario}").into());
    };

    let nc = async_nats::connect(&server).await?;
    let js = jetstream::new(nc);

    let config = ConsumerConfig {
        stream: stream.clone(),
        consumer_name: format!("bench-rust-{scenario}-{}", nuid_like()),
        auto_commit: mode == "auto",
        auto_commit_interval: std::time::Duration::from_secs(1),
        ..Default::default()
    };
    let consumer = OffsetConsumer::new(js, config, None).await?;

    let count = Arc::new(AtomicU64::new(0));
    let bytes = Arc::new(AtomicU64::new(0));
    let commits = Arc::new(AtomicU64::new(0));
    let last: Arc<Mutex<Option<JoinHandle<offset_consumer::errors::Result<()>>>>> =
        Arc::new(Mutex::new(None));
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
    let done_tx = Arc::new(Mutex::new(Some(done_tx)));
    let handle = tokio::runtime::Handle::current();
    let recv_ns = Arc::new(AtomicU64::new(0));
    let recv_c = recv_ns.clone();
    let c = consumer.clone();
    let (count_c, bytes_c, commits_c, last_c, done_c) = (
        count.clone(),
        bytes.clone(),
        commits.clone(),
        last.clone(),
        done_tx.clone(),
    );
    let mode_s = mode.to_string();
    let start = Instant::now();

    consumer
        .consume(move |msg| {
            let n = count_c.fetch_add(1, Ordering::Relaxed) + 1;
            if n > msgs {
                return;
            }
            bytes_c.fetch_add(msg.payload.len() as u64, Ordering::Relaxed);
            if every > 0 && n % every == 0 {
                // The handler is synchronous; commit through the runtime.
                if mode_s == "sync" {
                    tokio::task::block_in_place(|| handle.block_on(c.commit_sync()))
                        .expect("commit_sync");
                    commits_c.fetch_add(1, Ordering::Relaxed);
                } else if mode_s == "async" {
                    let h = tokio::task::block_in_place(|| handle.block_on(c.commit_async()))
                        .expect("commit_async");
                    if let Some(prev) = last_c.lock().unwrap().replace(h) {
                        drop(prev); // the chain keeps running; only the last handle is awaited
                    }
                    commits_c.fetch_add(1, Ordering::Relaxed);
                }
            }
            if n == msgs {
                recv_c.store(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                if let Some(tx) = done_c.lock().unwrap().take() {
                    let _ = tx.send(());
                }
            }
        })
        .await?;
    done_rx.await?;

    let last_handle = last.lock().unwrap().take();
    if let Some(h) = last_handle {
        h.await??;
    }
    if mode != "auto" {
        consumer.commit_sync().await?;
        commits.fetch_add(1, Ordering::Relaxed);
    }
    consumer.close().await?;
    let elapsed = start.elapsed();

    let info = consumer.info().await?.expect("info");
    println!(
        "{{\"lang\":\"rust\",\"scenario\":\"{}\",\"msgs\":{},\"bytes\":{},\"recv_ms\":{:.3},\"elapsed_ms\":{:.3},\"commits\":{},\"final_seq\":{}}}",
        scenario,
        msgs,
        bytes.load(Ordering::Relaxed),
        recv_ns.load(Ordering::Relaxed) as f64 / 1e6,
        elapsed.as_secs_f64() * 1000.0,
        commits.load(Ordering::Relaxed),
        info.seq
    );
    consumer.delete_consumer().await?;
    if info.seq < msgs {
        eprintln!("warning: final seq {} < msgs {}", info.seq, msgs);
    }
    Ok(())
}

fn nuid_like() -> String {
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{t:x}")
}
