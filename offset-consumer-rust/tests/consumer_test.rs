use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use async_nats::jetstream;
use futures::StreamExt;
use offset_consumer::{ConsumerConfig, OffsetConsumer, OffsetConsumerError, StartPosition};

const NUM_MESSAGES: u64 = 100;

/// Helper: start an embedded nats-server with JetStream enabled.
struct NatsServer {
    child: std::process::Child,
    port: u16,
    #[allow(dead_code)]
    store_dir: tempfile::TempDir, // kept alive to prevent cleanup until drop
}

impl NatsServer {
    fn start() -> Self {
        // A free port can be taken by a server of a parallel test between
        // the probe and the spawn: retry with another port if ours exits.
        for _ in 0..5 {
            if let Some(server) = Self::try_start() {
                return server;
            }
        }
        panic!("nats-server did not start");
    }

    fn try_start() -> Option<Self> {
        let store_dir = tempfile::tempdir().expect("tmpdir");
        // Find a free port
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let mut child = std::process::Command::new("nats-server")
            .args([
                "-js",
                "-a",
                "127.0.0.1",
                "-p",
                &port.to_string(),
                "-sd",
                store_dir.path().to_str().unwrap(),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("failed to start nats-server");

        // Wait until the server speaks the protocol (sends INFO).
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if child.try_wait().ok().flatten().is_some() {
                return None;
            }
            if Instant::now() > deadline {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
            if Self::sends_info(port) {
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }

        Some(NatsServer {
            child,
            port,
            store_dir,
        })
    }

    fn sends_info(port: u16) -> bool {
        use std::io::{BufRead, BufReader};
        let Ok(conn) = std::net::TcpStream::connect(("127.0.0.1", port)) else {
            return false;
        };
        let _ = conn.set_read_timeout(Some(Duration::from_secs(1)));
        let mut line = String::new();
        BufReader::new(conn).read_line(&mut line).is_ok() && line.starts_with("INFO")
    }

    fn url(&self) -> String {
        format!("nats://127.0.0.1:{}", self.port)
    }
}

impl Drop for NatsServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn setup_stream(url: &str, stream_name: &str) -> jetstream::Context {
    let nc = async_nats::connect(url).await.unwrap();
    let js = jetstream::new(nc);

    js.create_stream(jetstream::stream::Config {
        name: stream_name.to_string(),
        subjects: vec![format!("{stream_name}.>")],
        ..Default::default()
    })
    .await
    .unwrap();

    for i in 1..=NUM_MESSAGES {
        js.publish(format!("{stream_name}.{i}"), format!("msg-{i}").into())
            .await
            .unwrap()
            .await
            .unwrap();
    }

    js
}

fn config(stream: &str, name: &str) -> ConsumerConfig {
    ConsumerConfig {
        stream: stream.to_string(),
        consumer_name: name.to_string(),
        ..Default::default()
    }
}

async fn wait_until<F: Fn() -> bool>(timeout: Duration, cond: F) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    cond()
}

/// Starts consumption and waits until at least one message has been delivered.
async fn consume_until_delivered(consumer: &OffsetConsumer) -> Arc<AtomicU64> {
    let last = Arc::new(AtomicU64::new(0));
    let last_clone = last.clone();
    consumer
        .consume(move |msg| {
            if let Ok(info) = msg.info() {
                last_clone.store(info.stream_sequence, Ordering::Relaxed);
            }
        })
        .await
        .unwrap();
    assert!(
        wait_until(Duration::from_secs(5), || last.load(Ordering::Relaxed) > 0).await,
        "no messages delivered"
    );
    last
}

async fn consumer_names_with_prefix(
    js: &jetstream::Context,
    stream: &str,
    prefix: &str,
) -> Vec<String> {
    let stream = js.get_stream(stream).await.unwrap();
    let mut names = stream.consumer_names();
    let mut out = Vec::new();
    while let Some(name) = names.next().await {
        let name = name.unwrap();
        if name.starts_with(prefix) {
            out.push(name);
        }
    }
    out
}

#[tokio::test]
async fn test_basic_consume() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "basic-stream").await;

    let consumer = OffsetConsumer::new(js, config("basic-stream", "basic-test"), None)
        .await
        .unwrap();

    let count = Arc::new(AtomicU64::new(0));
    let count_clone = count.clone();
    consumer
        .consume(move |_msg| {
            count_clone.fetch_add(1, Ordering::Relaxed);
        })
        .await
        .unwrap();

    assert!(
        wait_until(Duration::from_secs(5), || count.load(Ordering::Relaxed)
            >= NUM_MESSAGES)
        .await
    );
    println!(
        "basic consume: received {} messages",
        count.load(Ordering::Relaxed)
    );

    consumer.close().await.unwrap();
}

#[tokio::test]
async fn test_commit_sync_and_resume() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "commit-stream").await;
    let cfg = config("commit-stream", "commit-test");

    // Phase 1: consume and commit
    let consumer = OffsetConsumer::new(js.clone(), cfg.clone(), None)
        .await
        .unwrap();
    let last = consume_until_delivered(&consumer).await;
    assert!(
        wait_until(Duration::from_secs(5), || last.load(Ordering::Relaxed)
            >= NUM_MESSAGES)
        .await
    );
    consumer.commit_sync().await.unwrap();

    let info = consumer.info().await.unwrap().expect("info after commit");
    println!("Phase 1: committed at seq {}", info.seq);
    assert_eq!(info.seq, NUM_MESSAGES);
    consumer.close().await.unwrap();

    // Phase 2: resume from committed position
    let consumer2 = OffsetConsumer::new(js.clone(), cfg, None).await.unwrap();
    let first_seq = Arc::new(AtomicU64::new(0));
    let first_seq_clone = first_seq.clone();
    consumer2
        .consume(move |msg| {
            if let Ok(info) = msg.info() {
                let _ = first_seq_clone.compare_exchange(
                    0,
                    info.stream_sequence,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
            }
        })
        .await
        .unwrap();

    js.publish("commit-stream.extra", "extra".into())
        .await
        .unwrap()
        .await
        .unwrap();

    assert!(
        wait_until(Duration::from_secs(5), || first_seq.load(Ordering::Relaxed)
            != 0)
        .await
    );
    let first = first_seq.load(Ordering::Relaxed);
    println!(
        "Phase 2: first message after resume at seq {first} (expected {})",
        NUM_MESSAGES + 1
    );
    assert_eq!(first, NUM_MESSAGES + 1);

    consumer2.close().await.unwrap();
}

#[tokio::test]
async fn test_commit_async() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "async-commit-stream").await;

    let consumer = OffsetConsumer::new(js, config("async-commit-stream", "async-test"), None)
        .await
        .unwrap();
    let last = consume_until_delivered(&consumer).await;
    assert!(
        wait_until(Duration::from_secs(5), || last.load(Ordering::Relaxed)
            >= NUM_MESSAGES)
        .await
    );
    let delivered = last.load(Ordering::Relaxed);

    let handle = consumer.commit_async().await.unwrap();
    handle.await.unwrap().unwrap();

    let info = consumer.info().await.unwrap().unwrap();
    assert_eq!(
        info.seq, delivered,
        "stored seq must match the delivered seq after the ack"
    );
    println!("commit_async succeeded, stored seq {}", info.seq);

    consumer.close().await.unwrap();
}

#[tokio::test]
async fn test_commit_async_chained() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "chained-stream").await;

    let consumer = OffsetConsumer::new(js, config("chained-stream", "chained-test"), None)
        .await
        .unwrap();
    consume_until_delivered(&consumer).await;

    let h1 = consumer.commit_async().await.unwrap();
    let h2 = consumer.commit_async().await.unwrap();
    let h3 = consumer.commit_async().await.unwrap();
    h1.await.unwrap().unwrap();
    h2.await.unwrap().unwrap();
    h3.await.unwrap().unwrap();
    println!("three pipelined async commits acknowledged");

    consumer.close().await.unwrap();
}

#[tokio::test]
async fn test_commit_async_nothing_to_commit() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "nothing-stream").await;

    let consumer = OffsetConsumer::new(js, config("nothing-stream", "nothing-test"), None)
        .await
        .unwrap();
    consumer
        .commit_async()
        .await
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    consumer.commit_sync().await.unwrap();
    consumer.close().await.unwrap();
}

#[tokio::test]
async fn test_auto_commit() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "autocommit-stream").await;

    let cfg = ConsumerConfig {
        auto_commit: true,
        auto_commit_interval: Duration::from_secs(1),
        ..config("autocommit-stream", "autocommit-test")
    };
    let consumer = OffsetConsumer::new(js, cfg, None).await.unwrap();
    let last = consume_until_delivered(&consumer).await;
    assert!(
        wait_until(Duration::from_secs(5), || last.load(Ordering::Relaxed)
            >= NUM_MESSAGES)
        .await
    );

    // Wait for auto-commit to fire
    tokio::time::sleep(Duration::from_secs(2)).await;
    consumer.close().await.unwrap();

    let info = consumer
        .info()
        .await
        .unwrap()
        .expect("info should exist after auto-commit");
    println!("Stored offset after auto-commit: {}", info.seq);
    assert_eq!(info.seq, NUM_MESSAGES);
}

#[tokio::test]
async fn test_seek_before_consuming() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "seek-stream").await;
    let cfg = config("seek-stream", "seek-test");

    let consumer = OffsetConsumer::new(js.clone(), cfg.clone(), None)
        .await
        .unwrap();
    consume_until_delivered(&consumer).await;
    consumer.commit_sync().await.unwrap();
    consumer.close().await.unwrap();

    let consumer2 = OffsetConsumer::new(js, cfg, None).await.unwrap();
    consumer2.seek(50).await.unwrap();

    let first_seq = Arc::new(AtomicU64::new(0));
    let first_seq_clone = first_seq.clone();
    consumer2
        .consume(move |msg| {
            if let Ok(info) = msg.info() {
                let _ = first_seq_clone.compare_exchange(
                    0,
                    info.stream_sequence,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
            }
        })
        .await
        .unwrap();

    assert!(
        wait_until(Duration::from_secs(5), || first_seq.load(Ordering::Relaxed)
            != 0)
        .await
    );
    let first = first_seq.load(Ordering::Relaxed);
    println!("First message after seek(50): seq {first} (expected 51)");
    assert_eq!(first, 51);

    consumer2.close().await.unwrap();
}

#[tokio::test]
async fn test_seek_restarts_with_new_generation() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "seekgen-stream").await;

    let consumer = OffsetConsumer::new(js.clone(), config("seekgen-stream", "seekgen-test"), None)
        .await
        .unwrap();

    let last = Arc::new(AtomicU64::new(0));
    let seeked = Arc::new(AtomicBool::new(false));
    let first_after_seek = Arc::new(AtomicU64::new(0));
    let (last_c, seeked_c, first_c) = (last.clone(), seeked.clone(), first_after_seek.clone());
    consumer
        .consume(move |msg| {
            if let Ok(info) = msg.info() {
                last_c.store(info.stream_sequence, Ordering::Relaxed);
                if seeked_c.load(Ordering::Relaxed) {
                    let _ = first_c.compare_exchange(
                        0,
                        info.stream_sequence,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    );
                }
            }
        })
        .await
        .unwrap();
    assert!(
        wait_until(Duration::from_secs(5), || last.load(Ordering::Relaxed)
            >= NUM_MESSAGES)
        .await
    );
    let owner = consumer.info().await.unwrap().unwrap().owner;

    let before = consumer_names_with_prefix(&js, "seekgen-stream", &format!("{owner}-g")).await;
    assert_eq!(
        before.len(),
        1,
        "expected one generation-1 consumer, got {before:?}"
    );
    assert!(
        before[0].starts_with(&format!("{owner}-g1")),
        "{}",
        before[0]
    );

    seeked.store(true, Ordering::Relaxed);
    consumer.seek(50).await.unwrap();

    assert!(
        wait_until(Duration::from_secs(5), || first_after_seek
            .load(Ordering::Relaxed)
            != 0)
        .await
    );
    assert_eq!(first_after_seek.load(Ordering::Relaxed), 51);

    let after = consumer_names_with_prefix(&js, "seekgen-stream", &format!("{owner}-g")).await;
    println!("consumers after seek: {after:?}");
    assert_eq!(
        after.len(),
        1,
        "expected exactly one generation-2 consumer, got {after:?}"
    );
    assert!(after[0].starts_with(&format!("{owner}-g2")), "{}", after[0]);

    consumer.close().await.unwrap();
}

#[tokio::test]
async fn test_delete_consumer() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "delete-stream").await;
    let cfg = config("delete-stream", "delete-test");

    let consumer = OffsetConsumer::new(js.clone(), cfg.clone(), None)
        .await
        .unwrap();
    consume_until_delivered(&consumer).await;
    consumer.commit_sync().await.unwrap();

    let info = consumer.info().await.unwrap().expect("info before delete");
    println!("Before delete: seq={}", info.seq);
    assert!(info.seq > 0);

    consumer.delete_consumer().await.unwrap();
    assert!(
        consumer.info().await.unwrap().is_none(),
        "info should be None after delete"
    );
    consumer.close().await.unwrap();
    assert!(
        consumer.info().await.unwrap().is_none(),
        "close after delete must not recreate the entry"
    );

    let consumer2 = OffsetConsumer::new(js, cfg, None).await.unwrap();
    consumer2.consume(|_| {}).await.unwrap();
    let info2 = consumer2.info().await.unwrap().unwrap();
    println!("After delete: seq={} (expected 0)", info2.seq);
    assert_eq!(info2.seq, 0);
    consumer2.close().await.unwrap();
}

#[tokio::test]
async fn test_invalid_consumer_name() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "names-stream").await;

    let long = "x".repeat(200);
    for name in [
        "",
        "bad.name",
        "has*star",
        "has>gt",
        "has space",
        "has/slash",
        long.as_str(),
    ] {
        let result = OffsetConsumer::new(js.clone(), config("names-stream", name), None).await;
        assert!(
            matches!(result, Err(OffsetConsumerError::InvalidConsumerName { .. })),
            "expected InvalidConsumerName for {name:?}, got {:?}",
            result.map(|_| ())
        );
    }

    let result = OffsetConsumer::new(js.clone(), config("bad stream", "ok"), None).await;
    match result {
        Err(e @ OffsetConsumerError::InvalidStreamName { .. }) => println!("{e}"),
        other => panic!("expected InvalidStreamName, got {:?}", other.map(|_| ())),
    }
    let result = OffsetConsumer::new(js.clone(), config("", "ok"), None).await;
    assert!(matches!(result, Err(OffsetConsumerError::StreamRequired)));

    let consumer = OffsetConsumer::new(js.clone(), config("names-stream", "Ok_name-1=x"), None)
        .await
        .expect("valid name rejected");
    consumer.close().await.unwrap();

    println!("Invalid consumer name validation works correctly");
}

#[tokio::test]
async fn test_start_position_end() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "end-stream").await;

    let cfg = ConsumerConfig {
        start_position: StartPosition::End,
        ..config("end-stream", "end-test")
    };
    let consumer = OffsetConsumer::new(js, cfg, None).await.unwrap();

    let count = Arc::new(AtomicU64::new(0));
    let count_clone = count.clone();
    consumer
        .consume(move |_msg| {
            count_clone.fetch_add(1, Ordering::Relaxed);
        })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(2)).await;
    let received = count.load(Ordering::Relaxed);
    println!("Messages received with StartPosition::End (no new publishes): {received}");
    assert_eq!(received, 0);

    consumer.close().await.unwrap();
}

#[tokio::test]
async fn test_already_consuming() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "already-stream").await;

    let consumer = OffsetConsumer::new(js, config("already-stream", "already-test"), None)
        .await
        .unwrap();
    consumer.consume(|_msg| {}).await.unwrap();

    let result = consumer.consume(|_msg| {}).await;
    assert!(matches!(result, Err(OffsetConsumerError::AlreadyConsuming)));
    println!("AlreadyConsuming works correctly");

    consumer.close().await.unwrap();
}

#[tokio::test]
async fn test_list_consumers() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "list-stream").await;

    for name in &["consumer-a", "consumer-b", "consumer-c"] {
        let consumer = OffsetConsumer::new(js.clone(), config("list-stream", name), None)
            .await
            .unwrap();
        consume_until_delivered(&consumer).await;
        consumer.commit_sync().await.unwrap();
        consumer.close().await.unwrap();
    }

    let consumers = offset_consumer::list_consumers(&js, "list-stream")
        .await
        .unwrap();
    assert_eq!(consumers.len(), 3);
    for name in ["consumer-a", "consumer-b", "consumer-c"] {
        let info = consumers.get(name).expect(name);
        assert!(info.seq > 0);
        assert!(
            info.owner.is_empty(),
            "closed consumers must have released ownership"
        );
    }
    println!("list_consumers returned {} consumers", consumers.len());
}

#[tokio::test]
async fn test_info_and_list_have_owner() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "owner-stream").await;

    let consumer = OffsetConsumer::new(js.clone(), config("owner-stream", "owner-test"), None)
        .await
        .unwrap();
    assert!(
        consumer.info().await.unwrap().is_none(),
        "expected no entry before consume (no claim yet)"
    );
    consume_until_delivered(&consumer).await;
    let info = consumer
        .info()
        .await
        .unwrap()
        .expect("entry right after consume (claim)");
    assert!(
        info.owner.starts_with("oc-owner-stream-owner-test-"),
        "{}",
        info.owner
    );
    assert_eq!(info.host, hostname::get().unwrap().to_string_lossy());
    assert_eq!(info.seq, 0, "expected seq 0 before any commit");

    consumer.commit_sync().await.unwrap();

    let list = offset_consumer::list_consumers(&js, "owner-stream")
        .await
        .unwrap();
    let listed = list.get("owner-test").expect("listed");
    assert_eq!(listed.owner, info.owner);
    assert!(listed.seq > 0);

    let after = consumer.info().await.unwrap().unwrap();
    assert_eq!(after.owner, info.owner, "commit must keep the owner");

    consumer.close().await.unwrap();
}

#[tokio::test]
async fn test_second_instance_refused() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "refused-stream").await;
    let cfg = config("refused-stream", "refused-test");

    let consumer = OffsetConsumer::new(js.clone(), cfg.clone(), None)
        .await
        .unwrap();
    consume_until_delivered(&consumer).await;
    let info = consumer.info().await.unwrap().unwrap();

    match OffsetConsumer::new(js.clone(), cfg, None).await {
        Err(OffsetConsumerError::AlreadyInUse { owner, host }) => {
            assert_eq!(owner, info.owner);
            assert_eq!(host, info.host);
            println!("refused: owner {owner} on {host}");
        }
        other => panic!("expected AlreadyInUse, got {:?}", other.map(|_| ())),
    }

    assert_eq!(consumer.info().await.unwrap().unwrap().owner, info.owner);
    consumer.close().await.unwrap();
}

#[tokio::test]
async fn test_release_on_close() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "release-stream").await;
    let cfg = config("release-stream", "release-test");

    let consumer = OffsetConsumer::new(js.clone(), cfg.clone(), None)
        .await
        .unwrap();
    consume_until_delivered(&consumer).await;
    consumer.commit_sync().await.unwrap();
    let info = consumer.info().await.unwrap().unwrap();
    let owner1 = info.owner.clone();
    let committed = info.seq;

    assert_eq!(
        consumer_names_with_prefix(&js, "release-stream", &format!("{owner1}-g"))
            .await
            .len(),
        1
    );
    consumer.close().await.unwrap();
    assert_eq!(
        consumer_names_with_prefix(&js, "release-stream", &owner1)
            .await
            .len(),
        0,
        "ordered consumer must be deleted on close"
    );

    let released = consumer.info().await.unwrap().unwrap();
    assert!(released.owner.is_empty());
    assert!(released.host.is_empty());
    assert_eq!(
        released.seq, committed,
        "close must keep the committed seq (auto_commit off)"
    );

    let consumer2 = OffsetConsumer::new(js, cfg, None).await.unwrap();
    consumer2.consume(|_| {}).await.unwrap();
    let info2 = consumer2.info().await.unwrap().unwrap();
    assert!(!info2.owner.is_empty());
    assert_ne!(info2.owner, owner1);
    assert_eq!(info2.seq, committed);
    consumer2.close().await.unwrap();
}

/// Close with a backlog of async commits persists the highest requested
/// position and releases ownership once the bounded wait expires.
#[tokio::test]
async fn test_close_with_commit_backlog() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "backlog-stream").await;

    let consumer = OffsetConsumer::new(js.clone(), config("backlog-stream", "backlog-test"), None)
        .await
        .unwrap();
    let last = consume_until_delivered(&consumer).await;
    assert!(
        wait_until(Duration::from_secs(5), || last.load(Ordering::Relaxed)
            >= NUM_MESSAGES)
        .await
    );
    let delivered = last.load(Ordering::Relaxed);
    let owner = consumer.info().await.unwrap().unwrap().owner;

    let mut last_handle = None;
    for _ in 0..20000 {
        last_handle = Some(consumer.commit_async().await.unwrap());
    }

    consumer.set_close_timeout(Duration::from_millis(100));
    let start = Instant::now();
    consumer.close().await.unwrap();
    assert!(
        start.elapsed() < Duration::from_secs(3),
        "close took {:?}",
        start.elapsed()
    );
    last_handle.unwrap().await.unwrap().unwrap();

    let info = consumer.info().await.unwrap().unwrap();
    assert_eq!(
        info.seq, delivered,
        "release write must persist the highest requested seq"
    );
    assert!(info.owner.is_empty(), "ownership must be released");
    assert!(consumer_names_with_prefix(&js, "backlog-stream", &owner)
        .await
        .is_empty());
    consumer.delete_consumer().await.unwrap();
}

#[tokio::test]
async fn test_bucket_delete_marker_ttl() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "ttl-stream").await;

    let consumer = OffsetConsumer::new(js.clone(), config("ttl-stream", "ttl-test"), None)
        .await
        .unwrap();

    let bucket = js.get_stream("KV_offset-consumers").await.unwrap();
    let cfg = &bucket.cached_info().config;
    assert_eq!(
        cfg.subject_delete_marker_ttl,
        Some(Duration::from_secs(600))
    );
    assert!(
        cfg.allow_message_ttl,
        "per-message TTL must be enabled on the bucket stream"
    );

    consumer.close().await.unwrap();
}

/// After a crash the new instance is refused until the crashed owner's
/// ordered consumer expires. async-nats fixes the inactive threshold at 30s,
/// so this test takes a little over half a minute.
#[tokio::test]
async fn test_crash_recovery() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "crash-stream").await;
    let cfg = config("crash-stream", "crash-test");
    let url = srv.url();

    // Run the first instance on its own runtime so that dropping the runtime
    // severs its connection without any orderly close.
    let (owner_tx, owner_rx) = std::sync::mpsc::channel::<String>();
    let crash_cfg = cfg.clone();
    let crashed = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let nc = async_nats::connect(&url).await.unwrap();
            let js = jetstream::new(nc);
            let consumer = OffsetConsumer::new(js, crash_cfg, None).await.unwrap();
            consume_until_delivered(&consumer).await;
            let owner = consumer.info().await.unwrap().unwrap().owner;
            owner_tx.send(owner).unwrap();
            // Keep the instance alive until the runtime is torn down.
            std::mem::forget(consumer);
        });
        rt.shutdown_background();
    });
    let owner1 = owner_rx.recv().unwrap();
    crashed.join().unwrap();

    match OffsetConsumer::new(js.clone(), cfg.clone(), None).await {
        Err(OffsetConsumerError::AlreadyInUse { .. }) => {}
        other => panic!(
            "expected refusal right after crash, got {:?}",
            other.map(|_| ())
        ),
    }

    let start = Instant::now();
    let recovered = loop {
        match OffsetConsumer::new(js.clone(), cfg.clone(), None).await {
            Ok(c) => break c,
            Err(OffsetConsumerError::AlreadyInUse { .. }) => {
                assert!(
                    start.elapsed() < Duration::from_secs(60),
                    "new instance never allowed after crash"
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => panic!("unexpected error while waiting for recovery: {e}"),
        }
    };
    assert!(consumer_names_with_prefix(&js, "crash-stream", &owner1)
        .await
        .is_empty());
    // The claim at consume waits the liveness wait before taking over.
    recovered.set_liveness_wait(SHORT_LIVENESS_WAIT);
    recovered.consume(|_| {}).await.unwrap();
    println!("recovered after {:?}", start.elapsed());
    assert_ne!(recovered.info().await.unwrap().unwrap().owner, owner1);
    recovered.close().await.unwrap();
}

/// Returns the stored sequence (0 when there is no entry).
async fn stored_seq(consumer: &OffsetConsumer) -> u64 {
    consumer.info().await.unwrap().map(|i| i.seq).unwrap_or(0)
}

async fn wait_stored(consumer: &OffsetConsumer, expected: u64, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if stored_seq(consumer).await == expected {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    stored_seq(consumer).await == expected
}

/// Blocks the calling (handler) thread until `release` is set. The handler
/// runs on a runtime worker, so it blocks the way an application should:
/// through block_in_place, which lets the worker's other tasks move on.
fn block_until(release: &AtomicBool) {
    tokio::task::block_in_place(|| {
        while !release.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(10));
        }
    });
}

fn stream_identity(info: &jetstream::stream::Info) -> String {
    info.created.unix_timestamp_nanos().to_string()
}

/// Deletes the stream, recreates it empty and publishes `n` messages.
async fn recreate_stream(js: &jetstream::Context, stream_name: &str, n: u64) {
    js.delete_stream(stream_name).await.unwrap();
    js.create_stream(jetstream::stream::Config {
        name: stream_name.to_string(),
        subjects: vec![format!("{stream_name}.>")],
        ..Default::default()
    })
    .await
    .unwrap();
    for _ in 0..n {
        js.publish(format!("{stream_name}.again"), "again".into())
            .await
            .unwrap()
            .await
            .unwrap();
    }
}

/// Error handler that raises `flag` when the stream recreation is reported.
type ErrorHandler = Arc<dyn Fn(&OffsetConsumerError) + Send + Sync>;

fn recreated_flag() -> (Arc<AtomicBool>, ErrorHandler) {
    let flag = Arc::new(AtomicBool::new(false));
    let f = flag.clone();
    let eh = Arc::new(move |e: &OffsetConsumerError| {
        if matches!(e, OffsetConsumerError::StreamRecreated { .. }) {
            f.store(true, Ordering::SeqCst);
        }
    });
    (flag, eh)
}

/// Auto-commit must not commit a message whose handler is still running.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_auto_commit_skips_message_in_handler() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "ac-skip-stream").await;
    let cfg = ConsumerConfig {
        auto_commit: true,
        auto_commit_interval: Duration::from_millis(100),
        ..config("ac-skip-stream", "ac-skip-test")
    };
    let consumer = OffsetConsumer::new(js, cfg, None).await.unwrap();

    let release = Arc::new(AtomicBool::new(false));
    let r = release.clone();
    consumer
        .consume(move |msg| {
            if msg.info().unwrap().stream_sequence == 3 {
                block_until(&r);
            }
        })
        .await
        .unwrap();

    assert!(
        wait_stored(&consumer, 2, Duration::from_secs(5)).await,
        "expected stored seq 2 while message 3 is in its handler, got {}",
        stored_seq(&consumer).await
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        stored_seq(&consumer).await,
        2,
        "auto-commit committed a message still in its handler"
    );
    release.store(true, Ordering::SeqCst);
    assert!(
        wait_stored(&consumer, NUM_MESSAGES, Duration::from_secs(5)).await,
        "expected all messages committed after release, got {}",
        stored_seq(&consumer).await
    );
    consumer.close().await.unwrap();
}

/// Close with auto-commit must not commit (nor wait for) a message whose
/// handler is still running.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_close_auto_commit_skips_message_in_handler() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "close-skip-stream").await;
    let cfg = ConsumerConfig {
        auto_commit: true,
        auto_commit_interval: Duration::from_secs(3600),
        ..config("close-skip-stream", "close-skip-test")
    };
    let consumer = OffsetConsumer::new(js.clone(), cfg.clone(), None)
        .await
        .unwrap();

    let entered = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let (e, r) = (entered.clone(), release.clone());
    consumer
        .consume(move |msg| {
            if msg.info().unwrap().stream_sequence == 5 {
                e.store(true, Ordering::SeqCst);
                block_until(&r);
            }
        })
        .await
        .unwrap();
    assert!(wait_until(Duration::from_secs(5), || entered.load(Ordering::SeqCst)).await);

    let start = Instant::now();
    consumer.close().await.unwrap();
    assert!(
        start.elapsed() < Duration::from_secs(4),
        "close waited for the handler: {:?}",
        start.elapsed()
    );
    release.store(true, Ordering::SeqCst);

    let consumer2 = OffsetConsumer::new(js, cfg, None).await.unwrap();
    assert_eq!(
        stored_seq(&consumer2).await,
        4,
        "expected stored seq 4 (message 5 was still in its handler)"
    );
    consumer2.close().await.unwrap();
}

/// StartPosition::End is resolved at creation (messages published between
/// creation and consume are delivered) and stored by the claim, so messages
/// published while the consumer is down are delivered on the next start.
#[tokio::test]
async fn test_start_position_end_persisted() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "end-persist-stream").await;
    let cfg = ConsumerConfig {
        start_position: StartPosition::End,
        ..config("end-persist-stream", "end-persist-test")
    };

    let consumer = OffsetConsumer::new(js.clone(), cfg.clone(), None)
        .await
        .unwrap();
    for _ in 0..2 {
        // Published after creation, before consume.
        js.publish("end-persist-stream.early", "early".into())
            .await
            .unwrap()
            .await
            .unwrap();
    }
    let last = consume_until_delivered(&consumer).await;
    assert!(
        wait_until(Duration::from_secs(5), || last.load(Ordering::Relaxed)
            == NUM_MESSAGES + 2)
        .await,
        "expected the messages published after creation, last delivered {}",
        last.load(Ordering::Relaxed)
    );
    consumer.commit_sync().await.unwrap();
    consumer.close().await.unwrap();

    for _ in 0..3 {
        js.publish("end-persist-stream.new", "new".into())
            .await
            .unwrap()
            .await
            .unwrap();
    }

    let consumer2 = OffsetConsumer::new(js, cfg, None).await.unwrap();
    let first = Arc::new(AtomicU64::new(0));
    let last = Arc::new(AtomicU64::new(0));
    let (f, l) = (first.clone(), last.clone());
    consumer2
        .consume(move |msg| {
            let seq = msg.info().unwrap().stream_sequence;
            let _ = f.compare_exchange(0, seq, Ordering::SeqCst, Ordering::SeqCst);
            l.store(seq, Ordering::SeqCst);
        })
        .await
        .unwrap();
    assert!(
        wait_until(Duration::from_secs(5), || last.load(Ordering::SeqCst)
            == NUM_MESSAGES + 5)
        .await,
        "expected the messages published while down, last delivered {}",
        last.load(Ordering::SeqCst)
    );
    assert_eq!(first.load(Ordering::SeqCst), NUM_MESSAGES + 3);
    consumer2.close().await.unwrap();
}

/// seek_to_beginning goes to the beginning even with StartPosition::End.
#[tokio::test]
async fn test_seek_to_beginning_with_start_position_end() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "seekbeg-stream").await;
    let cfg = ConsumerConfig {
        start_position: StartPosition::End,
        ..config("seekbeg-stream", "seekbeg-test")
    };
    let consumer = OffsetConsumer::new(js, cfg, None).await.unwrap();

    let first = Arc::new(AtomicU64::new(0));
    let last = Arc::new(AtomicU64::new(0));
    let (f, l) = (first.clone(), last.clone());
    consumer
        .consume(move |msg| {
            let seq = msg.info().unwrap().stream_sequence;
            let _ = f.compare_exchange(0, seq, Ordering::SeqCst, Ordering::SeqCst);
            l.store(seq, Ordering::SeqCst);
        })
        .await
        .unwrap();
    consumer.seek_to_beginning().await.unwrap();

    assert!(
        wait_until(Duration::from_secs(5), || last.load(Ordering::SeqCst)
            == NUM_MESSAGES)
        .await,
        "expected all messages after seek_to_beginning, last delivered {}",
        last.load(Ordering::SeqCst)
    );
    assert_eq!(first.load(Ordering::SeqCst), 1);
    consumer.close().await.unwrap();
}

/// An existing bucket's configuration is not overwritten.
#[tokio::test]
async fn test_existing_bucket_not_overwritten() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "bucket-stream").await;

    js.create_key_value(jetstream::kv::Config {
        bucket: "offset-consumers".to_string(),
        description: "operator managed".to_string(),
        history: 1,
        ..Default::default()
    })
    .await
    .unwrap();

    let consumer = OffsetConsumer::new(js.clone(), config("bucket-stream", "bucket-test"), None)
        .await
        .unwrap();
    let bucket = js.get_stream("KV_offset-consumers").await.unwrap();
    let cfg = &bucket.cached_info().config;
    assert_eq!(
        cfg.description.as_deref(),
        Some("operator managed"),
        "bucket configuration was overwritten"
    );
    assert_eq!(
        cfg.subject_delete_marker_ttl, None,
        "bucket configuration was overwritten"
    );
    consumer.close().await.unwrap();
}

/// Concurrent seeks leave exactly one ordered consumer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_seeks() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "conc-seek-stream").await;
    let consumer = OffsetConsumer::new(
        js.clone(),
        config("conc-seek-stream", "conc-seek-test"),
        None,
    )
    .await
    .unwrap();
    consume_until_delivered(&consumer).await;
    let owner = consumer.info().await.unwrap().unwrap().owner;

    let seeks: Vec<_> = (0..10u64)
        .map(|i| {
            let c = consumer.clone();
            tokio::spawn(async move { c.seek(i * 5).await })
        })
        .collect();
    for seek in seeks {
        seek.await.unwrap().unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let names = consumer_names_with_prefix(&js, "conc-seek-stream", &format!("{owner}-g")).await;
    assert_eq!(
        names.len(),
        1,
        "expected exactly one ordered consumer, got {names:?}"
    );
    consumer.close().await.unwrap();
}

/// The error handler may close the consumer when an auto-commit fails, even
/// blocking on the close from the auto-commit task.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_error_handler_can_close_on_auto_commit_failure() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "eh-close-stream").await;

    let slot: Arc<OnceLock<OffsetConsumer>> = Arc::new(OnceLock::new());
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let once = Arc::new(AtomicBool::new(false));
    let s = slot.clone();
    let eh = Arc::new(move |e: &OffsetConsumerError| {
        if e.to_string().contains("auto-commit failed") && !once.swap(true, Ordering::SeqCst) {
            if let Some(c) = s.get() {
                let handle = tokio::runtime::Handle::current();
                // The release write fails too (no bucket); only returning matters.
                let _ = tokio::task::block_in_place(|| handle.block_on(c.close()));
                let _ = tx.send(());
            }
        }
    });
    let cfg = ConsumerConfig {
        auto_commit: true,
        auto_commit_interval: Duration::from_millis(200),
        ..config("eh-close-stream", "eh-close-test")
    };
    let consumer = OffsetConsumer::new(js.clone(), cfg, Some(eh))
        .await
        .unwrap();
    let _ = slot.set(consumer.clone());

    consumer.consume(|_msg| {}).await.unwrap();
    // Make commits fail: without the bucket, KV writes get no responders.
    js.delete_key_value("offset-consumers").await.unwrap();

    assert!(
        tokio::time::timeout(Duration::from_secs(15), rx.recv())
            .await
            .is_ok(),
        "close called from the error handler did not return (deadlock)"
    );
}

/// A stored offset from a previous incarnation of the stream is reset.
#[tokio::test]
async fn test_stream_recreated_at_startup() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "recreate-start-stream").await;
    let cfg = config("recreate-start-stream", "recreate-start-test");

    let consumer = OffsetConsumer::new(js.clone(), cfg.clone(), None)
        .await
        .unwrap();
    let last = consume_until_delivered(&consumer).await;
    let old_identity = consumer.info().await.unwrap().unwrap().stream_created;
    assert!(!old_identity.is_empty());
    assert!(
        wait_until(Duration::from_secs(5), || last.load(Ordering::Relaxed)
            == NUM_MESSAGES)
        .await
    );
    consumer.commit_sync().await.unwrap();
    consumer.close().await.unwrap();

    // Passes the old position: only the stream identity tells.
    recreate_stream(&js, "recreate-start-stream", NUM_MESSAGES + 20).await;
    let (reported, eh) = recreated_flag();
    let consumer2 = OffsetConsumer::new(js.clone(), cfg, Some(eh))
        .await
        .unwrap();
    consumer2.consume(|_| {}).await.unwrap(); // the claim detects it
    assert!(
        reported.load(Ordering::SeqCst),
        "StreamRecreated not reported"
    );
    let info = consumer2.info().await.unwrap().unwrap();
    assert_eq!(info.seq, 0, "expected offset reset to 0");
    let current = stream_identity(
        js.get_stream("recreate-start-stream")
            .await
            .unwrap()
            .cached_info(),
    );
    assert_eq!(info.stream_created, current);
    assert_ne!(info.stream_created, old_identity);
    consumer2.close().await.unwrap();
}

/// Legacy entries (no stream identity) past the stream's end are reset.
#[tokio::test]
async fn test_stream_recreated_legacy_entry() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "legacy-stream").await;

    let kv = offset_consumer::kv::ensure_bucket(&js, 1).await.unwrap();
    kv.put(
        "legacy-stream.legacy-test",
        r#"{"seq":5000,"committed_at":"2026-01-01T00:00:00Z"}"#.into(),
    )
    .await
    .unwrap();

    let (reported, eh) = recreated_flag();
    let consumer = OffsetConsumer::new(js, config("legacy-stream", "legacy-test"), Some(eh))
        .await
        .unwrap();
    consumer.consume(|_| {}).await.unwrap();
    assert_eq!(
        stored_seq(&consumer).await,
        0,
        "expected legacy offset beyond the stream end to be reset"
    );
    assert!(
        reported.load(Ordering::SeqCst),
        "StreamRecreated not reported"
    );
    consumer.close().await.unwrap();
}

/// Recreates the stream while consuming with `n` messages and checks that the
/// recreation is detected and the new stream's messages are delivered.
async fn stream_recreated_while_consuming(stream_name: &str, n: u64) {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), stream_name).await;

    let (reported, eh) = recreated_flag();
    let consumer = OffsetConsumer::new(js.clone(), config(stream_name, "recreate-test"), Some(eh))
        .await
        .unwrap();
    let last = Arc::new(AtomicU64::new(0));
    let after_reset = Arc::new(AtomicU64::new(0));
    let first_after_reset = Arc::new(AtomicU64::new(0));
    let (l, a, f, r) = (
        last.clone(),
        after_reset.clone(),
        first_after_reset.clone(),
        reported.clone(),
    );
    consumer
        .consume(move |msg| {
            let seq = msg.info().unwrap().stream_sequence;
            l.store(seq, Ordering::SeqCst);
            if r.load(Ordering::SeqCst) {
                a.fetch_add(1, Ordering::SeqCst);
                let _ = f.compare_exchange(0, seq, Ordering::SeqCst, Ordering::SeqCst);
            }
        })
        .await
        .unwrap();
    assert!(
        wait_until(Duration::from_secs(5), || last.load(Ordering::SeqCst)
            == NUM_MESSAGES)
        .await,
        "initial messages not delivered"
    );

    recreate_stream(&js, stream_name, n).await;
    let start = Instant::now();
    assert!(
        wait_until(Duration::from_secs(20), || reported.load(Ordering::SeqCst)).await,
        "stream recreation not detected"
    );
    println!("recreation detected after {:?}", start.elapsed());
    assert!(
        wait_until(Duration::from_secs(10), || after_reset
            .load(Ordering::SeqCst)
            >= n)
        .await,
        "expected the {n} messages of the new stream, got {}",
        after_reset.load(Ordering::SeqCst)
    );
    assert_eq!(
        first_after_reset.load(Ordering::SeqCst),
        1,
        "the new stream must be consumed from its beginning"
    );

    consumer.commit_sync().await.unwrap();
    let info = consumer.info().await.unwrap().unwrap();
    let current = stream_identity(js.get_stream(stream_name).await.unwrap().cached_info());
    assert_eq!(
        info.stream_created, current,
        "commits must carry the new stream identity"
    );
    assert_eq!(info.seq, n);
    consumer.close().await.unwrap();
}

/// A shorter new stream: the recreated ordered consumer waits past its end,
/// so only the periodic stream check can tell.
#[tokio::test]
async fn test_stream_recreated_while_consuming() {
    stream_recreated_while_consuming("recreate-short-stream", 10).await;
}

/// A longer new stream: the recreated ordered consumer delivers from the old
/// position under a new name, which triggers the stream check.
#[tokio::test]
async fn test_stream_recreated_while_consuming_longer_stream() {
    stream_recreated_while_consuming("recreate-long-stream", NUM_MESSAGES + 50).await;
}

/// Publishes a header-only message with the given status line to `subject`
/// over a raw connection (clients cannot set a status line).
fn inject_status(port: u16, subject: &str, status: &str) {
    use std::io::{BufRead, BufReader, Write};
    let mut conn = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    let mut reader = BufReader::new(conn.try_clone().unwrap());
    let mut line = String::new();
    reader.read_line(&mut line).unwrap(); // INFO
    let hdr = format!("NATS/1.0 {status}\r\n\r\n");
    write!(
        conn,
        "CONNECT {{\"verbose\":false,\"pedantic\":false,\"headers\":true,\"protocol\":1}}\r\nHPUB {subject} {n} {n}\r\n{hdr}\r\nPING\r\n",
        n = hdr.len()
    )
    .unwrap();
    line.clear();
    reader.read_line(&mut line).unwrap();
    assert!(line.starts_with("PONG"), "unexpected reply: {line}");
}

/// An error yielded by the ordered stream is reported and consumption goes
/// on (it used to end the delivery loop silently).
#[tokio::test]
async fn test_recoverable_error_does_not_stop_consumption() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "errors-stream").await;

    // Learn the ordered consumer's inbox from its pull requests.
    let spy = async_nats::connect(&srv.url()).await.unwrap();
    let mut pulls = spy
        .subscribe("$JS.API.CONSUMER.MSG.NEXT.errors-stream.>")
        .await
        .unwrap();
    spy.flush().await.unwrap();

    let errors = Arc::new(AtomicU64::new(0));
    let e = errors.clone();
    let eh = Arc::new(move |err: &OffsetConsumerError| {
        println!("error handler: {err}");
        if err.to_string().contains("consumer error") {
            e.fetch_add(1, Ordering::SeqCst);
        }
    });
    let consumer =
        OffsetConsumer::new(js.clone(), config("errors-stream", "errors-test"), Some(eh))
            .await
            .unwrap();
    let last = consume_until_delivered(&consumer).await;
    assert!(
        wait_until(Duration::from_secs(5), || last.load(Ordering::Relaxed)
            == NUM_MESSAGES)
        .await
    );

    let pull = tokio::time::timeout(Duration::from_secs(5), pulls.next())
        .await
        .expect("no pull request seen")
        .unwrap();
    let inbox = pull
        .reply
        .expect("pull request without reply subject")
        .to_string();

    // An unexpected status makes async-nats yield an error; the stream stays usable.
    inject_status(srv.port, &inbox, "400 Bad Request");
    assert!(
        wait_until(Duration::from_secs(5), || errors.load(Ordering::SeqCst)
            >= 1)
        .await,
        "error not reported"
    );

    js.publish("errors-stream.more", "more".into())
        .await
        .unwrap()
        .await
        .unwrap();
    assert!(
        wait_until(Duration::from_secs(5), || last.load(Ordering::Relaxed)
            == NUM_MESSAGES + 1)
        .await,
        "consumption stopped after a recoverable error"
    );
    consumer.close().await.unwrap();
}

/// Short liveness wait used where a takeover wait is not under test.
const SHORT_LIVENESS_WAIT: Duration = Duration::from_millis(300);

/// Instances starting at the same time: exactly one wins the claim, and only
/// its ordered consumer exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_starters() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "starters-stream").await;

    let mut starters = Vec::new();
    for _ in 0..4 {
        let url = srv.url();
        starters.push(tokio::spawn(async move {
            let js = jetstream::new(async_nats::connect(&url).await.unwrap());
            let c =
                OffsetConsumer::new(js, config("starters-stream", "starters-test"), None).await?;
            c.set_liveness_wait(SHORT_LIVENESS_WAIT);
            match c.consume(|_| {}).await {
                Ok(()) => Ok(c),
                Err(e) => {
                    let _ = c.close().await;
                    Err(e)
                }
            }
        }));
    }
    let mut winners = Vec::new();
    for starter in starters {
        match starter.await.unwrap() {
            Ok(c) => winners.push(c),
            Err(OffsetConsumerError::AlreadyInUse { .. }) => {}
            Err(e) => panic!("unexpected error: {e}"),
        }
    }
    assert_eq!(winners.len(), 1, "expected exactly one winner");
    let winner = winners.pop().unwrap();
    let owner = winner.info().await.unwrap().unwrap().owner;
    let names =
        consumer_names_with_prefix(&js, "starters-stream", "oc-starters-stream-starters-test-")
            .await;
    assert!(
        names.len() == 1 && names[0].starts_with(&owner),
        "expected only the winner's ordered consumer, got {names:?}"
    );
    winner.close().await.unwrap();
}

/// Close does not release an entry that another instance claimed meanwhile
/// (an instance displaced without noticing): the entry is left untouched and
/// close returns AlreadyInUse.
#[tokio::test]
async fn test_close_skips_release_of_foreign_entry() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "foreign-stream").await;

    let consumer = OffsetConsumer::new(js.clone(), config("foreign-stream", "foreign-test"), None)
        .await
        .unwrap();
    consume_until_delivered(&consumer).await;
    let owner = consumer.info().await.unwrap().unwrap().owner;
    let kv = js.get_key_value("offset-consumers").await.unwrap();
    let other = r#"{"seq":3,"committed_at":"","owner":"oc-someone-else","host":"h"}"#;
    kv.put("foreign-stream.foreign-test", other.into())
        .await
        .unwrap();

    match consumer.close().await {
        Err(OffsetConsumerError::AlreadyInUse { owner: o, .. }) => assert_eq!(o, "oc-someone-else"),
        other => panic!("expected AlreadyInUse naming the new owner, got {other:?}"),
    }
    let info = consumer.info().await.unwrap().unwrap();
    assert_eq!(
        info.owner, "oc-someone-else",
        "close overwrote the other owner's entry"
    );
    assert_eq!(info.seq, 3, "close overwrote the other owner's entry");
    let left = consumer_names_with_prefix(&js, "foreign-stream", &owner).await;
    assert!(
        left.is_empty(),
        "consumers left behind after close: {left:?}"
    );
}

/// Close does not recreate an entry deleted by someone else.
#[tokio::test]
async fn test_close_skips_release_of_deleted_entry() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "deleted-stream").await;

    let consumer = OffsetConsumer::new(js.clone(), config("deleted-stream", "deleted-test"), None)
        .await
        .unwrap();
    consume_until_delivered(&consumer).await; // claim first
    assert!(
        consumer.info().await.unwrap().is_some(),
        "expected the claimed entry"
    );
    let kv = js.get_key_value("offset-consumers").await.unwrap();
    kv.delete("deleted-stream.deleted-test").await.unwrap();
    consumer.close().await.unwrap();
    assert!(
        consumer.info().await.unwrap().is_none(),
        "close recreated the deleted entry"
    );
}

/// The conditional release still succeeds right after a burst of async
/// commits whose acks may land between its read and its write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_close_releases_after_commit_burst() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "burst-stream").await;

    let consumer = OffsetConsumer::new(js, config("burst-stream", "burst-test"), None)
        .await
        .unwrap();
    let last = Arc::new(AtomicU64::new(0));
    let (c, l) = (consumer.clone(), last.clone());
    let handle = tokio::runtime::Handle::current();
    consumer
        .consume(move |msg| {
            // Submit the commit from the handler, as the Go test does.
            tokio::task::block_in_place(|| handle.block_on(c.commit_async()))
                .expect("commit_async");
            l.store(msg.info().unwrap().stream_sequence, Ordering::SeqCst);
        })
        .await
        .unwrap();
    assert!(
        wait_until(Duration::from_secs(5), || last.load(Ordering::SeqCst)
            == NUM_MESSAGES)
        .await,
        "messages not delivered"
    );
    consumer.close().await.unwrap();
    let info = consumer.info().await.unwrap().unwrap();
    assert_eq!(info.owner, "", "expected a released entry");
    assert_eq!(
        info.seq, NUM_MESSAGES,
        "expected a released entry at {NUM_MESSAGES}"
    );
}

/// Stores an entry owned by `owner`, an instance that has no consumer on the server.
async fn put_owned_entry(js: &jetstream::Context, key: &str, owner: &str, seq: u64) {
    let kv = offset_consumer::kv::ensure_bucket(js, 1).await.unwrap();
    let entry = format!(r#"{{"seq":{seq},"committed_at":"","owner":"{owner}","host":"h"}}"#);
    kv.put(key, entry.into()).await.unwrap();
}

/// Nothing is written before the first consume: construction, commits and
/// close leave no entry and no consumer behind.
#[tokio::test]
async fn test_no_writes_before_consume() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "no-writes-stream").await;

    let consumer = OffsetConsumer::new(
        js.clone(),
        config("no-writes-stream", "no-writes-test"),
        None,
    )
    .await
    .unwrap();
    consumer.commit_sync().await.unwrap();
    consumer
        .commit_async()
        .await
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    consumer.close().await.unwrap();
    assert!(
        consumer.info().await.unwrap().is_none(),
        "expected no entry"
    );
    let names = consumer_names_with_prefix(&js, "no-writes-stream", "oc-").await;
    assert!(names.is_empty(), "expected no consumers, got {names:?}");
}

/// Two instances may be constructed while the entry is unowned; the second
/// one to consume is refused because the first one's ordered consumer exists.
#[tokio::test]
async fn test_second_consume_refused() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "second-consume-stream").await;
    let cfg = config("second-consume-stream", "second-consume-test");

    let a = OffsetConsumer::new(js.clone(), cfg.clone(), None)
        .await
        .unwrap();
    let other = jetstream::new(async_nats::connect(&srv.url()).await.unwrap());
    let b = OffsetConsumer::new(other, cfg, None).await.unwrap();
    a.consume(|_| {}).await.unwrap();
    let a_owner = a.info().await.unwrap().unwrap().owner;
    match b.consume(|_| {}).await {
        Err(OffsetConsumerError::AlreadyInUse { owner, .. }) => assert_eq!(owner, a_owner),
        other => panic!("expected AlreadyInUse naming a, got {other:?}"),
    }
    b.close().await.unwrap();
    assert_eq!(
        a.info().await.unwrap().unwrap().owner,
        a_owner,
        "refused instance disturbed the entry"
    );
    a.close().await.unwrap();
}

/// An owner without a consumer is taken over only after the liveness wait,
/// keeping the stored offset.
#[tokio::test]
async fn test_takeover_after_liveness_wait() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "takeover-stream").await;

    put_owned_entry(&js, "takeover-stream.takeover-test", "oc-gone-owner", 7).await;
    let consumer = OffsetConsumer::new(js, config("takeover-stream", "takeover-test"), None)
        .await
        .unwrap();
    let wait = Duration::from_secs(1);
    consumer.set_liveness_wait(wait);
    let start = Instant::now();
    consumer.consume(|_| {}).await.unwrap();
    let elapsed = start.elapsed();
    assert!(
        elapsed >= wait,
        "took over after {elapsed:?}, before the liveness wait"
    );
    let info = consumer.info().await.unwrap().unwrap();
    assert_eq!(info.seq, 7, "expected the stored offset 7");
    assert!(
        info.owner.starts_with("oc-takeover-stream-takeover-test-"),
        "expected the entry claimed, got {}",
        info.owner
    );
    consumer.close().await.unwrap();
}

/// A consumer of the owner that appears during the wait (an ordered consumer
/// being recreated) makes the takeover fail.
#[tokio::test]
async fn test_consumer_appearing_during_wait_refuses() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "appearing-stream").await;
    let owner = "oc-resetting-owner";

    put_owned_entry(&js, "appearing-stream.appearing-test", owner, 0).await;
    let consumer = OffsetConsumer::new(
        js.clone(),
        config("appearing-stream", "appearing-test"),
        None,
    )
    .await
    .unwrap();
    consumer.set_liveness_wait(Duration::from_secs(1));
    let late_js = js.clone();
    let late = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let stream = late_js.get_stream("appearing-stream").await.unwrap();
        stream
            .create_consumer(jetstream::consumer::pull::Config {
                name: Some(format!("{owner}-g1_2")),
                inactive_threshold: Duration::from_secs(60),
                ..Default::default()
            })
            .await
            .unwrap();
    });
    match consumer.consume(|_| {}).await {
        Err(OffsetConsumerError::AlreadyInUse { owner: o, .. }) => assert_eq!(o, owner),
        other => panic!("expected AlreadyInUse, got {other:?}"),
    }
    late.await.unwrap();
}

/// A write to the entry during the wait (here a commit by the owner) makes
/// the CAS claim fail.
#[tokio::test]
async fn test_write_during_wait_refuses() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "write-wait-stream").await;
    let owner = "oc-committing-owner";
    let key = "write-wait-stream.write-wait-test";

    put_owned_entry(&js, key, owner, 3).await;
    let consumer = OffsetConsumer::new(
        js.clone(),
        config("write-wait-stream", "write-wait-test"),
        None,
    )
    .await
    .unwrap();
    consumer.set_liveness_wait(Duration::from_secs(1));
    let late_js = js.clone();
    let late = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        put_owned_entry(&late_js, key, owner, 4).await;
    });
    match consumer.consume(|_| {}).await {
        Err(OffsetConsumerError::AlreadyInUse { owner: o, .. }) => assert_eq!(o, owner),
        other => panic!("expected AlreadyInUse, got {other:?}"),
    }
    late.await.unwrap();
    let info = consumer.info().await.unwrap().unwrap();
    assert_eq!(info.owner, owner, "expected the owner's write to stand");
    assert_eq!(info.seq, 4, "expected the owner's write to stand");
}

/// A seek creates the new ordered consumer before deleting the old one: the
/// instance always has a consumer on the server.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_seek_keeps_consumer_visible() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "visible-stream").await;

    let consumer = OffsetConsumer::new(js.clone(), config("visible-stream", "visible-test"), None)
        .await
        .unwrap();
    consume_until_delivered(&consumer).await;
    let owner = consumer.info().await.unwrap().unwrap().owner;

    let stop = Arc::new(AtomicBool::new(false));
    let gaps = Arc::new(AtomicU64::new(0));
    let (st, g, poll_js, poll_owner) = (stop.clone(), gaps.clone(), js.clone(), owner.clone());
    let poller = tokio::spawn(async move {
        let stream = poll_js.get_stream("visible-stream").await.unwrap();
        while !st.load(Ordering::SeqCst) {
            let mut names = stream.consumer_names();
            let mut alive = false;
            let mut failed = false;
            while let Some(name) = names.next().await {
                match name {
                    Ok(name) if name.starts_with(&poll_owner) => alive = true,
                    Ok(_) => {}
                    Err(_) => failed = true,
                }
            }
            if !alive && !failed {
                g.fetch_add(1, Ordering::SeqCst);
            }
        }
    });
    for i in 0..20 {
        consumer.seek(i).await.unwrap();
    }
    stop.store(true, Ordering::SeqCst);
    poller.await.unwrap();
    assert_eq!(
        gaps.load(Ordering::SeqCst),
        0,
        "the instance had no consumer on the server during seeks"
    );
    let names = consumer_names_with_prefix(&js, "visible-stream", &owner).await;
    assert!(
        names.len() == 1 && names[0].starts_with(&format!("{owner}-g21")),
        "expected only the latest generation, got {names:?}"
    );
    consumer.close().await.unwrap();
}

/// A seek before the first consume is only recorded, and stored by the claim.
#[tokio::test]
async fn test_seek_before_consume_stored_by_claim() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "seek-claim-stream").await;

    let consumer = OffsetConsumer::new(js, config("seek-claim-stream", "seek-claim-test"), None)
        .await
        .unwrap();
    consumer.seek(42).await.unwrap();
    assert!(
        consumer.info().await.unwrap().is_none(),
        "expected no write before consume"
    );
    let first = Arc::new(AtomicU64::new(0));
    let f = first.clone();
    consumer
        .consume(move |msg| {
            let _ = f.compare_exchange(
                0,
                msg.info().unwrap().stream_sequence,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
        })
        .await
        .unwrap();
    assert_eq!(
        stored_seq(&consumer).await,
        42,
        "expected the claim to store 42"
    );
    assert!(wait_until(Duration::from_secs(5), || first.load(Ordering::SeqCst) != 0).await);
    assert_eq!(
        first.load(Ordering::SeqCst),
        43,
        "expected delivery from 43"
    );
    consumer.close().await.unwrap();
}

/// A stream recreated between construction and consume: the claim resolves
/// the start position on the new stream and reports the recreation.
#[tokio::test]
async fn test_stream_recreated_before_consume() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "recreate-before-stream").await;
    let cfg = config("recreate-before-stream", "recreate-before-test");

    let c1 = OffsetConsumer::new(js.clone(), cfg.clone(), None)
        .await
        .unwrap();
    let last = consume_until_delivered(&c1).await;
    assert!(
        wait_until(Duration::from_secs(5), || last.load(Ordering::Relaxed)
            == NUM_MESSAGES)
        .await
    );
    c1.commit_sync().await.unwrap();
    c1.close().await.unwrap();

    let (reported, eh) = recreated_flag();
    let c2 = OffsetConsumer::new(js.clone(), cfg, Some(eh))
        .await
        .unwrap();
    // Passes the old position: only the identity tells.
    recreate_stream(&js, "recreate-before-stream", NUM_MESSAGES + 20).await;
    let first = Arc::new(AtomicU64::new(0));
    let f = first.clone();
    c2.consume(move |msg| {
        let _ = f.compare_exchange(
            0,
            msg.info().unwrap().stream_sequence,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    })
    .await
    .unwrap();
    assert!(
        reported.load(Ordering::SeqCst),
        "StreamRecreated not reported"
    );
    assert_eq!(stored_seq(&c2).await, 0, "expected the offset reset to 0");
    assert!(wait_until(Duration::from_secs(5), || first.load(Ordering::SeqCst) != 0).await);
    assert_eq!(
        first.load(Ordering::SeqCst),
        1,
        "expected delivery from the new stream's beginning"
    );
    let current = stream_identity(
        js.get_stream("recreate-before-stream")
            .await
            .unwrap()
            .cached_info(),
    );
    assert_eq!(c2.info().await.unwrap().unwrap().stream_created, current);
    c2.close().await.unwrap();
}

/// Several starters taking over a dead owner all wake from the liveness wait
/// at the same moment (there is no jitter): the CAS against the revision read
/// before the wait lets exactly one of them win.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_takeover() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "takeover-race-stream").await;
    put_owned_entry(
        &js,
        "takeover-race-stream.takeover-race-test",
        "oc-dead-owner",
        7,
    )
    .await;

    let mut consumers = Vec::new();
    for _ in 0..4 {
        let js = jetstream::new(async_nats::connect(&srv.url()).await.unwrap());
        let c = OffsetConsumer::new(
            js,
            config("takeover-race-stream", "takeover-race-test"),
            None,
        )
        .await
        .unwrap();
        c.set_liveness_wait(Duration::from_millis(500));
        consumers.push(c);
    }
    let mut starters = Vec::new();
    for c in consumers {
        starters.push(tokio::spawn(async move {
            let result = c.consume(|_| {}).await;
            (c, result)
        }));
    }
    let mut wins = 0;
    let mut all = Vec::new();
    for starter in starters {
        let (c, result) = starter.await.unwrap();
        match result {
            Ok(()) => wins += 1,
            Err(OffsetConsumerError::AlreadyInUse { .. }) => {}
            Err(e) => panic!("unexpected error: {e}"),
        }
        all.push(c);
    }
    assert_eq!(wins, 1, "expected exactly one winner");
    let names = consumer_names_with_prefix(
        &js,
        "takeover-race-stream",
        "oc-takeover-race-stream-takeover-race-test-",
    )
    .await;
    assert_eq!(
        names.len(),
        1,
        "expected only the winner's ordered consumer, got {names:?}"
    );
    for c in all {
        let _ = c.close().await;
    }
}
