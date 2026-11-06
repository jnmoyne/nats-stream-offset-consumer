use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
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
        let store_dir = tempfile::tempdir().expect("tmpdir");
        // Find a free port
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let child = std::process::Command::new("nats-server")
            .args([
                "-js",
                "-p",
                &port.to_string(),
                "-sd",
                store_dir.path().to_str().unwrap(),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("failed to start nats-server");

        // Wait for the port to accept connections
        let deadline = Instant::now() + Duration::from_secs(10);
        while std::net::TcpStream::connect(("127.0.0.1", port)).is_err() {
            assert!(Instant::now() < deadline, "nats-server did not start");
            std::thread::sleep(Duration::from_millis(50));
        }

        NatsServer {
            child,
            port,
            store_dir,
        }
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

async fn consumer_names_with_prefix(js: &jetstream::Context, stream: &str, prefix: &str) -> Vec<String> {
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

    assert!(wait_until(Duration::from_secs(5), || count.load(Ordering::Relaxed) >= NUM_MESSAGES).await);
    println!("basic consume: received {} messages", count.load(Ordering::Relaxed));

    consumer.close().await.unwrap();
}

#[tokio::test]
async fn test_commit_sync_and_resume() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "commit-stream").await;
    let cfg = config("commit-stream", "commit-test");

    // Phase 1: consume and commit
    let consumer = OffsetConsumer::new(js.clone(), cfg.clone(), None).await.unwrap();
    let last = consume_until_delivered(&consumer).await;
    assert!(wait_until(Duration::from_secs(5), || last.load(Ordering::Relaxed) >= NUM_MESSAGES).await);
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
                let _ = first_seq_clone.compare_exchange(0, info.stream_sequence, Ordering::SeqCst, Ordering::SeqCst);
            }
        })
        .await
        .unwrap();

    js.publish("commit-stream.extra", "extra".into())
        .await
        .unwrap()
        .await
        .unwrap();

    assert!(wait_until(Duration::from_secs(5), || first_seq.load(Ordering::Relaxed) != 0).await);
    let first = first_seq.load(Ordering::Relaxed);
    println!("Phase 2: first message after resume at seq {first} (expected {})", NUM_MESSAGES + 1);
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
    assert!(wait_until(Duration::from_secs(5), || last.load(Ordering::Relaxed) >= NUM_MESSAGES).await);
    let delivered = last.load(Ordering::Relaxed);

    let handle = consumer.commit_async().await.unwrap();
    handle.await.unwrap().unwrap();

    let info = consumer.info().await.unwrap().unwrap();
    assert_eq!(info.seq, delivered, "stored seq must match the delivered seq after the ack");
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
    consumer.commit_async().await.unwrap().await.unwrap().unwrap();
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
    assert!(wait_until(Duration::from_secs(5), || last.load(Ordering::Relaxed) >= NUM_MESSAGES).await);

    // Wait for auto-commit to fire
    tokio::time::sleep(Duration::from_secs(2)).await;
    consumer.close().await.unwrap();

    let info = consumer.info().await.unwrap().expect("info should exist after auto-commit");
    println!("Stored offset after auto-commit: {}", info.seq);
    assert_eq!(info.seq, NUM_MESSAGES);
}

#[tokio::test]
async fn test_seek_before_consuming() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "seek-stream").await;
    let cfg = config("seek-stream", "seek-test");

    let consumer = OffsetConsumer::new(js.clone(), cfg.clone(), None).await.unwrap();
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
                let _ = first_seq_clone.compare_exchange(0, info.stream_sequence, Ordering::SeqCst, Ordering::SeqCst);
            }
        })
        .await
        .unwrap();

    assert!(wait_until(Duration::from_secs(5), || first_seq.load(Ordering::Relaxed) != 0).await);
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
    let owner = consumer.info().await.unwrap().unwrap().owner;

    let last = Arc::new(AtomicU64::new(0));
    let seeked = Arc::new(AtomicBool::new(false));
    let first_after_seek = Arc::new(AtomicU64::new(0));
    let (last_c, seeked_c, first_c) = (last.clone(), seeked.clone(), first_after_seek.clone());
    consumer
        .consume(move |msg| {
            if let Ok(info) = msg.info() {
                last_c.store(info.stream_sequence, Ordering::Relaxed);
                if seeked_c.load(Ordering::Relaxed) {
                    let _ = first_c.compare_exchange(0, info.stream_sequence, Ordering::SeqCst, Ordering::SeqCst);
                }
            }
        })
        .await
        .unwrap();
    assert!(wait_until(Duration::from_secs(5), || last.load(Ordering::Relaxed) >= NUM_MESSAGES).await);

    let before = consumer_names_with_prefix(&js, "seekgen-stream", &owner).await;
    assert_eq!(before.len(), 1, "expected one generation-1 consumer, got {before:?}");
    assert!(before[0].starts_with(&format!("{owner}-g1")), "{}", before[0]);

    seeked.store(true, Ordering::Relaxed);
    consumer.seek(50).await.unwrap();

    assert!(wait_until(Duration::from_secs(5), || first_after_seek.load(Ordering::Relaxed) != 0).await);
    assert_eq!(first_after_seek.load(Ordering::Relaxed), 51);

    let after = consumer_names_with_prefix(&js, "seekgen-stream", &owner).await;
    println!("consumers after seek: {after:?}");
    assert_eq!(after.len(), 1, "expected exactly one generation-2 consumer, got {after:?}");
    assert!(after[0].starts_with(&format!("{owner}-g2")), "{}", after[0]);

    consumer.close().await.unwrap();
}

#[tokio::test]
async fn test_delete_consumer() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "delete-stream").await;
    let cfg = config("delete-stream", "delete-test");

    let consumer = OffsetConsumer::new(js.clone(), cfg.clone(), None).await.unwrap();
    consume_until_delivered(&consumer).await;
    consumer.commit_sync().await.unwrap();

    let info = consumer.info().await.unwrap().expect("info before delete");
    println!("Before delete: seq={}", info.seq);
    assert!(info.seq > 0);

    consumer.delete_consumer().await.unwrap();
    assert!(consumer.info().await.unwrap().is_none(), "info should be None after delete");
    consumer.close().await.unwrap();
    assert!(consumer.info().await.unwrap().is_none(), "close after delete must not recreate the entry");

    let consumer2 = OffsetConsumer::new(js, cfg, None).await.unwrap();
    let info2 = consumer2.info().await.unwrap().unwrap();
    println!("After delete: seq={} (expected 0)", info2.seq);
    assert_eq!(info2.seq, 0);
    consumer2.close().await.unwrap();
}

#[tokio::test]
async fn test_invalid_consumer_name() {
    let srv = NatsServer::start();
    let nc = async_nats::connect(&srv.url()).await.unwrap();
    let js = jetstream::new(nc);

    let result = OffsetConsumer::new(js.clone(), config("test", ""), None).await;
    assert!(matches!(result, Err(OffsetConsumerError::InvalidConsumerName)));

    let result = OffsetConsumer::new(js.clone(), config("test", "bad.name"), None).await;
    assert!(matches!(result, Err(OffsetConsumerError::InvalidConsumerName)));

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

    let consumers = offset_consumer::list_consumers(&js, "list-stream").await.unwrap();
    assert_eq!(consumers.len(), 3);
    for name in ["consumer-a", "consumer-b", "consumer-c"] {
        let info = consumers.get(name).expect(name);
        assert!(info.seq > 0);
        assert!(info.owner.is_empty(), "closed consumers must have released ownership");
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
    let info = consumer.info().await.unwrap().expect("entry right after new (claim)");
    assert!(info.owner.starts_with("oc-owner-stream-owner-test-"), "{}", info.owner);
    assert_eq!(info.host, hostname::get().unwrap().to_string_lossy());
    assert!(chrono::DateTime::parse_from_rfc3339(&info.claimed_at).is_ok(), "{}", info.claimed_at);
    assert_eq!(info.seq, 0);

    consume_until_delivered(&consumer).await;
    consumer.commit_sync().await.unwrap();

    let list = offset_consumer::list_consumers(&js, "owner-stream").await.unwrap();
    let listed = list.get("owner-test").expect("listed");
    assert_eq!(listed.owner, info.owner);
    assert!(listed.seq > 0);

    let after = consumer.info().await.unwrap().unwrap();
    assert_eq!(after.owner, info.owner, "commit must keep the owner");
    assert_eq!(after.claimed_at, info.claimed_at, "commit must keep the claim time");

    consumer.close().await.unwrap();
}

#[tokio::test]
async fn test_second_instance_refused() {
    let srv = NatsServer::start();
    let js = setup_stream(&srv.url(), "refused-stream").await;
    let cfg = config("refused-stream", "refused-test");

    let consumer = OffsetConsumer::new(js.clone(), cfg.clone(), None).await.unwrap();
    consume_until_delivered(&consumer).await;
    let info = consumer.info().await.unwrap().unwrap();

    match OffsetConsumer::new(js.clone(), cfg, None).await {
        Err(OffsetConsumerError::AlreadyInUse { owner, host, claimed_at }) => {
            assert_eq!(owner, info.owner);
            assert_eq!(host, info.host);
            assert_eq!(claimed_at, info.claimed_at);
            println!("refused: owner {owner} on {host}, claimed at {claimed_at}");
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

    let consumer = OffsetConsumer::new(js.clone(), cfg.clone(), None).await.unwrap();
    consume_until_delivered(&consumer).await;
    consumer.commit_sync().await.unwrap();
    let info = consumer.info().await.unwrap().unwrap();
    let owner1 = info.owner.clone();
    let committed = info.seq;

    assert_eq!(consumer_names_with_prefix(&js, "release-stream", &owner1).await.len(), 1);
    consumer.close().await.unwrap();
    assert_eq!(
        consumer_names_with_prefix(&js, "release-stream", &owner1).await.len(),
        0,
        "ordered consumer must be deleted on close"
    );

    let released = consumer.info().await.unwrap().unwrap();
    assert!(released.owner.is_empty());
    assert!(released.host.is_empty());
    assert!(released.claimed_at.is_empty());
    assert_eq!(released.seq, committed, "close must keep the committed seq (auto_commit off)");

    let consumer2 = OffsetConsumer::new(js, cfg, None).await.unwrap();
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
    assert!(wait_until(Duration::from_secs(5), || last.load(Ordering::Relaxed) >= NUM_MESSAGES).await);
    let delivered = last.load(Ordering::Relaxed);
    let owner = consumer.info().await.unwrap().unwrap().owner;

    let mut last_handle = None;
    for _ in 0..20000 {
        last_handle = Some(consumer.commit_async().await.unwrap());
    }

    consumer.set_close_timeout(Duration::from_millis(100));
    let start = Instant::now();
    consumer.close().await.unwrap();
    assert!(start.elapsed() < Duration::from_secs(3), "close took {:?}", start.elapsed());
    last_handle.unwrap().await.unwrap().unwrap();

    let info = consumer.info().await.unwrap().unwrap();
    assert_eq!(info.seq, delivered, "release write must persist the highest requested seq");
    assert!(info.owner.is_empty(), "ownership must be released");
    assert!(consumer_names_with_prefix(&js, "backlog-stream", &owner).await.is_empty());
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
    assert_eq!(cfg.subject_delete_marker_ttl, Some(Duration::from_secs(600)));
    assert!(cfg.allow_message_ttl, "per-message TTL must be enabled on the bucket stream");

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
        other => panic!("expected refusal right after crash, got {:?}", other.map(|_| ())),
    }

    let start = Instant::now();
    let recovered = loop {
        match OffsetConsumer::new(js.clone(), cfg.clone(), None).await {
            Ok(c) => break c,
            Err(OffsetConsumerError::AlreadyInUse { .. }) => {
                assert!(start.elapsed() < Duration::from_secs(60), "new instance never allowed after crash");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => panic!("unexpected error while waiting for recovery: {e}"),
        }
    };
    println!("recovered after {:?}", start.elapsed());
    assert!(consumer_names_with_prefix(&js, "crash-stream", &owner1).await.is_empty());
    assert_ne!(recovered.info().await.unwrap().unwrap().owner, owner1);
    recovered.close().await.unwrap();
}
