# Quickstart: NATS JetStream Offset Consumer

Only one instance of a given stream/consumer-name pair may run at a time.
Construction fails with an "already in use" error while another live instance
owns the offset entry; after a clean close a new instance can start
immediately, after a crash once the previous instance's ordered consumer has
expired (inactive threshold, 30s by default where configurable).

---

## Go

```bash
go get github.com/nats-io/nats.go@v1.55.0 github.com/synadia-io/offset-consumer-go
```

```go
package main

import (
    "context"
    "errors"
    "fmt"
    "log"
    "os"
    "os/signal"
    "time"

    "github.com/nats-io/nats.go"
    "github.com/nats-io/nats.go/jetstream"
    oc "github.com/synadia-io/offset-consumer-go"
)

func main() {
    nc, _ := nats.Connect(nats.DefaultURL)
    defer nc.Drain()

    js, _ := jetstream.New(nc)
    ctx := context.Background()

    consumer, err := oc.NewConsumer(ctx, js, oc.ConsumerConfig{
        Stream:            "ORDERS",
        ConsumerName:      "billing-service",
        InactiveThreshold: 30 * time.Second, // default
    })
    var inUse *oc.AlreadyInUseError
    if errors.As(err, &inUse) {
        log.Fatalf("another instance is running: owner=%s host=%s since %s", inUse.Owner, inUse.Host, inUse.ClaimedAt)
    }
    if err != nil {
        log.Fatal(err)
    }
    defer consumer.Close() // deletes the ordered consumer and releases ownership

    err = consumer.Consume(func(msg jetstream.Msg) {
        md, _ := msg.Metadata()
        fmt.Printf("seq=%d subject=%s\n", md.Sequence.Stream, msg.Subject())
        // Process message...
    })
    if err != nil {
        log.Fatal(err)
    }

    // Periodically commit (async commits are pipelined; the future completes on ack)
    go func() {
        for range time.Tick(5 * time.Second) {
            f, err := consumer.CommitAsync()
            if err != nil {
                log.Printf("commit error: %v", err)
                continue
            }
            select {
            case <-f.Ok():
            case err := <-f.Err():
                log.Printf("commit failed: %v", err)
            }
        }
    }()

    sig := make(chan os.Signal, 1)
    signal.Notify(sig, os.Interrupt)
    <-sig
}
```

### Auto-Commit Mode

```go
consumer, _ := oc.NewConsumer(ctx, js, oc.ConsumerConfig{
    Stream:             "ORDERS",
    ConsumerName:       "billing-service",
    AutoCommit:         true,
    AutoCommitInterval: 5 * time.Second,
    ErrorHandler: func(c oc.OffsetConsumer, err error) {
        log.Printf("consumer error: %v", err)
    },
})
```

### Seek, Info, ListConsumers

```go
consumer.Seek(ctx, 100)        // next message delivered is 101
consumer.SeekToBeginning(ctx)
consumer.SeekToEnd(ctx)

info, _ := consumer.Info(ctx)  // nil if the entry does not exist
fmt.Println(info.Seq, info.CommittedAt, info.Owner, info.Host, info.ClaimedAt)

all, _ := oc.ListConsumers(ctx, js, "ORDERS") // map[consumerName]OffsetConsumerInfo
```

---

## Java

```kotlin
dependencies {
    implementation("io.nats:jnats:2.26.4")
    implementation("io.synadia:offset-consumer:1.0.0")
}
```

```java
Connection nc = Nats.connect();

ConsumerConfig config = new ConsumerConfig.Builder("ORDERS", "billing-service").build();

try (OffsetConsumer consumer = OffsetConsumerFactory.create(nc, config)) {
    consumer.consume(msg -> {
        System.out.printf("seq=%d subject=%s%n",
            msg.metaData().streamSequence(), msg.getSubject());
        // Process message...
    });

    consumer.commitSync();                                  // sync
    CompletableFuture<Void> future = consumer.commitAsync(); // async, completes on ack
    future.join();

    OffsetConsumerInfo info = consumer.info();              // null if the entry does not exist
    Map<String, OffsetConsumerInfo> all = OffsetConsumerFactory.listConsumers(nc, "ORDERS");
} catch (AlreadyInUseException e) {
    System.err.println("another instance is running: " + e.getOwner() + " on " + e.getHost());
}
```

### Auto-Commit Mode

```java
ConsumerConfig config = new ConsumerConfig.Builder("ORDERS", "billing-service")
    .autoCommit(true)
    .autoCommitInterval(Duration.ofSeconds(5))
    .errorHandler((consumer, err) -> System.err.println("Consumer error: " + err.getMessage()))
    .build();
```

The inactive threshold is not configurable with jnats; the server default of 5s applies.

---

## JavaScript/TypeScript

```bash
npm install @synadia-io/offset-consumer @nats-io/transport-node
```

```typescript
import { connect } from "@nats-io/transport-node";
import { AlreadyInUseError, createConsumer, listConsumers } from "@synadia-io/offset-consumer";

const nc = await connect({ servers: "nats://localhost:4222" });

let consumer;
try {
  consumer = await createConsumer(nc, {
    stream: "ORDERS",
    consumerName: "billing-service",
    inactiveThresholdMs: 30_000, // default
  });
} catch (err) {
  if (err instanceof AlreadyInUseError) {
    console.error(`another instance is running: ${err.owner} on ${err.host} since ${err.claimedAt}`);
  }
  throw err;
}

const messages = await consumer.consume(); // keeps yielding across seek() restarts
for await (const msg of messages) {
  console.log(`seq=${msg.seq} subject=${msg.subject}`);
  // Process message...
}

await consumer.commitSync();
await consumer.commitAsync();            // several may be pending; applied in order

const info = await consumer.info();      // null if the entry does not exist
const all = await listConsumers(nc, "ORDERS");

await consumer.close();                  // deletes the ordered consumer, releases ownership
await nc.close();
```

### Auto-Commit Mode

```typescript
const consumer = await createConsumer(nc, {
  stream: "ORDERS",
  consumerName: "billing-service",
  autoCommit: true,
  autoCommitIntervalMs: 5000,
  errorHandler: (consumer, err) => console.error("Consumer error:", err.message),
});
```

---

## Rust

```toml
[dependencies]
async-nats = "0.51"
offset-consumer = "0.1"
tokio = { version = "1", features = ["full"] }
```

```rust
use async_nats::jetstream;
use offset_consumer::{ConsumerConfig, OffsetConsumer, OffsetConsumerError};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let nc = async_nats::connect("nats://localhost:4222").await?;
    let js = jetstream::new(nc);

    let config = ConsumerConfig {
        stream: "ORDERS".into(),
        consumer_name: "billing-service".into(),
        ..Default::default()
    };

    let consumer = match OffsetConsumer::new(js.clone(), config, None).await {
        Err(OffsetConsumerError::AlreadyInUse { owner, host, claimed_at }) => {
            eprintln!("another instance is running: {owner} on {host} since {claimed_at}");
            return Ok(());
        }
        other => other?,
    };

    consumer
        .consume(|msg| {
            if let Ok(info) = msg.info() {
                println!("seq={} subject={}", info.stream_sequence, msg.subject);
            }
        })
        .await?;

    consumer.commit_sync().await?;
    let handle = consumer.commit_async().await?; // JoinHandle<Result<()>>
    handle.await??;

    let info = consumer.info().await?;           // None if the entry does not exist
    let all = offset_consumer::list_consumers(&js, "ORDERS").await?;

    consumer.close().await?;                     // deletes the ordered consumer, releases ownership
    Ok(())
}
```

The inactive threshold is fixed at 30s by async-nats.
