package io.synadia.offsetconsumer;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Throughput benchmark under different commit scenarios. Consumes --msgs
 * messages from --stream with a fresh consumer name and prints one JSON line.
 * Run with: ./gradlew -q bench -PbenchArgs="--scenario sync-100 --msgs 2000000"
 */
public final class Bench {

    public static void main(String[] args) throws Exception {
        String server = "nats://127.0.0.1:4222";
        String stream = "BENCH";
        long msgs = 2_000_000L;
        String scenario = "none";
        for (int i = 0; i + 1 < args.length; i += 2) {
            switch (args[i]) {
                case "--server": server = args[i + 1]; break;
                case "--stream": stream = args[i + 1]; break;
                case "--msgs": msgs = Long.parseLong(args[i + 1]); break;
                case "--scenario": scenario = args[i + 1]; break;
                default: throw new IllegalArgumentException("unknown flag " + args[i]);
            }
        }
        final long total = msgs;

        String mode;
        long every = 0;
        if (scenario.equals("none")) {
            mode = "none";
        } else if (scenario.startsWith("auto")) {
            mode = "auto";
        } else if (scenario.startsWith("async-")) {
            mode = "async";
            every = Long.parseLong(scenario.substring(6));
        } else if (scenario.startsWith("sync-")) {
            mode = "sync";
            every = Long.parseLong(scenario.substring(5));
        } else {
            throw new IllegalArgumentException("unknown scenario " + scenario);
        }
        final long commitEvery = every;

        Options options = Options.builder().server(server).build();
        try (Connection nc = Nats.connect(options)) {
            ConsumerConfig.Builder cb = new ConsumerConfig.Builder(stream,
                    "bench-java-" + scenario + "-" + System.nanoTime());
            if (mode.equals("auto")) {
                cb.autoCommit(true).autoCommitInterval(Duration.ofSeconds(1));
            }
            OffsetConsumer consumer = OffsetConsumerFactory.create(nc, cb.build());

            AtomicLong count = new AtomicLong();
            AtomicLong bytes = new AtomicLong();
            AtomicLong commits = new AtomicLong();
            AtomicReference<CompletableFuture<Void>> lastFuture = new AtomicReference<>();
            AtomicLong recvNs = new AtomicLong();
            CountDownLatch done = new CountDownLatch(1);

            final long start = System.nanoTime();
            consumer.consume(msg -> {
                long n = count.incrementAndGet();
                if (n > total) {
                    return;
                }
                bytes.addAndGet(msg.getData().length);
                try {
                    if (mode.equals("sync") && n % commitEvery == 0) {
                        consumer.commitSync();
                        commits.incrementAndGet();
                    } else if (mode.equals("async") && n % commitEvery == 0) {
                        lastFuture.set(consumer.commitAsync());
                        commits.incrementAndGet();
                    }
                } catch (CommitException e) {
                    throw new RuntimeException(e);
                }
                if (n == total) {
                    recvNs.set(System.nanoTime() - start);
                    done.countDown();
                }
            });
            done.await();

            CompletableFuture<Void> last = lastFuture.get();
            if (last != null) {
                last.join();
            }
            if (!mode.equals("auto")) {
                consumer.commitSync();
                commits.incrementAndGet();
            }
            consumer.close();
            long elapsedNs = System.nanoTime() - start;

            OffsetConsumerInfo info = consumer.info();
            System.out.printf("{\"lang\":\"java\",\"scenario\":\"%s\",\"msgs\":%d,\"bytes\":%d,\"recv_ms\":%.3f,\"elapsed_ms\":%.3f,\"commits\":%d,\"final_seq\":%d}%n",
                    scenario, total, bytes.get(), recvNs.get() / 1e6, elapsedNs / 1e6, commits.get(), info.getSeq());
            consumer.deleteConsumer();
            if (info.getSeq() < total) {
                System.err.println("warning: final seq " + info.getSeq() + " < msgs " + total);
            }
        }
    }
}
