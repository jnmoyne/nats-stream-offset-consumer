package io.synadia.offsetconsumer;

import nats.io.NatsServerRunner;
import io.nats.client.Connection;
import io.nats.client.ErrorListener;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the OffsetConsumer library.
 * Uses an embedded nats-server via jnats-server-runner.
 */
class OffsetConsumerTest {

    private static final String TEST_STREAM = "test-stream";
    private static final int NUM_MESSAGES = 100;

    private NatsServerRunner serverRunner;
    private Connection nc;
    private OffsetConsumer consumer;
    private String consumerName;

    private Connection startServerAndConnect() throws Exception {
        serverRunner = NatsServerRunner.builder().jetstream(true).build();
        Options options = Options.builder()
                .server(serverRunner.getURI())
                .errorListener(new ErrorListener() {})
                .build();
        nc = Nats.connect(options);
        return nc;
    }

    private void createStreamAndPublish() throws Exception {
        // Create stream
        nc.jetStreamManagement().addStream(StreamConfiguration.builder()
                .name(TEST_STREAM)
                .subjects("test.>")
                .storageType(StorageType.Memory)
                .build());

        // Publish messages
        for (int i = 1; i <= NUM_MESSAGES; i++) {
            nc.jetStream().publish("test." + i, ("msg-" + i).getBytes());
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        if (consumer != null) {
            try { consumer.deleteConsumer(); } catch (Exception ignored) {}
            try { consumer.close(); } catch (Exception ignored) {}
        }
        if (nc != null) {
            try { nc.close(); } catch (Exception ignored) {}
        }
        if (serverRunner != null) {
            try { serverRunner.close(); } catch (Exception ignored) {}
        }
    }

    private String uniqueName(String prefix) {
        consumerName = prefix + "-" + System.nanoTime();
        return consumerName;
    }

    private void waitForCondition(long timeoutMs, java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean()) {
            Thread.sleep(100);
            if (System.currentTimeMillis() > deadline) {
                fail("Timed out waiting for condition");
            }
        }
    }

    // Test 1: Basic consume
    @Test
    void testBasicConsume() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();
        String name = uniqueName("test-basic");

        ConsumerConfig config = new ConsumerConfig.Builder(TEST_STREAM, name).build();
        consumer = OffsetConsumerFactory.create(nc, config);

        AtomicLong count = new AtomicLong(0);
        consumer.consume(msg -> count.incrementAndGet());

        waitForCondition(5000, () -> count.get() >= NUM_MESSAGES);

        long n = count.get();
        assertTrue(n > 0, "Expected to receive messages, got 0");
        System.out.println("testBasicConsume: received " + n + " messages");
    }

    // Test 2: CommitSync and resume
    @Test
    void testCommitSyncAndResume() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();
        String name = uniqueName("test-resume");

        // Phase 1: consume some messages and commit
        ConsumerConfig config1 = new ConsumerConfig.Builder(TEST_STREAM, name).build();
        OffsetConsumer c1 = OffsetConsumerFactory.create(nc, config1);

        AtomicLong lastSeq = new AtomicLong(0);
        c1.consume(msg -> {
            long seq = msg.metaData().streamSequence();
            lastSeq.set(seq);
        });

        waitForCondition(5000, () -> lastSeq.get() >= 10);

        long committedSeq = lastSeq.get();
        assertTrue(committedSeq > 0, "No messages received in phase 1");

        c1.commitSync();
        System.out.println("testCommitSyncAndResume: phase 1 committed at seq " + committedSeq);
        c1.close();

        // Publish more messages so the resumed consumer has something to consume
        for (int i = NUM_MESSAGES + 1; i <= NUM_MESSAGES + 10; i++) {
            nc.jetStream().publish("test." + i, ("msg-" + i).getBytes());
        }

        // Phase 2: reopen and verify resume from committed position
        ConsumerConfig config2 = new ConsumerConfig.Builder(TEST_STREAM, name).build();
        consumer = OffsetConsumerFactory.create(nc, config2);

        AtomicLong firstResumeSeq = new AtomicLong(0);
        AtomicBoolean gotFirst = new AtomicBoolean(false);
        consumer.consume(msg -> {
            if (gotFirst.compareAndSet(false, true)) {
                firstResumeSeq.set(msg.metaData().streamSequence());
            }
        });

        waitForCondition(5000, () -> gotFirst.get());

        long resumeSeq = firstResumeSeq.get();
        System.out.println("testCommitSyncAndResume: phase 2 first message at seq " + resumeSeq
                + " (expected " + (committedSeq + 1) + ")");
        assertEquals(committedSeq + 1, resumeSeq,
                "Expected resume at seq " + (committedSeq + 1) + ", got " + resumeSeq);
    }

    // Test 3: CommitAsync
    @Test
    void testCommitAsync() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();
        String name = uniqueName("test-async");

        ConsumerConfig config = new ConsumerConfig.Builder(TEST_STREAM, name).build();
        consumer = OffsetConsumerFactory.create(nc, config);

        AtomicLong count = new AtomicLong(0);
        consumer.consume(msg -> count.incrementAndGet());

        waitForCondition(5000, () -> count.get() > 0);

        CompletableFuture<Void> future = consumer.commitAsync();
        future.get(5, TimeUnit.SECONDS);

        System.out.println("testCommitAsync: commitAsync succeeded, " + count.get() + " messages consumed");

        // Second async commit after first completed should also work
        CompletableFuture<Void> future2 = consumer.commitAsync();
        future2.get(5, TimeUnit.SECONDS);
    }

    // Test 4: AutoCommit
    @Test
    void testAutoCommit() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();
        String name = uniqueName("test-autocommit");

        ConsumerConfig config1 = new ConsumerConfig.Builder(TEST_STREAM, name)
                .autoCommit(true)
                .autoCommitInterval(Duration.ofSeconds(1))
                .build();
        OffsetConsumer c1 = OffsetConsumerFactory.create(nc, config1);

        AtomicLong count = new AtomicLong(0);
        c1.consume(msg -> count.incrementAndGet());

        waitForCondition(5000, () -> count.get() >= NUM_MESSAGES);

        // Wait for auto-commit to fire
        Thread.sleep(2000);

        long n = count.get();
        System.out.println("testAutoCommit: received " + n + " messages before close");

        c1.close();

        // Verify offset was committed by reopening
        ConsumerConfig config2 = new ConsumerConfig.Builder(TEST_STREAM, name).build();
        consumer = OffsetConsumerFactory.create(nc, config2);

        AtomicLong firstSeq = new AtomicLong(0);
        AtomicBoolean gotFirst = new AtomicBoolean(false);

        // Publish more messages for the resumed consumer
        for (int i = NUM_MESSAGES + 1; i <= NUM_MESSAGES + 10; i++) {
            nc.jetStream().publish("test." + i, ("msg-" + i).getBytes());
        }

        consumer.consume(msg -> {
            if (gotFirst.compareAndSet(false, true)) {
                firstSeq.set(msg.metaData().streamSequence());
            }
        });

        waitForCondition(5000, () -> gotFirst.get());

        long seq = firstSeq.get();
        assertTrue(seq > 1, "Expected non-zero stored offset after auto-commit, first seq was " + seq);
        System.out.println("testAutoCommit: after reopen, first message at seq " + seq);
    }

    // Test 5: Seek
    @Test
    void testSeek() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();
        String name = uniqueName("test-seek");

        // Phase 1: create consumer, consume, commit, close
        ConsumerConfig config1 = new ConsumerConfig.Builder(TEST_STREAM, name).build();
        OffsetConsumer c1 = OffsetConsumerFactory.create(nc, config1);

        AtomicLong lastSeq = new AtomicLong(0);
        c1.consume(msg -> lastSeq.set(msg.metaData().streamSequence()));

        waitForCondition(5000, () -> lastSeq.get() > 0);
        c1.commitSync();
        c1.close();

        // Phase 2: reopen, seek to 50, consume and check first message
        ConsumerConfig config2 = new ConsumerConfig.Builder(TEST_STREAM, name).build();
        consumer = OffsetConsumerFactory.create(nc, config2);

        consumer.seek(50);

        AtomicLong firstSeq = new AtomicLong(0);
        AtomicBoolean gotFirst = new AtomicBoolean(false);
        consumer.consume(msg -> {
            if (gotFirst.compareAndSet(false, true)) {
                firstSeq.set(msg.metaData().streamSequence());
            }
        });

        waitForCondition(5000, () -> gotFirst.get());

        long seq = firstSeq.get();
        System.out.println("testSeek: first message after Seek(50): seq " + seq + " (expected 51)");
        assertEquals(51, seq, "Expected seq 51 after Seek(50), got " + seq);
    }

    // Test 6: DeleteConsumer
    @Test
    void testDeleteConsumer() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();
        String name = uniqueName("test-delete");

        // Create and commit an offset
        ConsumerConfig config1 = new ConsumerConfig.Builder(TEST_STREAM, name).build();
        OffsetConsumer c1 = OffsetConsumerFactory.create(nc, config1);

        AtomicLong lastSeq = new AtomicLong(0);
        c1.consume(msg -> lastSeq.set(msg.metaData().streamSequence()));

        waitForCondition(5000, () -> lastSeq.get() > 0);
        c1.commitSync();
        c1.close();

        // Reopen — should have a stored offset
        ConsumerConfig config2 = new ConsumerConfig.Builder(TEST_STREAM, name).build();
        OffsetConsumer c2 = OffsetConsumerFactory.create(nc, config2);

        // Publish more so resumed consumer has something to consume
        for (int i = NUM_MESSAGES + 1; i <= NUM_MESSAGES + 10; i++) {
            nc.jetStream().publish("test." + i, ("msg-" + i).getBytes());
        }

        AtomicLong c2FirstSeq = new AtomicLong(0);
        AtomicBoolean c2GotFirst = new AtomicBoolean(false);
        c2.consume(msg -> {
            if (c2GotFirst.compareAndSet(false, true)) {
                c2FirstSeq.set(msg.metaData().streamSequence());
            }
        });

        waitForCondition(5000, () -> c2GotFirst.get());
        long beforeDeleteSeq = c2FirstSeq.get();
        assertTrue(beforeDeleteSeq > 1, "Expected non-zero startSeq before delete, got " + beforeDeleteSeq);
        System.out.println("testDeleteConsumer: before delete, first seq = " + beforeDeleteSeq);

        c2.deleteConsumer();
        c2.close();

        // New consumer with same name should start from beginning
        ConsumerConfig config3 = new ConsumerConfig.Builder(TEST_STREAM, name).build();
        consumer = OffsetConsumerFactory.create(nc, config3);

        AtomicLong c3FirstSeq = new AtomicLong(0);
        AtomicBoolean c3GotFirst = new AtomicBoolean(false);
        consumer.consume(msg -> {
            if (c3GotFirst.compareAndSet(false, true)) {
                c3FirstSeq.set(msg.metaData().streamSequence());
            }
        });

        waitForCondition(5000, () -> c3GotFirst.get());
        long afterDeleteSeq = c3FirstSeq.get();
        System.out.println("testDeleteConsumer: after delete, first seq = " + afterDeleteSeq + " (expected 1)");
        assertEquals(1, afterDeleteSeq, "Expected to start from seq 1 after delete, got " + afterDeleteSeq);
    }

    // Test 7: Invalid consumer name
    @Test
    void testInvalidConsumerName() {
        assertThrows(IllegalArgumentException.class, () ->
                new ConsumerConfig.Builder(TEST_STREAM, ""));

        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsServerRunner runner = NatsServerRunner.builder().jetstream(true).build()) {
                Options options = Options.builder()
                        .server(runner.getURI())
                        .errorListener(new ErrorListener() {})
                        .build();
                Connection tempNc = Nats.connect(options);
                try {
                    ConsumerConfig config = new ConsumerConfig.Builder(TEST_STREAM, "has.dot").build();
                    OffsetConsumerFactory.create(tempNc, config);
                } finally {
                    tempNc.close();
                }
            }
        });

        System.out.println("testInvalidConsumerName: validation works correctly");
    }

    // Test 8: Already consuming
    @Test
    void testAlreadyConsuming() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();
        String name = uniqueName("test-already");

        ConsumerConfig config = new ConsumerConfig.Builder(TEST_STREAM, name).build();
        consumer = OffsetConsumerFactory.create(nc, config);

        consumer.consume(msg -> {});

        assertThrows(IllegalStateException.class, () ->
                consumer.consume(msg -> {}));

        System.out.println("testAlreadyConsuming: IllegalStateException thrown correctly");
    }

    // Test 9: StartPosition End
    @Test
    void testStartPositionEnd() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();
        String name = uniqueName("test-startend");

        ConsumerConfig config = new ConsumerConfig.Builder(TEST_STREAM, name)
                .startPosition(StartPosition.END)
                .build();
        consumer = OffsetConsumerFactory.create(nc, config);

        AtomicLong count = new AtomicLong(0);
        consumer.consume(msg -> count.incrementAndGet());

        // With StartPositionEnd and no new publishes, should get 0 messages
        Thread.sleep(2000);
        long n = count.get();
        System.out.println("testStartPositionEnd: messages received (no new publishes): " + n);
        assertEquals(0, n, "Expected 0 messages with StartPositionEnd (no new publishes), got " + n);
    }
}
