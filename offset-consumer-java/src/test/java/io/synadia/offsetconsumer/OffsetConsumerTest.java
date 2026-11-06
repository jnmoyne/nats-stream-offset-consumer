package io.synadia.offsetconsumer;

import io.nats.NatsServerRunner;
import io.nats.client.Connection;
import io.nats.client.ErrorListener;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
        nc = connect();
        return nc;
    }

    private Connection connect() throws Exception {
        Options options = Options.builder()
                .server(serverRunner.getNatsLocalhostUri())
                .errorListener(new ErrorListener() {})
                .build();
        return Nats.connect(options);
    }

    private void createStreamAndPublish() throws Exception {
        nc.jetStreamManagement().addStream(StreamConfiguration.builder()
                .name(TEST_STREAM)
                .subjects("test.>")
                .storageType(StorageType.Memory)
                .build());

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
            Thread.sleep(50);
            if (System.currentTimeMillis() > deadline) {
                fail("Timed out waiting for condition");
            }
        }
    }

    private List<String> consumerNamesWithPrefix(String prefix) throws Exception {
        List<String> names = new ArrayList<>();
        for (String name : nc.jetStreamManagement().getConsumerNames(TEST_STREAM)) {
            if (name.startsWith(prefix)) {
                names.add(name);
            }
        }
        return names;
    }

    private OffsetConsumer consumeUntilDelivered(ConsumerConfig config) throws Exception {
        OffsetConsumer c = OffsetConsumerFactory.create(nc, config);
        AtomicLong lastSeq = new AtomicLong(0);
        c.consume(msg -> lastSeq.set(msg.metaData().streamSequence()));
        waitForCondition(5000, () -> lastSeq.get() > 0);
        return c;
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

        ConsumerConfig config1 = new ConsumerConfig.Builder(TEST_STREAM, name).build();
        OffsetConsumer c1 = OffsetConsumerFactory.create(nc, config1);

        AtomicLong lastSeq = new AtomicLong(0);
        c1.consume(msg -> lastSeq.set(msg.metaData().streamSequence()));

        waitForCondition(5000, () -> lastSeq.get() >= 10);

        long committedSeq = lastSeq.get();
        assertTrue(committedSeq > 0, "No messages received in phase 1");

        c1.commitSync();
        System.out.println("testCommitSyncAndResume: phase 1 committed at seq " + committedSeq);
        c1.close();

        for (int i = NUM_MESSAGES + 1; i <= NUM_MESSAGES + 10; i++) {
            nc.jetStream().publish("test." + i, ("msg-" + i).getBytes());
        }

        ConsumerConfig config2 = new ConsumerConfig.Builder(TEST_STREAM, name).build();
        consumer = OffsetConsumerFactory.create(nc, config2);

        AtomicLong firstResumeSeq = new AtomicLong(0);
        AtomicBoolean gotFirst = new AtomicBoolean(false);
        consumer.consume(msg -> {
            if (gotFirst.compareAndSet(false, true)) {
                firstResumeSeq.set(msg.metaData().streamSequence());
            }
        });

        waitForCondition(5000, gotFirst::get);

        long resumeSeq = firstResumeSeq.get();
        System.out.println("testCommitSyncAndResume: phase 2 first message at seq " + resumeSeq
                + " (expected " + (committedSeq + 1) + ")");
        assertEquals(committedSeq + 1, resumeSeq);
    }

    // Test 3: CommitAsync completes and the stored offset reflects the ack
    @Test
    void testCommitAsync() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();
        String name = uniqueName("test-async");

        ConsumerConfig config = new ConsumerConfig.Builder(TEST_STREAM, name).build();
        consumer = OffsetConsumerFactory.create(nc, config);

        AtomicLong lastSeq = new AtomicLong(0);
        consumer.consume(msg -> lastSeq.set(msg.metaData().streamSequence()));
        waitForCondition(5000, () -> lastSeq.get() >= NUM_MESSAGES);
        long delivered = lastSeq.get();

        CompletableFuture<Void> future = consumer.commitAsync();
        future.get(5, TimeUnit.SECONDS);

        OffsetConsumerInfo info = consumer.info();
        assertNotNull(info);
        assertEquals(delivered, info.getSeq(), "stored seq must match the delivered seq after the ack");
        System.out.println("testCommitAsync: committed seq " + info.getSeq());
    }

    // Test 4: several async commits may be pending at once; all complete in order
    @Test
    void testCommitAsyncChained() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();
        String name = uniqueName("test-chained");

        consumer = consumeUntilDelivered(new ConsumerConfig.Builder(TEST_STREAM, name).build());

        CompletableFuture<Void> f1 = consumer.commitAsync();
        CompletableFuture<Void> f2 = consumer.commitAsync();
        CompletableFuture<Void> f3 = consumer.commitAsync();
        f1.get(5, TimeUnit.SECONDS);
        f2.get(5, TimeUnit.SECONDS);
        f3.get(5, TimeUnit.SECONDS);
        System.out.println("testCommitAsyncChained: three pipelined async commits acknowledged");
    }

    // Test 5: nothing to commit completes immediately
    @Test
    void testCommitAsyncNothingToCommit() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();
        String name = uniqueName("test-nothing");

        consumer = OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, name).build());
        CompletableFuture<Void> f = consumer.commitAsync();
        assertTrue(f.isDone());
        f.get(1, TimeUnit.SECONDS);
        consumer.commitSync();
    }

    // Test 6: AutoCommit
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
        Thread.sleep(2000);

        long n = count.get();
        System.out.println("testAutoCommit: received " + n + " messages before close");

        c1.close();

        ConsumerConfig config2 = new ConsumerConfig.Builder(TEST_STREAM, name).build();
        consumer = OffsetConsumerFactory.create(nc, config2);

        AtomicLong firstSeq = new AtomicLong(0);
        AtomicBoolean gotFirst = new AtomicBoolean(false);

        for (int i = NUM_MESSAGES + 1; i <= NUM_MESSAGES + 10; i++) {
            nc.jetStream().publish("test." + i, ("msg-" + i).getBytes());
        }

        consumer.consume(msg -> {
            if (gotFirst.compareAndSet(false, true)) {
                firstSeq.set(msg.metaData().streamSequence());
            }
        });

        waitForCondition(5000, gotFirst::get);

        long seq = firstSeq.get();
        assertTrue(seq > 1, "Expected non-zero stored offset after auto-commit, first seq was " + seq);
        System.out.println("testAutoCommit: after reopen, first message at seq " + seq);
    }

    // Test 7: Seek before consuming
    @Test
    void testSeek() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();
        String name = uniqueName("test-seek");

        OffsetConsumer c1 = consumeUntilDelivered(new ConsumerConfig.Builder(TEST_STREAM, name).build());
        c1.commitSync();
        c1.close();

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

        waitForCondition(5000, gotFirst::get);

        long seq = firstSeq.get();
        System.out.println("testSeek: first message after Seek(50): seq " + seq + " (expected 51)");
        assertEquals(51, seq);
    }

    // Test 8: Seek while consuming restarts under a new generation prefix
    @Test
    void testSeekRestartsWithNewGeneration() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();
        String name = uniqueName("test-seekgen");

        consumer = OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, name).build());
        String owner = consumer.info().getOwner();

        AtomicLong lastSeq = new AtomicLong(0);
        AtomicBoolean seeked = new AtomicBoolean(false);
        AtomicLong firstAfterSeek = new AtomicLong(0);
        consumer.consume(msg -> {
            long seq = msg.metaData().streamSequence();
            lastSeq.set(seq);
            if (seeked.get()) {
                firstAfterSeek.compareAndSet(0, seq);
            }
        });
        waitForCondition(5000, () -> lastSeq.get() >= NUM_MESSAGES);

        List<String> before = consumerNamesWithPrefix(owner);
        assertEquals(1, before.size(), "expected one generation-1 consumer, got " + before);
        assertTrue(before.get(0).contains("-g1-"), before.get(0));

        seeked.set(true);
        consumer.seek(50);

        waitForCondition(5000, () -> firstAfterSeek.get() != 0);
        assertEquals(51, firstAfterSeek.get());

        waitForCondition(5000, () -> {
            try {
                List<String> after = consumerNamesWithPrefix(owner);
                return after.size() == 1 && after.get(0).contains("-g2-");
            } catch (Exception e) {
                return false;
            }
        });
        System.out.println("testSeekRestartsWithNewGeneration: " + consumerNamesWithPrefix(owner));
    }

    // Test 9: DeleteConsumer
    @Test
    void testDeleteConsumer() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();
        String name = uniqueName("test-delete");

        OffsetConsumer c1 = consumeUntilDelivered(new ConsumerConfig.Builder(TEST_STREAM, name).build());
        c1.commitSync();
        c1.close();

        ConsumerConfig config2 = new ConsumerConfig.Builder(TEST_STREAM, name).build();
        OffsetConsumer c2 = OffsetConsumerFactory.create(nc, config2);
        assertTrue(c2.info().getSeq() > 0, "expected stored offset before delete");

        c2.deleteConsumer();
        c2.close();
        assertNull(c2.info(), "close after deleteConsumer must not recreate the entry");

        ConsumerConfig config3 = new ConsumerConfig.Builder(TEST_STREAM, name).build();
        consumer = OffsetConsumerFactory.create(nc, config3);

        AtomicLong c3FirstSeq = new AtomicLong(0);
        AtomicBoolean c3GotFirst = new AtomicBoolean(false);
        consumer.consume(msg -> {
            if (c3GotFirst.compareAndSet(false, true)) {
                c3FirstSeq.set(msg.metaData().streamSequence());
            }
        });

        waitForCondition(5000, c3GotFirst::get);
        long afterDeleteSeq = c3FirstSeq.get();
        System.out.println("testDeleteConsumer: after delete, first seq = " + afterDeleteSeq + " (expected 1)");
        assertEquals(1, afterDeleteSeq);
    }

    // Test 10: Invalid consumer name
    @Test
    void testInvalidConsumerName() throws Exception {
        assertThrows(IllegalArgumentException.class, () ->
                new ConsumerConfig.Builder(TEST_STREAM, ""));

        startServerAndConnect();
        createStreamAndPublish();
        assertThrows(IllegalArgumentException.class, () -> {
            ConsumerConfig config = new ConsumerConfig.Builder(TEST_STREAM, "has.dot").build();
            OffsetConsumerFactory.create(nc, config);
        });

        System.out.println("testInvalidConsumerName: validation works correctly");
    }

    // Test 11: Already consuming
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

    // Test 12: StartPosition End
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

        Thread.sleep(2000);
        long n = count.get();
        System.out.println("testStartPositionEnd: messages received (no new publishes): " + n);
        assertEquals(0, n);
    }

    // Test 13: info() and listConsumers() expose the ownership fields
    @Test
    void testInfoAndListHaveOwner() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();
        String name = uniqueName("test-owner");

        consumer = OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, name).build());

        OffsetConsumerInfo info = consumer.info();
        assertNotNull(info, "expected an entry right after create (claim)");
        String wantPrefix = "oc-" + TEST_STREAM + "-" + name + "-";
        assertTrue(info.getOwner().startsWith(wantPrefix), info.getOwner());
        assertEquals(OffsetConsumerFactory.hostname(), info.getHost());
        Instant.parse(info.getClaimedAt());
        assertEquals(0, info.getSeq());

        AtomicLong lastSeq = new AtomicLong(0);
        consumer.consume(msg -> lastSeq.set(msg.metaData().streamSequence()));
        waitForCondition(5000, () -> lastSeq.get() > 0);
        consumer.commitSync();

        Map<String, OffsetConsumerInfo> list = OffsetConsumerFactory.listConsumers(nc, TEST_STREAM);
        OffsetConsumerInfo listed = list.get(name);
        assertNotNull(listed, "listConsumers missing " + name + ": " + list);
        assertEquals(info.getOwner(), listed.getOwner());
        assertTrue(listed.getSeq() > 0);

        OffsetConsumerInfo after = consumer.info();
        assertEquals(info.getOwner(), after.getOwner(), "commit must keep the owner");
        assertEquals(info.getClaimedAt(), after.getClaimedAt(), "commit must keep the claim time");
    }

    // Test 14: a second instance is refused while the first is alive
    @Test
    void testSecondInstanceRefused() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();
        String name = uniqueName("test-refused");

        consumer = consumeUntilDelivered(new ConsumerConfig.Builder(TEST_STREAM, name).build());
        OffsetConsumerInfo info = consumer.info();

        AlreadyInUseException e = assertThrows(AlreadyInUseException.class, () ->
                OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, name).build()));
        assertEquals(info.getOwner(), e.getOwner());
        assertEquals(info.getHost(), e.getHost());
        assertEquals(info.getClaimedAt(), e.getClaimedAt());
        System.out.println("testSecondInstanceRefused: " + e.getMessage());

        assertEquals(info.getOwner(), consumer.info().getOwner(), "refused attempt must not disturb the owner");
    }

    // Test 15: close deletes the ordered consumer and releases ownership
    @Test
    void testReleaseOnClose() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();
        String name = uniqueName("test-release");

        OffsetConsumer c1 = consumeUntilDelivered(new ConsumerConfig.Builder(TEST_STREAM, name).build());
        c1.commitSync();
        String owner1 = c1.info().getOwner();
        long committed = c1.info().getSeq();

        assertEquals(1, consumerNamesWithPrefix(owner1).size(), "expected one live ordered consumer before close");
        c1.close();
        assertEquals(0, consumerNamesWithPrefix(owner1).size(), "expected ordered consumer deleted on close");

        OffsetConsumerInfo released = c1.info();
        assertNotNull(released);
        assertEquals("", released.getOwner());
        assertEquals("", released.getHost());
        assertEquals("", released.getClaimedAt());
        assertEquals(committed, released.getSeq(), "close must keep the committed seq (autoCommit off)");

        consumer = OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, name).build());
        OffsetConsumerInfo info2 = consumer.info();
        assertFalse(info2.getOwner().isEmpty());
        assertNotEquals(owner1, info2.getOwner());
        assertEquals(committed, info2.getSeq());
    }

    // Test 16b: the bucket is created with a 10 minute delete-marker TTL
    @Test
    void testBucketDeleteMarkerTtl() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();
        consumer = OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, uniqueName("test-ttl")).build());

        io.nats.client.api.StreamConfiguration sc = nc.jetStreamManagement()
                .getStreamInfo("KV_" + OffsetConsumerFactory.KV_BUCKET_NAME)
                .getConfiguration();
        assertEquals(Duration.ofMinutes(10), sc.getSubjectDeleteMarkerTtl());
        assertTrue(sc.getAllowMessageTtl(), "per-message TTL must be enabled on the bucket stream");
    }

    // Test 16c: close with a backlog of async commits persists the highest
    // requested position and releases ownership once the bounded wait expires
    @Test
    void testCloseWithCommitBacklog() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();
        String name = uniqueName("test-backlog");

        OffsetConsumer c = OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, name).build());
        AtomicLong lastSeq = new AtomicLong(0);
        c.consume(msg -> lastSeq.set(msg.metaData().streamSequence()));
        waitForCondition(5000, () -> lastSeq.get() >= NUM_MESSAGES);
        long delivered = lastSeq.get();
        String owner = c.info().getOwner();

        CompletableFuture<Void> last = null;
        for (int i = 0; i < 20000; i++) {
            last = c.commitAsync();
        }

        long previous = OffsetConsumerImpl.closeTimeoutMillis;
        OffsetConsumerImpl.closeTimeoutMillis = 100;
        long start = System.currentTimeMillis();
        try {
            c.close();
        } finally {
            OffsetConsumerImpl.closeTimeoutMillis = previous;
        }
        long took = System.currentTimeMillis() - start;
        assertTrue(took < 3000, "close took " + took + " ms");
        last.get(5, TimeUnit.SECONDS);

        OffsetConsumerInfo info = c.info();
        assertEquals(delivered, info.getSeq(), "release write must persist the highest requested seq");
        assertEquals("", info.getOwner(), "ownership must be released");
        assertEquals(0, consumerNamesWithPrefix(owner).size());
        c.deleteConsumer();
    }

    // Test 16: after a crash the new instance is refused until the ordered
    // consumer expires (server default inactive threshold of 5s), then allowed
    @Test
    void testCrashRecovery() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();
        String name = uniqueName("test-crash");

        Connection crashed = connect();
        OffsetConsumer c1 = OffsetConsumerFactory.create(crashed, new ConsumerConfig.Builder(TEST_STREAM, name).build());
        AtomicLong lastSeq = new AtomicLong(0);
        c1.consume(msg -> lastSeq.set(msg.metaData().streamSequence()));
        waitForCondition(5000, () -> lastSeq.get() > 0);
        String owner1 = c1.info().getOwner();

        // Simulate a crash: drop the connection without closing the consumer.
        crashed.close();

        ConsumerConfig config = new ConsumerConfig.Builder(TEST_STREAM, name).build();
        assertThrows(AlreadyInUseException.class, () -> OffsetConsumerFactory.create(nc, config));

        long start = System.currentTimeMillis();
        long deadline = start + 30_000;
        OffsetConsumer recovered = null;
        while (recovered == null) {
            try {
                recovered = OffsetConsumerFactory.create(nc, config);
            } catch (AlreadyInUseException e) {
                assertTrue(System.currentTimeMillis() < deadline, "new instance never allowed after crash");
                Thread.sleep(250);
            }
        }
        consumer = recovered;
        System.out.println("testCrashRecovery: recovered after " + (System.currentTimeMillis() - start) + " ms");
        assertEquals(0, consumerNamesWithPrefix(owner1).size(), "crashed owner's consumer still present");
        assertNotEquals(owner1, consumer.info().getOwner());
    }
}
