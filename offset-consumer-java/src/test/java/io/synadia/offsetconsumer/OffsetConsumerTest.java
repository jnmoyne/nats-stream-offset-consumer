package io.synadia.offsetconsumer;

import io.nats.NatsServerRunner;
import io.nats.client.Connection;
import io.nats.client.ErrorListener;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.KeyValueConfiguration;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the OffsetConsumer library.
 * Uses an embedded nats-server via jnats-server-runner.
 */
class OffsetConsumerTest {

    private static final String TEST_STREAM = "test-stream";
    private static final int NUM_MESSAGES = 100;

    static {
        // Keep the takeover wait short in tests; tests of the wait set it explicitly.
        OffsetConsumerImpl.livenessWaitMillis = 300;
    }

    private NatsServerRunner serverRunner;
    private Connection nc;
    private OffsetConsumer consumer;
    private String consumerName;
    /** Extra consumers closed before the server stops (otherwise each close waits on timeouts). */
    private final List<OffsetConsumer> extra = new CopyOnWriteArrayList<>();

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
        for (OffsetConsumer c : extra) {
            try { c.close(); } catch (Exception ignored) {}
        }
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

        assertTrue(lastSeq.get() > 0, "No messages received in phase 1");

        c1.commitSync();
        // Delivery continues while committing: use the sequence actually stored.
        long committedSeq = c1.info().getSeq();
        assertTrue(committedSeq >= 10, "expected a committed seq >= 10, got " + committedSeq);
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
        String owner = ((OffsetConsumerImpl) consumer).owner();

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

        List<String> before = consumerNamesWithPrefix(owner + "-g");
        assertEquals(1, before.size(), "expected one generation-1 consumer, got " + before);
        assertTrue(before.get(0).contains("-g1-"), before.get(0));

        seeked.set(true);
        consumer.seek(50);

        waitForCondition(5000, () -> firstAfterSeek.get() != 0);
        assertEquals(51, firstAfterSeek.get());

        waitForCondition(5000, () -> {
            try {
                List<String> after = consumerNamesWithPrefix(owner + "-g");
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

        StringBuilder longName = new StringBuilder();
        for (int i = 0; i < 200; i++) {
            longName.append('x');
        }
        for (String name : new String[]{"has*star", "has>gt", "has space", "has/slash", longName.toString()}) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                    OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, name).build()), name);
            assertTrue(e.getMessage().startsWith("Invalid consumer name"), e.getMessage());
        }
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder("bad stream", "ok").build()));
        assertTrue(e.getMessage().startsWith("Invalid stream name"), e.getMessage());

        consumer = OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, "Ok_name-1=x").build());

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
        assertNull(consumer.info(), "create must not write the entry");

        AtomicLong lastSeq = new AtomicLong(0);
        consumer.consume(msg -> lastSeq.set(msg.metaData().streamSequence()));

        OffsetConsumerInfo info = consumer.info();
        assertNotNull(info, "expected an entry right after consume (claim)");
        String wantPrefix = "oc-" + TEST_STREAM + "-" + name + "-";
        assertTrue(info.getOwner().startsWith(wantPrefix), info.getOwner());
        assertEquals(OffsetConsumerFactory.hostname(), info.getHost());

        waitForCondition(5000, () -> lastSeq.get() > 0);
        consumer.commitSync();

        Map<String, OffsetConsumerInfo> list = OffsetConsumerFactory.listConsumers(nc, TEST_STREAM);
        OffsetConsumerInfo listed = list.get(name);
        assertNotNull(listed, "listConsumers missing " + name + ": " + list);
        assertEquals(info.getOwner(), listed.getOwner());
        assertTrue(listed.getSeq() > 0);

        OffsetConsumerInfo after = consumer.info();
        assertEquals(info.getOwner(), after.getOwner(), "commit must keep the owner");
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

        assertEquals(1, consumerNamesWithPrefix(owner1 + "-g").size(), "expected one live ordered consumer before close");
        c1.close();
        assertEquals(0, consumerNamesWithPrefix(owner1).size(), "expected ordered consumer deleted on close");

        OffsetConsumerInfo released = c1.info();
        assertNotNull(released);
        assertEquals("", released.getOwner());
        assertEquals("", released.getHost());
        assertEquals(committed, released.getSeq(), "close must keep the committed seq (autoCommit off)");

        // Immediate restart after close: the released entry is claimed without waiting.
        consumer = OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, name).build());
        consumer.consume(msg -> {});
        OffsetConsumerInfo info2 = consumer.info();
        assertFalse(info2.getOwner().isEmpty());
        assertNotEquals(owner1, info2.getOwner());
        assertEquals(committed, info2.getSeq());
        assertEquals(committed, ((OffsetConsumerImpl) consumer).startSeq());
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
        assertEquals(0, consumerNamesWithPrefix(owner1).size(), "crashed owner's consumer still present");
        // The claim at consume() waits the liveness wait before taking over.
        consumer.consume(msg -> {});
        System.out.println("testCrashRecovery: recovered after " + (System.currentTimeMillis() - start) + " ms");
        assertNotEquals(owner1, consumer.info().getOwner());
    }

    // ---- ported from the Go reference (processed sequence, END at creation,
    // bucket handling, lifecycle lock, stream recreation) ----

    private OffsetConsumer track(OffsetConsumer c) {
        extra.add(c);
        return c;
    }

    private long storedSeq(OffsetConsumer c) {
        try {
            OffsetConsumerInfo info = c.info();
            return info == null ? 0 : info.getSeq();
        } catch (Exception e) {
            return -1;
        }
    }

    private void publish(int n, String subject) throws Exception {
        for (int i = 0; i < n; i++) {
            nc.jetStream().publish(subject, "again".getBytes());
        }
    }

    /** Deletes the test stream, recreates it empty, and publishes n messages. */
    private void recreateStream(int n) throws Exception {
        nc.jetStreamManagement().deleteStream(TEST_STREAM);
        nc.jetStreamManagement().addStream(StreamConfiguration.builder()
                .name(TEST_STREAM)
                .subjects("test.>")
                .storageType(StorageType.Memory)
                .build());
        publish(n, "test.again");
    }

    // Auto-commit must not commit a message whose handler is still running.
    @Test
    void testAutoCommitSkipsMessageInHandler() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();

        consumer = OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, uniqueName("test-ac-skip"))
                .autoCommit(true)
                .autoCommitInterval(Duration.ofMillis(100))
                .build());
        CountDownLatch release = new CountDownLatch(1);
        consumer.consume(msg -> {
            if (msg.metaData().streamSequence() == 3) {
                release.await();
            }
        });

        OffsetConsumer c = consumer;
        waitForCondition(5000, () -> storedSeq(c) == 2);
        Thread.sleep(500);
        assertEquals(2, storedSeq(c), "auto-commit committed a message still in its handler");
        release.countDown();
        waitForCondition(5000, () -> storedSeq(c) == NUM_MESSAGES);
    }

    // close() with auto-commit must not commit a message whose handler is still running.
    @Test
    void testCloseAutoCommitSkipsMessageInHandler() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();
        String name = uniqueName("test-close-skip");

        OffsetConsumer c = track(OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, name)
                .autoCommit(true)
                .autoCommitInterval(Duration.ofHours(1))
                .build()));
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        c.consume(msg -> {
            if (msg.metaData().streamSequence() == 5) {
                entered.countDown();
                release.await();
            }
        });
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        c.close();
        release.countDown();

        assertEquals(4, storedSeq(c), "message 5 was still in its handler");
        consumer = OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, name).build());
        consumer.consume(msg -> {});
        assertEquals(4, ((OffsetConsumerImpl) consumer).startSeq());
    }

    // StartPosition.END is stored at creation, so messages published while the
    // consumer is down are delivered on the next start.
    @Test
    void testStartPositionEndPersisted() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();
        String name = uniqueName("test-end-persisted");

        // END is resolved at creation: messages published between creation and
        // consume() are delivered.
        OffsetConsumer c = track(OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, name)
                .startPosition(StartPosition.END).build()));
        publish(2, "test.early");
        AtomicLong cLast = new AtomicLong(0);
        c.consume(msg -> cLast.set(msg.metaData().streamSequence()));
        waitForCondition(5000, () -> cLast.get() == NUM_MESSAGES + 2);
        c.commitSync();
        c.close();

        publish(3, "test.new");

        consumer = OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, name)
                .startPosition(StartPosition.END).build());
        AtomicLong first = new AtomicLong(0);
        AtomicLong last = new AtomicLong(0);
        consumer.consume(msg -> {
            long seq = msg.metaData().streamSequence();
            first.compareAndSet(0, seq);
            last.set(seq);
        });
        waitForCondition(5000, () -> last.get() == NUM_MESSAGES + 5);
        assertEquals(NUM_MESSAGES + 3, first.get(), "expected the messages published while down");
    }

    // seekToBeginning goes to the beginning even with StartPosition.END.
    @Test
    void testSeekToBeginningWithStartPositionEnd() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();

        consumer = OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, uniqueName("test-end-begin"))
                .startPosition(StartPosition.END).build());
        AtomicLong first = new AtomicLong(0);
        AtomicLong last = new AtomicLong(0);
        consumer.consume(msg -> {
            long seq = msg.metaData().streamSequence();
            first.compareAndSet(0, seq);
            last.set(seq);
        });
        consumer.seekToBeginning();
        waitForCondition(5000, () -> last.get() == NUM_MESSAGES);
        assertEquals(1, first.get());
    }

    // An existing bucket's configuration is not overwritten.
    @Test
    void testExistingBucketNotOverwritten() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();

        nc.keyValueManagement().create(KeyValueConfiguration.builder()
                .name(OffsetConsumerFactory.KV_BUCKET_NAME)
                .description("operator managed")
                .maxHistoryPerKey(1)
                .storageType(StorageType.Memory)
                .build());
        consumer = OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, uniqueName("test-bucket")).build());

        StreamConfiguration sc = nc.jetStreamManagement()
                .getStreamInfo(OffsetConsumerFactory.KV_STREAM_NAME).getConfiguration();
        assertEquals("operator managed", sc.getDescription(), "bucket configuration was overwritten");
        assertEquals(StorageType.Memory, sc.getStorageType(), "bucket configuration was overwritten");
    }

    // Concurrent seeks leave exactly one ordered consumer.
    @Test
    void testConcurrentSeeks() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();

        consumer = consumeUntilDelivered(new ConsumerConfig.Builder(TEST_STREAM, uniqueName("test-concurrent")).build());
        String owner = ((OffsetConsumerImpl) consumer).owner();

        ExecutorService pool = Executors.newFixedThreadPool(10);
        try {
            List<Future<?>> futures = new ArrayList<>();
            CountDownLatch go = new CountDownLatch(1);
            for (int i = 0; i < 10; i++) {
                long seq = i * 5L;
                futures.add(pool.submit(() -> {
                    go.await();
                    consumer.seek(seq);
                    return null;
                }));
            }
            go.countDown();
            for (Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
        Thread.sleep(200);
        List<String> names = consumerNamesWithPrefix(owner + "-g");
        assertEquals(1, names.size(), "expected exactly one ordered consumer, got " + names);
    }

    // The ErrorHandler may call close() when an auto-commit fails.
    @Test
    void testErrorHandlerCanCloseOnAutoCommitFailure() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();

        CompletableFuture<Void> closed = new CompletableFuture<>();
        AtomicBoolean once = new AtomicBoolean(false);
        consumer = OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, uniqueName("test-eh-close"))
                .autoCommit(true)
                .autoCommitInterval(Duration.ofMillis(200))
                .errorHandler((oc, err) -> {
                    if (err instanceof CommitException && once.compareAndSet(false, true)) {
                        oc.close();
                        closed.complete(null);
                    }
                })
                .build());
        consumer.consume(msg -> {});
        // Make commits fail: without the bucket, KV writes get no responders.
        nc.keyValueManagement().delete(OffsetConsumerFactory.KV_BUCKET_NAME);

        closed.get(15, TimeUnit.SECONDS); // a deadlock would time out here
    }

    // A stored offset from a previous incarnation of the stream is reset.
    @Test
    void testStreamRecreatedAtStartup() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();
        String name = uniqueName("test-recreated");

        OffsetConsumer c1 = track(OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, name).build()));
        AtomicLong last = new AtomicLong(0);
        c1.consume(msg -> last.set(msg.metaData().streamSequence()));
        waitForCondition(5000, () -> last.get() == NUM_MESSAGES);
        c1.commitSync();
        String oldIdentity = c1.info().getStreamCreated();
        assertFalse(oldIdentity.isEmpty(), "entry must carry the stream identity");
        c1.close();

        recreateStream(NUM_MESSAGES + 20); // passes the old position: only the stream identity tells

        AtomicReference<StreamRecreatedException> reported = new AtomicReference<>();
        consumer = OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, name)
                .errorHandler((oc, err) -> {
                    if (err instanceof StreamRecreatedException) {
                        reported.set((StreamRecreatedException) err);
                    }
                })
                .build());
        assertNull(reported.get(), "create writes nothing and detects nothing");
        consumer.consume(msg -> {}); // the claim detects it
        assertNotNull(reported.get(), "StreamRecreatedException not reported");
        assertEquals(NUM_MESSAGES, reported.get().getStoredSeq());
        assertEquals(0, ((OffsetConsumerImpl) consumer).startSeq(), "expected offset reset to 0");
        OffsetConsumerInfo info = consumer.info();
        assertEquals(0, info.getSeq());
        assertFalse(info.getStreamCreated().isEmpty());
        assertNotEquals(oldIdentity, info.getStreamCreated(), "claim must store the new stream identity");
        assertEquals(info.getStreamCreated(),
                OffsetConsumerFactory.listConsumers(nc, TEST_STREAM).get(name).getStreamCreated());
    }

    // Legacy entries (no stream identity) past the stream's end are reset.
    @Test
    void testStreamRecreatedLegacyEntry() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();
        String name = uniqueName("test-legacy");

        nc.keyValueManagement().create(KeyValueConfiguration.builder()
                .name(OffsetConsumerFactory.KV_BUCKET_NAME)
                .maxHistoryPerKey(1)
                .build());
        nc.keyValue(OffsetConsumerFactory.KV_BUCKET_NAME)
                .put(TEST_STREAM + "." + name, "{\"seq\":5000,\"committed_at\":\"2026-01-01T00:00:00Z\"}".getBytes());

        consumer = OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, name).build());
        consumer.consume(msg -> {}); // the claim detects it
        assertEquals(0, ((OffsetConsumerImpl) consumer).startSeq(), "legacy offset beyond the stream end must be reset");
    }

    // A stream deleted and recreated while consuming is detected through the
    // ordered consumer reset, and consumption restarts from the beginning.
    @Test
    void testStreamRecreatedWhileConsuming() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();

        AtomicBoolean reported = new AtomicBoolean(false);
        consumer = OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, uniqueName("test-recreated-live"))
                .errorHandler((oc, err) -> {
                    if (err instanceof StreamRecreatedException) {
                        reported.set(true);
                    }
                })
                .build());
        AtomicLong last = new AtomicLong(0);
        AtomicLong afterReset = new AtomicLong(0);
        consumer.consume(msg -> {
            last.set(msg.metaData().streamSequence());
            if (reported.get()) {
                afterReset.incrementAndGet();
            }
        });
        waitForCondition(5000, () -> last.get() == NUM_MESSAGES);

        long start = System.currentTimeMillis();
        recreateStream(10);
        waitForCondition(30_000, reported::get);
        System.out.println("testStreamRecreatedWhileConsuming: detected after " + (System.currentTimeMillis() - start) + " ms");
        waitForCondition(10_000, () -> afterReset.get() >= 10);
        assertEquals(10, afterReset.get());
        assertEquals(10, last.get());
        String current = OffsetConsumerFactory.streamCreatedId(
                nc.jetStreamManagement().getStreamInfo(TEST_STREAM).getCreateTime());
        assertEquals(current, consumer.info().getStreamCreated(), "seek write must store the new stream identity");
    }

    // ---- ownership claim ----

    private void putEntry(String name, String json) throws Exception {
        nc.keyValueManagement().create(KeyValueConfiguration.builder()
                .name(OffsetConsumerFactory.KV_BUCKET_NAME)
                .maxHistoryPerKey(1)
                .build());
        nc.keyValue(OffsetConsumerFactory.KV_BUCKET_NAME).put(TEST_STREAM + "." + name, json.getBytes());
    }

    // Instances starting at the same time: exactly one wins the claim, and only
    // its ordered consumer exists.
    @Test
    void testConcurrentStarters() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();
        String name = uniqueName("test-starters");

        List<Connection> conns = new ArrayList<>();
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            for (int i = 0; i < 4; i++) {
                conns.add(connect());
            }
            CountDownLatch go = new CountDownLatch(1);
            List<Future<OffsetConsumer>> futures = new ArrayList<>();
            for (Connection conn : conns) {
                futures.add(pool.submit(() -> {
                    go.await();
                    OffsetConsumer c = null;
                    try {
                        c = OffsetConsumerFactory.create(conn, new ConsumerConfig.Builder(TEST_STREAM, name).build());
                        c.consume(msg -> {});
                        return c;
                    } catch (AlreadyInUseException e) {
                        if (c != null) {
                            c.close();
                        }
                        return null;
                    }
                }));
            }
            go.countDown();
            List<OffsetConsumer> winners = new ArrayList<>();
            for (Future<OffsetConsumer> f : futures) {
                OffsetConsumer c = f.get(30, TimeUnit.SECONDS);
                if (c != null) {
                    winners.add(track(c));
                }
            }
            assertEquals(1, winners.size(), "expected exactly one winner");
            String winner = ((OffsetConsumerImpl) winners.get(0)).owner();
            List<String> names = consumerNamesWithPrefix("oc-" + TEST_STREAM + "-" + name + "-");
            assertEquals(1, names.size(), "expected only the winner's ordered consumer, got " + names);
            assertTrue(names.get(0).startsWith(winner), "expected the winner's ordered consumer, got " + names);
            winners.get(0).close();
        } finally {
            pool.shutdownNow();
            for (Connection conn : conns) {
                try { conn.close(); } catch (Exception ignored) {}
            }
        }
    }

    // ---- conditional release ----

    // close() does not clear the claim of an instance admitted after this one.
    @Test
    void testCloseSkipsReleaseOfForeignEntry() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();
        String name = uniqueName("test-release-foreign");

        List<Exception> errors = new CopyOnWriteArrayList<>();
        consumer = consumeUntilDelivered(new ConsumerConfig.Builder(TEST_STREAM, name)
                .errorHandler((oc, err) -> errors.add(err)).build());
        String owner = ((OffsetConsumerImpl) consumer).owner();
        nc.keyValue(OffsetConsumerFactory.KV_BUCKET_NAME).put(TEST_STREAM + "." + name,
                "{\"seq\":3,\"committed_at\":\"\",\"owner\":\"oc-someone-else\",\"host\":\"h\",\"claimed_at\":\"\"}".getBytes());

        consumer.close();

        assertEquals(1, errors.size(), "expected one reported error, got " + errors);
        assertInstanceOf(CommitException.class, errors.get(0));
        AlreadyInUseException inUse = assertInstanceOf(AlreadyInUseException.class, errors.get(0).getCause());
        assertEquals("oc-someone-else", inUse.getOwner());
        OffsetConsumerInfo info = consumer.info();
        assertNotNull(info);
        assertEquals("oc-someone-else", info.getOwner(), "close() overwrote the other owner's entry");
        assertEquals(3, info.getSeq());
        assertEquals(0, consumerNamesWithPrefix(owner).size(), "consumers left behind after close");
    }

    // close() does not recreate an entry deleted by someone else.
    @Test
    void testCloseSkipsReleaseOfDeletedEntry() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();
        String name = uniqueName("test-release-deleted");

        List<Exception> errors = new CopyOnWriteArrayList<>();
        consumer = OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, name)
                .errorHandler((oc, err) -> errors.add(err)).build());
        consumer.consume(msg -> {}); // claims the entry
        nc.keyValue(OffsetConsumerFactory.KV_BUCKET_NAME).delete(TEST_STREAM + "." + name);

        consumer.close();

        assertTrue(errors.isEmpty(), "unexpected errors: " + errors);
        assertNull(consumer.info(), "close() recreated a deleted entry");
    }

    // The conditional release still succeeds right after a burst of async commits.
    @Test
    void testCloseReleasesAfterCommitBurst() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();
        String name = uniqueName("test-release-burst");

        List<Exception> errors = new CopyOnWriteArrayList<>();
        consumer = OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, name)
                .errorHandler((oc, err) -> errors.add(err)).build());
        AtomicLong last = new AtomicLong(0);
        consumer.consume(msg -> {
            consumer.commitAsync();
            last.set(msg.metaData().streamSequence());
        });
        waitForCondition(5000, () -> last.get() == NUM_MESSAGES);

        consumer.close();

        assertTrue(errors.isEmpty(), "unexpected errors: " + errors);
        OffsetConsumerInfo info = consumer.info();
        assertNotNull(info);
        assertEquals("", info.getOwner(), "expected a released entry");
        assertEquals(NUM_MESSAGES, info.getSeq());
    }

    // ---- claim at consume(), wait-and-recheck liveness, make-before-break seek ----

    /** Runs body with the liveness wait set to waitMillis. */
    private void withLivenessWait(long waitMillis, ThrowingRunnable body) throws Exception {
        long oldWait = OffsetConsumerImpl.livenessWaitMillis;
        OffsetConsumerImpl.livenessWaitMillis = waitMillis;
        try {
            body.run();
        } finally {
            OffsetConsumerImpl.livenessWaitMillis = oldWait;
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /** Stores an entry owned by owner, an instance that has no consumer on the server. */
    private void putOwnedEntry(String name, String owner, long seq) throws Exception {
        try {
            nc.keyValueManagement().create(KeyValueConfiguration.builder()
                    .name(OffsetConsumerFactory.KV_BUCKET_NAME)
                    .maxHistoryPerKey(1)
                    .build());
        } catch (Exception ignored) {
            // already exists
        }
        nc.keyValue(OffsetConsumerFactory.KV_BUCKET_NAME).put(TEST_STREAM + "." + name,
                ("{\"seq\":" + seq + ",\"committed_at\":\"\",\"owner\":\"" + owner + "\",\"host\":\"h\"}").getBytes());
    }

    // Nothing is written before the first consume(): construction, commits and
    // close() leave no entry and no consumer behind.
    @Test
    void testNoWritesBeforeConsume() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();

        consumer = OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, uniqueName("test-no-writes")).build());
        consumer.commitSync();
        consumer.commitAsync().get(5, TimeUnit.SECONDS);
        consumer.close();
        assertNull(consumer.info(), "expected no entry");
        assertEquals(List.of(), consumerNamesWithPrefix("oc-"), "expected no consumers");
    }

    // Two instances may be created while the entry is unowned; the second one
    // to consume() is refused because the first one's ordered consumer exists.
    @Test
    void testSecondConsumeRefused() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();
        String name = uniqueName("test-second-consume");

        consumer = OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, name).build());
        try (Connection other = connect()) {
            OffsetConsumer b = OffsetConsumerFactory.create(other, new ConsumerConfig.Builder(TEST_STREAM, name).build());
            consumer.consume(msg -> {});
            String aOwner = ((OffsetConsumerImpl) consumer).owner();
            AlreadyInUseException e = assertThrows(AlreadyInUseException.class, () -> b.consume(msg -> {}));
            assertEquals(aOwner, e.getOwner());
            b.close();
            OffsetConsumerInfo info = consumer.info();
            assertNotNull(info);
            assertEquals(aOwner, info.getOwner(), "refused instance disturbed the entry");
        }
    }

    // Several starters taking over a dead owner all wake from the liveness
    // wait at the same moment (there is no jitter): the CAS against the
    // revision read before the wait lets exactly one of them win.
    @Test
    void testConcurrentTakeover() throws Exception {
        withLivenessWait(500, () -> {
            startServerAndConnect();
            createStreamAndPublish();
            String name = uniqueName("test-takeover-race");
            putOwnedEntry(name, "oc-dead-owner", 7);

            List<Connection> conns = new ArrayList<>();
            ExecutorService pool = Executors.newFixedThreadPool(4);
            try {
                List<OffsetConsumer> consumers = new ArrayList<>();
                for (int i = 0; i < 4; i++) {
                    Connection conn = connect();
                    conns.add(conn);
                    consumers.add(OffsetConsumerFactory.create(conn, new ConsumerConfig.Builder(TEST_STREAM, name).build()));
                }
                CountDownLatch go = new CountDownLatch(1);
                List<Future<Boolean>> futures = new ArrayList<>();
                for (OffsetConsumer c : consumers) {
                    futures.add(pool.submit(() -> {
                        go.await();
                        try {
                            c.consume(msg -> {});
                            return true;
                        } catch (AlreadyInUseException e) {
                            return false;
                        }
                    }));
                }
                go.countDown();
                int wins = 0;
                for (Future<Boolean> f : futures) {
                    if (f.get(30, TimeUnit.SECONDS)) {
                        wins++;
                    }
                }
                assertEquals(1, wins, "expected exactly one winner");
                List<String> names = consumerNamesWithPrefix("oc-" + TEST_STREAM + "-" + name + "-");
                assertEquals(1, names.size(), "expected only the winner's ordered consumer, got " + names);
                for (OffsetConsumer c : consumers) {
                    c.close();
                }
            } finally {
                pool.shutdownNow();
                for (Connection conn : conns) {
                    try { conn.close(); } catch (Exception ignored) {}
                }
            }
        });
    }

    // An owner without a consumer is taken over only after the liveness wait,
    // keeping the stored offset.
    @Test
    void testTakeoverAfterLivenessWait() throws Exception {
        withLivenessWait(1000, () -> {
            startServerAndConnect();
            createStreamAndPublish();
            String name = uniqueName("test-takeover");

            putOwnedEntry(name, "oc-gone-owner", 7);
            consumer = OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, name).build());
            long start = System.nanoTime();
            consumer.consume(msg -> {});
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertTrue(elapsedMs >= 1000, "took over after " + elapsedMs + " ms, before the liveness wait");
            assertEquals(7, ((OffsetConsumerImpl) consumer).startSeq(), "expected the stored offset 7");
            assertEquals(((OffsetConsumerImpl) consumer).owner(), consumer.info().getOwner(), "expected the entry claimed");
        });
    }

    // A consumer of the owner that appears during the wait (an ordered
    // consumer being recreated) makes the takeover fail.
    @Test
    void testConsumerAppearingDuringWaitRefuses() throws Exception {
        withLivenessWait(1000, () -> {
            startServerAndConnect();
            createStreamAndPublish();
            String name = uniqueName("test-appearing");
            String owner = "oc-resetting-owner";

            putOwnedEntry(name, owner, 0);
            consumer = OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, name).build());
            Thread late = new Thread(() -> {
                try {
                    Thread.sleep(300);
                    nc.jetStreamManagement().addOrUpdateConsumer(TEST_STREAM, io.nats.client.api.ConsumerConfiguration.builder()
                            .name(owner + "-g1_2")
                            .inactiveThreshold(Duration.ofMinutes(1))
                            .build());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            late.start();
            assertThrows(AlreadyInUseException.class, () -> consumer.consume(msg -> {}));
            late.join();
        });
    }

    // A write to the entry during the wait (here a commit by the owner) makes
    // the CAS claim fail.
    @Test
    void testWriteDuringWaitRefuses() throws Exception {
        withLivenessWait(1000, () -> {
            startServerAndConnect();
            createStreamAndPublish();
            String name = uniqueName("test-write-during-wait");
            String owner = "oc-committing-owner";

            putOwnedEntry(name, owner, 3);
            consumer = OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, name).build());
            Thread writer = new Thread(() -> {
                try {
                    Thread.sleep(300);
                    putOwnedEntry(name, owner, 4);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            writer.start();
            assertThrows(AlreadyInUseException.class, () -> consumer.consume(msg -> {}));
            writer.join();
            OffsetConsumerInfo info = consumer.info();
            assertNotNull(info);
            assertEquals(owner, info.getOwner(), "expected the owner's write to stand");
            assertEquals(4, info.getSeq());
        });
    }

    // A seek creates the new ordered consumer before deleting the old one: the
    // instance always has a consumer on the server.
    @Test
    void testSeekKeepsConsumerVisible() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();

        consumer = consumeUntilDelivered(new ConsumerConfig.Builder(TEST_STREAM, uniqueName("test-seek-visible")).build());
        String owner = ((OffsetConsumerImpl) consumer).owner();

        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicLong gaps = new AtomicLong(0);
        Thread poller = new Thread(() -> {
            io.nats.client.StreamContext sc;
            try {
                sc = nc.getStreamContext(TEST_STREAM);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            while (!stop.get()) {
                try {
                    if (!OffsetConsumerFactory.isOwnerAlive(sc, owner)) {
                        gaps.incrementAndGet();
                    }
                } catch (Exception ignored) {
                    // a failed listing is not a gap
                }
            }
        });
        poller.start();
        try {
            for (int i = 0; i < 20; i++) {
                consumer.seek(i);
            }
        } finally {
            stop.set(true);
            poller.join();
        }
        assertEquals(0, gaps.get(), "the instance had no consumer on the server during seeks");
        List<String> names = consumerNamesWithPrefix(owner);
        assertEquals(1, names.size(), "expected only the latest generation, got " + names);
        assertTrue(names.get(0).startsWith(owner + "-g21"), "expected generation 21, got " + names);
    }

    // A seek before the first consume() is only recorded, and stored by the claim.
    @Test
    void testSeekBeforeConsumeStoredByClaim() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();

        consumer = OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, uniqueName("test-seek-before")).build());
        consumer.seek(42);
        assertNull(consumer.info(), "expected no write before consume()");
        AtomicLong first = new AtomicLong(0);
        consumer.consume(msg -> first.compareAndSet(0, msg.metaData().streamSequence()));
        assertEquals(42, storedSeq(consumer), "expected the claim to store 42");
        waitForCondition(5000, () -> first.get() != 0);
        assertEquals(43, first.get(), "expected delivery from 43");
    }

    // A stream recreated between create() and consume() is detected by the
    // claim, which resets the offset against the new stream.
    @Test
    void testStreamRecreatedBeforeConsume() throws Exception {
        startServerAndConnect();
        createStreamAndPublish();
        String name = uniqueName("test-recreated-before");

        OffsetConsumer c1 = track(OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, name).build()));
        AtomicLong last = new AtomicLong(0);
        c1.consume(msg -> last.set(msg.metaData().streamSequence()));
        waitForCondition(5000, () -> last.get() == NUM_MESSAGES);
        c1.commitSync();
        c1.close();

        AtomicBoolean reported = new AtomicBoolean(false);
        consumer = OffsetConsumerFactory.create(nc, new ConsumerConfig.Builder(TEST_STREAM, name)
                .errorHandler((oc, err) -> {
                    if (err instanceof StreamRecreatedException) {
                        reported.set(true);
                    }
                })
                .build());
        recreateStream(NUM_MESSAGES + 20); // passes the old position: only the identity tells
        consumer.consume(msg -> {});
        assertTrue(reported.get(), "StreamRecreatedException not reported");
        assertEquals(0, ((OffsetConsumerImpl) consumer).startSeq(), "expected the offset reset to 0");
        String currentId = OffsetConsumerFactory.streamCreatedId(
                nc.jetStreamManagement().getStreamInfo(TEST_STREAM).getCreateTime());
        assertEquals(currentId, consumer.info().getStreamCreated(), "claim must store the new stream identity");
    }
}
