package io.synadia.offsetconsumer;

import io.nats.client.JetStreamApiException;
import io.nats.client.MessageHandler;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public interface OffsetConsumer extends AutoCloseable {

    void consume(MessageHandler handler) throws IOException, JetStreamApiException;

    void commitSync() throws CommitException;

    CompletableFuture<Void> commitAsync() throws AsyncCommitInFlightException;

    void seek(long seq) throws CommitException, IOException, JetStreamApiException;

    void seekToBeginning() throws CommitException, IOException, JetStreamApiException;

    void seekToEnd() throws CommitException, IOException, JetStreamApiException;

    void deleteConsumer() throws IOException, JetStreamApiException;

    @Override
    void close();
}
