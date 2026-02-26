package io.synadia.offsetconsumer;

public class AsyncCommitInFlightException extends IllegalStateException {
    public AsyncCommitInFlightException() {
        super("Async commit already in flight");
    }
}
