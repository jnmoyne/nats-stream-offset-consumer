package io.synadia.offsetconsumer;

@FunctionalInterface
public interface ErrorHandler {
    void onError(OffsetConsumer consumer, Exception error);
}
