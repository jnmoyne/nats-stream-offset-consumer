package io.synadia.offsetconsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

public class ConsumerConfig {

    private final String stream;
    private final String consumerName;
    private final boolean autoCommit;
    private final Duration autoCommitInterval;
    private final List<String> filterSubjects;
    private final ErrorHandler errorHandler;
    private final StartPosition startPosition;

    private ConsumerConfig(Builder builder) {
        this.stream = builder.stream;
        this.consumerName = builder.consumerName;
        this.autoCommit = builder.autoCommit;
        this.autoCommitInterval = builder.autoCommitInterval;
        this.filterSubjects = builder.filterSubjects == null
                ? null
                : Collections.unmodifiableList(builder.filterSubjects);
        this.errorHandler = builder.errorHandler;
        this.startPosition = builder.startPosition;
    }

    public String getStream() {
        return stream;
    }

    public String getConsumerName() {
        return consumerName;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public Duration getAutoCommitInterval() {
        return autoCommitInterval;
    }

    public List<String> getFilterSubjects() {
        return filterSubjects;
    }

    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    public StartPosition getStartPosition() {
        return startPosition;
    }

    public static class Builder {

        private final String stream;
        private final String consumerName;
        private boolean autoCommit = false;
        private Duration autoCommitInterval = Duration.ofSeconds(5);
        private List<String> filterSubjects = null;
        private ErrorHandler errorHandler = null;
        private StartPosition startPosition = StartPosition.BEGINNING;

        public Builder(String stream, String consumerName) {
            if (stream == null || stream.isEmpty()) {
                throw new IllegalArgumentException("stream must not be null or empty");
            }
            if (consumerName == null || consumerName.isEmpty()) {
                throw new IllegalArgumentException("consumerName must not be null or empty");
            }
            this.stream = stream;
            this.consumerName = consumerName;
        }

        public Builder autoCommit(boolean autoCommit) {
            this.autoCommit = autoCommit;
            return this;
        }

        public Builder autoCommitInterval(Duration autoCommitInterval) {
            if (autoCommitInterval == null) {
                throw new IllegalArgumentException("autoCommitInterval must not be null");
            }
            this.autoCommitInterval = autoCommitInterval;
            return this;
        }

        public Builder filterSubjects(List<String> filterSubjects) {
            this.filterSubjects = filterSubjects;
            return this;
        }

        public Builder errorHandler(ErrorHandler errorHandler) {
            this.errorHandler = errorHandler;
            return this;
        }

        public Builder startPosition(StartPosition startPosition) {
            if (startPosition == null) {
                throw new IllegalArgumentException("startPosition must not be null");
            }
            this.startPosition = startPosition;
            return this;
        }

        public ConsumerConfig build() {
            return new ConsumerConfig(this);
        }
    }
}
