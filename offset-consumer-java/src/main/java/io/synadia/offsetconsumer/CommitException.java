package io.synadia.offsetconsumer;

public class CommitException extends Exception {
    public CommitException(String message) {
        super(message);
    }

    public CommitException(String message, Throwable cause) {
        super(message, cause);
    }
}
