package io.synadia.offsetconsumer;

public class CASConflictException extends CommitException {
    public CASConflictException(Throwable cause) {
        super("Offset was modified externally (CAS conflict)", cause);
    }
}
