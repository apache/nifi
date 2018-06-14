package org.apache.nifi.controller.queue.clustered.server;

import java.io.IOException;

public class TransactionAbortedException extends IOException {
    public TransactionAbortedException(final String message) {
        super(message);
    }

    public TransactionAbortedException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
