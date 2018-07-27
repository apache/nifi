package org.apache.nifi.controller.queue.clustered.server;

import java.io.IOException;

public class NotAuthorizedException extends IOException {
    public NotAuthorizedException(String message) {
        super(message);
    }
}
