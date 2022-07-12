package org.apache.nifi;

public class PropertyEncryptorException extends RuntimeException {
    public PropertyEncryptorException(final String message) {
        super(message);
    }

    public PropertyEncryptorException(final String message, final Throwable cause) {
        super(message, cause);
    }
}