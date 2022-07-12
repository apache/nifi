package org.apache.nifi.xml;

public class XMLCryptoException extends RuntimeException {
    public XMLCryptoException(final String message) {
        super(message);
    }

    public XMLCryptoException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
