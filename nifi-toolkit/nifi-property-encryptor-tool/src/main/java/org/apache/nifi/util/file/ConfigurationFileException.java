package org.apache.nifi.util.file;

public class ConfigurationFileException extends RuntimeException {

    public ConfigurationFileException(final String message) {
        super(message);
    }

    public ConfigurationFileException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
