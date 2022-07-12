package org.apache.nifi.util.file;

public class ConfigurationFileResolverException extends RuntimeException {

    public ConfigurationFileResolverException(String message) {
        super(message);
    }

    public ConfigurationFileResolverException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConfigurationFileResolverException(Throwable cause) {
        super(cause);
    }
}
