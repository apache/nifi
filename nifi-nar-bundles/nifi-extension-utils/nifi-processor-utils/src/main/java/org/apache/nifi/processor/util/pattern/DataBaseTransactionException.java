package org.apache.nifi.processor.util.pattern;

import org.apache.nifi.processor.exception.ProcessException;

/**
 * A simple Exception that indicate the some error about database transaction occurred,
 * and maybe we need to catch and handle it;
 * @author zhangcheng
 */
public class DataBaseTransactionException extends ProcessException {
    public DataBaseTransactionException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
