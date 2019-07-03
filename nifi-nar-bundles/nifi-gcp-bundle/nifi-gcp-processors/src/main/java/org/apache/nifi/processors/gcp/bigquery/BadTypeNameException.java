package org.apache.nifi.processors.gcp.bigquery;

/**
 * Exception thrown when a given type can't be transformed into a valid BigQuery type.
 * @author nicolas-delsaux
 *
 */
public class BadTypeNameException extends RuntimeException {

	public BadTypeNameException() {
	}

	public BadTypeNameException(String message) {
		super(message);
	}

	public BadTypeNameException(Throwable cause) {
		super(cause);
	}

	public BadTypeNameException(String message, Throwable cause) {
		super(message, cause);
	}

	public BadTypeNameException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
