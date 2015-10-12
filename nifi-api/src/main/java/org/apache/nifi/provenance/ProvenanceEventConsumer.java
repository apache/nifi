package org.apache.nifi.provenance;

/**
 * Strategy for implementing generic consumers.
 *
 * @param <T> the type of elements to consume.
 */
public interface ProvenanceEventConsumer {

	/**
	 * Consumes provided element
	 *
	 * @param element element of type T to be consumed
	 */
	void consume(ProvenanceEventRecord event);
}
