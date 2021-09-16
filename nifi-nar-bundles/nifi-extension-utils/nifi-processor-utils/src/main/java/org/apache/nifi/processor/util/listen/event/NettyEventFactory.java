package org.apache.nifi.processor.util.listen.event;

import java.util.Map;

/**
 * Factory to create instances of a given type of Event.
 */
public interface NettyEventFactory<E extends NettyEvent> {

    /**
     * The key in the metadata map for the sender.
     */
    String SENDER_KEY = "sender";

    /**
     * Creates an event for the given data and metadata.
     *
     * @param data raw data from a channel
     * @param metadata additional metadata
     *
     * @return an instance of the given type
     */
    E create(final byte[] data, final Map<String, String> metadata);

}