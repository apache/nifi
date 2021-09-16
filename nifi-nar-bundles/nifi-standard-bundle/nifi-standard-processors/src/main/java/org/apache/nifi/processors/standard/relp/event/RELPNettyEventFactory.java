package org.apache.nifi.processors.standard.relp.event;

import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.event.NettyEventFactory;

import java.util.Map;

/**
 * An EventFactory implementation to create RELPEvents.
 */
public class RELPNettyEventFactory implements NettyEventFactory<RELPNettyEvent> {

    @Override
    public RELPNettyEvent create(final byte[] data, final Map<String, String> metadata) {
        final long txnr = Long.valueOf(metadata.get(RELPMetadata.TXNR_KEY));
        final String command = metadata.get(RELPMetadata.COMMAND_KEY);
        final String sender = metadata.get(EventFactory.SENDER_KEY);
        return new RELPNettyEvent(sender, data, txnr, command);
    }

}
