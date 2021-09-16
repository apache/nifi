package org.apache.nifi.processor.util.listen.event;

/**
 * Standard implementation of Event.
 */
public class StandardNettyEvent implements NettyEvent {

    private final String sender;
    private final byte[] data;

    public StandardNettyEvent(final String sender, final byte[] data) {
        this.sender = sender;
        this.data = data;
    }

    @Override
    public String getSender() {
        return sender;
    }

    @Override
    public byte[] getData() {
        return data;
    }

}
