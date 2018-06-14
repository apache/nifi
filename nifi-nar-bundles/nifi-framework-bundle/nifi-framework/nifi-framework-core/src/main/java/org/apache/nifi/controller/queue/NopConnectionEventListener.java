package org.apache.nifi.controller.queue;

public class NopConnectionEventListener implements ConnectionEventListener {
    @Override
    public void triggerSourceEvent() {
    }

    @Override
    public void triggerDestinationEvent() {

    }
}
