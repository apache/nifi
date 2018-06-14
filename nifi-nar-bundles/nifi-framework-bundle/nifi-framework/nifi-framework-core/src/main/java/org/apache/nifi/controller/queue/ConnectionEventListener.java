package org.apache.nifi.controller.queue;

public interface ConnectionEventListener {
    void triggerSourceEvent();

    void triggerDestinationEvent();
}
