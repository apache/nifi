package org.apache.nifi.remote.protocol.raw;

import org.apache.nifi.nio.AutoCleared;
import org.apache.nifi.nio.MessageAction;
import org.apache.nifi.nio.MessageSequence;
import org.apache.nifi.nio.ReadUTF;
import org.apache.nifi.remote.protocol.RequestType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ReadRequestType implements MessageSequence {

    private static final Logger LOG = LoggerFactory.getLogger(ReadRequestType.class);

    private MessageAction readRequestType = new ReadUTF(type -> {
        try {
            LOG.debug("Received request {}", type);
            requestType = RequestType.valueOf(type);
        } catch (IllegalArgumentException e) {
            throw new IOException("Could not determine RequestType: received invalid value " + type);
        }
    });

    @AutoCleared
    private RequestType requestType;

    @Override
    public MessageAction getNextAction() {
        if (requestType == null) {
            return readRequestType;
        }
        return null;
    }

    public RequestType getRequestType() {
        return requestType;
    }
}
