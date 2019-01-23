package org.apache.nifi.remote.protocol.raw;

import org.apache.nifi.nio.MessageSequence;
import org.apache.nifi.nio.MultiMessageSequence;
import org.apache.nifi.remote.protocol.RequestType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HandleRequest implements MultiMessageSequence {

    private static final Logger LOG = LoggerFactory.getLogger(HandleRequest.class);

    private final ReadRequestType readRequestType = new ReadRequestType();
    private final RequestPeerList requestPeerList;
    private final RemoteResourceNegotiation negotiateFlowFileCodec;
    private final ReceiveFlowFiles receiveFlowFiles;
    private final TransferFlowFiles transferFlowFiles;

    private boolean shutdown;

    HandleRequest(RequestPeerList requestPeerList,
                  RemoteResourceNegotiation negotiateFlowFileCodec,
                  ReceiveFlowFiles receiveFlowFiles,
                  TransferFlowFiles transferFlowFiles) {
        this.requestPeerList = requestPeerList;
        this.receiveFlowFiles = receiveFlowFiles;
        this.negotiateFlowFileCodec = negotiateFlowFileCodec;
        this.transferFlowFiles = transferFlowFiles;
    }

    @Override
    public MessageSequence getNextSequence() {
        if (shutdown) {
            return null;
        }

        final RequestType requestType = readRequestType.getRequestType();
        if (requestType == null) {
            return readRequestType;
        }

        final MessageSequence nextSequence;
        switch (requestType) {
            case REQUEST_PEER_LIST:
                nextSequence = requestPeerList;
                break;
            case NEGOTIATE_FLOWFILE_CODEC:
                nextSequence = negotiateFlowFileCodec;
                break;
            case SEND_FLOWFILES:
                // Clients send FlowFiles those are received by this server.
                nextSequence = receiveFlowFiles;
                break;
            case RECEIVE_FLOWFILES:
                // Clients receive FlowFiles transferred by this server.
                nextSequence = transferFlowFiles;
                break;
            case SHUTDOWN:
                LOG.debug("SHUTTING down...");
                shutdown = true;
                return null;
            default:
                nextSequence = null;
        }

        if (nextSequence == null || nextSequence.isDone()) {
            // Start from reading request type.
            clear();
            return readRequestType;
        }

        return nextSequence;
    }

    public boolean isShutdown() {
        return shutdown;
    }

    void terminate() {
        shutdown = true;
        final MessageSequence nextSequence = getNextSequence();
        if (nextSequence instanceof StreamFlowFiles) {
            ((StreamFlowFiles) nextSequence).terminate();
        }
    }
}
