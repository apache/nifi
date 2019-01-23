package org.apache.nifi.remote.protocol.raw;

import org.apache.nifi.nio.AutoCleared;
import org.apache.nifi.nio.Configurations;
import org.apache.nifi.nio.MessageAction;
import org.apache.nifi.nio.MessageSequence;
import org.apache.nifi.nio.Rewind;
import org.apache.nifi.nio.stream.ChannelToStream;
import org.apache.nifi.nio.stream.StreamData;
import org.apache.nifi.nio.stream.StreamToChannel;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.PeerDescription;
import org.apache.nifi.remote.protocol.FlowFileRequest;
import org.apache.nifi.remote.protocol.ProcessingResult;
import org.apache.nifi.remote.protocol.ServerProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.function.Supplier;

abstract class StreamFlowFiles implements MessageSequence {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final StreamToChannel toChannel;
    private final ChannelToStream fromChannel;
    private final StreamData streamData;
    private final Rewind rewind;

    @AutoCleared
    private FlowFileRequest flowFileRequest;

    @AutoCleared
    private boolean waitingForCompletion;

    @AutoCleared
    private PeerDescription peerDescription;

    @AutoCleared
    private long startStreamingTimestamp;

    StreamFlowFiles(InetSocketAddress serverAddress,
                    Supplier<PeerDescription> peerDescriptionSupplier,
                    Supplier<String> clientDnProvider,
                    Supplier<ServerProtocol> protocolSupplier,
                    Configurations configs) {

        this.toChannel = new StreamToChannel(configs.getIoBufferSize(), configs.getIoTimeoutMillis());
        this.fromChannel = new ChannelToStream(configs.getIoBufferSize(), configs.getIoTimeoutMillis());

        this.streamData = new StreamData(fromChannel, toChannel, () -> {
            final ServerProtocol protocol = protocolSupplier.get();
            peerDescription = peerDescriptionSupplier.get();
            final String peerUri = "nifi://" + peerDescription.getHostname() + ":" + peerDescription.getPort();
            final PipedCommunicationsSession session = new PipedCommunicationsSession(
                fromChannel.getInputStream(),
                toChannel.getOutputStream());
            session.setUserDn(clientDnProvider.get());
            final String clusterUrl = "nifi://" + serverAddress.getHostName() + ":" + serverAddress.getPort();
            final Peer peer = new Peer(peerDescription, session, peerUri, clusterUrl);
            // Start the port to receive/transfer FlowFiles on its thread.
            startStreamingTimestamp = System.currentTimeMillis();

            log.debug("Start streaming FlowFiles from {}", peerDescription);
            flowFileRequest = startStreaming(protocol, peer);
        });

        this.rewind = new Rewind(fromChannel);
    }

    protected abstract FlowFileRequest startStreaming(ServerProtocol protocol, Peer peer);

    @Override
    public MessageAction getNextAction() {

        if (streamData.isDone()) {

            if (!rewind.isDone()) {
                // Need rewinding.
                return rewind;
            }

            // Already finished.
            log.debug("Finished streaming FlowFiles ({} ms) from {}",
                System.currentTimeMillis() - startStreamingTimestamp, peerDescription);
            return null;
        }

        if (waitingForCompletion) {
            // Already told from/toChannel to complete, but they haven't finished yet.
            return streamData;
        }

        if (flowFileRequest == null) {
            // Haven't started the input port yet.
            return streamData;
        }

        if (flowFileRequest.isExpired()) {
            log.info("FlowFileRequest from {} has been expired. {}", flowFileRequest.getPeer(), flowFileRequest.getProtocol());
            // TODO: Test expiration scenario
            return null;
        }

        final ProcessingResult processingResult = flowFileRequest.getResponseQueue().poll();
        if (processingResult == null) {
            // Haven't got a response from the input port yet.
            return streamData;
        }

        log.trace("processingResult={}", processingResult);
        fromChannel.complete();
        toChannel.complete();
        waitingForCompletion = true;

        if (!streamData.isDone()) {
            // Even if from/toChannel's complete are called, streamData does not finish immediately
            // if the fromChannel's counter part is still consuming streamed data.
            log.trace("There's still a consuming activity.");
            return streamData;
        }

        return rewind;
    }

    public void terminate() {
        streamData.terminate();
    }

}
