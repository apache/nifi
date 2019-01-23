package org.apache.nifi.remote.protocol.raw;

import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nio.Configurations;
import org.apache.nifi.nio.MessageSequence;
import org.apache.nifi.nio.MultiMessageSequence;
import org.apache.nifi.remote.PeerDescription;
import org.apache.nifi.remote.PeerDescriptionModifier;
import org.apache.nifi.remote.RemoteResourceManager;
import org.apache.nifi.remote.cluster.NodeInformant;
import org.apache.nifi.remote.cluster.NodeInformation;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.protocol.ServerProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.function.Supplier;

public class SiteToSiteMessageSequence implements MultiMessageSequence {

    private static final Logger LOG = LoggerFactory.getLogger(SiteToSiteMessageSequence.class);

    private VerifyMagicBytes verifyMagicBytes = new VerifyMagicBytes();
    private final RemoteResourceNegotiation negotiateProtocol;
    private final Handshake handshake;
    private final HandleRequest handleRequest;
    private PeerDescription peer;

    /**
     * Constructor.
     * @param serverAddress the address that the server is listening to.
     * @param self represents the node on which this process is running.
     * @param nodeInformant can be null if the NiFi is standalone mode.
     * @param peerDescriptionModifier can be null if peer description modification is disabled.
     * @param configs configurations.
     */
    public SiteToSiteMessageSequence(InetSocketAddress serverAddress,
                                     NodeInformation self, NodeInformant nodeInformant,
                                     PeerDescriptionModifier peerDescriptionModifier,
                                     Supplier<ProcessGroup> rootProcessGroupSupplier,
                                     Configurations configs) {

        final Supplier<PeerDescription> peerDescriptionSupplier = () -> peer;

        final RemoteResourceNegotiation negotiateFlowFileCodec =
            new RemoteResourceNegotiation("FlowFileCodec", RemoteResourceManager::createCodec);

        negotiateProtocol =
            new RemoteResourceNegotiation("ServerProtocol", protocolName -> {
                final NIOSocketFlowFileServerProtocol serverProtocol =
                    (NIOSocketFlowFileServerProtocol) RemoteResourceManager.createServerProtocol(protocolName);
                serverProtocol.setRootProcessGroup(rootProcessGroupSupplier.get());
                serverProtocol.setFlowFileCodecSupplier(() -> (FlowFileCodec) negotiateFlowFileCodec.getResource());
                return serverProtocol;
            });

        handshake = new Handshake(() -> (ServerProtocol) negotiateProtocol.getResource());

        final RequestPeerList requestPeerList = new RequestPeerList(self, nodeInformant,
            peerDescriptionModifier, peerDescriptionSupplier);

        final ReceiveFlowFiles receiveFlowFiles = new ReceiveFlowFiles(serverAddress,
            peerDescriptionSupplier,
            handshake::getClientDn,
            () -> (ServerProtocol) negotiateProtocol.getResource(),
            configs);

        final TransferFlowFiles transferFlowFiles = new TransferFlowFiles(serverAddress,
            peerDescriptionSupplier,
            handshake::getClientDn,
            () -> (ServerProtocol) negotiateProtocol.getResource(),
            configs);

        handleRequest = new HandleRequest(requestPeerList, negotiateFlowFileCodec, receiveFlowFiles, transferFlowFiles);
    }

    @Override
    public MessageSequence getNextSequence() {

        if (handleRequest.isShutdown()) {
            return null;
        }

        final MessageSequence nextSequence;
        if (!verifyMagicBytes.isDone()) {
            nextSequence = verifyMagicBytes;

        } else if (!negotiateProtocol.isDone()) {
            nextSequence = negotiateProtocol;

        } else if (!handshake.isDone()) {
            nextSequence = handshake;

        } else {
            nextSequence = handleRequest;
        }

        LOG.trace("Next sequence is {}", nextSequence);
        return nextSequence;
    }

    @Override
    public void onAccept(InetSocketAddress clientAddress, boolean secure) {
        String clientHostName = clientAddress.getHostName();
        final int slashIndex = clientHostName.indexOf("/");
        if (slashIndex == 0) {
            clientHostName = clientHostName.substring(1);
        } else if (slashIndex > 0) {
            clientHostName = clientHostName.substring(0, slashIndex);
        }

        final int clientPort = clientAddress.getPort();
        peer = new PeerDescription(clientHostName, clientPort, secure);
    }

    @Override
    public boolean shouldCloseConnectionWhenDone() {
        return true;
    }

    @Override
    public boolean isKnownException(Exception e) {
        // If the client sends SHUTDOWN and closes the connection immediately, the server may get this exception
        // when it reads next request from the channel.
        final String message = e.getMessage();
        return message != null
            && message.contains("Connection reset by peer")
            && getNextSequence() instanceof HandleRequest;
    }

    @Override
    public void onEndOfReadStream() {
        handleRequest.terminate();
    }
}
