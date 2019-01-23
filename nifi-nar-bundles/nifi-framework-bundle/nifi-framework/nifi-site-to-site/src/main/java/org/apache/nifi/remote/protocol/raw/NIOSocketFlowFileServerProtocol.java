package org.apache.nifi.remote.protocol.raw;

import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.PeerDescriptionModifiable;
import org.apache.nifi.remote.PeerDescriptionModifier;
import org.apache.nifi.remote.StandardVersionNegotiator;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.remote.cluster.ClusterNodeInformation;
import org.apache.nifi.remote.cluster.NodeInformation;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.remote.protocol.AbstractFlowFileServerProtocol;
import org.apache.nifi.remote.protocol.HandshakeProperties;
import org.apache.nifi.remote.protocol.RequestType;
import org.apache.nifi.remote.protocol.socket.SocketFlowFileServerProtocol;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class NIOSocketFlowFileServerProtocol extends AbstractFlowFileServerProtocol implements PeerDescriptionModifiable {

    // Version 6 added to support Zero-Master Clustering, which was introduced in NiFi 1.0.0
    private final VersionNegotiator versionNegotiator = new StandardVersionNegotiator(6, 5, 4, 3, 2, 1);

    private PeerDescriptionModifier peerDescriptionModifier;

    private Supplier<FlowFileCodec> flowFileCodecSupplier;

    @Override
    public void setPeerDescriptionModifier(PeerDescriptionModifier modifier) {
        peerDescriptionModifier = modifier;
    }

    void setHandshakeProperties(HandshakeProperties handshakeProperties) {
        this.handshakeProperties = handshakeProperties;
        this.handshakeCompleted = true;
    }

    void setFlowFileCodecSupplier(Supplier<FlowFileCodec> flowFileCodecSupplier) {
        this.flowFileCodecSupplier = flowFileCodecSupplier;
    }

    @Override
    public FlowFileCodec getPreNegotiatedCodec() {
        return flowFileCodecSupplier.get();
    }

    @Override
    protected HandshakeProperties doHandshake(Peer peer) throws IOException, HandshakeException {
        throw new UnsupportedOperationException("Implemented in non-blocking style. This method should never been called.");
    }

    @Override
    public FlowFileCodec negotiateCodec(Peer peer) throws IOException, ProtocolException {
        throw new UnsupportedOperationException("Implemented in non-blocking style. This method should never been called.");
    }

    @Override
    public RequestType getRequestType(Peer peer) throws IOException {
        throw new UnsupportedOperationException("Implemented in non-blocking style. This method should never been called.");
    }

    @Override
    public void sendPeerList(Peer peer, Optional<ClusterNodeInformation> clusterNodeInfo, NodeInformation self) throws IOException {
        throw new UnsupportedOperationException("Implemented in non-blocking style. This method should never been called.");
    }

    @Override
    public VersionNegotiator getVersionNegotiator() {
        return versionNegotiator;
    }

    @Override
    public String getResourceName() {
        return SocketFlowFileServerProtocol.RESOURCE_NAME;
    }

    @Override
    protected void validateHandshakeRequest(HandshakeProperties confirmed, String userDn, Map<String, String> properties) throws HandshakeException {
        super.validateHandshakeRequest(confirmed, userDn, properties);
    }
}
