package org.apache.nifi.remote.protocol.raw;

import org.apache.nifi.nio.Configurations;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.PeerDescription;
import org.apache.nifi.remote.protocol.FlowFileRequest;
import org.apache.nifi.remote.protocol.ServerProtocol;

import java.net.InetSocketAddress;
import java.util.function.Supplier;

class TransferFlowFiles extends StreamFlowFiles {

    TransferFlowFiles(InetSocketAddress serverAddress,
                      Supplier<PeerDescription> peerDescriptionSupplier,
                      Supplier<String> clientDnProvider,
                      Supplier<ServerProtocol> protocolSupplier,
                      Configurations configs) {
        super(serverAddress, peerDescriptionSupplier, clientDnProvider, protocolSupplier, configs);
    }

    @Override
    protected FlowFileRequest startStreaming(ServerProtocol protocol, Peer peer) {
        return protocol.getPort().startTransferringFlowFiles(peer, protocol);
    }
}
