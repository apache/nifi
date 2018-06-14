package org.apache.nifi.controller.queue.clustered.client;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.remote.StandardVersionNegotiator;
import org.apache.nifi.remote.VersionNegotiator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.ABORT_PROTOCOL_NEGOTIATION;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.REQEUST_DIFFERENT_VERSION;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.VERSION_ACCEPTED;

public class SocketLoadBalanceClient implements LoadBalanceClient {
    private static final Logger logger = LoggerFactory.getLogger(SocketLoadBalanceClient.class);

    private final NodeIdentifier nodeIdentifer;
    private final SSLContext sslContext;
    private final int timeoutMillis;
    private final String peerDescription;

    public SocketLoadBalanceClient(final NodeIdentifier nodeIdentifier, final SSLContext sslContext, final int timeoutMillis) {
        this.nodeIdentifer = nodeIdentifier;
        this.sslContext = sslContext;
        this.timeoutMillis = timeoutMillis;
        this.peerDescription = nodeIdentifier.getLoadBalanceAddress() + ":" + nodeIdentifier.getLoadBalancePort();
    }

    @Override
    public LoadBalanceTransaction createTransaction(final String connectionId) throws IOException {
        final String loadBalanceHost = getNodeIdentifier().getLoadBalanceAddress();
        final int loadBalancePort = getNodeIdentifier().getLoadBalancePort();

        final Socket socket = establishConnection(loadBalanceHost, loadBalancePort);
        final int protocolVersion = negotiateProtocolVersion(socket);

        final LoadBalanceFlowFileCodec loadBalanceFlowFileCodec = new StandardLoadBalanceFlowFileCodec();

        final LoadBalanceTransaction transaction = new SocketLoadBalanceTransaction(socket, loadBalanceFlowFileCodec, connectionId, peerDescription);
        logger.debug("Created Load Balance Transaction {} for Peer {} to load balance against Connection {}", transaction, peerDescription, connectionId);

        return transaction;
    }


    private int negotiateProtocolVersion(final Socket socket) throws IOException {
        final VersionNegotiator negotiator = new StandardVersionNegotiator(1);

        final OutputStream out = socket.getOutputStream();
        final InputStream in = socket.getInputStream();

        int recommendedVersion = 1;
        while (true) {
            logger.debug("Recommending to Peer {} that we use version {} of the Load Balance Protocol", recommendedVersion);
            out.write(recommendedVersion);
            out.flush();

            final int response = in.read();
            if (response < 0) {
                throw new EOFException("Requested that Peer " + peerDescription + " use version " + recommendedVersion + " of the Load Balance Protocol but encountered EOFException while waiting " +
                        "for a response");
            }

            if (response == VERSION_ACCEPTED) {
                logger.debug("Peer {} accepted version {} of the Load Balance Protocol. Will use this version.", peerDescription, recommendedVersion);
                return recommendedVersion;
            } else if ( response == REQEUST_DIFFERENT_VERSION) {
                final int requestedVersion = in.read();
                logger.debug("Peer {} requested that we use version {} of Load Balance Protocol instead of version {}", peerDescription, requestedVersion, recommendedVersion);

                if (negotiator.isVersionSupported(requestedVersion)) {
                    logger.debug("Accepting Version {}", requestedVersion);
                    return requestedVersion;
                } else {
                    final Integer preferred = negotiator.getPreferredVersion(requestedVersion);
                    if (preferred == null) {
                        logger.debug("Peer {} requested version {} of the Load Balance Protocol. This version is not acceptable. Aborting communications.", peerDescription, requestedVersion);
                        out.write(ABORT_PROTOCOL_NEGOTIATION);
                        out.flush();
                    } else {
                        recommendedVersion = preferred;
                        logger.debug("Recommending version {} instead", recommendedVersion);
                        continue;
                    }
                }
            }
        }
    }


    private Socket establishConnection(final String hostname, final int port) throws IOException {
        logger.debug("Establishing connection to {}:{}", hostname, port);

        final Socket socket;
        if (sslContext == null) {
            socket = new Socket();
        } else {
            socket = sslContext.getSocketFactory().createSocket();
        }

        socket.connect(new InetSocketAddress(hostname, port), timeoutMillis);
        socket.setSoTimeout(timeoutMillis);
        return socket;
    }

    @Override
    public NodeIdentifier getNodeIdentifier() {
        return nodeIdentifer;
    }
}