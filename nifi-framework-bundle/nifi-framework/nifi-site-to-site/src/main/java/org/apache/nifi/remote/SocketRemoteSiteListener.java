/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.remote;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.GeneralSecurityException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.remote.cluster.ClusterNodeInformation;
import org.apache.nifi.remote.cluster.NodeInformant;
import org.apache.nifi.remote.cluster.NodeInformation;
import org.apache.nifi.remote.exception.BadRequestException;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.exception.NotAuthorizedException;
import org.apache.nifi.remote.exception.RequestExpiredException;
import org.apache.nifi.remote.io.socket.SocketCommunicationsSession;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.RequestType;
import org.apache.nifi.remote.protocol.ServerProtocol;
import org.apache.nifi.security.cert.StandardPrincipalFormatter;
import org.apache.nifi.security.util.TlsPlatform;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SocketRemoteSiteListener implements RemoteSiteListener {

    private final int socketPort;
    private final SSLContext sslContext;
    private final NodeInformant nodeInformant;
    private final AtomicReference<ProcessGroup> rootGroup = new AtomicReference<>();
    private final NiFiProperties nifiProperties;
    private final PeerDescriptionModifier peerDescriptionModifier;

    private static final int EXCEPTION_THRESHOLD_MILLIS = 10_000;
    private volatile long tlsErrorLastSeen = -1;

    private final AtomicBoolean stopped = new AtomicBoolean(false);

    private static final Logger LOG = LoggerFactory.getLogger(SocketRemoteSiteListener.class);

    public SocketRemoteSiteListener(final int socketPort, final SSLContext sslContext, final NiFiProperties nifiProperties) {
        this(socketPort, sslContext, nifiProperties, null);
    }

    public SocketRemoteSiteListener(final int socketPort, final SSLContext sslContext, final NiFiProperties nifiProperties, final NodeInformant nodeInformant) {
        this.socketPort = socketPort;
        this.sslContext = sslContext;
        this.nifiProperties = nifiProperties;
        this.nodeInformant = nodeInformant;
        peerDescriptionModifier = new PeerDescriptionModifier(nifiProperties);
    }

    @Override
    public void setRootGroup(final ProcessGroup rootGroup) {
        this.rootGroup.set(rootGroup);
    }

    @Override
    public void start() throws IOException {
        final boolean secure = (sslContext != null);
        final List<Thread> threads = new ArrayList<>();

        stopped.set(false);

        final Thread listenerThread = new Thread(new Runnable() {
            private int threadCount = 0;

            @Override
            public void run() {

                try (final ServerSocket serverSocket = createServerSocket()) {
                    serverSocket.setSoTimeout(2000);

                    while (!stopped.get()) {

                        final Socket acceptedSocket = acceptConnection(serverSocket);

                        if (acceptedSocket == null) {
                            continue;
                        }

                        if (stopped.get()) {
                            break;
                        }

                        final Thread thread = createWorkerThread(acceptedSocket);
                        thread.setName("Site-to-Site Worker Thread-" + (threadCount++));
                        LOG.debug("Handing connection to {}", thread);
                        thread.start();
                        threads.add(thread);
                        threads.removeIf(t -> !t.isAlive());

                    }

                } catch (final IOException e) {
                    LOG.error("Unable to open server socket", e);
                }


                for (Thread thread : threads) {
                    if (thread != null) {
                        thread.interrupt();
                    }
                }
            }

            private Thread createWorkerThread(Socket socket) {
                return new Thread(new Runnable() {
                    @Override
                    public void run() {
                        LOG.debug("{} Determining URL of connection", this);
                        final InetAddress inetAddress = socket.getInetAddress();
                        String clientHostName = inetAddress.getHostName();
                        final int slashIndex = clientHostName.indexOf("/");
                        if (slashIndex == 0) {
                            clientHostName = clientHostName.substring(1);
                        } else if (slashIndex > 0) {
                            clientHostName = clientHostName.substring(0, slashIndex);
                        }

                        final int clientPort = socket.getPort();
                        final String peerUri = "nifi://" + clientHostName + ":" + clientPort;
                        LOG.debug("{} Connection URL is {}", this, peerUri);

                        final CommunicationsSession commsSession;
                        final String dn;
                        try {
                            if (secure) {
                                LOG.trace("{} Connection is secure", this);
                                final SSLSocket sslSocket = (SSLSocket) socket;
                                dn = getPeerIdentity(sslSocket);

                                commsSession = new SocketCommunicationsSession(socket);
                                commsSession.setUserDn(dn);

                            } else {
                                LOG.trace("{} Connection is not secure", this);
                                commsSession = new SocketCommunicationsSession(socket);
                                dn = null;
                            }
                        } catch (final Exception e) {
                            // TODO: Add SocketProtocolListener#handleTlsError logic here
                            String msg = String.format("RemoteSiteListener Unable to accept connection from %s due to %s", socket, e.getLocalizedMessage());
                            // Suppress repeated TLS errors
                            if (isTlsError(e)) {
                                boolean printedAsWarning = handleTlsError(msg);

                                // TODO: Move into handleTlsError and refactor shared behavior
                                // If the error was printed as a warning, reset the last seen timer
                                if (printedAsWarning) {
                                    tlsErrorLastSeen = System.currentTimeMillis();
                                }
                            } else {
                                LOG.error(msg);
                                if (LOG.isDebugEnabled()) {
                                    LOG.error("", e);
                                }
                            }
                            return;
                        }

                        LOG.info("Received connection from {}, User DN: {}", socket.getInetAddress(), dn);

                        final InputStream socketIn;
                        final OutputStream socketOut;

                        try {
                            socketIn = commsSession.getInput().getInputStream();
                            socketOut = commsSession.getOutput().getOutputStream();
                        } catch (final IOException e) {
                            LOG.error("Connection dropped from {} before any data was transmitted", peerUri);
                            try {
                                commsSession.close();
                            } catch (final IOException ignored) {
                            }

                            return;
                        }

                        final DataInputStream dis = new DataInputStream(socketIn);
                        final DataOutputStream dos = new DataOutputStream(socketOut);

                        ServerProtocol protocol = null;
                        Peer peer = null;
                        try {
                            // ensure that we are communicating with another NiFi
                            LOG.debug("Verifying magic bytes...");
                            verifyMagicBytes(dis, peerUri);

                            LOG.debug("Receiving Server Protocol Negotiation");
                            protocol = RemoteResourceFactory.receiveServerProtocolNegotiation(dis, dos);
                            protocol.setRootProcessGroup(rootGroup.get());
                            protocol.setNodeInformant(nodeInformant);
                            if (protocol instanceof PeerDescriptionModifiable) {
                                ((PeerDescriptionModifiable) protocol).setPeerDescriptionModifier(peerDescriptionModifier);
                            }

                            final PeerDescription description = new PeerDescription(clientHostName, clientPort, sslContext != null);
                            peer = new Peer(description, commsSession, peerUri, "nifi://localhost:" + getPort());
                            LOG.debug("Handshaking....");
                            protocol.handshake(peer);

                            if (!protocol.isHandshakeSuccessful()) {
                                LOG.error("Handshake failed with {}; closing connection", peer);
                                try {
                                    peer.close();
                                } catch (final IOException e) {
                                    LOG.warn("Failed to close {} due to {}", peer, e);
                                }

                                // no need to shutdown protocol because we failed to perform handshake
                                return;
                            }

                            commsSession.setTimeout((int) protocol.getRequestExpiration());

                            LOG.info("Successfully negotiated ServerProtocol {} Version {} with {}",
                                protocol.getResourceName(), protocol.getVersionNegotiator().getVersion(), peer);

                            try {
                                while (!protocol.isShutdown()) {
                                    LOG.trace("Getting Protocol Request Type...");

                                    int timeoutCount = 0;
                                    RequestType requestType = null;

                                    while (requestType == null) {
                                        try {
                                            requestType = protocol.getRequestType(peer);
                                        } catch (final SocketTimeoutException e) {
                                            // Give the timeout a bit longer (twice as long) to receive the Request Type,
                                            // in order to attempt to receive more data without shutting down the socket if we don't
                                            // have to.
                                            LOG.debug("{} Timed out waiting to receive RequestType using {} with {}", this, protocol, peer);
                                            timeoutCount++;
                                            requestType = null;

                                            if (timeoutCount >= 2) {
                                                throw e;
                                            }
                                        }
                                    }

                                    handleRequest(protocol, peer, requestType);
                                }
                                LOG.debug("Finished communicating with {} ({})", peer, protocol);
                            } catch (final Exception e) {
                                LOG.error("Unable to communicate with remote instance {} ({}); closing connection", peer, protocol, e);
                            }
                        } catch (final IOException e) {
                            LOG.error("Unable to communicate with remote instance {}; closing connection", peer, e);
                        } catch (final Throwable t) {
                            LOG.error("Handshake failed when communicating with {}; closing connection.", peerUri, t);
                        } finally {
                            LOG.trace("Cleaning up");
                            try {
                                if (protocol != null && peer != null) {
                                    protocol.shutdown(peer);
                                }
                            } catch (final Exception protocolException) {
                                LOG.warn("Failed to shutdown protocol", protocolException);
                            }

                            try {
                                if (peer != null) {
                                    peer.close();
                                }
                            } catch (final Exception peerException) {
                                LOG.warn("Failed to close peer; some resources may not be appropriately cleaned up", peerException);
                            }
                            LOG.trace("Finished cleaning up");
                        }
                    }
                });
            }
        });

        listenerThread.setName("Site-to-Site Listener");
        listenerThread.start();
    }

    private boolean isTlsError(final Throwable e) {
        final boolean tlsError;

        if (e instanceof SSLException || e instanceof GeneralSecurityException) {
            tlsError = true;
        } else if (e.getCause() == null) {
            tlsError = false;
        } else {
            tlsError = isTlsError(e.getCause());
        }

        return tlsError;
    }

    private String getPeerIdentity(final SSLSocket sslSocket) throws SSLPeerUnverifiedException {
        final SSLSession sslSession = sslSocket.getSession();
        final Certificate[] peerCertificates = sslSession.getPeerCertificates();
        if (peerCertificates == null || peerCertificates.length == 0) {
            throw new SSLPeerUnverifiedException(String.format("Peer [%s] certificates not found", sslSocket.getRemoteSocketAddress()));
        }

        final X509Certificate peerCertificate = (X509Certificate) peerCertificates[0];
        return StandardPrincipalFormatter.getInstance().getSubject(peerCertificate);
    }

    private boolean handleTlsError(String msg) {
        if (tlsErrorRecentlySeen()) {
            LOG.debug(msg);
            return false;
        } else {
            LOG.error(msg);
            return true;
        }
    }

    /**
     * Returns {@code true} if any related exception has occurred within the last
     * {@link #EXCEPTION_THRESHOLD_MILLIS} milliseconds. Does not evaluate the error locally,
     * simply checks the last time the timestamp was updated.
     *
     * @return true if the time since the last similar exception occurred is below the threshold
     */
    private boolean tlsErrorRecentlySeen() {
        long now = System.currentTimeMillis();
        return now - tlsErrorLastSeen < EXCEPTION_THRESHOLD_MILLIS;
    }

    private ServerSocket createServerSocket() throws IOException {
        if (sslContext != null) {
            final SSLServerSocket serverSocket = (SSLServerSocket) sslContext.getServerSocketFactory().createServerSocket(socketPort);
            serverSocket.setNeedClientAuth(true);
            // Set Preferred TLS Protocol Versions
            serverSocket.setEnabledProtocols(TlsPlatform.getPreferredProtocols().toArray(new String[0]));
            return serverSocket;
        } else {
            return new ServerSocket(socketPort);
        }
    }

    private Socket acceptConnection(ServerSocket serverSocket) {
        LOG.trace("Accepting Connection...");
        Socket acceptedSocket = null;
        try {
            while (!stopped.get() && acceptedSocket == null) {
                try {
                    acceptedSocket = serverSocket.accept();
                } catch (final SocketTimeoutException ste) {
                    LOG.trace("SocketTimeoutException occurred. {}", ste.getMessage());
                }
            }
        } catch (final IOException e) {
            LOG.error("RemoteSiteListener Unable to accept connection", e);
            return acceptedSocket;
        }
        LOG.trace("Got connection");
        return acceptedSocket;
    }

    private void handleRequest(final ServerProtocol protocol, final Peer peer, final RequestType requestType)
            throws IOException, NotAuthorizedException, BadRequestException, RequestExpiredException {
        LOG.debug("Request type from {} is {}", protocol, requestType);
        switch (requestType) {
            case NEGOTIATE_FLOWFILE_CODEC:
                protocol.negotiateCodec(peer);
                break;
            case RECEIVE_FLOWFILES:
                // peer wants to receive FlowFiles, so we will transfer FlowFiles.
                protocol.getPort().transferFlowFiles(peer, protocol);
                break;
            case SEND_FLOWFILES:
                // Peer wants to send FlowFiles, so we will receive.
                protocol.getPort().receiveFlowFiles(peer, protocol);
                break;
            case REQUEST_PEER_LIST:
                final Optional<ClusterNodeInformation> nodeInfo = (nodeInformant == null) ? Optional.empty() : Optional.of(nodeInformant.getNodeInformation());

                String remoteInputHostVal = nifiProperties.getRemoteInputHost();
                if (remoteInputHostVal == null) {
                    remoteInputHostVal = InetAddress.getLocalHost().getHostName();
                }
                final Boolean isSiteToSiteSecure = nifiProperties.isSiteToSiteSecure();
                final Integer apiPort = isSiteToSiteSecure ? nifiProperties.getSslPort() : nifiProperties.getPort();
                final NodeInformation self = new NodeInformation(remoteInputHostVal,
                        nifiProperties.getRemoteInputPort(),
                        nifiProperties.getRemoteInputHttpPort(),
                        apiPort != null ? apiPort : 0, // Avoid potential NullPointerException.
                        isSiteToSiteSecure, 0); // TotalFlowFiles doesn't matter if it's a standalone NiFi.

                protocol.sendPeerList(peer, nodeInfo, self);
                break;
            case SHUTDOWN:
                protocol.shutdown(peer);
                break;
        }
    }

    private int getPort() {
        return socketPort;
    }

    @Override
    public void stop() {
        stopped.set(true);
    }

    @Override
    public void destroy() {
    }

    private void verifyMagicBytes(final InputStream in, final String peerDescription) throws IOException {
        final byte[] receivedMagicBytes = new byte[CommunicationsSession.MAGIC_BYTES.length];

        // expect magic bytes
        try {
            for (int i = 0; i < receivedMagicBytes.length; i++) {
                receivedMagicBytes[i] = (byte) in.read();
            }
        } catch (final EOFException e) {
            throw new HandshakeException("Handshake failed (not enough bytes) when communicating with " + peerDescription);
        }

        if (!Arrays.equals(CommunicationsSession.MAGIC_BYTES, receivedMagicBytes)) {
            throw new HandshakeException("Handshake with " + peerDescription + " failed because the Magic Header was not present");
        }
    }
}
