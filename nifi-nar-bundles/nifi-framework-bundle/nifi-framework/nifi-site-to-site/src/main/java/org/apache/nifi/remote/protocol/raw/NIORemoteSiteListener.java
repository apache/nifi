package org.apache.nifi.remote.protocol.raw;

import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nio.Configurations;
import org.apache.nifi.nio.MessageSequenceHandler;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.remote.PeerDescriptionModifier;
import org.apache.nifi.remote.RemoteSiteListener;
import org.apache.nifi.remote.cluster.NodeInformant;
import org.apache.nifi.remote.cluster.NodeInformation;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class NIORemoteSiteListener implements RemoteSiteListener {

    private final int socketPort;
    private final SSLContext sslContext;
    private final NodeInformant nodeInformant;
    private final AtomicReference<ProcessGroup> rootGroup = new AtomicReference<>();
    private final NiFiProperties nifiProperties;
    private final PeerDescriptionModifier peerDescriptionModifier;

    private MessageSequenceHandler<SiteToSiteMessageSequence> handler;

    public NIORemoteSiteListener(final int socketPort,
                                   final SSLContext sslContext,
                                   final NiFiProperties nifiProperties,
                                   final NodeInformant nodeInformant) {
        this.socketPort = socketPort;
        this.sslContext = sslContext;
        this.nifiProperties = nifiProperties;
        this.nodeInformant = nodeInformant;
        peerDescriptionModifier = new PeerDescriptionModifier(nifiProperties);
    }

    @Override
    public void setRootGroup(ProcessGroup rootGroup) {
        this.rootGroup.set(rootGroup);
    }

    @Override
    public void start() throws IOException {

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

        final Configurations messageSequenceHandlerConfigs = new Configurations.Builder()
                .ioThreadPoolSize(nifiProperties.getIntegerProperty("nifi.remote.input.socket.nio.threads", 10))
                .ioTimeout(FormatUtils.getTimeDuration(
                    nifiProperties.getProperty("nifi.remote.input.socket.nio.timeout", "30 secs"),
                    TimeUnit.MILLISECONDS))
                .idleConnectionTimeoutMillis(FormatUtils.getTimeDuration(
                    nifiProperties.getProperty("nifi.remote.input.socket.nio.timeout.idle", "60 secs"),
                    TimeUnit.MILLISECONDS))
                .ioBufferSize(DataUnit.parseDataSize(
                    nifiProperties.getProperty("nifi.remote.input.socket.nio.buffer", "8 KB"), DataUnit.B).intValue())
                .build();

        final Thread listenerThread = new Thread(() -> {

            try (final ServerSocketChannel serverSocketChannel = ServerSocketChannel.open()) {

                final String remoteInputHost = nifiProperties.getRemoteInputHost();
                final InetSocketAddress serverAddress = StringUtils.isEmpty(remoteInputHost)
                    ? new InetSocketAddress(socketPort)
                    : new InetSocketAddress(remoteInputHost, socketPort);
                serverSocketChannel.bind(serverAddress);

                handler = new MessageSequenceHandler<>(
                    "NIOSiteToSite",
                    messageSequenceHandlerConfigs,
                    () -> new SiteToSiteMessageSequence(
                        serverAddress,
                        self,
                        nodeInformant,
                        peerDescriptionModifier,
                        rootGroup::get,
                        messageSequenceHandlerConfigs),
                    sequence -> {},
                    (sequence, e) -> {}
                );

                handler.start(serverSocketChannel, sslContext);

            } catch (IOException e) {
                throw new RuntimeException("Failed to start NIORemoteSiteListener due to " + e, e);
            }

        }, "NIO-Site-to-Site Listener");

        listenerThread.start();
    }

    @Override
    public void stop() {
        if (handler != null) {
            handler.stop();
        }
    }

    @Override
    public void destroy() {

    }
}
