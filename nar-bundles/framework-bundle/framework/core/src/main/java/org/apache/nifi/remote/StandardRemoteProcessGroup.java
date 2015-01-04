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

import static java.util.Objects.requireNonNull;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

import javax.net.ssl.SSLContext;
import javax.security.cert.CertificateExpiredException;
import javax.security.cert.CertificateNotYetValidException;
import javax.ws.rs.core.Response;

import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.exception.CommunicationsException;
import org.apache.nifi.controller.util.RemoteProcessGroupUtils;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.events.BulletinFactory;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.ProcessGroupCounts;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroupPortDescriptor;
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.apache.nifi.remote.exception.BadRequestException;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.exception.PortNotRunningException;
import org.apache.nifi.remote.exception.UnknownPortException;
import org.apache.nifi.remote.io.socket.SocketChannelCommunicationsSession;
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannel;
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannelCommunicationsSession;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.socket.SocketClientProtocol;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.entity.ControllerEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.UniformInterfaceException;

/**
 * Represents the Root Process Group of a remote NiFi Instance. Holds
 * information about that remote instance, as well as {@link IncomingPort}s and
 * {@link OutgoingPort}s for communicating with the remote instance.
 */
public class StandardRemoteProcessGroup implements RemoteProcessGroup {

    private static final Logger logger = LoggerFactory.getLogger(StandardRemoteProcessGroup.class);

    public static final String CONTROLLER_URI_PATH = "/controller";
    public static final String ROOT_GROUP_STATUS_URI_PATH = "/controller/process-groups/root/status";
    public static final long LISTENING_PORT_REFRESH_MILLIS = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

    // status codes
    public static final int OK_STATUS_CODE = Status.OK.getStatusCode();
    public static final int UNAUTHORIZED_STATUS_CODE = Status.UNAUTHORIZED.getStatusCode();
    public static final int FORBIDDEN_STATUS_CODE = Status.FORBIDDEN.getStatusCode();
    
    private final String id;

    private final URI targetUri;
    private final URI apiUri;
    private final String host;
    private final String protocol;
    private final ProcessScheduler scheduler;
    private final EventReporter eventReporter;

    private final AtomicReference<String> name = new AtomicReference<>();
    private final AtomicReference<Position> position = new AtomicReference<>();
    private final AtomicReference<String> comments = new AtomicReference<>();
    private final AtomicReference<ProcessGroup> processGroup;
    private final AtomicBoolean transmitting = new AtomicBoolean(false);
    private final FlowController flowController;
    private final SSLContext sslContext;
    private final AtomicReference<Boolean> pointsToCluster = new AtomicReference<>(null);
    private final AtomicBoolean targetIsUnreachable = new AtomicBoolean(false);

    private volatile String communicationsTimeout = "30 sec";
    private volatile String targetId;
    private volatile String yieldDuration = "10 sec";

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    // the following variables are all protected by the read/write lock above.
    // Maps a Port Name to an OutgoingPort that can be used to push files to that port
    private final Map<String, StandardRemoteGroupPort> inputPorts = new HashMap<>();
    // Maps a Port Name to a PullingPort that can be used to receive files from that port
    private final Map<String, StandardRemoteGroupPort> outputPorts = new HashMap<>();

    private ProcessGroupCounts counts = new ProcessGroupCounts(0, 0, 0, 0, 0, 0, 0, 0);
    private Long refreshContentsTimestamp = null;
    private Integer listeningPort;
    private long listeningPortRetrievalTime = 0L;
    private Boolean destinationSecure;

    private volatile String authorizationIssue;

    private volatile PeerStatusCache peerStatusCache;
    private static final long PEER_CACHE_MILLIS = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);

    private final ScheduledExecutorService backgroundThreadExecutor;

    public StandardRemoteProcessGroup(final String id, final String targetUri, final ProcessGroup processGroup,
            final FlowController flowController, final SSLContext sslContext) {
        this.id = requireNonNull(id);
        this.flowController = requireNonNull(flowController);
        final URI uri;
        try {
            uri = new URI(requireNonNull(targetUri));

            // Trim the trailing /
            String uriPath = uri.getPath();
            if (uriPath.endsWith("/")) {
                uriPath = uriPath.substring(0, uriPath.length() - 1);
            }
            final String apiPath = uri.getScheme() + "://" + uri.getHost() + ":" + uri.getPort() + uriPath + "-api";
            apiUri = new URI(apiPath);
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }

        this.host = uri.getHost();
        this.protocol = uri.getAuthority();
        this.targetUri = uri;
        this.targetId = null;
        this.processGroup = new AtomicReference<>(processGroup);
        this.sslContext = sslContext;
        this.scheduler = flowController.getProcessScheduler();
        this.authorizationIssue = "Establishing connection to " + targetUri;

        final BulletinRepository bulletinRepository = flowController.getBulletinRepository();
        eventReporter = new EventReporter() {
            @Override
            public void reportEvent(final Severity severity, final String category, final String message) {
                final String groupId = StandardRemoteProcessGroup.this.getProcessGroup().getIdentifier();
                final String sourceId = StandardRemoteProcessGroup.this.getIdentifier();
                final String sourceName = StandardRemoteProcessGroup.this.getName();
                bulletinRepository.addBulletin(BulletinFactory.createBulletin(groupId, sourceId, sourceName, category, severity.name(), message));
            }
        };

        final Runnable socketCleanup = new Runnable() {
            @Override
            public void run() {
                final Set<StandardRemoteGroupPort> ports = new HashSet<>();
                readLock.lock();
                try {
                    ports.addAll(inputPorts.values());
                    ports.addAll(outputPorts.values());
                } finally {
                    readLock.unlock();
                }

                for (final StandardRemoteGroupPort port : ports) {
                    port.cleanupSockets();
                }
            }
        };

        try {
            final File peersFile = getPeerPersistenceFile();
            this.peerStatusCache = new PeerStatusCache(recoverPersistedPeerStatuses(peersFile), peersFile.lastModified());
        } catch (final IOException e) {
            logger.error("{} Failed to recover persisted Peer Statuses due to {}", this, e);
        }

        final Runnable refreshPeers = new Runnable() {
            @Override
            public void run() {
                final PeerStatusCache existingCache = peerStatusCache;
                if (existingCache != null && (existingCache.getTimestamp() + PEER_CACHE_MILLIS > System.currentTimeMillis())) {
                    return;
                }

                Set<RemoteGroupPort> ports = getInputPorts();
                if (ports.isEmpty()) {
                    ports = getOutputPorts();
                }
                
                if (ports.isEmpty()){
                    return;
                }

                // it doesn't really matter which port we use. Since we are just getting the Peer Status,
                // if the server indicates that the port cannot receive data for whatever reason, we will
                // simply ignore the error.
                final RemoteGroupPort port = ports.iterator().next();

                try {
                    final Set<PeerStatus> statuses = fetchRemotePeerStatuses(port);
                    peerStatusCache = new PeerStatusCache(statuses);
                    logger.info("{} Successfully refreshed Peer Status; remote instance consists of {} peers", StandardRemoteProcessGroup.this, statuses.size());
                } catch (Exception e) {
                    logger.warn("{} Unable to refresh Remote Group's peers due to {}", StandardRemoteProcessGroup.this, e);
                    if (logger.isDebugEnabled()) {
                        logger.warn("", e);
                    }
                }
            }
        };

        final Runnable checkAuthorizations = new InitializationTask();

        backgroundThreadExecutor = new FlowEngine(1, "Remote Process Group " + id + ": " + targetUri);
        backgroundThreadExecutor.scheduleWithFixedDelay(checkAuthorizations, 0L, 30L, TimeUnit.SECONDS);
        backgroundThreadExecutor.scheduleWithFixedDelay(refreshPeers, 0, 5, TimeUnit.SECONDS);
        backgroundThreadExecutor.scheduleWithFixedDelay(socketCleanup, 10L, 10L, TimeUnit.SECONDS);
    }

    @Override
    public void reinitialize(boolean isClustered) {
        this.pointsToCluster.set(null);
        backgroundThreadExecutor.submit(new InitializationTask());
    }

    @Override
    public void onRemove() {
        backgroundThreadExecutor.shutdown();

        final File file = getPeerPersistenceFile();
        if (file.exists() && !file.delete()) {
            logger.warn("Failed to remove {}. This file should be removed manually.", file);
        }
    }

    @Override
    public void shutdown() {
        backgroundThreadExecutor.shutdown();
    }
    
    @Override
    public String getIdentifier() {
        return id;
    }

    @Override
    public ProcessGroup getProcessGroup() {
        return processGroup.get();
    }

    @Override
    public void setProcessGroup(final ProcessGroup group) {
        this.processGroup.set(group);
    }

    public void setTargetId(final String targetId) {
        this.targetId = targetId;
    }

    /**
     * @return the ID of the Root Group on the remote instance
     */
    public String getTargetId() {
        return targetId;
    }

    public String getProtocol() {
        return protocol;
    }

    @Override
    public String getName() {
        final String name = this.name.get();
        return (name == null) ? targetUri.toString() : name;
    }

    @Override
    public void setName(final String name) {
        this.name.set(name);
    }

    @Override
    public String getCommunicationsTimeout() {
        return communicationsTimeout;
    }

    @Override
    public void setCommunicationsTimeout(final String timePeriod) throws IllegalArgumentException {
        // verify the timePeriod is legit
        try {
            final long millis = FormatUtils.getTimeDuration(timePeriod, TimeUnit.MILLISECONDS);
            if (millis <= 0) {
                throw new IllegalArgumentException("Time Period must be more than 0 milliseconds; Invalid Time Period: " + timePeriod);
            }
            if (millis > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Timeout is too long; cannot be greater than " + Integer.MAX_VALUE + " milliseconds");
            }
            this.communicationsTimeout = timePeriod;
        } catch (final Exception e) {
            throw new IllegalArgumentException("Invalid Time Period: " + timePeriod);
        }
    }

    @Override
    public int getCommunicationsTimeout(final TimeUnit timeUnit) {
        return (int) FormatUtils.getTimeDuration(communicationsTimeout, timeUnit);
    }

    @Override
    public String getComments() {
        return comments.get();
    }

    @Override
    public void setComments(final String comments) {
        this.comments.set(comments);
    }

    @Override
    public Position getPosition() {
        return position.get();
    }

    @Override
    public void setPosition(final Position position) {
        this.position.set(position);
    }

    @Override
    public URI getTargetUri() {
        return targetUri;
    }

    @Override
    public String getAuthorizationIssue() {
        return authorizationIssue;
    }

    public String getHost() {
        return host;
    }

    public int getInputPortCount() {
        readLock.lock();
        try {
            return inputPorts.size();
        } finally {
            readLock.unlock();
        }
    }

    public int getOutputPortCount() {
        readLock.lock();
        try {
            return outputPorts.size();
        } finally {
            readLock.unlock();
        }
    }

    public boolean containsInputPort(final String id) {
        readLock.lock();
        try {
            return inputPorts.containsKey(id);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Changes the currently configured input ports to the ports described in
     * the given set. If any port is currently configured that is not in the set
     * given, that port will be shutdown and removed. If any port is currently
     * not configured and is in the set given, that port will be instantiated
     * and started.
     *
     * @param ports the new ports
     *
     * @throws NullPointerException if the given argument is null
     */
    @Override
    public void setInputPorts(final Set<RemoteProcessGroupPortDescriptor> ports) {
        writeLock.lock();
        try {
            final List<String> newPortIds = new ArrayList<>();
            for (final RemoteProcessGroupPortDescriptor descriptor : ports) {
                newPortIds.add(descriptor.getId());

                if (!inputPorts.containsKey(descriptor.getId())) {
                    addInputPort(descriptor);
                }

                // set the comments to ensure current description
                final StandardRemoteGroupPort sendPort = inputPorts.get(descriptor.getId());
                sendPort.setTargetExists(true);
                sendPort.setName(descriptor.getName());
                if (descriptor.isTargetRunning() != null) {
                    sendPort.setTargetRunning(descriptor.isTargetRunning());
                }
                sendPort.setComments(descriptor.getComments());
            }

            // See if we have any ports that no longer exist; cannot be removed within the loop because it would cause
            // a ConcurrentModificationException.
            final Iterator<Map.Entry<String, StandardRemoteGroupPort>> itr = inputPorts.entrySet().iterator();
            while (itr.hasNext()) {
                final Map.Entry<String, StandardRemoteGroupPort> entry = itr.next();
                if (!newPortIds.contains(entry.getKey())) {
                    final StandardRemoteGroupPort port = entry.getValue();
                    port.setTargetExists(false);

                    // If port has incoming connection, it will be cleaned up when the connection is removed
                    if (!port.hasIncomingConnection()) {
                        itr.remove();
                    }
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Returns a boolean indicating whether or not an Output Port exists with
     * the given ID
     *
     * @param id
     * @return <code>true</code> if an Output Port exists with the given ID,
     * <code>false</code> otherwise.
     */
    public boolean containsOutputPort(final String id) {
        readLock.lock();
        try {
            return outputPorts.containsKey(id);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Changes the currently configured output ports to the ports described in
     * the given set. If any port is currently configured that is not in the set
     * given, that port will be shutdown and removed. If any port is currently
     * not configured and is in the set given, that port will be instantiated
     * and started.
     *
     * @param ports the new ports
     *
     * @throws NullPointerException if the given argument is null
     */
    @Override
    public void setOutputPorts(final Set<RemoteProcessGroupPortDescriptor> ports) {
        writeLock.lock();
        try {
            final List<String> newPortIds = new ArrayList<>();
            for (final RemoteProcessGroupPortDescriptor descriptor : requireNonNull(ports)) {
                newPortIds.add(descriptor.getId());

                if (!outputPorts.containsKey(descriptor.getId())) {
                    addOutputPort(descriptor);
                }

                // set the comments to ensure current description
                final StandardRemoteGroupPort receivePort = outputPorts.get(descriptor.getId());
                receivePort.setTargetExists(true);
                receivePort.setName(descriptor.getName());
                if (descriptor.isTargetRunning() != null) {
                    receivePort.setTargetRunning(descriptor.isTargetRunning());
                }
                receivePort.setComments(descriptor.getComments());
            }

            // See if we have any ports that no longer exist; cannot be removed within the loop because it would cause
            // a ConcurrentModificationException.
            final Iterator<Map.Entry<String, StandardRemoteGroupPort>> itr = outputPorts.entrySet().iterator();
            while (itr.hasNext()) {
                final Map.Entry<String, StandardRemoteGroupPort> entry = itr.next();
                if (!newPortIds.contains(entry.getKey())) {
                    final StandardRemoteGroupPort port = entry.getValue();
                    port.setTargetExists(false);

                    // If port has connections, it will be cleaned up when connections are removed
                    if (port.getConnections().isEmpty()) {
                        itr.remove();
                    }
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Shuts down and removes the given port
     *
     *
     * @throws NullPointerException if the given output Port is null
     * @throws IllegalStateException if the port does not belong to this remote
     * process group
     */
    @Override
    public void removeNonExistentPort(final RemoteGroupPort port) {
        writeLock.lock();
        try {
            if (requireNonNull(port).getTargetExists()) {
                throw new IllegalStateException("Cannot remove Remote Port " + port + " because it still exists on the Remote Instance");
            }
            if (!port.getConnections().isEmpty() || port.hasIncomingConnection()) {
                throw new IllegalStateException("Cannot remove Remote Port because it is connected to other components");
            }

            scheduler.stopPort(port);

            if (outputPorts.containsKey(port.getIdentifier())) {
                outputPorts.remove(port.getIdentifier());
            } else {
                if (!inputPorts.containsKey(port.getIdentifier())) {
                    throw new IllegalStateException("Cannot remove Remote Port because it does not belong to this Remote Process Group");
                }

                inputPorts.remove(port.getIdentifier());
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void removeAllNonExistentPorts() {
        writeLock.lock();
        try {
            final Set<String> inputPortIds = new HashSet<>();
            final Set<String> outputPortIds = new HashSet<>();

            for (final Map.Entry<String, StandardRemoteGroupPort> entry : inputPorts.entrySet()) {
                final RemoteGroupPort port = entry.getValue();

                if (port.getTargetExists()) {
                    continue;
                }

                // If there's a connection, we don't remove it.
                if (port.hasIncomingConnection()) {
                    continue;
                }

                inputPortIds.add(entry.getKey());
            }

            for (final Map.Entry<String, StandardRemoteGroupPort> entry : outputPorts.entrySet()) {
                final RemoteGroupPort port = entry.getValue();

                if (port.getTargetExists()) {
                    continue;
                }

                // If there's a connection, we don't remove it.
                if (!port.getConnections().isEmpty()) {
                    continue;
                }

                outputPortIds.add(entry.getKey());
            }

            for (final String id : inputPortIds) {
                inputPorts.remove(id);
            }
            for (final String id : outputPortIds) {
                outputPorts.remove(id);
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Adds an Output Port to this Remote Process Group that is described by
     * this DTO.
     *
     * @param descriptor
     *
     * @throws IllegalStateException if an Output Port already exists with the
     * ID given by dto.getId()
     */
    private void addOutputPort(final RemoteProcessGroupPortDescriptor descriptor) {
        writeLock.lock();
        try {
            if (outputPorts.containsKey(requireNonNull(descriptor).getId())) {
                throw new IllegalStateException("Output Port with ID " + descriptor.getId() + " already exists");
            }

            final StandardRemoteGroupPort port = new StandardRemoteGroupPort(descriptor.getId(), descriptor.getName(), getProcessGroup(),
                    this, TransferDirection.RECEIVE, ConnectableType.REMOTE_OUTPUT_PORT, sslContext, scheduler);
            outputPorts.put(descriptor.getId(), port);

            if (descriptor.getConcurrentlySchedulableTaskCount() != null) {
                port.setMaxConcurrentTasks(descriptor.getConcurrentlySchedulableTaskCount());
            }
            if (descriptor.getUseCompression() != null) {
                port.setUseCompression(descriptor.getUseCompression());
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Returns an {@link RemoteGroupPort} that can be used to send FlowFiles to
     * the port whose ID is given on the remote instance
     *
     * @param portIdentifier the ID of the Port to send FlowFiles to
     * @return
     */
    @Override
    public RemoteGroupPort getInputPort(final String portIdentifier) {
        readLock.lock();
        try {
            if (requireNonNull(portIdentifier).startsWith(id + "-")) {
                return inputPorts.get(portIdentifier.substring(id.length() + 1));
            } else {
                return inputPorts.get(portIdentifier);
            }
        } finally {
            readLock.unlock();
        }
    }

    /**
     * @return a set of {@link OutgoingPort}s used for transmitting FlowFiles to
     * the remote instance
     */
    @Override
    public Set<RemoteGroupPort> getInputPorts() {
        readLock.lock();
        try {
            final Set<RemoteGroupPort> set = new HashSet<>();
            set.addAll(inputPorts.values());
            return set;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Adds an InputPort to this ProcessGroup that is described by the given
     * DTO.
     *
     * @param descriptor
     *
     * @throws IllegalStateException if an Input Port already exists with the ID
     * given by the ID of the DTO.
     */
    private void addInputPort(final RemoteProcessGroupPortDescriptor descriptor) {
        writeLock.lock();
        try {
            if (inputPorts.containsKey(descriptor.getId())) {
                throw new IllegalStateException("Input Port with ID " + descriptor.getId() + " already exists");
            }

            final StandardRemoteGroupPort port = new StandardRemoteGroupPort(descriptor.getId(), descriptor.getName(), getProcessGroup(), this,
                    TransferDirection.SEND, ConnectableType.REMOTE_INPUT_PORT, sslContext, scheduler);

            if (descriptor.getConcurrentlySchedulableTaskCount() != null) {
                port.setMaxConcurrentTasks(descriptor.getConcurrentlySchedulableTaskCount());
            }
            if (descriptor.getUseCompression() != null) {
                port.setUseCompression(descriptor.getUseCompression());
            }

            inputPorts.put(descriptor.getId(), port);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Returns an {@link RemoteGroupPort} that can be used to receive FlowFiles
     * from the port whose name is given on the remote instance
     *
     * @param portIdentifier the name of the Port to receive FlowFiles from
     * @return
     */
    @Override
    public RemoteGroupPort getOutputPort(final String portIdentifier) {
        readLock.lock();
        try {
            if (requireNonNull(portIdentifier).startsWith(id + "-")) {
                return outputPorts.get(portIdentifier.substring(id.length() + 1));
            } else {
                return outputPorts.get(portIdentifier);
            }
        } finally {
            readLock.unlock();
        }
    }

    /**
     * @return a set of {@link RemoteGroupPort}s used for receiving FlowFiles
     * from the remote instance
     */
    @Override
    public Set<RemoteGroupPort> getOutputPorts() {
        readLock.lock();
        try {
            final Set<RemoteGroupPort> set = new HashSet<>();
            set.addAll(outputPorts.values());
            return set;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public String toString() {
        return "RemoteProcessGroup[" + targetUri + "]";
    }

    @Override
    public ProcessGroupCounts getCounts() {
        readLock.lock();
        try {
            return counts;
        } finally {
            readLock.unlock();
        }
    }

    private void setCounts(final ProcessGroupCounts counts) {
        writeLock.lock();
        try {
            this.counts = counts;
        } finally {
            writeLock.unlock();
        }
    }

    private ProcessGroup getRootGroup() {
        return getRootGroup(getProcessGroup());
    }

    private ProcessGroup getRootGroup(final ProcessGroup context) {
        final ProcessGroup parent = context.getParent();
        return (parent == null) ? context : getRootGroup(parent);
    }

    private boolean isWebApiSecure() {
        return targetUri.toString().toLowerCase().startsWith("https");
    }

    private void refreshFlowContentsFromLocal() {
        final ProcessGroup rootGroup = getRootGroup();
        setName(rootGroup.getName());
        setTargetId(rootGroup.getIdentifier());
        setComments(rootGroup.getComments());
        setCounts(rootGroup.getCounts());

        final Set<RemoteProcessGroupPortDescriptor> convertedInputPorts = new HashSet<>();
        for (final Port port : rootGroup.getInputPorts()) {
            convertedInputPorts.add(convertPortToRemotePortDescriptor(port));
        }

        final Set<RemoteProcessGroupPortDescriptor> convertedOutputPorts = new HashSet<>();
        for (final Port port : rootGroup.getOutputPorts()) {
            convertedOutputPorts.add(convertPortToRemotePortDescriptor(port));
        }

        setInputPorts(convertedInputPorts);
        setOutputPorts(convertedOutputPorts);

        writeLock.lock();
        try {
            final NiFiProperties props = NiFiProperties.getInstance();
            this.destinationSecure = props.isSiteToSiteSecure();
            this.listeningPort = props.getRemoteInputPort();

            refreshContentsTimestamp = System.currentTimeMillis();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Date getLastRefreshTime() {
        readLock.lock();
        try {
            return (refreshContentsTimestamp == null) ? null : new Date(refreshContentsTimestamp);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void refreshFlowContents() throws CommunicationsException {
        // check to see if we know whether we pointing to a local cluster or not -
        // we must know this before we attempt to update since querying the cluster
        // will cause a deadlock if we are in the context of another replicated request
        if (pointsToCluster.get() == null) {
            return;
        }

        if (pointsToCluster.get()) {
            refreshFlowContentsFromLocal();
            return;
        }

        if (targetIsUnreachable.get()) {
            return;
        }

        final RemoteProcessGroupUtils utils = new RemoteProcessGroupUtils(isWebApiSecure() ? sslContext : null);
        final String uriVal = apiUri.toString() + CONTROLLER_URI_PATH;
        URI uri;
        try {
            uri = new URI(uriVal);
        } catch (final URISyntaxException e) {
            throw new CommunicationsException("Invalid URI: " + uriVal);
        }

        try {
            // perform the request
            final ClientResponse response = utils.get(uri, getCommunicationsTimeout(TimeUnit.MILLISECONDS));
            
            if (!Response.Status.Family.SUCCESSFUL.equals(response.getStatusInfo().getFamily())) {
                writeLock.lock();
                try {
                    for (final Iterator<StandardRemoteGroupPort> iter = inputPorts.values().iterator(); iter.hasNext();) {
                        final StandardRemoteGroupPort inputPort = iter.next();
                        if (!inputPort.hasIncomingConnection()) {
                            iter.remove();
                        }
                    }

                    for (final Iterator<StandardRemoteGroupPort> iter = outputPorts.values().iterator(); iter.hasNext();) {
                        final StandardRemoteGroupPort outputPort = iter.next();
                        if (outputPort.getConnections().isEmpty()) {
                            iter.remove();
                        }
                    }
                } finally {
                    writeLock.unlock();
                }

                // consume the entity entirely
                response.getEntity(String.class);
                throw new CommunicationsException("Unable to communicate with Remote NiFi at URI " + uriVal + ". Got HTTP Error Code " + response.getStatus() + ": " + response.getStatusInfo().getReasonPhrase());
            }

            final ControllerEntity entity = response.getEntity(ControllerEntity.class);
            final ControllerDTO dto = entity.getController();

            writeLock.lock();
            try {
                if (dto.getInputPorts() != null) {
                    setInputPorts(convertRemotePort(dto.getInputPorts()));
                }
                if (dto.getOutputPorts() != null) {
                    setOutputPorts(convertRemotePort(dto.getOutputPorts()));
                }

                // set the controller details
                setTargetId(dto.getId());
                setName(dto.getName());
                setComments(dto.getComments());

                // get the component counts
                int inputPortCount = 0;
                if (dto.getInputPortCount() != null) {
                    inputPortCount = dto.getInputPortCount();
                }
                int outputPortCount = 0;
                if (dto.getOutputPortCount() != null) {
                    outputPortCount = dto.getOutputPortCount();
                }
                int runningCount = 0;
                if (dto.getRunningCount() != null) {
                    runningCount = dto.getRunningCount();
                }
                int stoppedCount = 0;
                if (dto.getStoppedCount() != null) {
                    stoppedCount = dto.getStoppedCount();
                }
                int invalidCount = 0;
                if (dto.getInvalidCount() != null) {
                    invalidCount = dto.getInvalidCount();
                }
                int disabledCount = 0;
                if (dto.getDisabledCount() != null) {
                    disabledCount = dto.getDisabledCount();
                }

                int activeRemotePortCount = 0;
                if (dto.getActiveRemotePortCount() != null) {
                    activeRemotePortCount = dto.getActiveRemotePortCount();
                }

                int inactiveRemotePortCount = 0;
                if (dto.getInactiveRemotePortCount() != null) {
                    inactiveRemotePortCount = dto.getInactiveRemotePortCount();
                }

                this.listeningPort = dto.getRemoteSiteListeningPort();
                this.destinationSecure = dto.isSiteToSiteSecure();

                final ProcessGroupCounts newCounts = new ProcessGroupCounts(inputPortCount, outputPortCount,
                        runningCount, stoppedCount, invalidCount, disabledCount, activeRemotePortCount, inactiveRemotePortCount);
                setCounts(newCounts);
                this.refreshContentsTimestamp = System.currentTimeMillis();
            } finally {
                writeLock.unlock();
            }
        } catch (final ClientHandlerException | UniformInterfaceException e) {
            throw new CommunicationsException(e);
        }
    }

    /**
     * Converts a set of ports into a set of remote process group ports.
     *
     * @param ports
     * @return
     */
    private Set<RemoteProcessGroupPortDescriptor> convertRemotePort(final Set<PortDTO> ports) {
        Set<RemoteProcessGroupPortDescriptor> remotePorts = null;
        if (ports != null) {
            remotePorts = new LinkedHashSet<>(ports.size());
            for (PortDTO port : ports) {
                final StandardRemoteProcessGroupPortDescriptor descriptor = new StandardRemoteProcessGroupPortDescriptor();
                final ScheduledState scheduledState = ScheduledState.valueOf(port.getState());
                descriptor.setId(port.getId());
                descriptor.setName(port.getName());
                descriptor.setComments(port.getComments());
                descriptor.setTargetRunning(ScheduledState.RUNNING.equals(scheduledState));
                remotePorts.add(descriptor);
            }
        }
        return remotePorts;
    }

    private RemoteProcessGroupPortDescriptor convertPortToRemotePortDescriptor(final Port port) {
        final StandardRemoteProcessGroupPortDescriptor descriptor = new StandardRemoteProcessGroupPortDescriptor();
        descriptor.setComments(port.getComments());
        descriptor.setExists(true);
        descriptor.setGroupId(port.getProcessGroup().getIdentifier());
        descriptor.setId(port.getIdentifier());
        descriptor.setName(port.getName());
        descriptor.setTargetRunning(port.isRunning());
        return descriptor;
    }

    /**
     * @return the port that the remote instance is listening on for
     * site-to-site communication, or <code>null</code> if the remote instance
     * is not configured to allow site-to-site communications.
     *
     * @throws IOException if unable to communicate with the remote instance
     */
    @Override
    public Integer getListeningPort() throws IOException {
        Integer listeningPort;
        readLock.lock();
        try {
            listeningPort = this.listeningPort;
            if (listeningPort != null && this.listeningPortRetrievalTime > System.currentTimeMillis() - LISTENING_PORT_REFRESH_MILLIS) {
                return listeningPort;
            }
        } finally {
            readLock.unlock();
        }

        final RemoteProcessGroupUtils utils = new RemoteProcessGroupUtils(isWebApiSecure() ? sslContext : null);
        listeningPort = utils.getRemoteListeningPort(apiUri.toString(), getCommunicationsTimeout(TimeUnit.MILLISECONDS));
        writeLock.lock();
        try {
            this.listeningPort = listeningPort;
            this.listeningPortRetrievalTime = System.currentTimeMillis();
        } finally {
            writeLock.unlock();
        }

        return listeningPort;
    }

    @Override
    public boolean isTransmitting() {
        return transmitting.get();
    }

    @Override
    public void startTransmitting() {
        writeLock.lock();
        try {
            verifyCanStartTransmitting();

            // Check if any port is invalid
            boolean invalidPort = false;
            for (final Port port : getInputPorts()) {
                if (!port.isValid()) {
                    invalidPort = true;
                    break;
                }
            }

            if (!invalidPort) {
                for (final Port port : getOutputPorts()) {
                    if (!port.isValid()) {
                        invalidPort = true;
                    }
                }
            }

            // if any port is invalid, refresh contents to check if it is still invalid
            boolean allPortsInvalid = invalidPort;
            if (invalidPort) {
                try {
                    refreshFlowContents();
                } catch (final CommunicationsException e) {
                    logger.warn("{} Attempted to refresh Flow Contents because at least one port is invalid but failed due to {}", this, e);
                }

                for (final Port port : getInputPorts()) {
                    if (port.isValid()) {
                        allPortsInvalid = false;
                        break;
                    }
                }
                for (final Port port : getOutputPorts()) {
                    if (port.isValid()) {
                        allPortsInvalid = false;
                        break;
                    }
                }
            }

            if (allPortsInvalid) {
                throw new IllegalStateException("Cannot Enable Transmission because all Input Ports & Output Ports to this Remote Process Group are in invalid states");
            }

            for (final Port port : getInputPorts()) {
                // if port is not valid, don't start it because it will never become valid.
                // Validation is based on connections and whether or not the remote target exists.
                if (port.isValid() && port.hasIncomingConnection()) {
                    scheduler.startPort(port);
                }
            }

            for (final Port port : getOutputPorts()) {
                if (port.isValid() && !port.getConnections().isEmpty()) {
                    scheduler.startPort(port);
                }
            }

            transmitting.set(true);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void startTransmitting(final RemoteGroupPort port) {
        writeLock.lock();
        try {
            if (!inputPorts.containsValue(port) && !outputPorts.containsValue(port)) {
                throw new IllegalArgumentException("Port does not belong to this Remote Process Group");
            }

            port.verifyCanStart();

            scheduler.startPort(port);

            transmitting.set(true);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void stopTransmitting() {
        writeLock.lock();
        try {
            verifyCanStopTransmitting();

            for (final RemoteGroupPort port : getInputPorts()) {
                scheduler.stopPort(port);
            }

            for (final RemoteGroupPort port : getOutputPorts()) {
                scheduler.stopPort(port);
            }

            // Wait for the ports to stop
            for (final RemoteGroupPort port : getInputPorts()) {
                while (port.isRunning()) {
                    try {
                        Thread.sleep(50L);
                    } catch (final InterruptedException e) {
                    }
                }
            }

            for (final RemoteGroupPort port : getOutputPorts()) {
                while (port.isRunning()) {
                    try {
                        Thread.sleep(50L);
                    } catch (final InterruptedException e) {
                    }
                }
            }

            transmitting.set(false);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void stopTransmitting(final RemoteGroupPort port) {
        writeLock.lock();
        try {
            if (!inputPorts.containsValue(port) && !outputPorts.containsValue(port)) {
                throw new IllegalArgumentException("Port does not belong to this Remote Process Group");
            }

            port.verifyCanStop();

            scheduler.stopPort(port);

            // Wait for the port to stop
            while (port.isRunning()) {
                try {
                    Thread.sleep(50L);
                } catch (final InterruptedException e) {
                }
            }

            // Determine if any other ports are still running
            boolean stillTransmitting = false;
            for (final Port inputPort : getInputPorts()) {
                if (inputPort.isRunning()) {
                    stillTransmitting = true;
                    break;
                }
            }

            if (!stillTransmitting) {
                for (final Port outputPort : getOutputPorts()) {
                    if (outputPort.isRunning()) {
                        stillTransmitting = true;
                        break;
                    }
                }
            }

            transmitting.set(stillTransmitting);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean isSecure() throws CommunicationsException {
        Boolean secure;
        readLock.lock();
        try {
            secure = this.destinationSecure;
            if (secure != null) {
                return secure;
            }
        } finally {
            readLock.unlock();
        }

        refreshFlowContents();

        readLock.lock();
        try {
            secure = this.destinationSecure;
            if (secure == null) {
                throw new CommunicationsException("Unable to determine whether or not site-to-site communications with peer should be secure");
            }

            return secure;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Boolean getSecureFlag() {
        readLock.lock();
        try {
            return this.destinationSecure;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean isSiteToSiteEnabled() {
        readLock.lock();
        try {
            return this.listeningPort != null;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public CommunicationsSession establishSiteToSiteConnection() throws IOException {
        final URI uri = apiUri;
        final String destinationUri = uri.toString();
        CommunicationsSession commsSession = null;
        try {
            if (isSecure()) {
                if (sslContext == null) {
                    throw new IOException("Unable to communicate with " + getTargetUri() + " because it requires Secure Site-to-Site communications, but this instance is not configured for secure communications");
                }

                final Integer listeningPort = getListeningPort();
                if (listeningPort == null) {
                    throw new IOException("Remote instance is not configured to allow incoming Site-to-Site connections");
                }

                final SSLSocketChannel socketChannel = new SSLSocketChannel(sslContext, uri.getHost(), listeningPort, true);
                socketChannel.connect();
                commsSession = new SSLSocketChannelCommunicationsSession(socketChannel, destinationUri);

                try {
                    commsSession.setUserDn(socketChannel.getDn());
                } catch (final CertificateNotYetValidException | CertificateExpiredException ex) {
                    throw new IOException(ex);
                }
            } else {
                final SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress(uri.getHost(), getListeningPort()));

                commsSession = new SocketChannelCommunicationsSession(socketChannel, destinationUri);
            }

            commsSession.getOutput().getOutputStream().write(CommunicationsSession.MAGIC_BYTES);

            commsSession.setUri("nifi://" + uri.getHost() + ":" + uri.getPort());
        } catch (final IOException e) {
            if (commsSession != null) {
                try {
                    commsSession.close();
                } catch (final IOException ignore) {
                }
            }

            throw e;
        }
        return commsSession;
    }

    @Override
    public EventReporter getEventReporter() {
        return eventReporter;
    }

    private class InitializationTask implements Runnable {

        @Override
        public void run() {
            try {
                final RemoteProcessGroupUtils utils = new RemoteProcessGroupUtils(isWebApiSecure() ? sslContext : null);
                final ClientResponse response = utils.get(new URI(apiUri + CONTROLLER_URI_PATH), getCommunicationsTimeout(TimeUnit.MILLISECONDS));
                
                final int statusCode = response.getStatus();
                
                if ( statusCode == OK_STATUS_CODE ) {
                    final ControllerEntity entity = response.getEntity(ControllerEntity.class);
                    final ControllerDTO dto = entity.getController();

                    if (dto.getRemoteSiteListeningPort() == null) {
                        authorizationIssue = "Remote instance is not configured to allow Site-to-Site communications at this time.";
                    } else {
                        authorizationIssue = null;
                    }

                    writeLock.lock();
                    try {
                        listeningPort = dto.getRemoteSiteListeningPort();
                        destinationSecure = dto.isSiteToSiteSecure();
                    } finally {
                        writeLock.unlock();
                    }

                    final String remoteInstanceId = dto.getInstanceId();
                    boolean isPointingToCluster = flowController.getInstanceId().equals(remoteInstanceId);
                    pointsToCluster.set(isPointingToCluster);
                } else if ( statusCode == UNAUTHORIZED_STATUS_CODE ) {
                    try {
                        final ClientResponse requestAccountResponse = utils.issueRegistrationRequest(apiUri.toString());
                        if (Response.Status.Family.SUCCESSFUL.equals(requestAccountResponse.getStatusInfo().getFamily()) ) {
                            logger.info("{} Issued a Request to communicate with remote instance", this);
                        } else {
                            logger.error("{} Failed to request account: got unexpected response code of {}:{}", new Object[]{
                                this, requestAccountResponse.getStatus(), requestAccountResponse.getStatusInfo().getReasonPhrase()});
                        }
                    } catch (final Exception e) {
                        logger.error("{} Failed to request account due to {}", this, e.toString());
                        if (logger.isDebugEnabled()) {
                            logger.error("", e);
                        }
                    }

                    authorizationIssue = response.getEntity(String.class);
                } else if ( statusCode == FORBIDDEN_STATUS_CODE ) {
                    authorizationIssue = response.getEntity(String.class);
                } else {
                    final String message = response.getEntity(String.class);
                    logger.warn("{} When communicating with remote instance, got unexpected response code {}:{} with entity: {}",
                            new Object[]{this, response.getStatus(), response.getStatusInfo().getReasonPhrase(), message});
                    authorizationIssue = "Unable to determine Site-to-Site availability.";
                }
            } catch (Exception e) {
                logger.warn(String.format("Unable to connect to %s due to %s", StandardRemoteProcessGroup.this, e));
                getEventReporter().reportEvent(Severity.WARNING, "Site to Site", String.format("Unable to connect to %s due to %s",
                        StandardRemoteProcessGroup.this.getTargetUri().toString(), e));
            }
        }
    }

    @Override
    public void setYieldDuration(final String yieldDuration) {
        // verify the syntax
        if (!FormatUtils.TIME_DURATION_PATTERN.matcher(yieldDuration).matches()) {
            throw new IllegalArgumentException("Improperly formatted Time Period; should be of syntax <number> <unit> where <number> is a positive integer and unit is one of the valid Time Units, such as nanos, millis, sec, min, hour, day");
        }

        this.yieldDuration = yieldDuration;
    }

    @Override
    public String getYieldDuration() {
        return yieldDuration;
    }

    @Override
    public void verifyCanDelete() {
        verifyCanDelete(false);
    }

    @Override
    public void verifyCanDelete(final boolean ignoreConnections) {
        readLock.lock();
        try {
            if (isTransmitting()) {
                throw new IllegalStateException(this + " is transmitting");
            }

            for (final Port port : inputPorts.values()) {
                if (!ignoreConnections && port.hasIncomingConnection()) {
                    throw new IllegalStateException(this + " is the destination of another component");
                }
                if (port.isRunning()) {
                    throw new IllegalStateException(this + " has running Port: " + port);
                }
            }

            for (final Port port : outputPorts.values()) {
                if (!ignoreConnections) {
                    for (final Connection connection : port.getConnections()) {
                        connection.verifyCanDelete();
                    }
                }

                if (port.isRunning()) {
                    throw new IllegalStateException(this + " has running Port: " + port);
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void verifyCanStartTransmitting() {
        readLock.lock();
        try {
            if (isTransmitting()) {
                throw new IllegalStateException(this + " is already transmitting");
            }

            for (final StandardRemoteGroupPort port : inputPorts.values()) {
                if (port.isRunning()) {
                    throw new IllegalStateException(this + " has running Port: " + port);
                }

                if (port.hasIncomingConnection() && !port.getTargetExists()) {
                    throw new IllegalStateException(this + " has a Connection to Port " + port + ", but that Port no longer exists on the remote system");
                }
            }

            for (final StandardRemoteGroupPort port : outputPorts.values()) {
                if (port.isRunning()) {
                    throw new IllegalStateException(this + " has running Port: " + port);
                }

                if (!port.getConnections().isEmpty() && !port.getTargetExists()) {
                    throw new IllegalStateException(this + " has a Connection to Port " + port + ", but that Port no longer exists on the remote system");
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void verifyCanStopTransmitting() {
        if (!isTransmitting()) {
            throw new IllegalStateException(this + " is not transmitting");
        }
    }

    @Override
    public void verifyCanUpdate() {
        readLock.lock();
        try {
            if (isTransmitting()) {
                throw new IllegalStateException(this + " is currently transmitting");
            }

            for (final Port port : inputPorts.values()) {
                if (port.isRunning()) {
                    throw new IllegalStateException(this + " has running Port: " + port);
                }
            }

            for (final Port port : outputPorts.values()) {
                if (port.isRunning()) {
                    throw new IllegalStateException(this + " has running Port: " + port);
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Set<PeerStatus> getPeerStatuses() {
        final PeerStatusCache cache = this.peerStatusCache;
        if (cache == null || cache.getStatuses() == null || cache.getStatuses().isEmpty()) {
            return null;
        }

        if (cache.getTimestamp() + PEER_CACHE_MILLIS < System.currentTimeMillis()) {
            final Set<PeerStatus> equalizedSet = new HashSet<>(cache.getStatuses().size());
            for (final PeerStatus status : cache.getStatuses()) {
                final PeerStatus equalizedStatus = new PeerStatus(status.getHostname(), status.getPort(), status.isSecure(), 1);
                equalizedSet.add(equalizedStatus);
            }

            return equalizedSet;
        }

        return cache.getStatuses();
    }

    private Set<PeerStatus> fetchRemotePeerStatuses(final RemoteGroupPort port) throws IOException, HandshakeException, UnknownPortException, PortNotRunningException, BadRequestException {
        final CommunicationsSession commsSession = establishSiteToSiteConnection();
        final Peer peer = new Peer(commsSession, "nifi://" + getTargetUri().getHost() + ":" + getListeningPort());
        final SocketClientProtocol clientProtocol = new SocketClientProtocol();
        clientProtocol.setPort(port);
        final DataInputStream dis = new DataInputStream(commsSession.getInput().getInputStream());
        final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());
        try {
            RemoteResourceFactory.initiateResourceNegotiation(clientProtocol, dis, dos);
        } catch (final HandshakeException e) {
            throw new BadRequestException(e.toString());
        }

        clientProtocol.handshake(peer);
        final Set<PeerStatus> peerStatuses = clientProtocol.getPeerStatuses(peer);
        persistPeerStatuses(peerStatuses);

        try {
            clientProtocol.shutdown(peer);
        } catch (final IOException e) {
            final String message = String.format("%s Failed to shutdown protocol when updating list of peers due to %s", this, e.toString());
            logger.warn(message);
            if (logger.isDebugEnabled()) {
                logger.warn("", e);
            }
            getEventReporter().reportEvent(Severity.WARNING, "Site to Site", message);
        }

        try {
            peer.close();
        } catch (final IOException e) {
            final String message = String.format("%s Failed to close resources when updating list of peers due to %s", this, e.toString());
            logger.warn(message);
            if (logger.isDebugEnabled()) {
                logger.warn("", e);
            }
            getEventReporter().reportEvent(Severity.WARNING, "Site to Site", message);
        }

        return peerStatuses;
    }

    private File getPeerPersistenceFile() {
        final File stateDir = NiFiProperties.getInstance().getPersistentStateDirectory();
        return new File(stateDir, getIdentifier() + ".peers");
    }

    private void persistPeerStatuses(final Set<PeerStatus> statuses) {
        final File peersFile = getPeerPersistenceFile();
        try (final OutputStream fos = new FileOutputStream(peersFile);
                final OutputStream out = new BufferedOutputStream(fos)) {

            for (final PeerStatus status : statuses) {
                final String line = status.getHostname() + ":" + status.getPort() + ":" + status.isSecure() + "\n";
                out.write(line.getBytes(StandardCharsets.UTF_8));
            }

        } catch (final IOException e) {
            logger.error("Failed to persist list of Peers due to {}; if restarted and peer's NCM is down, may be unable to transfer data until communications with NCM are restored", e.toString(), e);
        }
    }

    private Set<PeerStatus> recoverPersistedPeerStatuses(final File file) throws IOException {
        if (!file.exists()) {
            return null;
        }

        final Set<PeerStatus> statuses = new HashSet<>();
        try (final InputStream fis = new FileInputStream(file);
                final BufferedReader reader = new BufferedReader(new InputStreamReader(fis))) {

            String line;
            while ((line = reader.readLine()) != null) {
                final String[] splits = line.split(Pattern.quote(":"));
                if (splits.length != 3) {
                    continue;
                }

                final String hostname = splits[0];
                final int port = Integer.parseInt(splits[1]);
                final boolean secure = Boolean.parseBoolean(splits[2]);

                statuses.add(new PeerStatus(hostname, port, secure, 1));
            }
        }

        return statuses;
    }

    private static class PeerStatusCache {

        private final Set<PeerStatus> statuses;
        private final long timestamp;

        public PeerStatusCache(final Set<PeerStatus> statuses) {
            this(statuses, System.currentTimeMillis());
        }

        public PeerStatusCache(final Set<PeerStatus> statuses, final long timestamp) {
            this.statuses = statuses;
            this.timestamp = timestamp;
        }

        public Set<PeerStatus> getStatuses() {
            return statuses;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }
}
