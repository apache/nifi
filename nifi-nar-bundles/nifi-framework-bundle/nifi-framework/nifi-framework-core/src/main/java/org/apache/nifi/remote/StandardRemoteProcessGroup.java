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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.exception.CommunicationsException;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.events.BulletinFactory;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroupCounts;
import org.apache.nifi.groups.RemoteProcessGroupPortDescriptor;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.remote.protocol.http.HttpProxy;
import org.apache.nifi.remote.util.SiteToSiteRestApiClient;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.ComponentType;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Represents the Root Process Group of a remote NiFi Instance. Holds
 * information about that remote instance, as well as Incoming Ports and
 * Outgoing Ports for communicating with the remote instance.
 */
public class StandardRemoteProcessGroup implements RemoteProcessGroup {

    private static final Logger logger = LoggerFactory.getLogger(StandardRemoteProcessGroup.class);

    // status codes
    private static final int UNAUTHORIZED_STATUS_CODE = Response.Status.UNAUTHORIZED.getStatusCode();
    private static final int FORBIDDEN_STATUS_CODE = Response.Status.FORBIDDEN.getStatusCode();

    private final String id;

    private volatile String targetUris;
    private final ProcessScheduler scheduler;
    private final EventReporter eventReporter;
    private final NiFiProperties nifiProperties;
    private final long remoteContentsCacheExpiration;
    private volatile boolean initialized = false;

    private final AtomicReference<String> name = new AtomicReference<>();
    private final AtomicReference<Position> position = new AtomicReference<>(new Position(0D, 0D));
    private final AtomicReference<String> comments = new AtomicReference<>();
    private final AtomicReference<ProcessGroup> processGroup;
    private final AtomicBoolean transmitting = new AtomicBoolean(false);
    private final AtomicReference<String> versionedComponentId = new AtomicReference<>();
    private final SSLContext sslContext;

    private volatile String communicationsTimeout = "30 sec";
    private volatile String targetId;
    private volatile String yieldDuration = "10 sec";
    private volatile SiteToSiteTransportProtocol transportProtocol = SiteToSiteTransportProtocol.RAW;
    private volatile String proxyHost;
    private volatile Integer proxyPort;
    private volatile String proxyUser;
    private volatile String proxyPassword;

    private String networkInterfaceName;
    private InetAddress localAddress;
    private ValidationResult nicValidationResult;


    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    // the following variables are all protected by the read/write lock above.
    // Maps a Port Name to an OutgoingPort that can be used to push files to that port
    private final Map<String, StandardRemoteGroupPort> inputPorts = new HashMap<>();
    // Maps a Port Name to a PullingPort that can be used to receive files from that port
    private final Map<String, StandardRemoteGroupPort> outputPorts = new HashMap<>();

    private RemoteProcessGroupCounts counts = new RemoteProcessGroupCounts(0, 0);
    private Long refreshContentsTimestamp = null;
    private Boolean destinationSecure;
    private Integer listeningPort;
    private Integer listeningHttpPort;

    private volatile String authorizationIssue;

    private final ScheduledExecutorService backgroundThreadExecutor;

    public StandardRemoteProcessGroup(final String id, final String targetUris, final ProcessGroup processGroup, final ProcessScheduler processScheduler,
                                      final BulletinRepository bulletinRepository, final SSLContext sslContext, final NiFiProperties nifiProperties) {
        this.nifiProperties = nifiProperties;
        this.id = requireNonNull(id);

        this.targetUris = targetUris;
        this.targetId = null;
        this.processGroup = new AtomicReference<>(processGroup);
        this.sslContext = sslContext;
        this.scheduler = processScheduler;
        this.authorizationIssue = "Establishing connection to " + targetUris;

        final String expirationPeriod = nifiProperties.getProperty(NiFiProperties.REMOTE_CONTENTS_CACHE_EXPIRATION, "30 secs");
        remoteContentsCacheExpiration = FormatUtils.getTimeDuration(expirationPeriod, TimeUnit.MILLISECONDS);

        eventReporter = new EventReporter() {
            private static final long serialVersionUID = 1L;

            @Override
            public void reportEvent(final Severity severity, final String category, final String message) {
                final String groupId = StandardRemoteProcessGroup.this.getProcessGroup().getIdentifier();
                final String groupName = StandardRemoteProcessGroup.this.getProcessGroup().getName();
                final String sourceId = StandardRemoteProcessGroup.this.getIdentifier();
                final String sourceName = StandardRemoteProcessGroup.this.getName();
                bulletinRepository.addBulletin(BulletinFactory.createBulletin(groupId, groupName, sourceId, ComponentType.REMOTE_PROCESS_GROUP,
                        sourceName, category, severity.name(), message));
            }
        };

        backgroundThreadExecutor = new FlowEngine(1, "Remote Process Group " + id, true);
    }

    @Override
    public void initialize() {
        if (initialized) {
            return;
        }

        initialized = true;
        backgroundThreadExecutor.submit(() -> {
            try {
                refreshFlowContents();
            } catch (final Exception e) {
                logger.warn("Unable to communicate with remote instance {}", new Object[] {this, e});
            }
        });

        final Runnable checkAuthorizations = new InitializationTask();
        backgroundThreadExecutor.scheduleWithFixedDelay(checkAuthorizations, 0L, 60L, TimeUnit.SECONDS);
    }

    @Override
    public void setTargetUris(final String targetUris) {
        requireNonNull(targetUris);

        // only attempt to update the target uris if they have changed
        if (targetUris.equals(this.targetUris)) {
            return;
        }

        verifyCanUpdate();

        this.targetUris = targetUris;
        backgroundThreadExecutor.submit(new InitializationTask());
    }

    @Override
    public void reinitialize(boolean isClustered) {
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
    public String getProcessGroupIdentifier() {
        final ProcessGroup procGroup = getProcessGroup();
        return procGroup == null ? null : procGroup.getIdentifier();
    }

    @Override
    public Authorizable getParentAuthorizable() {
        return getProcessGroup();
    }

    @Override
    public Resource getResource() {
        return ResourceFactory.getComponentResource(ResourceType.RemoteProcessGroup, getIdentifier(), getName());
    }

    @Override
    public ProcessGroup getProcessGroup() {
        return processGroup.get();
    }

    @Override
    public void setProcessGroup(final ProcessGroup group) {
        this.processGroup.set(group);
        for (final RemoteGroupPort port : getInputPorts()) {
            port.setProcessGroup(group);
        }
        for (final RemoteGroupPort port : getOutputPorts()) {
            port.setProcessGroup(group);
        }
    }

    public void setTargetId(final String targetId) {
        this.targetId = targetId;
    }

    @Override
    public void setTransportProtocol(final SiteToSiteTransportProtocol transportProtocol) {
        this.transportProtocol = transportProtocol;
    }

    @Override
    public SiteToSiteTransportProtocol getTransportProtocol() {
        return transportProtocol;
    }

    @Override
    public String getProxyHost() {
        return proxyHost;
    }

    @Override
    public void setProxyHost(String proxyHost) {
        this.proxyHost = proxyHost;
    }

    @Override
    public Integer getProxyPort() {
        return proxyPort;
    }

    @Override
    public void setProxyPort(Integer proxyPort) {
        this.proxyPort = proxyPort;
    }

    @Override
    public String getProxyUser() {
        return proxyUser;
    }

    @Override
    public void setProxyUser(String proxyUser) {
        this.proxyUser = proxyUser;
    }

    @Override
    public String getProxyPassword() {
        return proxyPassword;
    }

    @Override
    public void setProxyPassword(String proxyPassword) {
        this.proxyPassword = proxyPassword;
    }

    /**
     * @return the ID of the Root Group on the remote instance
     */
    public String getTargetId() {
        return targetId;
    }

    @Override
    public String getName() {
        final String name = this.name.get();
        return name == null ? getTargetUri() : name;
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
    public String getTargetUri() {
        return SiteToSiteRestApiClient.getFirstUrl(targetUris);
    }

    @Override
    public String getTargetUris() {
        return targetUris;
    }

    @Override
    public String getAuthorizationIssue() {
        return authorizationIssue;
    }

    @Override
    public Collection<ValidationResult> validate() {
        return (nicValidationResult == null) ? Collections.emptyList() : Collections.singletonList(nicValidationResult);
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
     * @param pruneUnusedPorts if true, any ports that are not included in the given set of ports
     *            and that do not have any incoming connections will be removed.
     *
     * @throws NullPointerException if the given argument is null
     */
    @Override
    public void setInputPorts(final Set<RemoteProcessGroupPortDescriptor> ports, final boolean pruneUnusedPorts) {
        writeLock.lock();
        try {
            final List<String> newPortTargetIds = new ArrayList<>();
            for (final RemoteProcessGroupPortDescriptor descriptor : ports) {
                newPortTargetIds.add(descriptor.getTargetId());

                final Map<String, StandardRemoteGroupPort> inputPortByTargetId = inputPorts.values().stream()
                    .collect(Collectors.toMap(StandardRemoteGroupPort::getTargetIdentifier, Function.identity()));

                final Map<String, StandardRemoteGroupPort> inputPortByName = inputPorts.values().stream()
                    .collect(Collectors.toMap(StandardRemoteGroupPort::getName, Function.identity()));

                // Check if we have a matching port already and add the port if not. We determine a matching port
                // by first finding a port that has the same Target ID. If none exists, then we try to find a port with
                // the same name. We do this because if the URL of this RemoteProcessGroup is changed, then we expect
                // the NiFi at the new URL to have a Port with the same name but a different Identifier. This is okay
                // because Ports are required to have unique names.
                StandardRemoteGroupPort sendPort = inputPortByTargetId.get(descriptor.getTargetId());
                if (sendPort == null) {
                    sendPort = inputPortByName.get(descriptor.getName());
                    if (sendPort == null) {
                        sendPort = addInputPort(descriptor);
                    } else {
                        sendPort.setTargetIdentifier(descriptor.getTargetId());
                    }
                }

                // set the comments to ensure current description
                sendPort.setTargetExists(true);
                sendPort.setName(descriptor.getName());
                if (descriptor.isTargetRunning() != null) {
                    sendPort.setTargetRunning(descriptor.isTargetRunning());
                }
                sendPort.setComments(descriptor.getComments());
            }

            // See if we have any ports that no longer exist; cannot be removed within the loop because it would cause
            // a ConcurrentModificationException.
            if (pruneUnusedPorts) {
                final Iterator<StandardRemoteGroupPort> itr = inputPorts.values().iterator();
                while (itr.hasNext()) {
                    final StandardRemoteGroupPort port = itr.next();
                    if (!newPortTargetIds.contains(port.getTargetIdentifier())) {
                        port.setTargetExists(false);
                        port.setTargetRunning(false);

                        // If port has incoming connection, it will be cleaned up when the connection is removed
                        if (!port.hasIncomingConnection()) {
                            itr.remove();
                        }
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
     * @param id identifier of port
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
     * @param pruneUnusedPorts if true, will remove any ports that are not in the given list and that have
     *            no outgoing connections
     *
     * @throws NullPointerException if the given argument is null
     */
    @Override
    public void setOutputPorts(final Set<RemoteProcessGroupPortDescriptor> ports, final boolean pruneUnusedPorts) {
        writeLock.lock();
        try {
            final List<String> newPortTargetIds = new ArrayList<>();
            for (final RemoteProcessGroupPortDescriptor descriptor : requireNonNull(ports)) {
                newPortTargetIds.add(descriptor.getTargetId());

                final Map<String, StandardRemoteGroupPort> outputPortByTargetId = outputPorts.values().stream()
                    .collect(Collectors.toMap(StandardRemoteGroupPort::getTargetIdentifier, Function.identity()));

                final Map<String, StandardRemoteGroupPort> outputPortByName = outputPorts.values().stream()
                    .collect(Collectors.toMap(StandardRemoteGroupPort::getName, Function.identity()));

                // Check if we have a matching port already and add the port if not. We determine a matching port
                // by first finding a port that has the same Target ID. If none exists, then we try to find a port with
                // the same name. We do this because if the URL of this RemoteProcessGroup is changed, then we expect
                // the NiFi at the new URL to have a Port with the same name but a different Identifier. This is okay
                // because Ports are required to have unique names.
                StandardRemoteGroupPort receivePort = outputPortByTargetId.get(descriptor.getTargetId());
                if (receivePort == null) {
                    receivePort = outputPortByName.get(descriptor.getName());
                    if (receivePort == null) {
                        receivePort = addOutputPort(descriptor);
                    } else {
                        receivePort.setTargetIdentifier(descriptor.getTargetId());
                    }
                }

                // set the comments to ensure current description
                receivePort.setTargetExists(true);
                receivePort.setName(descriptor.getName());
                if (descriptor.isTargetRunning() != null) {
                    receivePort.setTargetRunning(descriptor.isTargetRunning());
                }
                receivePort.setComments(descriptor.getComments());
            }

            // See if we have any ports that no longer exist; cannot be removed within the loop because it would cause
            // a ConcurrentModificationException.
            if (pruneUnusedPorts) {
                final Iterator<StandardRemoteGroupPort> itr = outputPorts.values().iterator();
                while (itr.hasNext()) {
                    final StandardRemoteGroupPort port = itr.next();
                    if (!newPortTargetIds.contains(port.getTargetIdentifier())) {
                        port.setTargetExists(false);
                        port.setTargetRunning(false);

                        // If port has connections, it will be cleaned up when connections are removed
                        if (port.getConnections().isEmpty()) {
                            itr.remove();
                        }
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
                throw new IllegalStateException("Cannot remove Remote Port " + port.getIdentifier() + " because it still exists on the Remote Instance");
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


    /**
     * Adds an Output Port to this Remote Process Group that is described by
     * this DTO.
     *
     * @param descriptor
     *
     * @throws IllegalStateException if an Output Port already exists with the
     * ID given by dto.getId()
     */
    private StandardRemoteGroupPort addOutputPort(final RemoteProcessGroupPortDescriptor descriptor) {
        writeLock.lock();
        try {
            if (outputPorts.containsKey(requireNonNull(descriptor).getId())) {
                throw new IllegalStateException("Output Port with ID " + descriptor.getId() + " already exists");
            }

            final StandardRemoteGroupPort port = new StandardRemoteGroupPort(descriptor.getId(), descriptor.getTargetId(), descriptor.getName(), getProcessGroup(),
                this, TransferDirection.RECEIVE, ConnectableType.REMOTE_OUTPUT_PORT, sslContext, scheduler, nifiProperties);
            outputPorts.put(descriptor.getId(), port);

            if (descriptor.getConcurrentlySchedulableTaskCount() != null) {
                port.setMaxConcurrentTasks(descriptor.getConcurrentlySchedulableTaskCount());
            }
            if (descriptor.getUseCompression() != null) {
                port.setUseCompression(descriptor.getUseCompression());
            }
            if (descriptor.getBatchCount() != null && descriptor.getBatchCount() > 0) {
                port.setBatchCount(descriptor.getBatchCount());
            }
            if (!StringUtils.isBlank(descriptor.getBatchSize())) {
                port.setBatchSize(descriptor.getBatchSize());
            }
            if (!StringUtils.isBlank(descriptor.getBatchDuration())) {
                port.setBatchDuration(descriptor.getBatchDuration());
            }
            port.setVersionedComponentId(descriptor.getVersionedComponentId());

            return port;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * @param portIdentifier the ID of the Port to send FlowFiles to
     * @return {@link RemoteGroupPort} that can be used to send FlowFiles to the
     * port whose ID is given on the remote instance
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
     * @return a set of Outgoing Ports used for transmitting FlowFiles to
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
     * @param descriptor port descriptor
     *
     * @throws IllegalStateException if an Input Port already exists with the ID
     * given by the ID of the DTO.
     */
    private StandardRemoteGroupPort addInputPort(final RemoteProcessGroupPortDescriptor descriptor) {
        writeLock.lock();
        try {
            if (inputPorts.containsKey(descriptor.getId())) {
                throw new IllegalStateException("Input Port with ID " + descriptor.getId() + " already exists");
            }

            // We need to generate the port's UUID deterministically because we need
            // all nodes in a cluster to use the same UUID. However, we want the ID to be
            // unique for each Remote Group Port, so that if we have multiple RPG's pointing
            // to the same target, we have unique ID's for each of those ports.
            final StandardRemoteGroupPort port = new StandardRemoteGroupPort(descriptor.getId(), descriptor.getTargetId(), descriptor.getName(), getProcessGroup(), this,
                TransferDirection.SEND, ConnectableType.REMOTE_INPUT_PORT, sslContext, scheduler, nifiProperties);

            if (descriptor.getConcurrentlySchedulableTaskCount() != null) {
                port.setMaxConcurrentTasks(descriptor.getConcurrentlySchedulableTaskCount());
            }
            if (descriptor.getUseCompression() != null) {
                port.setUseCompression(descriptor.getUseCompression());
            }
            if (descriptor.getBatchCount() != null && descriptor.getBatchCount() > 0) {
                port.setBatchCount(descriptor.getBatchCount());
            }
            if (!StringUtils.isBlank(descriptor.getBatchSize())) {
                port.setBatchSize(descriptor.getBatchSize());
            }
            if (!StringUtils.isBlank(descriptor.getBatchDuration())) {
                port.setBatchDuration(descriptor.getBatchDuration());
            }
            port.setVersionedComponentId(descriptor.getVersionedComponentId());

            inputPorts.put(descriptor.getId(), port);
            return port;
        } finally {
            writeLock.unlock();
        }
    }

    private String generatePortId(final String targetId) {
        return UUID.nameUUIDFromBytes((this.getIdentifier() + targetId).getBytes(StandardCharsets.UTF_8)).toString();
    }

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
        return "RemoteProcessGroup[" + targetUris + "]";
    }

    @Override
    public RemoteProcessGroupCounts getCounts() {
        readLock.lock();
        try {
            return counts;
        } finally {
            readLock.unlock();
        }
    }

    private void setCounts(final RemoteProcessGroupCounts counts) {
        writeLock.lock();
        try {
            this.counts = counts;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Date getLastRefreshTime() {
        readLock.lock();
        try {
            return refreshContentsTimestamp == null ? null : new Date(refreshContentsTimestamp);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void refreshFlowContents() throws CommunicationsException {
        if (!initialized) {
            return;
        }

        try {
            // perform the request
            final ControllerDTO dto;
            try (final SiteToSiteRestApiClient apiClient = getSiteToSiteRestApiClient()) {
                dto = apiClient.getController(targetUris);
            } catch (IOException e) {
                throw new CommunicationsException("Unable to communicate with Remote NiFi at URI " + targetUris + " due to: " + e.getMessage());
            }

            writeLock.lock();
            try {
                if (dto.getInputPorts() != null) {
                    setInputPorts(convertRemotePort(dto.getInputPorts()), true);
                }
                if (dto.getOutputPorts() != null) {
                    setOutputPorts(convertRemotePort(dto.getOutputPorts()), true);
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

                this.listeningPort = dto.getRemoteSiteListeningPort();
                this.listeningHttpPort = dto.getRemoteSiteHttpListeningPort();
                this.destinationSecure = dto.isSiteToSiteSecure();

                final RemoteProcessGroupCounts newCounts = new RemoteProcessGroupCounts(inputPortCount, outputPortCount);
                setCounts(newCounts);
                this.refreshContentsTimestamp = System.currentTimeMillis();
            } finally {
                writeLock.unlock();
            }
        } catch (final IOException e) {
            throw new CommunicationsException(e);
        }
    }

    @Override
    public String getNetworkInterface() {
        readLock.lock();
        try {
            return networkInterfaceName;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void setNetworkInterface(final String interfaceName) {
        writeLock.lock();
        try {
            this.networkInterfaceName = interfaceName;
            if (interfaceName == null) {
                this.localAddress = null;
                this.nicValidationResult = null;
            } else {
                try {
                    final Enumeration<InetAddress> inetAddresses = NetworkInterface.getByName(interfaceName).getInetAddresses();

                    if (inetAddresses.hasMoreElements()) {
                        this.localAddress = inetAddresses.nextElement();
                        this.nicValidationResult = null;
                    } else {
                        this.localAddress = null;
                        this.nicValidationResult = new ValidationResult.Builder()
                            .input(interfaceName)
                            .subject("Network Interface Name")
                            .valid(false)
                            .explanation("No IP Address could be found that is bound to the interface with name " + interfaceName)
                            .build();
                    }
                } catch (final Exception e) {
                    this.localAddress = null;
                    this.nicValidationResult = new ValidationResult.Builder()
                        .input(interfaceName)
                        .subject("Network Interface Name")
                        .valid(false)
                        .explanation("Could not obtain Network Interface with name " + interfaceName)
                        .build();
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public InetAddress getLocalAddress() {
        readLock.lock();
        try {
            if (nicValidationResult != null && !nicValidationResult.isValid()) {
                return null;
            }

            return localAddress;
        } finally {
            readLock.unlock();
        }
    }

    private SiteToSiteRestApiClient getSiteToSiteRestApiClient() {
        SiteToSiteRestApiClient apiClient = new SiteToSiteRestApiClient(sslContext, new HttpProxy(proxyHost, proxyPort, proxyUser, proxyPassword), getEventReporter());
        apiClient.setConnectTimeoutMillis(getCommunicationsTimeout(TimeUnit.MILLISECONDS));
        apiClient.setReadTimeoutMillis(getCommunicationsTimeout(TimeUnit.MILLISECONDS));
        apiClient.setLocalAddress(getLocalAddress());
        apiClient.setCacheExpirationMillis(remoteContentsCacheExpiration);
        return apiClient;
    }

    /**
     * Converts a set of ports into a set of remote process group ports.
     *
     * @param ports to convert
     * @return descriptors of ports
     */
    private Set<RemoteProcessGroupPortDescriptor> convertRemotePort(final Set<PortDTO> ports) {
        Set<RemoteProcessGroupPortDescriptor> remotePorts = null;
        if (ports != null) {
            remotePorts = new LinkedHashSet<>(ports.size());
            for (final PortDTO port : ports) {
                final StandardRemoteProcessGroupPortDescriptor descriptor = new StandardRemoteProcessGroupPortDescriptor();
                final ScheduledState scheduledState = ScheduledState.valueOf(port.getState());
                descriptor.setId(generatePortId(port.getId()));
                descriptor.setTargetId(port.getId());
                descriptor.setName(port.getName());
                descriptor.setComments(port.getComments());
                descriptor.setTargetRunning(ScheduledState.RUNNING.equals(scheduledState));
                remotePorts.add(descriptor);
            }
        }
        return remotePorts;
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
            return (this.listeningPort != null || this.listeningHttpPort != null);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public EventReporter getEventReporter() {
        return eventReporter;
    }

    private class InitializationTask implements Runnable {

        @Override
        public void run() {
            if (!initialized) {
                return;
            }

            try (final SiteToSiteRestApiClient apiClient = getSiteToSiteRestApiClient()) {
                try {
                    final ControllerDTO dto = apiClient.getController(targetUris);

                    if (dto.getRemoteSiteListeningPort() == null && SiteToSiteTransportProtocol.RAW.equals(transportProtocol)) {
                        authorizationIssue = "Remote instance is not configured to allow RAW Site-to-Site communications at this time.";
                    } else if (dto.getRemoteSiteHttpListeningPort() == null && SiteToSiteTransportProtocol.HTTP.equals(transportProtocol)) {
                        authorizationIssue = "Remote instance is not configured to allow HTTP Site-to-Site communications at this time.";
                    } else {
                        authorizationIssue = null;
                    }

                    writeLock.lock();
                    try {
                        listeningPort = dto.getRemoteSiteListeningPort();
                        listeningHttpPort = dto.getRemoteSiteHttpListeningPort();
                        destinationSecure = dto.isSiteToSiteSecure();
                    } finally {
                        writeLock.unlock();
                    }
                } catch (SiteToSiteRestApiClient.HttpGetFailedException e) {

                    if (e.getResponseCode() == UNAUTHORIZED_STATUS_CODE) {
                        try {
                            // attempt to issue a registration request in case the target instance is a 0.x
                            final boolean isApiSecure = apiClient.getBaseUrl().toLowerCase().startsWith("https");
                            final RemoteNiFiUtils utils = new RemoteNiFiUtils(isApiSecure ? sslContext : null);
                            final Response requestAccountResponse = utils.issueRegistrationRequest(apiClient.getBaseUrl());
                            if (Response.Status.Family.SUCCESSFUL.equals(requestAccountResponse.getStatusInfo().getFamily())) {
                                logger.info("{} Issued a Request to communicate with remote instance", this);
                            } else {
                                logger.error("{} Failed to request account: got unexpected response code of {}:{}", this,
                                        requestAccountResponse.getStatus(), requestAccountResponse.getStatusInfo().getReasonPhrase());
                            }
                        } catch (final Exception re) {
                            logger.error("{} Failed to request account due to {}", this, re.toString());
                            if (logger.isDebugEnabled()) {
                                logger.error("", re);
                            }
                        }

                        authorizationIssue = e.getDescription();
                    } else if (e.getResponseCode() == FORBIDDEN_STATUS_CODE) {
                        authorizationIssue = e.getDescription();
                    } else {
                        final String message = e.getDescription();
                        logger.warn("{} When communicating with remote instance, got unexpected result. {}",
                                new Object[]{this, message});
                        authorizationIssue = "Unable to determine Site-to-Site availability.";
                    }
                }

            } catch (final Exception e) {
                logger.warn(String.format("Unable to connect to %s due to %s", StandardRemoteProcessGroup.this, e));
                getEventReporter().reportEvent(Severity.WARNING, "Site to Site", String.format("Unable to connect to %s due to %s",
                        StandardRemoteProcessGroup.this.getTargetUris(), e));
            }
        }
    }

    @Override
    public void setYieldDuration(final String yieldDuration) {
        // verify the syntax
        if (!FormatUtils.TIME_DURATION_PATTERN.matcher(yieldDuration).matches()) {
            throw new IllegalArgumentException("Improperly formatted Time Period; should be of syntax <number> <unit> where "
                    + "<number> is a positive integer and unit is one of the valid Time Units, such as nanos, millis, sec, min, hour, day");
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
                throw new IllegalStateException(this.getIdentifier() + " is transmitting");
            }

            for (final Port port : inputPorts.values()) {
                if (!ignoreConnections && port.hasIncomingConnection()) {
                    throw new IllegalStateException(this.getIdentifier() + " is the destination of another component");
                }
                if (port.isRunning()) {
                    throw new IllegalStateException(this.getIdentifier() + " has running Port: " + port.getIdentifier());
                }
            }

            for (final Port port : outputPorts.values()) {
                if (!ignoreConnections) {
                    for (final Connection connection : port.getConnections()) {
                        connection.verifyCanDelete();
                    }
                }

                if (port.isRunning()) {
                    throw new IllegalStateException(this.getIdentifier() + " has running Port: " + port.getIdentifier());
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
                throw new IllegalStateException(this.getIdentifier() + " is already transmitting");
            }

            for (final StandardRemoteGroupPort port : inputPorts.values()) {
                if (port.isRunning()) {
                    throw new IllegalStateException(this.getIdentifier() + " has running Port: " + port.getIdentifier());
                }

                if (port.hasIncomingConnection() && !port.getTargetExists()) {
                    throw new IllegalStateException(this.getIdentifier() + " has a Connection to Port " + port.getIdentifier() + ", but that Port no longer exists on the remote system");
                }

                if (port.hasIncomingConnection()) {
                    port.verifyCanStart();
                }
            }

            for (final StandardRemoteGroupPort port : outputPorts.values()) {
                if (port.isRunning()) {
                    throw new IllegalStateException(this.getIdentifier() + " has running Port: " + port.getIdentifier());
                }

                if (!port.getConnections().isEmpty() && !port.getTargetExists()) {
                    throw new IllegalStateException(this.getIdentifier() + " has a Connection to Port " + port.getIdentifier() + ", but that Port no longer exists on the remote system");
                }

                if (!port.getConnections().isEmpty()) {
                    port.verifyCanStart();
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void verifyCanStopTransmitting() {
        if (!isTransmitting()) {
            throw new IllegalStateException(this.getIdentifier() + " is not transmitting");
        }
    }

    @Override
    public void verifyCanUpdate() {
        readLock.lock();
        try {
            if (isTransmitting()) {
                throw new IllegalStateException(this.getIdentifier() + " is currently transmitting");
            }

            for (final Port port : inputPorts.values()) {
                if (port.isRunning()) {
                    throw new IllegalStateException(this.getIdentifier() + " has running Port: " + port.getIdentifier());
                }
            }

            for (final Port port : outputPorts.values()) {
                if (port.isRunning()) {
                    throw new IllegalStateException(this.getIdentifier() + " has running Port: " + port.getIdentifier());
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    private File getPeerPersistenceFile() {
        final File stateDir = nifiProperties.getPersistentStateDirectory();
        return new File(stateDir, getIdentifier() + ".peers");
    }

    @Override
    public Optional<String> getVersionedComponentId() {
        return Optional.ofNullable(versionedComponentId.get());
    }

    @Override
    public void setVersionedComponentId(final String versionedComponentId) {
        boolean updated = false;
        while (!updated) {
            final String currentId = this.versionedComponentId.get();

            if (currentId == null) {
                updated = this.versionedComponentId.compareAndSet(null, versionedComponentId);
            } else if (currentId.equals(versionedComponentId)) {
                return;
            } else if (versionedComponentId == null) {
                updated = this.versionedComponentId.compareAndSet(currentId, null);
            } else {
                throw new IllegalStateException(this + " is already under version control");
            }
        }
    }
}
