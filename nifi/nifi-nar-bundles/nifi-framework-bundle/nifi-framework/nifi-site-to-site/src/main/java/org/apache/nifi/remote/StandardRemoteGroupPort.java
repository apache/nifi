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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.net.ssl.SSLContext;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.remote.client.socket.EndpointConnectionState;
import org.apache.nifi.remote.client.socket.EndpointConnectionStatePool;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.exception.PortNotRunningException;
import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.remote.exception.TransmissionDisabledException;
import org.apache.nifi.remote.exception.UnknownPortException;
import org.apache.nifi.remote.protocol.ClientProtocol;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.socket.SocketClientProtocol;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandardRemoteGroupPort extends RemoteGroupPort {
    public static final String USER_AGENT = "NiFi-Site-to-Site";
    public static final String CONTENT_TYPE = "application/octet-stream";
    
    public static final int GZIP_COMPRESSION_LEVEL = 1;
    
    private static final String CATEGORY = "Site to Site";
    
    private static final Logger logger = LoggerFactory.getLogger(StandardRemoteGroupPort.class);
    private final RemoteProcessGroup remoteGroup;
    private final AtomicBoolean useCompression = new AtomicBoolean(false);
    private final AtomicBoolean targetExists = new AtomicBoolean(true);
    private final AtomicBoolean targetRunning = new AtomicBoolean(true);
    private final TransferDirection transferDirection;
    
    private final EndpointConnectionStatePool connectionStatePool;
    
    private final Set<CommunicationsSession> activeCommsChannels = new HashSet<>();
    private final Lock interruptLock = new ReentrantLock();
    private boolean shutdown = false;   // guarded by codecLock
    
    public StandardRemoteGroupPort(final String id, final String name, final ProcessGroup processGroup, final RemoteProcessGroup remoteGroup, 
            final TransferDirection direction, final ConnectableType type, final SSLContext sslContext, final ProcessScheduler scheduler) {
        // remote group port id needs to be unique but cannot just be the id of the port
        // in the remote group instance. this supports referencing the same remote
        // instance more than once.
        super(id, name, processGroup, type, scheduler);
        
        this.remoteGroup = remoteGroup;
        this.transferDirection = direction;
        setScheduldingPeriod(MINIMUM_SCHEDULING_NANOS + " nanos");
        
        connectionStatePool = remoteGroup.getConnectionPool();
    }
    
    @Override
    public boolean isTargetRunning() {
        return targetRunning.get();
    }

    public void setTargetRunning(boolean targetRunning) {
        this.targetRunning.set(targetRunning);
    }
    
    @Override
    public boolean isTriggerWhenEmpty() {
        return getConnectableType() == ConnectableType.REMOTE_OUTPUT_PORT;
    }
    
    @Override
    public void shutdown() {
    	super.shutdown();
        interruptLock.lock();
        try {
            this.shutdown = true;
        } finally {
            interruptLock.unlock();
        }
    }
    
    @Override
    public void onSchedulingStart() {
        super.onSchedulingStart();
        
        interruptLock.lock();
        try {
            this.shutdown = false;
        } finally {
            interruptLock.unlock();
        }
    }
    
    
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        if ( !remoteGroup.isTransmitting() ) {
            logger.debug("{} {} is not transmitting; will not send/receive", this, remoteGroup);
            return;
        }

        if ( getConnectableType() == ConnectableType.REMOTE_INPUT_PORT && session.getQueueSize().getObjectCount() == 0 ) {
            logger.debug("{} No data to send", this);
            return;
        }
        
        String url = getRemoteProcessGroup().getTargetUri().toString();
        
        final EndpointConnectionState connectionState;
        try {
        	connectionState = connectionStatePool.getEndpointConnectionState(this, transferDirection);
        } catch (final PortNotRunningException e) {
            context.yield();
            this.targetRunning.set(false);
            final String message = String.format("%s failed to communicate with %s because the remote instance indicates that the port is not in a valid state", this, url);
            logger.error(message);
            remoteGroup.getEventReporter().reportEvent(Severity.ERROR, CATEGORY, message);
            return;
        } catch (final UnknownPortException e) {
            context.yield();
            this.targetExists.set(false);
            final String message = String.format("%s failed to communicate with %s because the remote instance indicates that the port no longer exists", this, url);
            logger.error(message);
            remoteGroup.getEventReporter().reportEvent(Severity.ERROR, CATEGORY, message);
            return;
        } catch (final HandshakeException | IOException e) {
            final String message = String.format("%s failed to communicate with %s due to %s", this, url, e.toString());
            logger.error(message);
            if ( logger.isDebugEnabled() ) {
                logger.error("", e);
            }
            remoteGroup.getEventReporter().reportEvent(Severity.ERROR, CATEGORY, message);
            session.rollback();
            return;
        }
        
        if ( connectionState == null ) {
            logger.debug("{} Unable to determine the next peer to communicate with; all peers must be penalized, so yielding context", this);
            context.yield();
            return;
        }
        
        FlowFileCodec codec = connectionState.getCodec();
        SocketClientProtocol protocol = connectionState.getSocketClientProtocol();
        final Peer peer = connectionState.getPeer();
        url = peer.getUrl();
        
        try {
            interruptLock.lock();
            try {
                if ( shutdown ) {
                    peer.getCommunicationsSession().interrupt();
                }
                
                activeCommsChannels.add(peer.getCommunicationsSession());
            } finally {
                interruptLock.unlock();
            }
            
            if ( getConnectableType() == ConnectableType.REMOTE_INPUT_PORT ) {
                transferFlowFiles(peer, protocol, context, session, codec);
            } else {
                receiveFlowFiles(peer, protocol, context, session, codec);
            }

            interruptLock.lock();
            try {
                if ( shutdown ) {
                    peer.getCommunicationsSession().interrupt();
                }
                
                activeCommsChannels.remove(peer.getCommunicationsSession());
            } finally {
                interruptLock.unlock();
            }

            session.commit();
            
            connectionState.setLastTimeUsed();
            connectionStatePool.offer(connectionState);
        } catch (final TransmissionDisabledException e) {
            cleanup(protocol, peer);
            session.rollback();
        } catch (final Exception e) {
            connectionStatePool.penalize(peer, getYieldPeriod(TimeUnit.MILLISECONDS));

            final String message = String.format("%s failed to communicate with %s (%s) due to %s", this, peer == null ? url : peer, protocol, e.toString());
            logger.error(message);
            if ( logger.isDebugEnabled() ) {
                logger.error("", e);
            }
            
            cleanup(protocol, peer);
            
            remoteGroup.getEventReporter().reportEvent(Severity.ERROR, CATEGORY, message);
            session.rollback();
        }
    }

    
    private void cleanup(final SocketClientProtocol protocol, final Peer peer) {
        if ( protocol != null && peer != null ) {
            try {
                protocol.shutdown(peer);
            } catch (final TransmissionDisabledException e) {
                // User disabled transmission.... do nothing.
                logger.debug(this + " Transmission Disabled by User");
            } catch (IOException e1) {
            }
        }
        
        if ( peer != null ) {
            try {
                peer.close();
            } catch (final TransmissionDisabledException e) {
                // User disabled transmission.... do nothing.
                logger.debug(this + " Transmission Disabled by User");
            } catch (IOException e1) {
            }
        }
    }
    
    @Override
    public String getYieldPeriod() {
        // delegate yield duration to remote process group
        return remoteGroup.getYieldDuration();
    }
    
    
    private void transferFlowFiles(final Peer peer, final ClientProtocol protocol, final ProcessContext context, final ProcessSession session, final FlowFileCodec codec) throws IOException, ProtocolException {
        protocol.transferFlowFiles(peer, context, session, codec);
    }
    
    private void receiveFlowFiles(final Peer peer, final ClientProtocol protocol, final ProcessContext context, final ProcessSession session, final FlowFileCodec codec) throws IOException, ProtocolException {
        protocol.receiveFlowFiles(peer, context, session, codec);
    }

    @Override
    public boolean getTargetExists() {
        return targetExists.get();
    }

    @Override
    public boolean isValid() {
        return getValidationErrors().isEmpty();
    }

    @Override
    public Collection<ValidationResult> getValidationErrors() {
        final Collection<ValidationResult> validationErrors = new ArrayList<>();
        ValidationResult error = null;
        if (!targetExists.get()) {
            error = new ValidationResult.Builder()
                .explanation(String.format("Remote instance indicates that port '%s' no longer exists.", getName()))
                .subject(String.format("Remote port '%s'", getName()))
                .valid(false)
                .build();
        } else if ( getConnectableType() == ConnectableType.REMOTE_OUTPUT_PORT && getConnections(Relationship.ANONYMOUS).isEmpty() ) {
            error = new ValidationResult.Builder()
                .explanation(String.format("Port '%s' has no outbound connections", getName()))
                .subject(String.format("Remote port '%s'", getName()))
                .valid(false)
                .build();
        }
        
        if ( error != null ) {
            validationErrors.add(error);
        }
        
        return validationErrors;
    }
    
    @Override
    public void verifyCanStart() {
        super.verifyCanStart();
        
        if ( getConnectableType() == ConnectableType.REMOTE_INPUT_PORT && getIncomingConnections().isEmpty() ) {
            throw new IllegalStateException("Port " + getName() + " has no incoming connections");
        }
    }
    
    @Override
    public void setUseCompression(final boolean useCompression) {
        this.useCompression.set(useCompression);
    }
    
    @Override
    public boolean isUseCompression() {
        return useCompression.get();
    }
    
    @Override
    public String toString() {
        return "RemoteGroupPort[name=" + getName() + ",target=" + remoteGroup.getTargetUri().toString() + "]";
    }

    @Override
    public RemoteProcessGroup getRemoteProcessGroup() {
        return remoteGroup;
    }
    
    @Override
    public TransferDirection getTransferDirection() {
        return (getConnectableType() == ConnectableType.REMOTE_INPUT_PORT) ? TransferDirection.SEND : TransferDirection.RECEIVE;
    }
    
    public void setTargetExists(final boolean exists) {
        this.targetExists.set(exists);
    }
    
    @Override
    public void removeConnection(final Connection connection) throws IllegalArgumentException, IllegalStateException {
        super.removeConnection(connection);
        
        // If the Port no longer exists on the remote instance and this is the last Connection, tell 
        // RemoteProcessGroup to remove me
        if ( !getTargetExists() && !hasIncomingConnection() && getConnections().isEmpty() ) {
            remoteGroup.removeNonExistentPort(this);
        }
    }
    
    
    @Override
    public SchedulingStrategy getSchedulingStrategy() {
        return SchedulingStrategy.TIMER_DRIVEN;
    }
    
    
    @Override
    public boolean isSideEffectFree() {
        return false;
    }
}
