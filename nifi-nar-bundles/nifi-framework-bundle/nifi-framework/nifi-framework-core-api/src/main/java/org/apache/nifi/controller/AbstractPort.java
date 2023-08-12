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
package org.apache.nifi.controller;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.components.PortFunction;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.CharacterFilterUtils;
import org.apache.nifi.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.Objects.requireNonNull;

public abstract class AbstractPort implements Port {
    private static final Logger logger = LoggerFactory.getLogger(AbstractPort.class);

    public static final Relationship PORT_RELATIONSHIP = new Relationship.Builder()
            .description("The relationship through which all Flow Files are transferred")
            .name("")
            .build();

    private static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;
    private static final String DEFAULT_MAX_BACKOFF_PERIOD = "10 mins";

    private final List<Relationship> relationships;

    private final String id;
    private final ConnectableType type;
    private final AtomicReference<String> name;
    private final AtomicReference<Position> position;
    private final AtomicReference<String> comments;
    private final AtomicReference<ProcessGroup> processGroup = new AtomicReference<>();
    private final AtomicBoolean lossTolerant;
    private final AtomicReference<ScheduledState> scheduledState;
    private final AtomicInteger concurrentTaskCount;
    private final AtomicReference<String> penalizationPeriod;
    private final AtomicReference<String> yieldPeriod;
    private final AtomicReference<String> schedulingPeriod;
    private final AtomicReference<String> versionedComponentId = new AtomicReference<>();
    private final AtomicLong schedulingNanos;
    private final AtomicLong yieldExpiration;
    private final AtomicReference<PortFunction> portFunction = new AtomicReference<>(PortFunction.STANDARD);
    private final ProcessScheduler processScheduler;

    private final Set<Connection> outgoingConnections;
    private final List<Connection> incomingConnections;

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    public AbstractPort(final String id, final String name, final ConnectableType type, final ProcessScheduler scheduler) {
        this.id = requireNonNull(id);
        this.name = new AtomicReference<>(requireNonNull(name));
        position = new AtomicReference<>(new Position(0D, 0D));
        outgoingConnections = new HashSet<>();
        incomingConnections = new ArrayList<>();
        comments = new AtomicReference<>();
        lossTolerant = new AtomicBoolean(false);
        concurrentTaskCount = new AtomicInteger(1);
        processScheduler = scheduler;

        final List<Relationship> relationshipList = new ArrayList<>();
        relationshipList.add(PORT_RELATIONSHIP);
        relationships = Collections.unmodifiableList(relationshipList);
        this.type = type;
        penalizationPeriod = new AtomicReference<>("30 sec");
        yieldPeriod = new AtomicReference<>("1 sec");
        yieldExpiration = new AtomicLong(0L);
        schedulingPeriod = new AtomicReference<>("0 millis");
        schedulingNanos = new AtomicLong(MINIMUM_SCHEDULING_NANOS);
        scheduledState = new AtomicReference<>(ScheduledState.STOPPED);
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
    public String getName() {
        return name.get();
    }

    @Override
    public void setName(final String name) {
        if (this.name.get().equals(name)) {
            return;
        }

        final ProcessGroup parentGroup = this.processGroup.get();
        if (getConnectableType() == ConnectableType.INPUT_PORT) {
            if (parentGroup.getInputPortByName(name) != null) {
                throw new IllegalStateException("A port with the same name already exists.");
            }
        } else if (getConnectableType() == ConnectableType.OUTPUT_PORT) {
            if (parentGroup.getOutputPortByName(name) != null) {
                throw new IllegalStateException("A port with the same name already exists.");
            }
        }

        this.name.set(name);
    }

    @Override
    public Authorizable getParentAuthorizable() {
        return getProcessGroup();
    }

    @Override
    public Resource getResource() {
        final ResourceType resourceType = ConnectableType.INPUT_PORT.equals(getConnectableType()) ? ResourceType.InputPort : ResourceType.OutputPort;
        return ResourceFactory.getComponentResource(resourceType, getIdentifier(), getName());
    }

    @Override
    public ProcessGroup getProcessGroup() {
        return processGroup.get();
    }

    @Override
    public void setProcessGroup(final ProcessGroup newGroup) {
        if (this.processGroup.get() != null && !Objects.equals(newGroup, this.processGroup.get())) {
            // Process Group is changing. For a Port, we effectively want to consider this the same as
            // deleting an old port and creating a new one, in terms of tracking the port to a versioned flow.
            // This ensures that we have a unique versioned component id not only in the given process group but also
            // between the given group and its parent and all children. This is important for ports because we can
            // connect to/from them between Process Groups, so we need to ensure unique IDs.
            versionedComponentId.set(null);
        }

        this.processGroup.set(newGroup);
    }

    @Override
    public String getComments() {
        return comments.get();
    }

    @Override
    public void setComments(final String comments) {
        this.comments.set(CharacterFilterUtils.filterInvalidXmlCharacters(comments));
    }

    @Override
    public Collection<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public Relationship getRelationship(final String relationshipName) {
        if (PORT_RELATIONSHIP.getName().equals(relationshipName)) {
            return PORT_RELATIONSHIP;
        }
        return null;
    }

    @Override
    public void addConnection(final Connection connection) throws IllegalArgumentException {
        writeLock.lock();
        try {
            if (!requireNonNull(connection).getSource().equals(this)) {
                if (connection.getDestination().equals(this)) {
                    // don't add the connection twice. This may occur if we have a self-loop because we will be told
                    // to add the connection once because we are the source and again because we are the destination.
                    if (!incomingConnections.contains(connection)) {
                        incomingConnections.add(connection);
                    }

                    return;
                } else {
                    throw new IllegalArgumentException("Cannot add a connection to a LocalPort for which the LocalPort is neither the Source nor the Destination");
                }
            }

            for (final Relationship relationship : connection.getRelationships()) {
                if (!relationship.equals(PORT_RELATIONSHIP)) {
                    throw new IllegalArgumentException("No relationship with name " + relationship + " exists for Local Ports");
                }
            }

            // don't add the connection twice. This may occur if we have a self-loop because we will be told
            // to add the connection once because we are the source and again because we are the destination.
            if (!outgoingConnections.contains(connection)) {
                outgoingConnections.add(connection);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean hasIncomingConnection() {
        readLock.lock();
        try {
            return !incomingConnections.isEmpty();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();

        try {
            onTrigger(context, session);
        } catch (final Throwable t) {
            session.rollback();
            throw t;
        }

        session.commitAsync();
    }

    public abstract void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException;

    @Override
    public void updateConnection(final Connection connection) throws IllegalStateException {
        if (requireNonNull(connection).getSource().equals(this)) {
            writeLock.lock();
            try {
                if (!outgoingConnections.remove(connection)) {
                    throw new IllegalStateException("No Connection with ID " + connection.getIdentifier() + " is currently registered with this Port");
                }
                outgoingConnections.add(connection);
            } finally {
                writeLock.unlock();
            }
        } else if (connection.getDestination().equals(this)) {
            writeLock.lock();
            try {
                if (!incomingConnections.remove(connection)) {
                    throw new IllegalStateException("No Connection with ID " + connection.getIdentifier() + " is currently registered with this Port");
                }
                incomingConnections.add(connection);
            } finally {
                writeLock.unlock();
            }
        } else {
            throw new IllegalStateException("The given connection is not currently registered for this Port");
        }
    }

    @Override
    public void removeConnection(final Connection connection) throws IllegalArgumentException, IllegalStateException {
        writeLock.lock();
        try {
            if (!requireNonNull(connection).getSource().equals(this)) {
                final boolean existed = incomingConnections.remove(connection);
                if (!existed) {
                    throw new IllegalStateException("The given connection is not currently registered for this Port");
                }
                return;
            }

            if (!canConnectionBeRemoved(connection)) {
                // TODO: Determine which processors will be broken if connection is removed, rather than just returning a boolean
                throw new IllegalStateException("Connection " + connection.getIdentifier() + " cannot be removed");
            }

            final boolean removed = outgoingConnections.remove(connection);
            if (!removed) {
                throw new IllegalStateException("Connection " + connection.getIdentifier() + " is not registered with " + this.getIdentifier());
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Verify that removing this connection will not prevent this Port from
     * still being connected via each relationship
     *
     * @param connection to test for removal
     * @return true if can be removed
     */
    private boolean canConnectionBeRemoved(final Connection connection) {
        final Connectable source = connection.getSource();
        if (!source.isRunning()) {
            // we don't have to verify that this Connectable is still connected because it's okay to make
            // the source invalid since it is not running.
            return true;
        }

        for (final Relationship relationship : source.getRelationships()) {
            if (source.isAutoTerminated(relationship)) {
                continue;
            }

            final Set<Connection> connectionsForRelationship = source.getConnections(relationship);
            if (connectionsForRelationship == null || connectionsForRelationship.isEmpty()) {
                return false;
            }
        }

        return true;
    }

    @Override
    public Set<Connection> getConnections() {
        readLock.lock();
        try {
            return Collections.unmodifiableSet(outgoingConnections);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Set<Connection> getConnections(final Relationship relationship) {
        readLock.lock();
        try {
            if (relationship.equals(PORT_RELATIONSHIP)) {
                return Collections.unmodifiableSet(outgoingConnections);
            }

            throw new IllegalArgumentException("No relationship with name " + relationship.getName() + " exists for Local Ports");
        } finally {
            readLock.unlock();
        }
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
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("id", getIdentifier()).toString();
    }

    @Override
    public List<Connection> getIncomingConnections() {
        readLock.lock();
        try {
            return Collections.unmodifiableList(incomingConnections);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public abstract boolean isValid();

    @Override
    public boolean isAutoTerminated(final Relationship relationship) {
        return false;
    }

    @Override
    public boolean isLossTolerant() {
        return lossTolerant.get();
    }

    @Override
    public void setLossTolerant(boolean lossTolerant) {
        this.lossTolerant.set(lossTolerant);
    }

    @Override
    public void setMaxConcurrentTasks(final int taskCount) {
        if (taskCount < 1) {
            throw new IllegalArgumentException();
        }
        concurrentTaskCount.set(taskCount);
    }

    @Override
    public int getMaxConcurrentTasks() {
        return concurrentTaskCount.get();
    }

    @Override
    public void shutdown() {
        scheduledState.set(ScheduledState.STOPPED);
        logger.info("{} shutdown", this);
    }

    @Override
    public void onSchedulingStart() {
        scheduledState.set(ScheduledState.RUNNING);
        logger.info("{} started", this);
    }

    @Override
    public void disable() {
        final boolean updated = scheduledState.compareAndSet(ScheduledState.STOPPED, ScheduledState.DISABLED);
        if (!updated) {
            throw new IllegalStateException("Port cannot be disabled because it is not stopped");
        }
        logger.info("{} disabled", this);
    }

    public void enable() {
        final boolean updated = scheduledState.compareAndSet(ScheduledState.DISABLED, ScheduledState.STOPPED);
        if (!updated) {
            throw new IllegalStateException("Port cannot be enabled because it is not disabled");
        }
        logger.info("{} enabled", this);
    }

    @Override
    public boolean isRunning() {
        return getScheduledState().equals(ScheduledState.RUNNING) || processScheduler.getActiveThreadCount(this) > 0;
    }

    @Override
    public ScheduledState getScheduledState() {
        return scheduledState.get();
    }

    @Override
    public ConnectableType getConnectableType() {
        return type;
    }

    @Override
    public void setYieldPeriod(final String yieldPeriod) {
        final long yieldMillis = FormatUtils.getTimeDuration(requireNonNull(yieldPeriod), TimeUnit.MILLISECONDS);
        if (yieldMillis < 0) {
            throw new IllegalArgumentException("Yield duration must be positive");
        }
        this.yieldPeriod.set(yieldPeriod);
    }

    @Override
    public void setSchedulingPeriod(final String schedulingPeriod) {
        final long schedulingNanos = FormatUtils.getTimeDuration(requireNonNull(schedulingPeriod), TimeUnit.NANOSECONDS);
        if (schedulingNanos < 0) {
            throw new IllegalArgumentException("Scheduling Period must be positive");
        }

        this.schedulingPeriod.set(schedulingPeriod);
        this.schedulingNanos.set(Math.max(MINIMUM_SCHEDULING_NANOS, schedulingNanos));
    }

    @Override
    public long getPenalizationPeriod(final TimeUnit timeUnit) {
        return FormatUtils.getTimeDuration(getPenalizationPeriod(), timeUnit == null ? DEFAULT_TIME_UNIT : timeUnit);
    }

    @Override
    public String getPenalizationPeriod() {
        return penalizationPeriod.get();
    }

    @Override
    public void yield() {
        final long yieldMillis = getYieldPeriod(TimeUnit.MILLISECONDS);
        this.yield(yieldMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void yield(final long yieldDuration, final TimeUnit timeUnit) {
        final long yieldMillis = timeUnit.toMillis(yieldDuration);
        yieldExpiration.set(Math.max(yieldExpiration.get(), System.currentTimeMillis() + yieldMillis));
    }

    @Override
    public long getYieldExpiration() {
        return yieldExpiration.get();
    }

    @Override
    public long getSchedulingPeriod(final TimeUnit timeUnit) {
        return timeUnit.convert(schedulingNanos.get(), TimeUnit.NANOSECONDS);
    }

    @Override
    public String getSchedulingPeriod() {
        return schedulingPeriod.get();
    }

    @Override
    public void setPenalizationPeriod(final String penalizationPeriod) {
        this.penalizationPeriod.set(penalizationPeriod);
    }

    @Override
    public String getYieldPeriod() {
        return yieldPeriod.get();
    }

    @Override
    public long getYieldPeriod(final TimeUnit timeUnit) {
        return FormatUtils.getTimeDuration(getYieldPeriod(), timeUnit == null ? DEFAULT_TIME_UNIT : timeUnit);
    }

    @Override
    public void verifyCanDelete() throws IllegalStateException {
        verifyCanDelete(false);
    }

    @Override
    public void verifyCanDelete(final boolean ignoreConnections) {
        readLock.lock();
        try {
            if (isRunning()) {
                throw new IllegalStateException(this.getIdentifier() + " is running");
            }

            if (!ignoreConnections) {
                for (final Connection connection : outgoingConnections) {
                    connection.verifyCanDelete();
                }

                for (final Connection connection : incomingConnections) {
                    if (connection.getSource().equals(this)) {
                        connection.verifyCanDelete();
                    } else {
                        throw new IllegalStateException(this.getIdentifier() + " is the destination of another component");
                    }
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void verifyCanStart() {
        readLock.lock();
        try {
            switch (scheduledState.get()) {
                case DISABLED:
                    throw new IllegalStateException(this.getIdentifier() + " cannot be started because it is disabled");
                case RUNNING:
                    throw new IllegalStateException(this.getIdentifier() + " cannot be started because it is already running");
                case STOPPED:
                    break;
            }
            verifyNoActiveThreads();

            final Collection<ValidationResult> validationResults = getValidationErrors();
            if (!validationResults.isEmpty()) {
                final String portType = getConnectableType() == ConnectableType.INPUT_PORT ? "Input Port" : "Output Port";
                final String message = String.format("%s %s is not in a valid state: %s", portType, getIdentifier(), validationResults.iterator().next().getExplanation());
                throw new IllegalStateException(message);
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void verifyCanStop() {
        if (getScheduledState() != ScheduledState.RUNNING) {
            throw new IllegalStateException(this.getIdentifier() + " is not scheduled to run");
        }
    }

    @Override
    public void verifyCanUpdate() {
        readLock.lock();
        try {
            if (isRunning()) {
                throw new IllegalStateException(this.getIdentifier() + " is not stopped");
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void verifyCanEnable() {
        readLock.lock();
        try {
            if (getScheduledState() != ScheduledState.DISABLED) {
                throw new IllegalStateException(this.getIdentifier() + " is not disabled");
            }

            verifyNoActiveThreads();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void verifyCanDisable() {
        readLock.lock();
        try {
            if (getScheduledState() != ScheduledState.STOPPED) {
                throw new IllegalStateException(this.getIdentifier() + " is not stopped");
            }
            verifyNoActiveThreads();
        } finally {
            readLock.unlock();
        }
    }

    private void verifyNoActiveThreads() throws IllegalStateException {
        final int threadCount = processScheduler.getActiveThreadCount(this);
        if (threadCount > 0) {
            throw new IllegalStateException(this.getIdentifier() + " has " + threadCount + " threads still active");
        }
    }

    @Override
    public void verifyCanClearState() {
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

    @Override
    public int getRetryCount() {
        return 0;
    }

    @Override
    public void setRetryCount(Integer retryCount) {
    }

    @Override
    public Set<String> getRetriedRelationships() {
        return Collections.EMPTY_SET;
    }

    @Override
    public void setRetriedRelationships(Set<String> retriedRelationships) {
    }

    @Override
    public boolean isRelationshipRetried(Relationship relationship) {
        return false;
    }

    @Override
    public BackoffMechanism getBackoffMechanism() {
        return BackoffMechanism.PENALIZE_FLOWFILE;
    }

    @Override
    public void setBackoffMechanism(BackoffMechanism backoffMechanism) {
    }

    @Override
    public String getMaxBackoffPeriod() {
        return DEFAULT_MAX_BACKOFF_PERIOD;
    }

    @Override
    public void setMaxBackoffPeriod(String maxBackoffPeriod) {
    }

    @Override
    public String evaluateParameters(String value) {
        return value;
    }

    @Override
    public PortFunction getPortFunction() {
        return portFunction.get();
    }

    public void setPortFunction(final PortFunction portFunction) {
        this.portFunction.set(requireNonNull(portFunction));
    }
}
