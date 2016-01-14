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

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.TriggerWhenAnyDestinationAvailable;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.logging.LogRepositoryFactory;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.FormatUtils;
import org.quartz.CronExpression;
import org.slf4j.LoggerFactory;

/**
 * ProcessorNode provides thread-safe access to a FlowFileProcessor as it exists within a controlled flow. This node keeps track of the processor, its scheduling information and its relationships to
 * other processors and whatever scheduled futures exist for it. Must be thread safe.
 *
 */
public class StandardProcessorNode extends ProcessorNode implements Connectable {

    public static final String BULLETIN_OBSERVER_ID = "bulletin-observer";

    public static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;
    public static final String DEFAULT_YIELD_PERIOD = "1 sec";
    public static final String DEFAULT_PENALIZATION_PERIOD = "30 sec";
    private final AtomicReference<ProcessGroup> processGroup;
    private final Processor processor;
    private final AtomicReference<String> identifier;
    private final Map<Connection, Connectable> destinations;
    private final Map<Relationship, Set<Connection>> connections;
    private final AtomicReference<Set<Relationship>> undefinedRelationshipsToTerminate;
    private final AtomicReference<List<Connection>> incomingConnectionsRef;
    private final ReentrantReadWriteLock rwLock;
    private final Lock readLock;
    private final Lock writeLock;
    private final AtomicBoolean isolated;
    private final AtomicBoolean lossTolerant;
    private final AtomicReference<ScheduledState> scheduledState;
    private final AtomicReference<String> comments;
    private final AtomicReference<Position> position;
    private final AtomicReference<String> annotationData;
    private final AtomicReference<String> schedulingPeriod; // stored as string so it's presented to user as they entered it
    private final AtomicReference<String> yieldPeriod;
    private final AtomicReference<String> penalizationPeriod;
    private final AtomicReference<Map<String, String>> style;
    private final AtomicInteger concurrentTaskCount;
    private final AtomicLong yieldExpiration;
    private final AtomicLong schedulingNanos;
    private final boolean triggerWhenEmpty;
    private final boolean sideEffectFree;
    private final boolean triggeredSerially;
    private final boolean triggerWhenAnyDestinationAvailable;
    private final boolean eventDrivenSupported;
    private final boolean batchSupported;
    private final Requirement inputRequirement;
    private final ValidationContextFactory validationContextFactory;
    private final ProcessScheduler processScheduler;
    private long runNanos = 0L;

    private SchedulingStrategy schedulingStrategy; // guarded by read/write lock

    @SuppressWarnings("deprecation")
    public StandardProcessorNode(final Processor processor, final String uuid, final ValidationContextFactory validationContextFactory,
        final ProcessScheduler scheduler, final ControllerServiceProvider controllerServiceProvider) {
        super(processor, uuid, validationContextFactory, controllerServiceProvider);

        this.processor = processor;
        identifier = new AtomicReference<>(uuid);
        destinations = new HashMap<>();
        connections = new HashMap<>();
        incomingConnectionsRef = new AtomicReference<List<Connection>>(new ArrayList<Connection>());
        scheduledState = new AtomicReference<>(ScheduledState.STOPPED);
        rwLock = new ReentrantReadWriteLock(false);
        readLock = rwLock.readLock();
        writeLock = rwLock.writeLock();
        lossTolerant = new AtomicBoolean(false);
        final Set<Relationship> emptySetOfRelationships = new HashSet<>();
        undefinedRelationshipsToTerminate = new AtomicReference<>(emptySetOfRelationships);
        comments = new AtomicReference<>("");
        schedulingPeriod = new AtomicReference<>("0 sec");
        schedulingNanos = new AtomicLong(MINIMUM_SCHEDULING_NANOS);
        yieldPeriod = new AtomicReference<>(DEFAULT_YIELD_PERIOD);
        yieldExpiration = new AtomicLong(0L);
        concurrentTaskCount = new AtomicInteger(1);
        position = new AtomicReference<>(new Position(0D, 0D));
        style = new AtomicReference<>(Collections.unmodifiableMap(new HashMap<String, String>()));
        this.processGroup = new AtomicReference<>();
        processScheduler = scheduler;
        annotationData = new AtomicReference<>();
        isolated = new AtomicBoolean(false);
        penalizationPeriod = new AtomicReference<>(DEFAULT_PENALIZATION_PERIOD);

        final Class<?> procClass = processor.getClass();
        triggerWhenEmpty = procClass.isAnnotationPresent(TriggerWhenEmpty.class) || procClass.isAnnotationPresent(org.apache.nifi.processor.annotation.TriggerWhenEmpty.class);
        sideEffectFree = procClass.isAnnotationPresent(SideEffectFree.class) || procClass.isAnnotationPresent(org.apache.nifi.processor.annotation.SideEffectFree.class);
        batchSupported = procClass.isAnnotationPresent(SupportsBatching.class) || procClass.isAnnotationPresent(org.apache.nifi.processor.annotation.SupportsBatching.class);
        triggeredSerially = procClass.isAnnotationPresent(TriggerSerially.class) || procClass.isAnnotationPresent(org.apache.nifi.processor.annotation.TriggerSerially.class);
        triggerWhenAnyDestinationAvailable = procClass.isAnnotationPresent(TriggerWhenAnyDestinationAvailable.class)
            || procClass.isAnnotationPresent(org.apache.nifi.processor.annotation.TriggerWhenAnyDestinationAvailable.class);
        this.validationContextFactory = validationContextFactory;
        eventDrivenSupported = (procClass.isAnnotationPresent(EventDriven.class)
            || procClass.isAnnotationPresent(org.apache.nifi.processor.annotation.EventDriven.class)) && !triggeredSerially && !triggerWhenEmpty;

        final boolean inputRequirementPresent = procClass.isAnnotationPresent(InputRequirement.class);
        if (inputRequirementPresent) {
            inputRequirement = procClass.getAnnotation(InputRequirement.class).value();
        } else {
            inputRequirement = Requirement.INPUT_ALLOWED;
        }

        schedulingStrategy = SchedulingStrategy.TIMER_DRIVEN;
    }

    /**
     * @return comments about this specific processor instance
     */
    @Override
    public String getComments() {
        return comments.get();
    }

    /**
     * Provides and opportunity to retain information about this particular processor instance
     *
     * @param comments new comments
     */
    @Override
    public void setComments(final String comments) {
        writeLock.lock();
        try {
            if (isRunning()) {
                throw new IllegalStateException("Cannot modify Processor configuration while the Processor is running");
            }
            this.comments.set(comments);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public ScheduledState getScheduledState() {
        return scheduledState.get();
    }

    @Override
    public Position getPosition() {
        return position.get();
    }

    @Override
    public void setPosition(Position position) {
        this.position.set(position);
    }

    @Override
    public Map<String, String> getStyle() {
        return style.get();
    }

    @Override
    public void setStyle(final Map<String, String> style) {
        if (style != null) {
            this.style.set(Collections.unmodifiableMap(new HashMap<>(style)));
        }
    }

    @Override
    public String getIdentifier() {
        return identifier.get();
    }

    /**
     * @return if true flow file content generated by this processor is considered loss tolerant
     */
    @Override
    public boolean isLossTolerant() {
        return lossTolerant.get();
    }

    @Override
    public boolean isIsolated() {
        return isolated.get();
    }

    /**
     * @return true if the processor has the {@link TriggerWhenEmpty} annotation, false otherwise.
     */
    @Override
    public boolean isTriggerWhenEmpty() {
        return triggerWhenEmpty;
    }

    /**
     * @return true if the processor has the {@link SideEffectFree} annotation, false otherwise.
     */
    @Override
    public boolean isSideEffectFree() {
        return sideEffectFree;
    }

    @Override
    public boolean isHighThroughputSupported() {
        return batchSupported;
    }

    /**
     * @return true if the processor has the {@link TriggerWhenAnyDestinationAvailable} annotation, false otherwise.
     */
    @Override
    public boolean isTriggerWhenAnyDestinationAvailable() {
        return triggerWhenAnyDestinationAvailable;
    }

    /**
     * Indicates whether flow file content made by this processor must be persisted
     *
     * @param lossTolerant tolerant
     */
    @Override
    public void setLossTolerant(final boolean lossTolerant) {
        writeLock.lock();
        try {
            if (isRunning()) {
                throw new IllegalStateException("Cannot modify Processor configuration while the Processor is running");
            }
            this.lossTolerant.set(lossTolerant);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Indicates whether the processor runs on only the primary node.
     *
     * @param isolated isolated
     */
    public void setIsolated(final boolean isolated) {
        writeLock.lock();
        try {
            if (isRunning()) {
                throw new IllegalStateException("Cannot modify Processor configuration while the Processor is running");
            }
            this.isolated.set(isolated);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean isAutoTerminated(final Relationship relationship) {
        final Set<Relationship> terminatable = undefinedRelationshipsToTerminate.get();
        if (terminatable == null) {
            return false;
        }
        return terminatable.contains(relationship);
    }

    @Override
    public void setAutoTerminatedRelationships(final Set<Relationship> terminate) {
        writeLock.lock();
        try {
            if (isRunning()) {
                throw new IllegalStateException("Cannot modify Processor configuration while the Processor is running");
            }

            for (final Relationship rel : terminate) {
                if (!getConnections(rel).isEmpty()) {
                    throw new IllegalStateException("Cannot mark relationship '" + rel.getName() + "' as auto-terminated because Connection already exists with this relationship");
                }
            }
            undefinedRelationshipsToTerminate.set(new HashSet<>(terminate));
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * @return an unmodifiable Set that contains all of the ProcessorRelationship objects that are configured to be auto-terminated
     */
    @Override
    public Set<Relationship> getAutoTerminatedRelationships() {
        Set<Relationship> relationships = undefinedRelationshipsToTerminate.get();
        if (relationships == null) {
            relationships = new HashSet<>();
        }
        return Collections.unmodifiableSet(relationships);
    }

    /**
     * @return the value of the processor's {@link CapabilityDescription} annotation, if one exists, else <code>null</code>.
     */
    @SuppressWarnings("deprecation")
    public String getProcessorDescription() {
        CapabilityDescription capDesc = processor.getClass().getAnnotation(CapabilityDescription.class);
        String description = null;
        if (capDesc != null) {
            description = capDesc.value();
        } else {
            final org.apache.nifi.processor.annotation.CapabilityDescription deprecatedCapDesc = processor.getClass().getAnnotation(org.apache.nifi.processor.annotation.CapabilityDescription.class);
            if (deprecatedCapDesc != null) {
                description = deprecatedCapDesc.value();
            }
        }

        return description;
    }

    @Override
    public void setName(final String name) {
        writeLock.lock();
        try {
            if (isRunning()) {
                throw new IllegalStateException("Cannot modify Processor configuration while the Processor is running");
            }
            super.setName(name);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * @param timeUnit determines the unit of time to represent the scheduling period. If null will be reported in units of {@link #DEFAULT_SCHEDULING_TIME_UNIT}
     * @return the schedule period that should elapse before subsequent cycles of this processor's tasks
     */
    @Override
    public long getSchedulingPeriod(final TimeUnit timeUnit) {
        return timeUnit.convert(schedulingNanos.get(), TimeUnit.NANOSECONDS);
    }

    @Override
    public boolean isEventDrivenSupported() {
        readLock.lock();
        try {
            return this.eventDrivenSupported;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Updates the Scheduling Strategy used for this Processor
     *
     * @param schedulingStrategy strategy
     *
     * @throws IllegalArgumentException if the SchedulingStrategy is not not applicable for this Processor
     */
    @Override
    public void setSchedulingStrategy(final SchedulingStrategy schedulingStrategy) {
        writeLock.lock();
        try {
            if (schedulingStrategy == SchedulingStrategy.EVENT_DRIVEN && !eventDrivenSupported) {
                // not valid. Just ignore it. We don't throw an Exception because if a developer changes a Processor so that
                // it no longer supports EventDriven mode, we don't want the app to fail to startup if it was already in Event-Driven
                // Mode. Instead, we will simply leave it in Timer-Driven mode
                return;
            }

            this.schedulingStrategy = schedulingStrategy;
            setIsolated(schedulingStrategy == SchedulingStrategy.PRIMARY_NODE_ONLY);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * @return the currently configured scheduling strategy
     */
    @Override
    public SchedulingStrategy getSchedulingStrategy() {
        readLock.lock();
        try {
            return this.schedulingStrategy;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public String getSchedulingPeriod() {
        return schedulingPeriod.get();
    }

    @Override
    public void setScheduldingPeriod(final String schedulingPeriod) {
        writeLock.lock();
        try {
            if (isRunning()) {
                throw new IllegalStateException("Cannot modify Processor configuration while the Processor is running");
            }

            switch (schedulingStrategy) {
                case CRON_DRIVEN: {
                    try {
                        new CronExpression(schedulingPeriod);
                    } catch (final Exception e) {
                        throw new IllegalArgumentException("Scheduling Period is not a valid cron expression: " + schedulingPeriod);
                    }
                }
                    break;
                case PRIMARY_NODE_ONLY:
                case TIMER_DRIVEN: {
                    final long schedulingNanos = FormatUtils.getTimeDuration(requireNonNull(schedulingPeriod), TimeUnit.NANOSECONDS);
                    if (schedulingNanos < 0) {
                        throw new IllegalArgumentException("Scheduling Period must be positive");
                    }
                    this.schedulingNanos.set(Math.max(MINIMUM_SCHEDULING_NANOS, schedulingNanos));
                }
                    break;
                case EVENT_DRIVEN:
                default:
                    return;
            }

            this.schedulingPeriod.set(schedulingPeriod);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public long getRunDuration(final TimeUnit timeUnit) {
        readLock.lock();
        try {
            return timeUnit.convert(this.runNanos, TimeUnit.NANOSECONDS);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void setRunDuration(final long duration, final TimeUnit timeUnit) {
        writeLock.lock();
        try {
            if (duration < 0) {
                throw new IllegalArgumentException("Run Duration must be non-negative value; cannot set to " + timeUnit.toSeconds(duration) + " seconds");
            }

            this.runNanos = timeUnit.toNanos(duration);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public long getYieldPeriod(final TimeUnit timeUnit) {
        return FormatUtils.getTimeDuration(getYieldPeriod(), timeUnit == null ? DEFAULT_TIME_UNIT : timeUnit);
    }

    @Override
    public String getYieldPeriod() {
        return yieldPeriod.get();
    }

    @Override
    public void setYieldPeriod(final String yieldPeriod) {
        writeLock.lock();
        try {
            if (isRunning()) {
                throw new IllegalStateException("Cannot modify Processor configuration while the Processor is running");
            }
            final long yieldMillis = FormatUtils.getTimeDuration(requireNonNull(yieldPeriod), TimeUnit.MILLISECONDS);
            if (yieldMillis < 0) {
                throw new IllegalArgumentException("Yield duration must be positive");
            }
            this.yieldPeriod.set(yieldPeriod);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Causes the processor not to be scheduled for some period of time. This duration can be obtained and set via the {@link #getYieldPeriod(TimeUnit)} and {@link #setYieldPeriod(long, TimeUnit)}
     * methods.
     */
    @Override
    public void yield() {
        final long yieldMillis = getYieldPeriod(TimeUnit.MILLISECONDS);
        yield(yieldMillis, TimeUnit.MILLISECONDS);

        final String yieldDuration = (yieldMillis > 1000) ? (yieldMillis / 1000) + " seconds" : yieldMillis + " milliseconds";
        LoggerFactory.getLogger(processor.getClass()).debug("{} has chosen to yield its resources; will not be scheduled to run again for {}", processor, yieldDuration);
    }

    @Override
    public void yield(final long period, final TimeUnit timeUnit) {
        final long yieldMillis = TimeUnit.MILLISECONDS.convert(period, timeUnit);
        yieldExpiration.set(Math.max(yieldExpiration.get(), System.currentTimeMillis() + yieldMillis));

        processScheduler.yield(this);
    }

    /**
     * @return the number of milliseconds since Epoch at which time this processor is to once again be scheduled.
     */
    @Override
    public long getYieldExpiration() {
        return yieldExpiration.get();
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
    public void setPenalizationPeriod(final String penalizationPeriod) {
        writeLock.lock();
        try {
            if (isRunning()) {
                throw new IllegalStateException("Cannot modify Processor configuration while the Processor is running");
            }
            final long penalizationMillis = FormatUtils.getTimeDuration(requireNonNull(penalizationPeriod), TimeUnit.MILLISECONDS);
            if (penalizationMillis < 0) {
                throw new IllegalArgumentException("Penalization duration must be positive");
            }
            this.penalizationPeriod.set(penalizationPeriod);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Determines the number of concurrent tasks that may be running for this processor.
     *
     * @param taskCount a number of concurrent tasks this processor may have running
     * @throws IllegalArgumentException if the given value is less than 1
     */
    @Override
    public void setMaxConcurrentTasks(final int taskCount) {
        writeLock.lock();
        try {
            if (isRunning()) {
                throw new IllegalStateException("Cannot modify Processor configuration while the Processor is running");
            }
            if (taskCount < 1 && getSchedulingStrategy() != SchedulingStrategy.EVENT_DRIVEN) {
                throw new IllegalArgumentException();
            }
            if (!triggeredSerially) {
                concurrentTaskCount.set(taskCount);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean isTriggeredSerially() {
        return triggeredSerially;
    }

    /**
     * @return the number of tasks that may execute concurrently for this processor
     */
    @Override
    public int getMaxConcurrentTasks() {
        return concurrentTaskCount.get();
    }

    @Override
    public LogLevel getBulletinLevel() {
        return LogRepositoryFactory.getRepository(getIdentifier()).getObservationLevel(BULLETIN_OBSERVER_ID);
    }

    @Override
    public void setBulletinLevel(final LogLevel level) {
        LogRepositoryFactory.getRepository(getIdentifier()).setObservationLevel(BULLETIN_OBSERVER_ID, level);
    }

    @Override
    public Set<Connection> getConnections() {
        final Set<Connection> allConnections = new HashSet<>();
        readLock.lock();
        try {
            for (final Set<Connection> connectionSet : connections.values()) {
                allConnections.addAll(connectionSet);
            }
        } finally {
            readLock.unlock();
        }

        return allConnections;
    }

    @Override
    public List<Connection> getIncomingConnections() {
        return incomingConnectionsRef.get();
    }

    @Override
    public Set<Connection> getConnections(final Relationship relationship) {
        final Set<Connection> applicableConnections;
        readLock.lock();
        try {
            applicableConnections = connections.get(relationship);
        } finally {
            readLock.unlock();
        }
        return (applicableConnections == null) ? Collections.<Connection> emptySet() : Collections.unmodifiableSet(applicableConnections);
    }

    @Override
    public void addConnection(final Connection connection) {
        Objects.requireNonNull(connection, "connection cannot be null");

        if (!connection.getSource().equals(this) && !connection.getDestination().equals(this)) {
            throw new IllegalStateException("Cannot a connection to a ProcessorNode for which the ProcessorNode is neither the Source nor the Destination");
        }

        writeLock.lock();
        try {
            List<Connection> updatedIncoming = null;
            if (connection.getDestination().equals(this)) {
                // don't add the connection twice. This may occur if we have a self-loop because we will be told
                // to add the connection once because we are the source and again because we are the destination.
                final List<Connection> incomingConnections = incomingConnectionsRef.get();
                updatedIncoming = new ArrayList<>(incomingConnections);
                if (!updatedIncoming.contains(connection)) {
                    updatedIncoming.add(connection);
                }
            }

            if (connection.getSource().equals(this)) {
                // don't add the connection twice. This may occur if we have a self-loop because we will be told
                // to add the connection once because we are the source and again because we are the destination.
                if (!destinations.containsKey(connection)) {
                    for (final Relationship relationship : connection.getRelationships()) {
                        final Relationship rel = getRelationship(relationship.getName());
                        Set<Connection> set = connections.get(rel);
                        if (set == null) {
                            set = new HashSet<>();
                            connections.put(rel, set);
                        }

                        set.add(connection);

                        destinations.put(connection, connection.getDestination());
                    }

                    final Set<Relationship> autoTerminated = this.undefinedRelationshipsToTerminate.get();
                    if (autoTerminated != null) {
                        autoTerminated.removeAll(connection.getRelationships());
                        this.undefinedRelationshipsToTerminate.set(autoTerminated);
                    }
                }
            }

            if (updatedIncoming != null) {
                incomingConnectionsRef.set(Collections.unmodifiableList(updatedIncoming));
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean hasIncomingConnection() {
        return !incomingConnectionsRef.get().isEmpty();
    }

    @Override
    public void updateConnection(final Connection connection) throws IllegalStateException {
        if (requireNonNull(connection).getSource().equals(this)) {
            writeLock.lock();
            try {
                //
                // update any relationships
                //
                // first check if any relations were removed.
                final List<Relationship> existingRelationships = new ArrayList<>();
                for (final Map.Entry<Relationship, Set<Connection>> entry : connections.entrySet()) {
                    if (entry.getValue().contains(connection)) {
                        existingRelationships.add(entry.getKey());
                    }
                }

                for (final Relationship rel : connection.getRelationships()) {
                    if (!existingRelationships.contains(rel)) {
                        // relationship was removed. Check if this is legal.
                        final Set<Connection> connectionsForRelationship = getConnections(rel);
                        if (connectionsForRelationship != null && connectionsForRelationship.size() == 1 && this.isRunning() && !isAutoTerminated(rel) && getRelationships().contains(rel)) {
                            // if we are running and we do not terminate undefined relationships and this is the only
                            // connection that defines the given relationship, and that relationship is required,
                            // then it is not legal to remove this relationship from this connection.
                            throw new IllegalStateException("Cannot remove relationship " + rel.getName() + " from Connection because doing so would invalidate Processor "
                                + this + ", which is currently running");
                        }
                    }
                }

                // remove the connection from any list that currently contains
                for (final Set<Connection> list : connections.values()) {
                    list.remove(connection);
                }

                // add the connection in for all relationships listed.
                for (final Relationship rel : connection.getRelationships()) {
                    Set<Connection> set = connections.get(rel);
                    if (set == null) {
                        set = new HashSet<>();
                        connections.put(rel, set);
                    }
                    set.add(connection);
                }

                // update to the new destination
                destinations.put(connection, connection.getDestination());

                final Set<Relationship> autoTerminated = this.undefinedRelationshipsToTerminate.get();
                if (autoTerminated != null) {
                    autoTerminated.removeAll(connection.getRelationships());
                    this.undefinedRelationshipsToTerminate.set(autoTerminated);
                }
            } finally {
                writeLock.unlock();
            }
        }

        if (connection.getDestination().equals(this)) {
            writeLock.lock();
            try {
                // update our incoming connections -- we can just remove & re-add the connection to
                // update the list.
                final List<Connection> incomingConnections = incomingConnectionsRef.get();
                final List<Connection> updatedIncoming = new ArrayList<>(incomingConnections);
                updatedIncoming.remove(connection);
                updatedIncoming.add(connection);
                incomingConnectionsRef.set(Collections.unmodifiableList(updatedIncoming));
            } finally {
                writeLock.unlock();
            }
        }
    }

    @Override
    public void removeConnection(final Connection connection) {
        boolean connectionRemoved = false;

        if (requireNonNull(connection).getSource().equals(this)) {
            for (final Relationship relationship : connection.getRelationships()) {
                final Set<Connection> connectionsForRelationship = getConnections(relationship);
                if ((connectionsForRelationship == null || connectionsForRelationship.size() <= 1) && isRunning()) {
                    throw new IllegalStateException("This connection cannot be removed because its source is running and removing it will invalidate this processor");
                }
            }

            writeLock.lock();
            try {
                for (final Set<Connection> connectionList : this.connections.values()) {
                    connectionList.remove(connection);
                }

                connectionRemoved = (destinations.remove(connection) != null);
            } finally {
                writeLock.unlock();
            }
        }

        if (connection.getDestination().equals(this)) {
            writeLock.lock();
            try {
                final List<Connection> incomingConnections = incomingConnectionsRef.get();
                if (incomingConnections.contains(connection)) {
                    final List<Connection> updatedIncoming = new ArrayList<>(incomingConnections);
                    updatedIncoming.remove(connection);
                    incomingConnectionsRef.set(Collections.unmodifiableList(updatedIncoming));
                    return;
                }
            } finally {
                writeLock.unlock();
            }
        }

        if (!connectionRemoved) {
            throw new IllegalArgumentException("Cannot remove a connection from a ProcessorNode for which the ProcessorNode is not the Source");
        }
    }

    /**
     * @param relationshipName name
     * @return the relationship for this nodes processor for the given name or creates a new relationship for the given name
     */
    @Override
    public Relationship getRelationship(final String relationshipName) {
        final Relationship specRel = new Relationship.Builder().name(relationshipName).build();
        Relationship returnRel = specRel;

        final Set<Relationship> relationships;
        try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
            relationships = processor.getRelationships();
        }

        for (final Relationship rel : relationships) {
            if (rel.equals(specRel)) {
                returnRel = rel;
                break;
            }
        }
        return returnRel;
    }

    @Override
    public Processor getProcessor() {
        return this.processor;
    }

    /**
     * @return the Set of destination processors for all relationships excluding any destinations that are this processor itself (self-loops)
     */
    public Set<Connectable> getDestinations() {
        final Set<Connectable> nonSelfDestinations = new HashSet<>();
        readLock.lock();
        try {
            for (final Connectable connectable : destinations.values()) {
                if (connectable != this) {
                    nonSelfDestinations.add(connectable);
                }
            }
        } finally {
            readLock.unlock();
        }
        return nonSelfDestinations;
    }

    public Set<Connectable> getDestinations(final Relationship relationship) {
        readLock.lock();
        try {
            final Set<Connectable> destinationSet = new HashSet<>();
            final Set<Connection> relationshipConnections = connections.get(relationship);
            if (relationshipConnections != null) {
                for (final Connection connection : relationshipConnections) {
                    destinationSet.add(destinations.get(connection));
                }
            }
            return destinationSet;
        } finally {
            readLock.unlock();
        }
    }

    public Set<Relationship> getUndefinedRelationships() {
        final Set<Relationship> undefined = new HashSet<>();
        readLock.lock();
        try {
            final Set<Relationship> relationships;
            try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                relationships = processor.getRelationships();
            }

            if (relationships == null) {
                return undefined;
            }
            for (final Relationship relation : relationships) {
                final Set<Connection> connectionSet = this.connections.get(relation);
                if (connectionSet == null || connectionSet.isEmpty()) {
                    undefined.add(relation);
                }
            }
        } finally {
            readLock.unlock();
        }
        return undefined;
    }

    /**
     * Determines if the given node is a destination for this node
     *
     * @param node node
     * @return true if is a direct destination node; false otherwise
     */
    boolean isRelated(final ProcessorNode node) {
        readLock.lock();
        try {
            return this.destinations.containsValue(node);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean isRunning() {
        readLock.lock();
        try {
            return getScheduledState().equals(ScheduledState.RUNNING) || processScheduler.getActiveThreadCount(this) > 0;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public int getActiveThreadCount() {
        readLock.lock();
        try {
            return processScheduler.getActiveThreadCount(this);
        } finally {
            readLock.unlock();
        }
    }

    List<Connection> getIncomingNonLoopConnections() {
        final List<Connection> connections = getIncomingConnections();
        final List<Connection> nonLoopConnections = new ArrayList<>(connections.size());
        for (final Connection connection : connections) {
            if (!connection.getSource().equals(this)) {
                nonLoopConnections.add(connection);
            }
        }

        return nonLoopConnections;
    }

    @Override
    public boolean isValid() {
        readLock.lock();
        try {
            final ValidationContext validationContext = validationContextFactory.newValidationContext(getProperties(), getAnnotationData());

            final Collection<ValidationResult> validationResults;
            try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                validationResults = getProcessor().validate(validationContext);
            }

            for (final ValidationResult result : validationResults) {
                if (!result.isValid()) {
                    return false;
                }
            }

            for (final Relationship undef : getUndefinedRelationships()) {
                if (!isAutoTerminated(undef)) {
                    return false;
                }
            }

            switch (getInputRequirement()) {
                case INPUT_ALLOWED:
                    break;
                case INPUT_FORBIDDEN: {
                    if (!getIncomingNonLoopConnections().isEmpty()) {
                        return false;
                    }
                    break;
                }
                case INPUT_REQUIRED: {
                    if (getIncomingNonLoopConnections().isEmpty()) {
                        return false;
                    }
                    break;
                }
            }
        } catch (final Throwable t) {
            return false;
        } finally {
            readLock.unlock();
        }

        return true;
    }

    @Override
    public Collection<ValidationResult> getValidationErrors() {
        final List<ValidationResult> results = new ArrayList<>();
        readLock.lock();
        try {
            final ValidationContext validationContext = validationContextFactory.newValidationContext(getProperties(), getAnnotationData());

            final Collection<ValidationResult> validationResults;
            try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                validationResults = getProcessor().validate(validationContext);
            }

            for (final ValidationResult result : validationResults) {
                if (!result.isValid()) {
                    results.add(result);
                }
            }

            for (final Relationship relationship : getUndefinedRelationships()) {
                if (!isAutoTerminated(relationship)) {
                    final ValidationResult error = new ValidationResult.Builder()
                        .explanation("Relationship '" + relationship.getName() + "' is not connected to any component and is not auto-terminated")
                        .subject("Relationship " + relationship.getName())
                        .valid(false)
                        .build();
                    results.add(error);
                }
            }

            switch (getInputRequirement()) {
                case INPUT_ALLOWED:
                    break;
                case INPUT_FORBIDDEN: {
                    final int incomingConnCount = getIncomingNonLoopConnections().size();
                    if (incomingConnCount != 0) {
                        results.add(new ValidationResult.Builder()
                            .explanation("Processor does not allow upstream connections but currently has " + incomingConnCount)
                            .subject("Upstream Connections")
                            .valid(false)
                            .build());
                    }
                    break;
                }
                case INPUT_REQUIRED: {
                    if (getIncomingNonLoopConnections().isEmpty()) {
                        results.add(new ValidationResult.Builder()
                            .explanation("Processor requires an upstream connection but currently has none")
                            .subject("Upstream Connections")
                            .valid(false)
                            .build());
                    }
                    break;
                }
            }
        } catch (final Throwable t) {
            results.add(new ValidationResult.Builder().explanation("Failed to run validation due to " + t.toString()).valid(false).build());
        } finally {
            readLock.unlock();
        }
        return results;
    }

    @Override
    public Requirement getInputRequirement() {
        return inputRequirement;
    }

    /**
     * Establishes node equality (based on the processor's identifier)
     *
     * @param other node
     * @return true if equal
     */
    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof ProcessorNode)) {
            return false;
        }
        final ProcessorNode on = (ProcessorNode) other;
        return new EqualsBuilder().append(identifier.get(), on.getIdentifier()).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(7, 67).append(identifier).toHashCode();
    }

    @Override
    public Collection<Relationship> getRelationships() {
        try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
            return getProcessor().getRelationships();
        }
    }

    @Override
    public String toString() {
        try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
            return getProcessor().toString();
        }
    }

    @Override
    public ProcessGroup getProcessGroup() {
        return processGroup.get();
    }

    @Override
    public void setProcessGroup(final ProcessGroup group) {
        writeLock.lock();
        try {
            this.processGroup.set(group);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) {
        try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
            processor.onTrigger(context, sessionFactory);
        }
    }

    @Override
    public ConnectableType getConnectableType() {
        return ConnectableType.PROCESSOR;
    }

    @Override
    public void setScheduledState(final ScheduledState scheduledState) {
        this.scheduledState.set(scheduledState);
        if (!scheduledState.equals(ScheduledState.RUNNING)) { // if user stops processor, clear yield expiration
            yieldExpiration.set(0L);
        }
    }

    @Override
    public void setAnnotationData(final String data) {
        writeLock.lock();
        try {
            if (isRunning()) {
                throw new IllegalStateException("Cannot set AnnotationData while processor is running");
            }

            this.annotationData.set(data);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public String getAnnotationData() {
        return annotationData.get();
    }

    @Override
    public Collection<ValidationResult> validate(final ValidationContext validationContext) {
        return getValidationErrors();
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
                throw new IllegalStateException(this + " is running");
            }

            if (!ignoreConnections) {
                for (final Set<Connection> connectionSet : connections.values()) {
                    for (final Connection connection : connectionSet) {
                        connection.verifyCanDelete();
                    }
                }

                for (final Connection connection : incomingConnectionsRef.get()) {
                    if (connection.getSource().equals(this)) {
                        connection.verifyCanDelete();
                    } else {
                        throw new IllegalStateException(this + " is the destination of another component");
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
            switch (getScheduledState()) {
                case DISABLED:
                    throw new IllegalStateException(this + " cannot be started because it is disabled");
                case RUNNING:
                    throw new IllegalStateException(this + " cannot be started because it is already running");
                case STOPPED:
                    break;
            }
            verifyNoActiveThreads();

            if (!isValid()) {
                throw new IllegalStateException(this + " is not in a valid state");
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void verifyCanStart(final Set<ControllerServiceNode> ignoredReferences) {
        switch (getScheduledState()) {
            case DISABLED:
                throw new IllegalStateException(this + " cannot be started because it is disabled");
            case RUNNING:
                throw new IllegalStateException(this + " cannot be started because it is already running");
            case STOPPED:
                break;
        }
        verifyNoActiveThreads();

        final Set<String> ids = new HashSet<>();
        for (final ControllerServiceNode node : ignoredReferences) {
            ids.add(node.getIdentifier());
        }

        final Collection<ValidationResult> validationResults = getValidationErrors(ids);
        for (final ValidationResult result : validationResults) {
            if (!result.isValid()) {
                throw new IllegalStateException(this + " cannot be started because it is not valid: " + result);
            }
        }
    }

    @Override
    public void verifyCanStop() {
        if (getScheduledState() != ScheduledState.RUNNING) {
            throw new IllegalStateException(this + " is not scheduled to run");
        }
    }

    @Override
    public void verifyCanUpdate() {
        readLock.lock();
        try {
            if (isRunning()) {
                throw new IllegalStateException(this + " is not stopped");
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
                throw new IllegalStateException(this + " is not disabled");
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
                throw new IllegalStateException(this + " is not stopped");
            }
            verifyNoActiveThreads();
        } finally {
            readLock.unlock();
        }
    }

    private void verifyNoActiveThreads() throws IllegalStateException {
        final int threadCount = processScheduler.getActiveThreadCount(this);
        if (threadCount > 0) {
            throw new IllegalStateException(this + " has " + threadCount + " threads still active");
        }
    }

    @Override
    public void verifyModifiable() throws IllegalStateException {
        if (isRunning()) {
            throw new IllegalStateException("Cannot modify Processor configuration while the Processor is running");
        }
    }
}
