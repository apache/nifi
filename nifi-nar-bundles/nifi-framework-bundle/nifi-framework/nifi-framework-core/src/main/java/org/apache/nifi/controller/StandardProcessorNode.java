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

import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.TriggerWhenAnyDestinationAvailable;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.DeprecationNotice;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.scheduling.ScheduleState;
import org.apache.nifi.controller.scheduling.SchedulingAgent;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.logging.LogRepositoryFactory;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.SimpleProcessLogger;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.CharacterFilterUtils;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.ReflectionUtils;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

/**
 * ProcessorNode provides thread-safe access to a FlowFileProcessor as it exists
 * within a controlled flow. This node keeps track of the processor, its
 * scheduling information and its relationships to other processors and whatever
 * scheduled futures exist for it. Must be thread safe.
 *
 */
public class StandardProcessorNode extends ProcessorNode implements Connectable {

    private static final Logger LOG = LoggerFactory.getLogger(StandardProcessorNode.class);

    public static final String BULLETIN_OBSERVER_ID = "bulletin-observer";

    public static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;
    public static final String DEFAULT_YIELD_PERIOD = "1 sec";
    public static final String DEFAULT_PENALIZATION_PERIOD = "30 sec";
    private final AtomicReference<ProcessGroup> processGroup;
    private final AtomicReference<ProcessorDetails> processorRef;
    private final AtomicReference<String> identifier;
    private final Map<Connection, Connectable> destinations;
    private final Map<Relationship, Set<Connection>> connections;
    private final AtomicReference<Set<Relationship>> undefinedRelationshipsToTerminate;
    private final AtomicReference<List<Connection>> incomingConnectionsRef;
    private final AtomicBoolean lossTolerant;
    private final AtomicReference<String> comments;
    private final AtomicReference<Position> position;
    private final AtomicReference<String> schedulingPeriod; // stored as string so it's presented to user as they entered it
    private final AtomicReference<String> yieldPeriod;
    private final AtomicReference<String> penalizationPeriod;
    private final AtomicReference<Map<String, String>> style;
    private final AtomicInteger concurrentTaskCount;
    private final AtomicLong yieldExpiration;
    private final AtomicLong schedulingNanos;
    private final ProcessScheduler processScheduler;
    private long runNanos = 0L;
    private volatile long yieldNanos;
    private final NiFiProperties nifiProperties;

    private SchedulingStrategy schedulingStrategy; // guarded by read/write lock
                                                   // ??????? NOT any more
    private ExecutionNode executionNode;

    public StandardProcessorNode(final LoggableComponent<Processor> processor, final String uuid,
                                 final ValidationContextFactory validationContextFactory, final ProcessScheduler scheduler,
                                 final ControllerServiceProvider controllerServiceProvider, final NiFiProperties nifiProperties,
                                 final ComponentVariableRegistry variableRegistry, final ReloadComponent reloadComponent) {

        this(processor, uuid, validationContextFactory, scheduler, controllerServiceProvider,
            processor.getComponent().getClass().getSimpleName(), processor.getComponent().getClass().getCanonicalName(), nifiProperties, variableRegistry, reloadComponent, false);
    }

    public StandardProcessorNode(final LoggableComponent<Processor> processor, final String uuid,
                                 final ValidationContextFactory validationContextFactory, final ProcessScheduler scheduler,
                                 final ControllerServiceProvider controllerServiceProvider,
                                 final String componentType, final String componentCanonicalClass, final NiFiProperties nifiProperties,
                                 final ComponentVariableRegistry variableRegistry, final ReloadComponent reloadComponent, final boolean isExtensionMissing) {

        super(uuid, validationContextFactory, controllerServiceProvider, componentType, componentCanonicalClass, variableRegistry, reloadComponent, isExtensionMissing);

        final ProcessorDetails processorDetails = new ProcessorDetails(processor);
        this.processorRef = new AtomicReference<>(processorDetails);

        identifier = new AtomicReference<>(uuid);
        destinations = new HashMap<>();
        connections = new HashMap<>();
        incomingConnectionsRef = new AtomicReference<>(new ArrayList<>());
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
        penalizationPeriod = new AtomicReference<>(DEFAULT_PENALIZATION_PERIOD);
        this.nifiProperties = nifiProperties;

        schedulingStrategy = SchedulingStrategy.TIMER_DRIVEN;
        executionNode = ExecutionNode.ALL;
        try {
            if (processorDetails.getProcClass().isAnnotationPresent(DefaultSchedule.class)) {
                DefaultSchedule dsc = processorDetails.getProcClass().getAnnotation(DefaultSchedule.class);
                try {
                    this.setSchedulingStrategy(dsc.strategy());
                } catch (Throwable ex) {
                    LOG.error(String.format("Error while setting scheduling strategy from DefaultSchedule annotation: %s", ex.getMessage()), ex);
                }
                try {
                    this.setScheduldingPeriod(dsc.period());
                } catch (Throwable ex) {
                    this.setSchedulingStrategy(SchedulingStrategy.TIMER_DRIVEN);
                    LOG.error(String.format("Error while setting scheduling period from DefaultSchedule annotation: %s", ex.getMessage()), ex);
                }
                if (!processorDetails.isTriggeredSerially()) {
                    try {
                        setMaxConcurrentTasks(dsc.concurrentTasks());
                    } catch (Throwable ex) {
                        LOG.error(String.format("Error while setting max concurrent tasks from DefaultSchedule annotation: %s", ex.getMessage()), ex);
                    }
                }
            }
        } catch (Throwable ex) {
            LOG.error(String.format("Error while setting default schedule from DefaultSchedule annotation: %s",ex.getMessage()),ex);
        }
    }

    @Override
    public ConfigurableComponent getComponent() {
        return processorRef.get().getProcessor();
    }

    @Override
    public ComponentLog getLogger() {
        return processorRef.get().getComponentLog();
    }

    @Override
    public BundleCoordinate getBundleCoordinate() {
        return processorRef.get().getBundleCoordinate();
    }

    /**
     * @return comments about this specific processor instance
     */
    @Override
    public String getComments() {
        return comments.get();
    }

    @Override
    public Authorizable getParentAuthorizable() {
        return getProcessGroup();
    }

    @Override
    public Resource getResource() {
        return ResourceFactory.getComponentResource(ResourceType.Processor, getIdentifier(), getName());
    }

    @Override
    public boolean isRestricted() {
        return getProcessor().getClass().isAnnotationPresent(Restricted.class);
    }

    @Override
    public boolean isDeprecated() {
        return getProcessor().getClass().isAnnotationPresent(DeprecationNotice.class);
    }


    /**
     * Provides and opportunity to retain information about this particular
     * processor instance
     *
     * @param comments
     *            new comments
     */
    @Override
    public synchronized void setComments(final String comments) {
        this.comments.set(CharacterFilterUtils.filterInvalidXmlCharacters(comments));
    }

    @Override
    public Position getPosition() {
        return position.get();
    }

    @Override
    public synchronized void setPosition(final Position position) {
        this.position.set(position);
    }

    @Override
    public Map<String, String> getStyle() {
        return style.get();
    }

    @Override
    public synchronized void setStyle(final Map<String, String> style) {
        if (style != null) {
            this.style.set(Collections.unmodifiableMap(new HashMap<>(style)));
        }
    }

    @Override
    public String getIdentifier() {
        return identifier.get();
    }

    /**
     * @return if true flow file content generated by this processor is
     *         considered loss tolerant
     */
    @Override
    public boolean isLossTolerant() {
        return lossTolerant.get();
    }

    @Override
    public boolean isIsolated() {
        return schedulingStrategy == SchedulingStrategy.PRIMARY_NODE_ONLY || executionNode == ExecutionNode.PRIMARY;
    }

    /**
     * @return true if the processor has the {@link TriggerWhenEmpty}
     *         annotation, false otherwise.
     */
    @Override
    public boolean isTriggerWhenEmpty() {
        return processorRef.get().isTriggerWhenEmpty();
    }

    /**
     * @return true if the processor has the {@link SideEffectFree} annotation,
     *         false otherwise.
     */
    @Override
    public boolean isSideEffectFree() {
        return processorRef.get().isSideEffectFree();
    }

    @Override
    public boolean isHighThroughputSupported() {
        return processorRef.get().isBatchSupported();
    }

    /**
     * @return true if the processor has the
     *         {@link TriggerWhenAnyDestinationAvailable} annotation, false
     *         otherwise.
     */
    @Override
    public boolean isTriggerWhenAnyDestinationAvailable() {
        return processorRef.get().isTriggerWhenAnyDestinationAvailable();
    }

    /**
     * Indicates whether flow file content made by this processor must be
     * persisted
     *
     * @param lossTolerant
     *            tolerant
     */
    @Override
    public synchronized void setLossTolerant(final boolean lossTolerant) {
        if (isRunning()) {
            throw new IllegalStateException("Cannot modify Processor configuration while the Processor is running");
        }
        this.lossTolerant.set(lossTolerant);
    }

    @Override
    public boolean isAutoTerminated(final Relationship relationship) {
        if (relationship.isAutoTerminated() && getConnections(relationship).isEmpty()) {
            return true;
        }
        final Set<Relationship> terminatable = undefinedRelationshipsToTerminate.get();
        return terminatable == null ? false : terminatable.contains(relationship);
    }

    @Override
    public void setAutoTerminatedRelationships(final Set<Relationship> terminate) {
        if (isRunning()) {
            throw new IllegalStateException("Cannot modify Processor configuration while the Processor is running");
        }

        for (final Relationship rel : terminate) {
            if (!getConnections(rel).isEmpty()) {
                throw new IllegalStateException("Cannot mark relationship '" + rel.getName()
                        + "' as auto-terminated because Connection already exists with this relationship");
            }
        }
        undefinedRelationshipsToTerminate.set(new HashSet<>(terminate));
    }

    /**
     * @return an unmodifiable Set that contains all of the
     *         ProcessorRelationship objects that are configured to be
     *         auto-terminated
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
     * @return the value of the processor's {@link CapabilityDescription}
     *         annotation, if one exists, else <code>null</code>.
     */
    public String getProcessorDescription() {
        final Processor processor = processorRef.get().getProcessor();
        final CapabilityDescription capDesc = processor.getClass().getAnnotation(CapabilityDescription.class);
        String description = null;
        if (capDesc != null) {
            description = capDesc.value();
        }
        return description;
    }

    @Override
    public synchronized void setName(final String name) {
        super.setName(name);
    }

    /**
     * @param timeUnit
     *            determines the unit of time to represent the scheduling
     *            period. If null will be reported in units of
     *            {@link #DEFAULT_SCHEDULING_TIME_UNIT}
     * @return the schedule period that should elapse before subsequent cycles
     *         of this processor's tasks
     */
    @Override
    public long getSchedulingPeriod(final TimeUnit timeUnit) {
        return timeUnit.convert(schedulingNanos.get(), TimeUnit.NANOSECONDS);
    }

    @Override
    public boolean isEventDrivenSupported() {
        return processorRef.get().isEventDrivenSupported();
    }

    /**
     * Updates the Scheduling Strategy used for this Processor
     *
     * @param schedulingStrategy
     *            strategy
     *
     * @throws IllegalArgumentException
     *             if the SchedulingStrategy is not not applicable for this
     *             Processor
     */
    @Override
    public synchronized void setSchedulingStrategy(final SchedulingStrategy schedulingStrategy) {
        if (schedulingStrategy == SchedulingStrategy.EVENT_DRIVEN && !processorRef.get().isEventDrivenSupported()) {
            // not valid. Just ignore it. We don't throw an Exception because if
            // a developer changes a Processor so that
            // it no longer supports EventDriven mode, we don't want the app to
            // fail to startup if it was already in Event-Driven
            // Mode. Instead, we will simply leave it in Timer-Driven mode
            return;
        }

        this.schedulingStrategy = schedulingStrategy;
    }

    /**
     * @return the currently configured scheduling strategy
     */
    @Override
    public SchedulingStrategy getSchedulingStrategy() {
        return this.schedulingStrategy;
    }

    @Override
    public String getSchedulingPeriod() {
        return schedulingPeriod.get();
    }

    @Override
    public synchronized void setScheduldingPeriod(final String schedulingPeriod) {
        if (isRunning()) {
            throw new IllegalStateException("Cannot modify Processor configuration while the Processor is running");
        }

        switch (schedulingStrategy) {
        case CRON_DRIVEN: {
            try {
                new CronExpression(schedulingPeriod);
            } catch (final Exception e) {
                throw new IllegalArgumentException(
                        "Scheduling Period is not a valid cron expression: " + schedulingPeriod);
            }
        }
            break;
        case PRIMARY_NODE_ONLY:
        case TIMER_DRIVEN: {
            final long schedulingNanos = FormatUtils.getTimeDuration(requireNonNull(schedulingPeriod),
                    TimeUnit.NANOSECONDS);
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
    }

    @Override
    public synchronized void setExecutionNode(final ExecutionNode executionNode) {
        this.executionNode = executionNode;
    }

    @Override
    public ExecutionNode getExecutionNode() {
        return this.executionNode;
    }

    @Override
    public long getRunDuration(final TimeUnit timeUnit) {
        return timeUnit.convert(this.runNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public synchronized void setRunDuration(final long duration, final TimeUnit timeUnit) {
        if (duration < 0) {
            throw new IllegalArgumentException("Run Duration must be non-negative value; cannot set to "
                    + timeUnit.toSeconds(duration) + " seconds");
        }

        this.runNanos = timeUnit.toNanos(duration);
    }

    @Override
    public long getYieldPeriod(final TimeUnit timeUnit) {
        final TimeUnit unit = (timeUnit == null ? DEFAULT_TIME_UNIT : timeUnit);
        return unit.convert(yieldNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public String getYieldPeriod() {
        return yieldPeriod.get();
    }

    @Override
    public synchronized void setYieldPeriod(final String yieldPeriod) {
        if (isRunning()) {
            throw new IllegalStateException("Cannot modify Processor configuration while the Processor is running");
        }
        final long yieldNanos = FormatUtils.getTimeDuration(requireNonNull(yieldPeriod), TimeUnit.NANOSECONDS);
        if (yieldNanos < 0) {
            throw new IllegalArgumentException("Yield duration must be positive");
        }
        this.yieldPeriod.set(yieldPeriod);
        this.yieldNanos = yieldNanos;
    }

    /**
     * Causes the processor not to be scheduled for some period of time. This
     * duration can be obtained and set via the
     * {@link #getYieldPeriod(TimeUnit)} and
     * {@link #setYieldPeriod(long, TimeUnit)} methods.
     */
    @Override
    public void yield() {
        final Processor processor = processorRef.get().getProcessor();
        final long yieldMillis = getYieldPeriod(TimeUnit.MILLISECONDS);
        yield(yieldMillis, TimeUnit.MILLISECONDS);

        final String yieldDuration = (yieldMillis > 1000) ? (yieldMillis / 1000) + " seconds"
                : yieldMillis + " milliseconds";
        LoggerFactory.getLogger(processor.getClass()).debug(
                "{} has chosen to yield its resources; will not be scheduled to run again for {}", processor,
                yieldDuration);
    }

    @Override
    public void yield(final long period, final TimeUnit timeUnit) {
        final long yieldMillis = TimeUnit.MILLISECONDS.convert(period, timeUnit);
        yieldExpiration.set(Math.max(yieldExpiration.get(), System.currentTimeMillis() + yieldMillis));

        processScheduler.yield(this);
    }

    /**
     * @return the number of milliseconds since Epoch at which time this
     *         processor is to once again be scheduled.
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
    public synchronized void setPenalizationPeriod(final String penalizationPeriod) {
        if (isRunning()) {
            throw new IllegalStateException("Cannot modify Processor configuration while the Processor is running");
        }
        final long penalizationMillis = FormatUtils.getTimeDuration(requireNonNull(penalizationPeriod),
                TimeUnit.MILLISECONDS);
        if (penalizationMillis < 0) {
            throw new IllegalArgumentException("Penalization duration must be positive");
        }
        this.penalizationPeriod.set(penalizationPeriod);
    }

    /**
     * Determines the number of concurrent tasks that may be running for this
     * processor.
     *
     * @param taskCount
     *            a number of concurrent tasks this processor may have running
     * @throws IllegalArgumentException
     *             if the given value is less than 1
     */
    @Override
    public synchronized void setMaxConcurrentTasks(final int taskCount) {
        if (isRunning()) {
            throw new IllegalStateException("Cannot modify Processor configuration while the Processor is running");
        }
        if (taskCount < 1 && getSchedulingStrategy() != SchedulingStrategy.EVENT_DRIVEN) {
            throw new IllegalArgumentException();
        }
        if (!isTriggeredSerially()) {
            concurrentTaskCount.set(taskCount);
        }
    }

    @Override
    public boolean isTriggeredSerially() {
        return processorRef.get().isTriggeredSerially();
    }

    /**
     * @return the number of tasks that may execute concurrently for this
     *         processor
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
    public synchronized void setBulletinLevel(final LogLevel level) {
        LogRepositoryFactory.getRepository(getIdentifier()).setObservationLevel(BULLETIN_OBSERVER_ID, level);
    }

    @Override
    public Set<Connection> getConnections() {
        final Set<Connection> allConnections = new HashSet<>();
        for (final Set<Connection> connectionSet : connections.values()) {
            allConnections.addAll(connectionSet);
        }

        return allConnections;
    }

    @Override
    public List<Connection> getIncomingConnections() {
        return incomingConnectionsRef.get();
    }

    @Override
    public Set<Connection> getConnections(final Relationship relationship) {
        final Set<Connection> applicableConnections = connections.get(relationship);
        return (applicableConnections == null) ? Collections.<Connection> emptySet()
                : Collections.unmodifiableSet(applicableConnections);
    }

    @Override
    public void addConnection(final Connection connection) {
        Objects.requireNonNull(connection, "connection cannot be null");

        if (!connection.getSource().equals(this) && !connection.getDestination().equals(this)) {
            throw new IllegalStateException(
                    "Cannot a connection to a ProcessorNode for which the ProcessorNode is neither the Source nor the Destination");
        }

        List<Connection> updatedIncoming = null;
        if (connection.getDestination().equals(this)) {
            // don't add the connection twice. This may occur if we have a
            // self-loop because we will be told
            // to add the connection once because we are the source and again
            // because we are the destination.
            final List<Connection> incomingConnections = incomingConnectionsRef.get();
            updatedIncoming = new ArrayList<>(incomingConnections);
            if (!updatedIncoming.contains(connection)) {
                updatedIncoming.add(connection);
            }
        }

        if (connection.getSource().equals(this)) {
            // don't add the connection twice. This may occur if we have a
            // self-loop because we will be told
            // to add the connection once because we are the source and again
            // because we are the destination.
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
    }

    @Override
    public boolean hasIncomingConnection() {
        return !incomingConnectionsRef.get().isEmpty();
    }

    @Override
    public void updateConnection(final Connection connection) throws IllegalStateException {
        if (requireNonNull(connection).getSource().equals(this)) {
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
                    if (connectionsForRelationship != null && connectionsForRelationship.size() == 1 && this.isRunning()
                            && !isAutoTerminated(rel) && getRelationships().contains(rel)) {
                        // if we are running and we do not terminate undefined
                        // relationships and this is the only
                        // connection that defines the given relationship, and
                        // that relationship is required,
                        // then it is not legal to remove this relationship from
                        // this connection.
                        throw new IllegalStateException("Cannot remove relationship " + rel.getName()
                                + " from Connection because doing so would invalidate Processor " + this
                                + ", which is currently running");
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
        }

        if (connection.getDestination().equals(this)) {
            // update our incoming connections -- we can just remove & re-add
            // the connection to update the list.
            final List<Connection> incomingConnections = incomingConnectionsRef.get();
            final List<Connection> updatedIncoming = new ArrayList<>(incomingConnections);
            updatedIncoming.remove(connection);
            updatedIncoming.add(connection);
            incomingConnectionsRef.set(Collections.unmodifiableList(updatedIncoming));
        }
    }

    @Override
    public void removeConnection(final Connection connection) {
        boolean connectionRemoved = false;

        if (requireNonNull(connection).getSource().equals(this)) {
            for (final Relationship relationship : connection.getRelationships()) {
                final Set<Connection> connectionsForRelationship = getConnections(relationship);
                if ((connectionsForRelationship == null || connectionsForRelationship.size() <= 1) && isRunning()) {
                    throw new IllegalStateException(
                            "This connection cannot be removed because its source is running and removing it will invalidate this processor");
                }
            }

            for (final Set<Connection> connectionList : this.connections.values()) {
                connectionList.remove(connection);
            }

            connectionRemoved = (destinations.remove(connection) != null);
        }

        if (connection.getDestination().equals(this)) {
            final List<Connection> incomingConnections = incomingConnectionsRef.get();
            if (incomingConnections.contains(connection)) {
                final List<Connection> updatedIncoming = new ArrayList<>(incomingConnections);
                updatedIncoming.remove(connection);
                incomingConnectionsRef.set(Collections.unmodifiableList(updatedIncoming));
                return;
            }
        }

        if (!connectionRemoved) {
            throw new IllegalArgumentException(
                    "Cannot remove a connection from a ProcessorNode for which the ProcessorNode is not the Source");
        }
    }

    /**
     * @param relationshipName
     *            name
     * @return the relationship for this nodes processor for the given name or
     *         creates a new relationship for the given name
     */
    @Override
    public Relationship getRelationship(final String relationshipName) {
        final Relationship specRel = new Relationship.Builder().name(relationshipName).build();
        Relationship returnRel = specRel;

        final Set<Relationship> relationships;
        final Processor processor = processorRef.get().getProcessor();
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(processor.getClass(), processor.getIdentifier())) {
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
        return processorRef.get().getProcessor();
    }

    @Override
    public synchronized void setProcessor(final LoggableComponent<Processor> processor) {
        if (isRunning()) {
            throw new IllegalStateException("Cannot modify Processor configuration while the Processor is running");
        }

        final ProcessorDetails processorDetails = new ProcessorDetails(processor);
        processorRef.set(processorDetails);
    }

    @Override
    public synchronized void reload(final Set<URL> additionalUrls) throws ProcessorInstantiationException {
        if (isRunning()) {
            throw new IllegalStateException("Cannot reload Processor while the Processor is running");
        }

        getReloadComponent().reload(this, getCanonicalClassName(), getBundleCoordinate(), additionalUrls);
    }

    /**
     * @return the Set of destination processors for all relationships excluding
     *         any destinations that are this processor itself (self-loops)
     */
    public Set<Connectable> getDestinations() {
        final Set<Connectable> nonSelfDestinations = new HashSet<>();
        for (final Connectable connectable : destinations.values()) {
            if (connectable != this) {
                nonSelfDestinations.add(connectable);
            }
        }
        return nonSelfDestinations;
    }

    public Set<Connectable> getDestinations(final Relationship relationship) {
        final Set<Connectable> destinationSet = new HashSet<>();
        final Set<Connection> relationshipConnections = connections.get(relationship);
        if (relationshipConnections != null) {
            for (final Connection connection : relationshipConnections) {
                destinationSet.add(destinations.get(connection));
            }
        }
        return destinationSet;
    }

    public Set<Relationship> getUndefinedRelationships() {
        final Set<Relationship> undefined = new HashSet<>();
        final Set<Relationship> relationships;
        final Processor processor = processorRef.get().getProcessor();
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(processor.getClass(), processor.getIdentifier())) {
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
        return undefined;
    }

    /**
     * Determines if the given node is a destination for this node
     *
     * @param node
     *            node
     * @return true if is a direct destination node; false otherwise
     */
    boolean isRelated(final ProcessorNode node) {
        return this.destinations.containsValue(node);
    }

    @Override
    public boolean isRunning() {
        return getScheduledState().equals(ScheduledState.RUNNING) || processScheduler.getActiveThreadCount(this) > 0;
    }

    @Override
    public int getActiveThreadCount() {
        return processScheduler.getActiveThreadCount(this);
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
        try {
            final ValidationContext validationContext = getValidationContext();
            final Collection<ValidationResult> validationResults = super.validate(validationContext);

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
            LOG.warn("Failed during validation", t);
            return false;
        }
        return true;
    }

    @Override
    public Collection<ValidationResult> getValidationErrors() {
        final List<ValidationResult> results = new ArrayList<>();
        try {
            // Processors may go invalid while RUNNING, but only validating while STOPPED is a trade-off
            // we are willing to make in order to save on validation costs that would be unnecessary most of the time.
            if (getScheduledState() == ScheduledState.STOPPED) {
                final ValidationContext validationContext = getValidationContext();

                final Collection<ValidationResult> validationResults = super.validate(validationContext);

                for (final ValidationResult result : validationResults) {
                    if (!result.isValid()) {
                        results.add(result);
                    }
                }

                for (final Relationship relationship : getUndefinedRelationships()) {
                    if (!isAutoTerminated(relationship)) {
                        final ValidationResult error = new ValidationResult.Builder()
                                .explanation("Relationship '" + relationship.getName()
                                        + "' is not connected to any component and is not auto-terminated")
                                .subject("Relationship " + relationship.getName()).valid(false).build();
                        results.add(error);
                    }
                }

                switch (getInputRequirement()) {
                    case INPUT_ALLOWED:
                        break;
                    case INPUT_FORBIDDEN: {
                        final int incomingConnCount = getIncomingNonLoopConnections().size();
                        if (incomingConnCount != 0) {
                            results.add(new ValidationResult.Builder().explanation(
                                    "Processor does not allow upstream connections but currently has " + incomingConnCount)
                                    .subject("Upstream Connections").valid(false).build());
                        }
                        break;
                    }
                    case INPUT_REQUIRED: {
                        if (getIncomingNonLoopConnections().isEmpty()) {
                            results.add(new ValidationResult.Builder()
                                    .explanation("Processor requires an upstream connection but currently has none")
                                    .subject("Upstream Connections").valid(false).build());
                        }
                        break;
                    }
                }
            }
        } catch (final Throwable t) {
            results.add(new ValidationResult.Builder().explanation("Failed to run validation due to " + t.toString())
                    .valid(false).build());
        }
        return results;
    }

    @Override
    public Requirement getInputRequirement() {
        return processorRef.get().getInputRequirement();
    }

    /**
     * Establishes node equality (based on the processor's identifier)
     *
     * @param other
     *            node
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
        final Processor processor = processorRef.get().getProcessor();
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(processor.getClass(), processor.getIdentifier())) {
            return getProcessor().getRelationships();
        }
    }

    @Override
    public String toString() {
        final Processor processor = processorRef.get().getProcessor();
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(processor.getClass(), processor.getIdentifier())) {
            return getProcessor().toString();
        }
    }

    @Override
    public ProcessGroup getProcessGroup() {
        return processGroup.get();
    }

    @Override
    public synchronized void setProcessGroup(final ProcessGroup group) {
        this.processGroup.set(group);
        invalidateValidationContext();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) {
        final Processor processor = processorRef.get().getProcessor();
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(processor.getClass(), processor.getIdentifier())) {
            processor.onTrigger(context, sessionFactory);
        }
    }

    @Override
    public ConnectableType getConnectableType() {
        return ConnectableType.PROCESSOR;
    }

    @Override
    public void setAnnotationData(final String data) {
        Assert.state(!isRunning(), "Cannot set AnnotationData while processor is running");
        super.setAnnotationData(data);
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
        if (isRunning()) {
            throw new IllegalStateException(this.getIdentifier() + " is running");
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
                    throw new IllegalStateException(this.getIdentifier() + " is the destination of another component");
                }
            }
        }
    }

    @Override
    public void verifyCanStart() {
        this.verifyCanStart(null);
    }

    @Override
    public void verifyCanStart(final Set<ControllerServiceNode> ignoredReferences) {
        final ScheduledState currentState = getPhysicalScheduledState();
        if (currentState != ScheduledState.STOPPED && currentState != ScheduledState.DISABLED) {
            throw new IllegalStateException(this.getIdentifier() + " cannot be started because it is not stopped. Current state is " + currentState.name());
        }

        verifyNoActiveThreads();

        if (ignoredReferences != null) {
            final Set<String> ids = new HashSet<>();
            for (final ControllerServiceNode node : ignoredReferences) {
                ids.add(node.getIdentifier());
            }

            final Collection<ValidationResult> validationResults = getValidationErrors(ids);
            for (final ValidationResult result : validationResults) {
                if (!result.isValid()) {
                    throw new IllegalStateException(this.getIdentifier() + " cannot be started because it is not valid: " + result);
                }
            }
        } else {
            if (!isValid()) {
                throw new IllegalStateException(this.getIdentifier() + " is not in a valid state");
            }
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
        if (isRunning()) {
            throw new IllegalStateException(this.getIdentifier() + " is not stopped");
        }
    }

    @Override
    public void verifyCanEnable() {
        if (getScheduledState() != ScheduledState.DISABLED) {
            throw new IllegalStateException(this.getIdentifier() + " is not disabled");
        }

        verifyNoActiveThreads();
    }

    @Override
    public void verifyCanDisable() {
        if (getScheduledState() != ScheduledState.STOPPED) {
            throw new IllegalStateException(this.getIdentifier() + " is not stopped");
        }
        verifyNoActiveThreads();
    }

    @Override
    public void verifyCanClearState() throws IllegalStateException {
        verifyCanUpdate();
    }

    private void verifyNoActiveThreads() throws IllegalStateException {
        final int threadCount = processScheduler.getActiveThreadCount(this);
        if (threadCount > 0) {
            throw new IllegalStateException(this.getIdentifier() + " has " + threadCount + " threads still active");
        }
    }

    @Override
    public void verifyModifiable() throws IllegalStateException {
        if (isRunning()) {
            throw new IllegalStateException("Cannot modify Processor configuration while the Processor is running");
        }
    }

    /**
     * Will idempotently start the processor using the following sequence: <i>
     * <ul>
     * <li>Validate Processor's state (e.g., PropertyDescriptors,
     * ControllerServices etc.)</li>
     * <li>Transition (atomically) Processor's scheduled state form STOPPED to
     * STARTING. If the above state transition succeeds, then execute the start
     * task (asynchronously) which will be re-tried until @OnScheduled is
     * executed successfully and "schedulingAgentCallback' is invoked, or until
     * STOP operation is initiated on this processor. If state transition fails
     * it means processor is already being started and WARN message will be
     * logged explaining it.</li>
     * </ul>
     * </i>
     * <p>
     * Any exception thrown while invoking operations annotated with @OnSchedule
     * will be caught and logged after which @OnUnscheduled operation will be
     * invoked (quietly) and the start sequence will be repeated (re-try) after
     * delay provided by 'administrativeYieldMillis'.
     * </p>
     * <p>
     * Upon successful completion of start sequence (@OnScheduled -&gt;
     * 'schedulingAgentCallback') the attempt will be made to transition
     * processor's scheduling state to RUNNING at which point processor is
     * considered to be fully started and functioning. If upon successful
     * invocation of @OnScheduled operation the processor can not be
     * transitioned to RUNNING state (e.g., STOP operation was invoked on the
     * processor while it's @OnScheduled operation was executing), the
     * processor's @OnUnscheduled operation will be invoked and its scheduling
     * state will be set to STOPPED at which point the processor is considered
     * to be fully stopped.
     * </p>
     */
    @Override
    public <T extends ProcessContext & ControllerServiceLookup> void start(final ScheduledExecutorService taskScheduler,
            final long administrativeYieldMillis, final T processContext, final SchedulingAgentCallback schedulingAgentCallback) {
        if (!this.isValid()) {
            throw new IllegalStateException( "Processor " + this.getName() + " is not in a valid state due to " + this.getValidationErrors());
        }
        final Processor processor = processorRef.get().getProcessor();
        final ComponentLog procLog = new SimpleProcessLogger(StandardProcessorNode.this.getIdentifier(), processor);

        final boolean starting;
        synchronized (this) {
            starting = this.scheduledState.compareAndSet(ScheduledState.STOPPED, ScheduledState.STARTING);
        }

        if (starting) { // will ensure that the Processor represented by this node can only be started once
            final Runnable startProcRunnable = new Runnable() {
                @Override
                public void run() {
                    try {
                        invokeTaskAsCancelableFuture(schedulingAgentCallback, new Callable<Void>() {
                            @Override
                            public Void call() throws Exception {
                                try (final NarCloseable nc = NarCloseable.withComponentNarLoader(processor.getClass(), processor.getIdentifier())) {
                                    ReflectionUtils.invokeMethodsWithAnnotation(OnScheduled.class, processor, processContext);
                                    return null;
                                }
                            }
                        });

                        if (scheduledState.compareAndSet(ScheduledState.STARTING, ScheduledState.RUNNING)) {
                            schedulingAgentCallback.trigger(); // callback provided by StandardProcessScheduler to essentially initiate component's onTrigger() cycle
                        } else { // can only happen if stopProcessor was called before service was transitioned to RUNNING state
                            try (final NarCloseable nc = NarCloseable.withComponentNarLoader(processor.getClass(), processor.getIdentifier())) {
                                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnUnscheduled.class, processor, processContext);
                            }
                            scheduledState.set(ScheduledState.STOPPED);
                        }
                    } catch (final Exception e) {
                        final Throwable cause = e instanceof InvocationTargetException ? e.getCause() : e;
                        procLog.error("{} failed to invoke @OnScheduled method due to {}; processor will not be scheduled to run for {} seconds",
                                new Object[]{StandardProcessorNode.this.getProcessor(), cause, administrativeYieldMillis / 1000L}, cause);
                        LOG.error("Failed to invoke @OnScheduled method due to {}", cause.toString(), cause);

                        ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnUnscheduled.class, processor, processContext);
                        ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnStopped.class, processor, processContext);

                        if (scheduledState.get() != ScheduledState.STOPPING) { // make sure we only continue retry loop if STOP action wasn't initiated
                            taskScheduler.schedule(this, administrativeYieldMillis, TimeUnit.MILLISECONDS);
                        } else {
                            scheduledState.set(ScheduledState.STOPPED);
                        }
                    }
                }
            };
            taskScheduler.execute(startProcRunnable);
        } else {
            final String procName = processorRef.getClass().getSimpleName();
            LOG.warn("Can not start '" + procName
                    + "' since it's already in the process of being started or it is DISABLED - "
                    + scheduledState.get());
            procLog.warn("Can not start '" + procName
                    + "' since it's already in the process of being started or it is DISABLED - "
                    + scheduledState.get());
        }
    }

    /**
     * Will idempotently stop the processor using the following sequence: <i>
     * <ul>
     * <li>Transition (atomically) Processor's scheduled state from RUNNING to
     * STOPPING. If the above state transition succeeds, then invoke any method
     * on the Processor with the {@link OnUnscheduled} annotation. Once those methods
     * have been called and returned (either normally or exceptionally), start checking
     * to see if all of the Processor's active threads have finished. If not, check again
     * every 100 milliseconds until they have.
     * Once all after threads have completed, the processor's @OnStopped operation will be invoked
     * and its scheduled state is set to STOPPED which completes processor stop
     * sequence.</li>
     * </ul>
     * </i>
     *
     * <p>
     * If for some reason processor's scheduled state can not be transitioned to
     * STOPPING (e.g., the processor didn't finish @OnScheduled operation when
     * stop was called), the attempt will be made to transition processor's
     * scheduled state from STARTING to STOPPING which will allow
     * {@link #start(ScheduledExecutorService, long, ProcessContext, Runnable)}
     * method to initiate processor's shutdown upon exiting @OnScheduled
     * operation, otherwise the processor's scheduled state will remain
     * unchanged ensuring that multiple calls to this method are idempotent.
     * </p>
     */
    @Override
    public <T extends ProcessContext & ControllerServiceLookup> CompletableFuture<Void> stop(final ScheduledExecutorService scheduler,
        final T processContext, final SchedulingAgent schedulingAgent, final ScheduleState scheduleState) {

        final Processor processor = processorRef.get().getProcessor();
        LOG.info("Stopping processor: " + processor.getClass());

        final CompletableFuture<Void> future = new CompletableFuture<>();
        if (this.scheduledState.compareAndSet(ScheduledState.RUNNING, ScheduledState.STOPPING)) { // will ensure that the Processor represented by this node can only be stopped once
            scheduleState.incrementActiveThreadCount();

            // will continue to monitor active threads, invoking OnStopped once there are no
            // active threads (with the exception of the thread performing shutdown operations)
            scheduler.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (scheduleState.isScheduled()) {
                            schedulingAgent.unschedule(StandardProcessorNode.this, scheduleState);
                            try (final NarCloseable nc = NarCloseable.withComponentNarLoader(processor.getClass(), processor.getIdentifier())) {
                                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnUnscheduled.class, processor, processContext);
                            }
                        }

                        // all threads are complete if the active thread count is 1. This is because this thread that is
                        // performing the lifecycle actions counts as 1 thread.
                        final boolean allThreadsComplete = scheduleState.getActiveThreadCount() == 1;
                        if (allThreadsComplete) {
                            try (final NarCloseable nc = NarCloseable.withComponentNarLoader(processor.getClass(), processor.getIdentifier())) {
                                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnStopped.class, processor, processContext);
                            }

                            scheduleState.decrementActiveThreadCount();
                            scheduledState.set(ScheduledState.STOPPED);
                            future.complete(null);
                        } else {
                            // Not all of the active threads have finished. Try again in 100 milliseconds.
                            scheduler.schedule(this, 100, TimeUnit.MILLISECONDS);
                        }
                    } catch (final Exception e) {
                        LOG.warn("Failed while shutting down processor " + processor, e);
                    }
                }
            });
        } else {
            // We do compareAndSet() instead of set() to ensure that Processor
            // stoppage is handled consistently including a condition where
            // Processor never got a chance to transition to RUNNING state
            // before stop() was called. If that happens the stop processor
            // routine will be initiated in start() method, otherwise the IF
            // part will handle the stop processor routine.
            this.scheduledState.compareAndSet(ScheduledState.STARTING, ScheduledState.STOPPING);
            future.complete(null);
        }

        return future;
    }

    /**
     * Will invoke lifecycle operation (OnScheduled or OnUnscheduled)
     * asynchronously to ensure that it could be interrupted if stop action was
     * initiated on the processor that may be infinitely blocking in such
     * operation. While this approach paves the way for further enhancements
     * related to managing processor'slife-cycle operation at the moment the
     * interrupt will not happen automatically. This is primarily to preserve
     * the existing behavior of the NiFi where stop operation can only be
     * invoked once the processor is started. Unfortunately that could mean that
     * the processor may be blocking indefinitely in lifecycle operation
     * (OnScheduled or OnUnscheduled). To deal with that a new NiFi property has
     * been introduced <i>nifi.processor.scheduling.timeout</i> which allows one
     * to set the time (in milliseconds) of how long to wait before canceling
     * such lifecycle operation (OnScheduled or OnUnscheduled) allowing
     * processor's stop sequence to proceed. The default value for this property
     * is {@link Long#MAX_VALUE}.
     * <p>
     * NOTE: Canceling the task does not guarantee that the task will actually
     * completes (successfully or otherwise), since cancellation of the task
     * will issue a simple Thread.interrupt(). However code inside of lifecycle
     * operation (OnScheduled or OnUnscheduled) is written purely and will
     * ignore thread interrupts you may end up with runaway thread which may
     * eventually require NiFi reboot. In any event, the above explanation will
     * be logged (WARN) informing a user so further actions could be taken.
     * </p>
     */
    private <T> void invokeTaskAsCancelableFuture(final SchedulingAgentCallback callback, final Callable<T> task) {
        final Processor processor = processorRef.get().getProcessor();
        final String timeoutString = nifiProperties.getProperty(NiFiProperties.PROCESSOR_SCHEDULING_TIMEOUT);
        final long onScheduleTimeout = timeoutString == null ? 60000
                : FormatUtils.getTimeDuration(timeoutString.trim(), TimeUnit.MILLISECONDS);
        final Future<?> taskFuture = callback.invokeMonitoringTask(task);
        try {
            taskFuture.get(onScheduleTimeout, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            LOG.warn("Thread was interrupted while waiting for processor '" + processor.getClass().getSimpleName()
                    + "' lifecycle OnScheduled operation to finish.");
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while executing one of processor's OnScheduled tasks.", e);
        } catch (final TimeoutException e) {
            taskFuture.cancel(true);
            LOG.warn("Timed out while waiting for OnScheduled of '"
                    + processor.getClass().getSimpleName()
                    + "' processor to finish. An attempt is made to cancel the task via Thread.interrupt(). However it does not "
                    + "guarantee that the task will be canceled since the code inside current OnScheduled operation may "
                + "have been written to ignore interrupts which may result in a runaway thread. This could lead to more issues, "
                    + "eventually requiring NiFi to be restarted. This is usually a bug in the target Processor '"
                    + processor + "' that needs to be documented, reported and eventually fixed.");
            throw new RuntimeException("Timed out while executing one of processor's OnScheduled task.", e);
        } catch (final ExecutionException e){
            throw new RuntimeException("Failed while executing one of processor's OnScheduled task.", e);
        } finally {
            callback.postMonitor();
        }
    }

    @Override
    public String getProcessGroupIdentifier() {
        final ProcessGroup group = getProcessGroup();
        return group == null ? null : group.getIdentifier();
    }

}
