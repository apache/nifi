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
import org.apache.nifi.components.validation.ValidationState;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.scheduling.LifecycleState;
import org.apache.nifi.controller.scheduling.SchedulingAgent;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.tasks.ActiveTask;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.logging.LogRepositoryFactory;
import org.apache.nifi.nar.ExtensionManager;
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
import org.apache.nifi.util.ReflectionUtils;
import org.apache.nifi.util.ThreadUtils;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

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
    private final AtomicReference<List<Connection>> incomingConnections;
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
    private final AtomicReference<String> versionedComponentId = new AtomicReference<>();
    private final ProcessScheduler processScheduler;
    private long runNanos = 0L;
    private volatile long yieldNanos;
    private volatile ScheduledState desiredState = ScheduledState.STOPPED;
    private volatile LogLevel bulletinLevel = LogLevel.WARN;

    private SchedulingStrategy schedulingStrategy; // guarded by read/write lock
                                                   // ??????? NOT any more
    private ExecutionNode executionNode;
    private final Map<Thread, ActiveTask> activeThreads = new HashMap<>(48);
    private final int hashCode;
    private volatile boolean hasActiveThreads = false;

    public StandardProcessorNode(final LoggableComponent<Processor> processor, final String uuid,
                                 final ValidationContextFactory validationContextFactory, final ProcessScheduler scheduler,
                                 final ControllerServiceProvider controllerServiceProvider, final ComponentVariableRegistry variableRegistry,
                                 final ReloadComponent reloadComponent, final ExtensionManager extensionManager, final ValidationTrigger validationTrigger) {

        this(processor, uuid, validationContextFactory, scheduler, controllerServiceProvider, processor.getComponent().getClass().getSimpleName(),
            processor.getComponent().getClass().getCanonicalName(), variableRegistry, reloadComponent, extensionManager, validationTrigger, false);
    }

    public StandardProcessorNode(final LoggableComponent<Processor> processor, final String uuid,
                                 final ValidationContextFactory validationContextFactory, final ProcessScheduler scheduler,
                                 final ControllerServiceProvider controllerServiceProvider,
                                 final String componentType, final String componentCanonicalClass, final ComponentVariableRegistry variableRegistry,
                                 final ReloadComponent reloadComponent, final ExtensionManager extensionManager, final ValidationTrigger validationTrigger,
                                 final boolean isExtensionMissing) {

        super(uuid, validationContextFactory, controllerServiceProvider, componentType, componentCanonicalClass, variableRegistry, reloadComponent,
                extensionManager, validationTrigger, isExtensionMissing);

        final ProcessorDetails processorDetails = new ProcessorDetails(processor);
        this.processorRef = new AtomicReference<>(processorDetails);

        identifier = new AtomicReference<>(uuid);
        destinations = new HashMap<>();
        connections = new HashMap<>();
        incomingConnections = new AtomicReference<>(new ArrayList<>());
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

        schedulingStrategy = SchedulingStrategy.TIMER_DRIVEN;
        executionNode = isExecutionNodeRestricted() ? ExecutionNode.PRIMARY : ExecutionNode.ALL;
        this.hashCode = new HashCodeBuilder(7, 67).append(identifier).toHashCode();

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
    public TerminationAwareLogger getLogger() {
        return processorRef.get().getComponentLog();
    }

    @Override
    public Object getRunnableComponent() {
        return getProcessor();
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
    public Class<?> getComponentClass() {
        return getProcessor().getClass();
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
    @SuppressWarnings("deprecation")
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
    public boolean isSessionBatchingSupported() {
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
     *  Indicates whether the processor's executionNode configuration is restricted to run only in primary node
     */
    @Override
    public boolean isExecutionNodeRestricted(){
        return processorRef.get().isExecutionNodeRestricted();
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
        LOG.debug("Resetting Validation State of {} due to setting auto-terminated relationships", this);
        resetValidationState();
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
     * @param timeUnit determines the unit of time to represent the scheduling period.
     * @return the schedule period that should elapse before subsequent cycles of this processor's tasks
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
    @SuppressWarnings("deprecation")
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
        if (this.isExecutionNodeRestricted()) {
            this.executionNode = ExecutionNode.PRIMARY;
        } else {
            this.executionNode = executionNode;
        }
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
     * {@link #setYieldPeriod(String)}.
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

        final long penalizationMillis = FormatUtils.getTimeDuration(requireNonNull(penalizationPeriod), TimeUnit.MILLISECONDS);
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
            throw new IllegalArgumentException("Cannot set Concurrent Tasks to " + taskCount + " for component "
                    + getIdentifier() + " because Scheduling Strategy is not Event Driven");
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
     * @return the number of tasks that may execute concurrently for this processor
     */
    @Override
    public int getMaxConcurrentTasks() {
        return concurrentTaskCount.get();
    }

    @Override
    public LogLevel getBulletinLevel() {
        return bulletinLevel;
    }

    @Override
    public synchronized void setBulletinLevel(final LogLevel level) {
        LogRepositoryFactory.getRepository(getIdentifier()).setObservationLevel(BULLETIN_OBSERVER_ID, level);
        this.bulletinLevel = level;
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
        return incomingConnections.get();
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

        try {
            List<Connection> updatedIncoming = null;
            if (connection.getDestination().equals(this)) {
                // don't add the connection twice. This may occur if we have a
                // self-loop because we will be told
                // to add the connection once because we are the source and again
                // because we are the destination.
                final List<Connection> incomingConnections = getIncomingConnections();
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
                setIncomingConnections(Collections.unmodifiableList(updatedIncoming));
            }
        } finally {
            LOG.debug("Resetting Validation State of {} due to connection added", this);
            resetValidationState();
        }
    }

    @Override
    public boolean hasIncomingConnection() {
        return !getIncomingConnections().isEmpty();
    }

    @Override
    public void updateConnection(final Connection connection) throws IllegalStateException {
        try {
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
                final List<Connection> incomingConnections = getIncomingConnections();
                final List<Connection> updatedIncoming = new ArrayList<>(incomingConnections);
                updatedIncoming.remove(connection);
                updatedIncoming.add(connection);
                setIncomingConnections(Collections.unmodifiableList(updatedIncoming));
            }
        } finally {
            // need to perform validation in case selected relationships were changed.
            LOG.debug("Resetting Validation State of {} due to updating connection", this);
            resetValidationState();
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
            final List<Connection> incomingConnections = getIncomingConnections();
            if (incomingConnections.contains(connection)) {
                final List<Connection> updatedIncoming = new ArrayList<>(incomingConnections);
                updatedIncoming.remove(connection);
                setIncomingConnections(Collections.unmodifiableList(updatedIncoming));
                return;
            }
        }

        if (!connectionRemoved) {
            throw new IllegalArgumentException(
                    "Cannot remove a connection from a ProcessorNode for which the ProcessorNode is not the Source");
        }

        LOG.debug("Resetting Validation State of {} due to connection removed", this);
        resetValidationState();
    }

    private void setIncomingConnections(final List<Connection> incoming) {
        this.incomingConnections.set(incoming);
        LOG.debug("Resetting Validation State of {} due to setting incoming connections", this);
        resetValidationState();
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
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(getExtensionManager(), processor.getClass(), processor.getIdentifier())) {
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
        String additionalResourcesFingerprint = ClassLoaderUtils.generateAdditionalUrlsFingerprint(additionalUrls);
        setAdditionalResourcesFingerprint(additionalResourcesFingerprint);
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
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(getExtensionManager(), processor.getClass(), processor.getIdentifier())) {
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
        return getScheduledState().equals(ScheduledState.RUNNING) || hasActiveThreads;
    }

    @Override
    public boolean isValidationNecessary() {
        switch (getScheduledState()) {
            case STOPPED:
            case STOPPING:
                return true;
        }

        return false;
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
    public Collection<ValidationResult> getValidationErrors() {
        final ValidationState validationState = getValidationState();
        return validationState.getValidationErrors();
    }

    @Override
    protected Collection<ValidationResult> computeValidationErrors(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();
        try {
            final Collection<ValidationResult> validationResults = super.computeValidationErrors(validationContext);

            validationResults.stream()
                .filter(result -> !result.isValid())
                .forEach(results::add);

            // Ensure that any relationships that don't have a connection defined are auto-terminated
            for (final Relationship relationship : getUndefinedRelationships()) {
                if (!isAutoTerminated(relationship)) {
                    final ValidationResult error = new ValidationResult.Builder()
                        .explanation("Relationship '" + relationship.getName()
                            + "' is not connected to any component and is not auto-terminated")
                        .subject("Relationship " + relationship.getName()).valid(false).build();
                    results.add(error);
                }
            }

            // Ensure that the requirements of the InputRequirement are met.
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
        } catch (final Throwable t) {
            LOG.error("Failed to perform validation", t);
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
        return hashCode;
    }

    @Override
    public Collection<Relationship> getRelationships() {
        final Processor processor = processorRef.get().getProcessor();
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(getExtensionManager(), processor.getClass(), processor.getIdentifier())) {
            return getProcessor().getRelationships();
        }
    }

    @Override
    public String toString() {
        final Processor processor = processorRef.get().getProcessor();
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(getExtensionManager(), processor.getClass(), processor.getIdentifier())) {
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
        LOG.debug("Resetting Validation State of {} due to setting process group", this);
        resetValidationState();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) {
        final Processor processor = processorRef.get().getProcessor();

        activateThread();
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(getExtensionManager(), processor.getClass(), processor.getIdentifier())) {
            processor.onTrigger(context, sessionFactory);
        } finally {
            deactivateThread();
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

            for (final Connection connection : getIncomingConnections()) {
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

        switch (getValidationStatus()) {
            case VALID:
                return;
            case VALIDATING:
                throw new IllegalStateException("Processor with ID " + getIdentifier() + " cannot be started because its validation is still being performed");
        }

        final Collection<ValidationResult> validationErrors = getValidationErrors(ignoredReferences);
        if (ignoredReferences != null && !validationErrors.isEmpty()) {
            throw new IllegalStateException("Processor with ID " + getIdentifier() + " cannot be started because it is not currently valid");
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
        if (hasActiveThreads) {
            final int threadCount = getActiveThreadCount();
            if (threadCount > 0) {
                throw new IllegalStateException(this.getIdentifier() + " has " + threadCount + " threads still active");
            }
        }
    }

    @Override
    public void verifyModifiable() throws IllegalStateException {
        if (isRunning()) {
            throw new IllegalStateException("Cannot modify Processor configuration while the Processor is running");
        }
    }

    @Override
    public void enable() {
        desiredState = ScheduledState.STOPPED;
        final boolean updated = scheduledState.compareAndSet(ScheduledState.DISABLED, ScheduledState.STOPPED);

        if (updated) {
            LOG.info("{} enabled so ScheduledState transitioned from DISABLED to STOPPED", this);
        } else {
            LOG.info("{} enabled but not currently DISABLED so set desired state to STOPPED; current state is {}", this, scheduledState.get());
        }
    }

    @Override
    public void disable() {
        desiredState = ScheduledState.DISABLED;
        final boolean updated = scheduledState.compareAndSet(ScheduledState.STOPPED, ScheduledState.DISABLED);

        if (updated) {
            LOG.info("{} disabled so ScheduledState transitioned from STOPPED to DISABLED", this);
        } else {
            LOG.info("{} disabled but not currently STOPPED so set desired state to DISABLED; current state is {}", this, scheduledState.get());
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
    public void start(final ScheduledExecutorService taskScheduler, final long administrativeYieldMillis, final long timeoutMillis, final ProcessContext processContext,
            final SchedulingAgentCallback schedulingAgentCallback, final boolean failIfStopping) {

        final Processor processor = processorRef.get().getProcessor();
        final ComponentLog procLog = new SimpleProcessLogger(StandardProcessorNode.this.getIdentifier(), processor);

        ScheduledState currentState;
        boolean starting;
        synchronized (this) {
            currentState = this.scheduledState.get();

            if (currentState == ScheduledState.STOPPED) {
                starting = this.scheduledState.compareAndSet(ScheduledState.STOPPED, ScheduledState.STARTING);
                if (starting) {
                    desiredState = ScheduledState.RUNNING;
                }
            } else if (currentState == ScheduledState.STOPPING && !failIfStopping) {
                desiredState = ScheduledState.RUNNING;
                return;
            } else {
                starting = false;
            }
        }

        if (starting) { // will ensure that the Processor represented by this node can only be started once
            hasActiveThreads = true;
            initiateStart(taskScheduler, administrativeYieldMillis, timeoutMillis, processContext, schedulingAgentCallback);
        } else {
            final String procName = processorRef.get().toString();
            LOG.warn("Cannot start {} because it is not currently stopped. Current state is {}", procName, currentState);
            procLog.warn("Cannot start {} because it is not currently stopped. Current state is {}", new Object[] {procName, currentState});
        }
    }

    private synchronized void activateThread() {
        final Thread thread = Thread.currentThread();
        final Long timestamp = System.currentTimeMillis();
        activeThreads.put(thread, new ActiveTask(timestamp));
    }

    private synchronized void deactivateThread() {
        activeThreads.remove(Thread.currentThread());
    }

    @Override
    public synchronized List<ActiveThreadInfo> getActiveThreads() {
        final long now = System.currentTimeMillis();
        final ThreadMXBean mbean = ManagementFactory.getThreadMXBean();
        final ThreadInfo[] infos = mbean.dumpAllThreads(true, true);
        final long[] deadlockedThreadIds = mbean.findDeadlockedThreads();
        final long[] monitorDeadlockThreadIds = mbean.findMonitorDeadlockedThreads();

        final Map<Long, ThreadInfo> threadInfoMap = Stream.of(infos)
            .collect(Collectors.toMap(info -> info.getThreadId(), Function.identity(), (a, b) -> a));

        final List<ActiveThreadInfo> threadList = new ArrayList<>(activeThreads.size());
        for (final Map.Entry<Thread, ActiveTask> entry : activeThreads.entrySet()) {
            final Thread thread = entry.getKey();
            final ActiveTask activeTask = entry.getValue();
            final Long timestamp = activeTask.getStartTime();
            final long activeMillis = now - timestamp;
            final ThreadInfo threadInfo = threadInfoMap.get(thread.getId());

            final String stackTrace = ThreadUtils.createStackTrace(thread, threadInfo, deadlockedThreadIds, monitorDeadlockThreadIds, activeMillis);

            final ActiveThreadInfo activeThreadInfo = new ActiveThreadInfo(thread.getName(), stackTrace, activeMillis, activeTask.isTerminated());
            threadList.add(activeThreadInfo);
        }

        return threadList;
    }

    @Override
    public synchronized int getTerminatedThreadCount() {
        int count = 0;
        for (final ActiveTask task : activeThreads.values()) {
            if (task.isTerminated()) {
                count++;
            }
        }

        return count;
    }


    @Override
    public int terminate() {
        verifyCanTerminate();

        int count = 0;
        for (final Map.Entry<Thread, ActiveTask> entry : activeThreads.entrySet()) {
            final Thread thread = entry.getKey();
            final ActiveTask activeTask = entry.getValue();

            if (!activeTask.isTerminated()) {
                activeTask.terminate();

                thread.setName(thread.getName() + " <Terminated Task>");
                count++;
            }

            thread.interrupt();
        }

        getLogger().terminate();
        scheduledState.set(ScheduledState.STOPPED);
        hasActiveThreads = false;

        return count;
    }

    @Override
    public boolean isTerminated(final Thread thread) {
        final ActiveTask activeTask = activeThreads.get(thread);
        if (activeTask == null) {
            return false;
        }

        return activeTask.isTerminated();
    }

    @Override
    public void verifyCanTerminate() {
        if (getScheduledState() != ScheduledState.STOPPED) {
            throw new IllegalStateException("Processor is not stopped");
        }
    }


    private void initiateStart(final ScheduledExecutorService taskScheduler, final long administrativeYieldMillis, final long timeoutMilis,
            final ProcessContext processContext, final SchedulingAgentCallback schedulingAgentCallback) {

        final Processor processor = getProcessor();
        final ComponentLog procLog = new SimpleProcessLogger(StandardProcessorNode.this.getIdentifier(), processor);

        // Completion Timestamp is set to MAX_VALUE because we don't want to timeout until the task has a chance to run.
        final AtomicLong completionTimestampRef = new AtomicLong(Long.MAX_VALUE);

        // Create a task to invoke the @OnScheduled annotation of the processor
        final Callable<Void> startupTask = () -> {
            final ScheduledState currentScheduleState = scheduledState.get();
            if (currentScheduleState == ScheduledState.STOPPING || currentScheduleState == ScheduledState.STOPPED) {
                LOG.debug("{} is stopped. Will not call @OnScheduled lifecycle methods or begin trigger onTrigger() method", StandardProcessorNode.this);
                schedulingAgentCallback.onTaskComplete();
                scheduledState.set(ScheduledState.STOPPED);
                return null;
            }

            final ValidationStatus validationStatus = getValidationStatus();
            if (validationStatus != ValidationStatus.VALID) {
                LOG.debug("Cannot start {} because Processor is currently not valid; will try again after 5 seconds", StandardProcessorNode.this);

                // re-initiate the entire process
                final Runnable initiateStartTask = () -> initiateStart(taskScheduler, administrativeYieldMillis, timeoutMilis, processContext, schedulingAgentCallback);
                taskScheduler.schedule(initiateStartTask, 5, TimeUnit.SECONDS);

                schedulingAgentCallback.onTaskComplete();
                return null;
            }

            LOG.debug("Invoking @OnScheduled methods of {}", processor);

            // Now that the task has been scheduled, set the timeout
            completionTimestampRef.set(System.currentTimeMillis() + timeoutMilis);

            try (final NarCloseable nc = NarCloseable.withComponentNarLoader(getExtensionManager(), processor.getClass(), processor.getIdentifier())) {
                try {
                    activateThread();
                    try {
                        ReflectionUtils.invokeMethodsWithAnnotation(OnScheduled.class, processor, processContext);
                    } finally {
                        deactivateThread();
                    }

                    if (desiredState == ScheduledState.RUNNING && scheduledState.compareAndSet(ScheduledState.STARTING, ScheduledState.RUNNING)) {
                        LOG.debug("Successfully completed the @OnScheduled methods of {}; will now start triggering processor to run", processor);
                        schedulingAgentCallback.trigger(); // callback provided by StandardProcessScheduler to essentially initiate component's onTrigger() cycle
                    } else {
                        LOG.info("Successfully invoked @OnScheduled methods of {} but scheduled state is no longer STARTING so will stop processor now; current state = {}, desired state = {}",
                            processor, scheduledState.get(), desiredState);

                        // can only happen if stopProcessor was called before service was transitioned to RUNNING state
                        activateThread();
                        try {
                            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnUnscheduled.class, processor, processContext);
                            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnStopped.class, processor, processContext);
                            hasActiveThreads = false;
                        } finally {
                            deactivateThread();
                        }

                        scheduledState.set(ScheduledState.STOPPED);

                        if (desiredState == ScheduledState.DISABLED) {
                            final boolean disabled = scheduledState.compareAndSet(ScheduledState.STOPPED, ScheduledState.DISABLED);
                            if (disabled) {
                                LOG.info("After stopping {}, determined that Desired State is DISABLED so disabled processor", processor);
                            }
                        }
                    }
                } finally {
                    schedulingAgentCallback.onTaskComplete();
                }
            } catch (final Exception e) {
                procLog.error("Failed to properly initialize Processor. If still scheduled to run, NiFi will attempt to "
                    + "initialize and run the Processor again after the 'Administrative Yield Duration' has elapsed. Failure is due to " + e, e);

                // If processor's task completed Exceptionally, then we want to retry initiating the start (if Processor is still scheduled to run).
                try (final NarCloseable nc = NarCloseable.withComponentNarLoader(getExtensionManager(), processor.getClass(), processor.getIdentifier())) {
                    activateThread();
                    try {
                        ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnUnscheduled.class, processor, processContext);
                        ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnStopped.class, processor, processContext);
                        hasActiveThreads = false;
                    } finally {
                        deactivateThread();
                    }
                }

                // make sure we only continue retry loop if STOP action wasn't initiated
                if (scheduledState.get() != ScheduledState.STOPPING) {
                    // re-initiate the entire process
                    final Runnable initiateStartTask = () -> initiateStart(taskScheduler, administrativeYieldMillis, timeoutMilis, processContext, schedulingAgentCallback);
                    taskScheduler.schedule(initiateStartTask, administrativeYieldMillis, TimeUnit.MILLISECONDS);
                } else {
                    scheduledState.set(ScheduledState.STOPPED);
                }
            }

            return null;
        };

        // Trigger the task in a background thread.
        final Future<?> taskFuture = schedulingAgentCallback.scheduleTask(startupTask);

        // Trigger a task periodically to check if @OnScheduled task completed. Once it has,
        // this task will call SchedulingAgentCallback#onTaskComplete.
        // However, if the task times out, we need to be able to cancel the monitoring. So, in order
        // to do this, we use #scheduleWithFixedDelay and then make that Future available to the task
        // itself by placing it into an AtomicReference.
        final AtomicReference<Future<?>> futureRef = new AtomicReference<>();
        final Runnable monitoringTask = new Runnable() {
            @Override
            public void run() {
                Future<?> monitoringFuture = futureRef.get();
                if (monitoringFuture == null) { // Future is not yet available. Just return and wait for the next invocation.
                    return;
                }

                monitorAsyncTask(taskFuture, monitoringFuture, completionTimestampRef.get());
            }
        };

        final Future<?> future = taskScheduler.scheduleWithFixedDelay(monitoringTask, 1, 10, TimeUnit.MILLISECONDS);
        futureRef.set(future);
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
     * {@link #start(ScheduledExecutorService, long, long, ProcessContext, SchedulingAgentCallback, boolean)}
     * method to initiate processor's shutdown upon exiting @OnScheduled
     * operation, otherwise the processor's scheduled state will remain
     * unchanged ensuring that multiple calls to this method are idempotent.
     * </p>
     */
    @Override
    public CompletableFuture<Void> stop(final ProcessScheduler processScheduler, final ScheduledExecutorService executor, final ProcessContext processContext,
            final SchedulingAgent schedulingAgent, final LifecycleState scheduleState) {

        final Processor processor = processorRef.get().getProcessor();
        LOG.info("Stopping processor: " + processor.getClass());
        desiredState = ScheduledState.STOPPED;

        final CompletableFuture<Void> future = new CompletableFuture<>();
        if (this.scheduledState.compareAndSet(ScheduledState.RUNNING, ScheduledState.STOPPING)) { // will ensure that the Processor represented by this node can only be stopped once
            scheduleState.incrementActiveThreadCount(null);

            // will continue to monitor active threads, invoking OnStopped once there are no
            // active threads (with the exception of the thread performing shutdown operations)
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (scheduleState.isScheduled()) {
                            schedulingAgent.unschedule(StandardProcessorNode.this, scheduleState);

                            activateThread();
                            try (final NarCloseable nc = NarCloseable.withComponentNarLoader(getExtensionManager(), processor.getClass(), processor.getIdentifier())) {
                                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnUnscheduled.class, processor, processContext);
                            } finally {
                                deactivateThread();
                            }
                        }

                        // all threads are complete if the active thread count is 1. This is because this thread that is
                        // performing the lifecycle actions counts as 1 thread.
                        final boolean allThreadsComplete = scheduleState.getActiveThreadCount() == 1;
                        if (allThreadsComplete) {
                            activateThread();
                            try (final NarCloseable nc = NarCloseable.withComponentNarLoader(getExtensionManager(), processor.getClass(), processor.getIdentifier())) {
                                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnStopped.class, processor, processContext);
                            } finally {
                                deactivateThread();
                            }

                            scheduleState.decrementActiveThreadCount(null);
                            hasActiveThreads = false;
                            scheduledState.set(ScheduledState.STOPPED);
                            future.complete(null);

                            // This can happen only when we join a cluster. In such a case, we can inherit a flow from the cluster that says that
                            // the Processor is to be running. However, if the Processor is already in the process of stopping, we cannot immediately
                            // start running the Processor. As a result, we check here, since the Processor is stopped, and then immediately start the
                            // Processor if need be.
                            final ScheduledState desired = StandardProcessorNode.this.desiredState;
                            if (desired == ScheduledState.RUNNING) {
                                LOG.info("Finished stopping {} but desired state is now RUNNING so will start processor", this);
                                processScheduler.startProcessor(StandardProcessorNode.this, true);
                            } else if (desired == ScheduledState.DISABLED) {
                                final boolean updated = scheduledState.compareAndSet(ScheduledState.STOPPED, ScheduledState.DISABLED);

                                if (updated) {
                                    LOG.info("Finished stopping {} but desired state is now DISABLED so disabled processor", this);
                                } else {
                                    LOG.info("Finished stopping {} but desired state is now DISABLED. Scheduled State could not be transitioned from STOPPED to DISABLED, "
                                        + "though, so will allow the other thread to finish state transition. Current state is {}", this, scheduledState.get());
                                }
                            }
                        } else {
                            // Not all of the active threads have finished. Try again in 100 milliseconds.
                            executor.schedule(this, 100, TimeUnit.MILLISECONDS);
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

    @Override
    public ScheduledState getDesiredState() {
        return desiredState;
    }

    private void monitorAsyncTask(final Future<?> taskFuture, final Future<?> monitoringFuture, final long completionTimestamp) {
        if (taskFuture.isDone()) {
            monitoringFuture.cancel(false); // stop scheduling this task
        } else if (System.currentTimeMillis() > completionTimestamp) {
            // Task timed out. Request an interrupt of the processor task
            taskFuture.cancel(true);

            // Stop monitoring the processor. We have interrupted the thread so that's all we can do. If the processor responds to the interrupt, then
            // it will be re-scheduled. If it does not, then it will either keep the thread indefinitely or eventually finish, at which point
            // the Processor will begin running.
            monitoringFuture.cancel(false);

            final Processor processor = processorRef.get().getProcessor();
            LOG.warn("Timed out while waiting for OnScheduled of "
                + processor + " to finish. An attempt is made to cancel the task via Thread.interrupt(). However it does not "
                + "guarantee that the task will be canceled since the code inside current OnScheduled operation may "
                + "have been written to ignore interrupts which may result in a runaway thread. This could lead to more issues, "
                + "eventually requiring NiFi to be restarted. This is usually a bug in the target Processor '"
                + processor + "' that needs to be documented, reported and eventually fixed.");
        }
    }

    @Override
    public String getProcessGroupIdentifier() {
        final ProcessGroup group = getProcessGroup();
        return group == null ? null : group.getIdentifier();
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
