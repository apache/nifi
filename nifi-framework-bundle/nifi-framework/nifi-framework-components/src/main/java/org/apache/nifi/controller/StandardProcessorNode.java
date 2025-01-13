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

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.TriggerWhenAnyDestinationAvailable;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.DeprecationNotice;
import org.apache.nifi.annotation.lifecycle.OnConfigurationRestored;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.annotation.notification.OnPrimaryNodeStateChange;
import org.apache.nifi.annotation.notification.PrimaryNodeState;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
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
import org.apache.nifi.logging.StandardLoggingContext;
import org.apache.nifi.migration.ControllerServiceCreationDetails;
import org.apache.nifi.migration.ControllerServiceFactory;
import org.apache.nifi.migration.StandardPropertyConfiguration;
import org.apache.nifi.migration.StandardRelationshipConfiguration;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.InstanceClassLoader;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.parameter.ExpressionLanguageAgnosticParameterParser;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.parameter.ParameterParser;
import org.apache.nifi.parameter.ParameterReference;
import org.apache.nifi.parameter.ParameterTokenList;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.SimpleProcessLogger;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.CharacterFilterUtils;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.ReflectionUtils;
import org.apache.nifi.util.ThreadUtils;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.support.CronExpression;

import java.lang.management.ThreadInfo;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * ProcessorNode provides thread-safe access to a FlowFileProcessor as it exists
 * within a controlled flow. This node keeps track of the processor, its
 * scheduling information and its relationships to other processors and whatever
 * scheduled futures exist for it. Must be thread safe.
 *
 */
public class StandardProcessorNode extends ProcessorNode implements Connectable {

    private static final Logger LOG = LoggerFactory.getLogger(StandardProcessorNode.class);


    public static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;
    public static final String DEFAULT_YIELD_PERIOD = "1 sec";
    public static final String DEFAULT_PENALIZATION_PERIOD = "30 sec";
    private static final String RUN_SCHEDULE = "Run Schedule";

    private final AtomicReference<ProcessGroup> processGroup;
    private final AtomicReference<ProcessorDetails> processorRef;
    private final String identifier;
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
    private volatile List<ParameterReference> parameterReferences = Collections.emptyList();
    private final AtomicReference<List<CompletableFuture<Void>>> stopFutures = new AtomicReference<>(new ArrayList<>());

    private SchedulingStrategy schedulingStrategy; // guarded by synchronized keyword
    private ExecutionNode executionNode;
    private final Map<Thread, ActiveTask> activeThreads = new ConcurrentHashMap<>(48);
    private final int hashCode;
    private volatile boolean hasActiveThreads = false;

    private volatile int retryCount;
    private volatile Set<String> retriedRelationships;
    private volatile BackoffMechanism backoffMechanism;
    private volatile String maxBackoffPeriod;

    public StandardProcessorNode(final LoggableComponent<Processor> processor, final String uuid,
                                 final ValidationContextFactory validationContextFactory, final ProcessScheduler scheduler,
                                 final ControllerServiceProvider controllerServiceProvider, final ReloadComponent reloadComponent,
                                 final ExtensionManager extensionManager, final ValidationTrigger validationTrigger) {

        this(processor, uuid, validationContextFactory, scheduler, controllerServiceProvider, processor.getComponent().getClass().getSimpleName(),
            processor.getComponent().getClass().getCanonicalName(), reloadComponent, extensionManager, validationTrigger, false);
    }

    public StandardProcessorNode(final LoggableComponent<Processor> processor, final String uuid,
                                 final ValidationContextFactory validationContextFactory, final ProcessScheduler scheduler,
                                 final ControllerServiceProvider controllerServiceProvider, final String componentType, final String componentCanonicalClass,
                                 final ReloadComponent reloadComponent, final ExtensionManager extensionManager, final ValidationTrigger validationTrigger,
                                 final boolean isExtensionMissing) {

        super(uuid, validationContextFactory, controllerServiceProvider, componentType, componentCanonicalClass, reloadComponent,
                extensionManager, validationTrigger, isExtensionMissing);

        final ProcessorDetails processorDetails = new ProcessorDetails(processor);
        this.processorRef = new AtomicReference<>(processorDetails);

        identifier = uuid;
        destinations = new ConcurrentHashMap<>();
        connections = new ConcurrentHashMap<>();
        incomingConnections = new AtomicReference<>(new ArrayList<>());
        lossTolerant = new AtomicBoolean(false);
        undefinedRelationshipsToTerminate = new AtomicReference<>(Collections.emptySet());
        comments = new AtomicReference<>("");
        schedulingPeriod = new AtomicReference<>("0 sec");
        schedulingNanos = new AtomicLong(MINIMUM_SCHEDULING_NANOS);
        yieldPeriod = new AtomicReference<>(DEFAULT_YIELD_PERIOD);
        yieldNanos = Math.round(FormatUtils.getPreciseTimeDuration(DEFAULT_YIELD_PERIOD, TimeUnit.NANOSECONDS));
        yieldExpiration = new AtomicLong(0L);
        concurrentTaskCount = new AtomicInteger(1);
        position = new AtomicReference<>(new Position(0D, 0D));
        style = new AtomicReference<>(Map.of());
        this.processGroup = new AtomicReference<>();
        processScheduler = scheduler;
        penalizationPeriod = new AtomicReference<>(DEFAULT_PENALIZATION_PERIOD);

        schedulingStrategy = SchedulingStrategy.TIMER_DRIVEN;
        executionNode = isExecutionNodeRestricted() ? ExecutionNode.PRIMARY : ExecutionNode.ALL;
        this.hashCode = new HashCodeBuilder(7, 67).append(identifier).toHashCode();

        retryCount = DEFAULT_RETRY_COUNT;
        retriedRelationships = new HashSet<>();
        backoffMechanism = DEFAULT_BACKOFF_MECHANISM;
        maxBackoffPeriod = DEFAULT_MAX_BACKOFF_PERIOD;

        try {
            if (processorDetails.getProcClass().isAnnotationPresent(DefaultSchedule.class)) {
                DefaultSchedule dsc = processorDetails.getProcClass().getAnnotation(DefaultSchedule.class);
                setSchedulingStrategy(dsc.strategy());
                setSchedulingPeriod(dsc.period());
                setMaxConcurrentTasks(dsc.concurrentTasks());
            }
        } catch (final Exception e) {
            LOG.error("Error while setting default schedule from DefaultSchedule annotation", e);
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
    public void setStyle(final Map<String, String> style) {
        if (style != null) {
            this.style.set(Map.copyOf(style));
        }
    }

    @Override
    public String getIdentifier() {
        return identifier;
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
        return executionNode == ExecutionNode.PRIMARY;
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
    public boolean isExecutionNodeRestricted() {
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
            throw new IllegalStateException("Cannot modify configuration of " + this + " while the Processor is running");
        }
        this.lossTolerant.set(lossTolerant);
    }

    @Override
    public boolean isAutoTerminated(final Relationship relationship) {
        final boolean markedAutoTerminate = relationship.isAutoTerminated() || undefinedRelationshipsToTerminate.get().contains(relationship);
        return markedAutoTerminate && getConnections(relationship).isEmpty();
    }

    @Override
    public void setAutoTerminatedRelationships(final Set<Relationship> terminate) {
        if (isRunning()) {
            throw new IllegalStateException("Cannot modify configuration of " + this + " while the Processor is running");
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
    public synchronized void setSchedulingPeriod(final String schedulingPeriod) {
        if (isRunning()) {
            throw new IllegalStateException("Cannot modify configuration of " + this + " while the Processor is running");
        }

        //Before setting the new Configuration references, we need to remove the current ones from reference counts.
        parameterReferences.forEach(parameterReference -> decrementReferenceCounts(parameterReference.getParameterName()));

        //Setting the new Configuration references.
        final ParameterParser parameterParser = new ExpressionLanguageAgnosticParameterParser();
        final ParameterTokenList parameterTokenList = parameterParser.parseTokens(schedulingPeriod);

        parameterReferences = new ArrayList<>(parameterTokenList.toReferenceList());
        parameterReferences.forEach(parameterReference -> incrementReferenceCounts(parameterReference.getParameterName()));

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
            throw new IllegalArgumentException("Run Duration of " + this + " cannot be set to a negative value; cannot set to "
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
            throw new IllegalStateException("Cannot modify configuration of " + this + " while the Processor is running");
        }
        final long yieldNanos = FormatUtils.getTimeDuration(Objects.requireNonNull(yieldPeriod), TimeUnit.NANOSECONDS);
        if (yieldNanos < 0) {
            throw new IllegalArgumentException("Yield duration of " + this + " cannot be set to a negative value: " + yieldNanos + " nanos");
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
        this.yield(yieldMillis, TimeUnit.MILLISECONDS);

        final String yieldDuration = (yieldMillis > 1000) ? (yieldMillis / 1000) + " seconds" : yieldMillis + " milliseconds";
        LoggerFactory.getLogger(processor.getClass()).trace("{} has chosen to yield its resources; will not be scheduled to run again for {}", processor, yieldDuration);
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
            throw new IllegalStateException("Cannot modify configuration of " + this + " while the Processor is running");
        }

        final long penalizationMillis = FormatUtils.getTimeDuration(Objects.requireNonNull(penalizationPeriod), TimeUnit.MILLISECONDS);
        if (penalizationMillis < 0) {
            throw new IllegalArgumentException("Penalization duration of " + this + " cannot be set to a negative value: " + penalizationMillis + " millis");
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
            throw new IllegalStateException("Cannot modify configuration of " + this + " while the Processor is running");
        }

        if (taskCount < 1) {
            throw new IllegalArgumentException("Cannot set Concurrent Tasks to " + taskCount + " for component " + this);
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
        LogRepositoryFactory.getRepository(getIdentifier()).setObservationLevel(level);
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
        return (applicableConnections == null) ? Collections.emptySet() : Collections.unmodifiableSet(applicableConnections);
    }

    @Override
    public void addConnection(final Connection connection) {
        Objects.requireNonNull(connection, "connection cannot be null");

        if (!connection.getSource().equals(this) && !connection.getDestination().equals(this)) {
            throw new IllegalStateException("Cannot add a connection to " + this + " because the ProcessorNode is neither the Source nor the Destination");
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
                        final Set<Connection> set = connections.computeIfAbsent(rel, k -> new HashSet<>());
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
            if (Objects.requireNonNull(connection).getSource().equals(this)) {
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

                            // if we are running and we do not terminate undefined relationships and this is the only
                            // connection that defines the given relationship, and that relationship is required,
                            // then it is not legal to remove this relationship from this connection.
                            throw new IllegalStateException("Cannot remove relationship " + rel.getName()
                                + " from Connection " + connection + " because doing so would invalidate " + this
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
                    final Set<Connection> set = connections.computeIfAbsent(rel, k -> new HashSet<>());
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

        if (Objects.requireNonNull(connection).getSource().equals(this)) {
            for (final Relationship relationship : connection.getRelationships()) {
                final Set<Connection> connectionsForRelationship = getConnections(relationship);
                if ((connectionsForRelationship == null || connectionsForRelationship.size() <= 1) && isRunning()) {
                    throw new IllegalStateException(connection +  " cannot be removed because its source is running and removing it will invalidate " + this);
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
            throw new IllegalArgumentException("Cannot remove " + connection + " from " + this + " because the ProcessorNode is not the Source");
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
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(getExtensionManager(), processor.getClass(), processor.getIdentifier())) {
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
            throw new IllegalStateException("Cannot modify configuration of " + this + " while the Processor is running");
        }

        final ProcessorDetails processorDetails = new ProcessorDetails(processor);
        processorRef.set(processorDetails);
    }

    @Override
    public synchronized void reload(final Set<URL> additionalUrls) throws ProcessorInstantiationException {
        final String additionalResourcesFingerprint = ClassLoaderUtils.generateAdditionalUrlsFingerprint(additionalUrls, determineClasloaderIsolationKey());
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


    public Set<Relationship> getUndefinedRelationships() {
        final Set<Relationship> undefined = new HashSet<>();
        final Set<Relationship> relationships;
        final Processor processor = processorRef.get().getProcessor();
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(getExtensionManager(), processor.getClass(), processor.getIdentifier())) {
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

    @Override
    public boolean isRunning() {
        final ScheduledState state = getScheduledState();
        return state == ScheduledState.RUNNING || state == ScheduledState.STARTING || hasActiveThreads;
    }

    @Override
    public boolean isValidationNecessary() {
        return switch (getPhysicalScheduledState()) {
            case STOPPED, STOPPING, STARTING -> true;
            default -> false;
        };
    }

    @Override
    public Optional<ProcessGroup> getParentProcessGroup() {
        return Optional.ofNullable(processGroup.get());
    }

    @Override
    public int getActiveThreadCount() {
        final int activeThreadCount = processScheduler.getActiveThreadCount(this);

        // When getScheduledState() is called, we map the 'physical' state of STOPPING to STOPPED. This is done in order to maintain
        // backward compatibility because the UI and other clients will not know of the (relatively newer) 'STOPPING' state.
        // Because of there previously was no STOPPING state, the way to determine of a processor had truly stopped was to check if its
        // Scheduled State was STOPPED AND it had no active threads.
        //
        // Also, we can have a situation in which a processor is started while invalid. Before the processor becomes valid, it can be stopped.
        // In this situation, the processor state will become STOPPING until the background thread checks the state, calls any necessary lifecycle methods,
        // and finally updates the state to STOPPED. In the interim, we have a situation where a call to getScheduledState() returns STOPPED and there are no
        // active threads, which the client will interpret as the processor being fully stopped. However, in this situation, an attempt to update the processor, etc.
        // will fail because the processor is not truly fully stopped.
        //
        // To prevent this situation, we return 1 for the number of active tasks when the processor is considered STOPPING. In doing this, we ensure that the condition
        // of (getScheduledState() == STOPPED and activeThreads == 0) never happens while the processor is still stopping.
        //
        // This probably is calling for a significant refactoring / rethinking of this class. It would make sense, for example, to extract some of the logic into a separate
        // StateTransition class as we've done with Controller Services. That would at least more cleanly encapsulate this logic. However, this is a simple enough work around for the time being.
        if (activeThreadCount == 0 && getPhysicalScheduledState() == ScheduledState.STOPPING) {
            return 1;
        }

        return activeThreadCount;
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
    public List<ConfigVerificationResult> verifyConfiguration(final ProcessContext context, final ComponentLog logger, final Map<String, String> attributes, final ExtensionManager extensionManager) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        try {
            verifyCanPerformVerification();

            final long startNanos = System.nanoTime();
            // Call super's verifyConfig, which will perform component validation
            results.addAll(super.verifyConfig(context.getProperties(), context.getAnnotationData(), getProcessGroup().getParameterContext()));
            final long validationComplete = System.nanoTime();

            // If any invalid outcomes from validation, we do not want to perform additional verification, because we only run additional verification when the component is valid.
            // This is done in order to make it much simpler to develop these verifications, since the developer doesn't have to worry about whether or not the given values are valid.
            if (!results.isEmpty() && results.stream().anyMatch(result -> result.getOutcome() == Outcome.FAILED)) {
                return results;
            }

            final Processor processor = getProcessor();
            if (processor instanceof VerifiableProcessor) {
                LOG.debug("{} is a VerifiableProcessor. Will perform full verification of configuration.", this);
                final VerifiableProcessor verifiable = (VerifiableProcessor) getProcessor();

                // Check if the given configuration requires a different classloader than the current configuration
                final boolean classpathDifferent = isClasspathDifferent(context.getProperties());

                if (classpathDifferent) {
                    // Create a classloader for the given configuration and use that to verify the component's configuration
                    final Bundle bundle = extensionManager.getBundle(getBundleCoordinate());
                    final Set<URL> classpathUrls = getAdditionalClasspathResources(context.getProperties().keySet(), descriptor -> context.getProperty(descriptor).getValue());

                    final String classloaderIsolationKey = getClassLoaderIsolationKey(context);

                    final ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
                    try (final InstanceClassLoader detectedClassLoader = extensionManager.createInstanceClassLoader(getComponentType(), getIdentifier(), bundle, classpathUrls, false,
                                classloaderIsolationKey)) {
                        Thread.currentThread().setContextClassLoader(detectedClassLoader);
                        results.addAll(verifiable.verify(context, logger, attributes));
                    } finally {
                        Thread.currentThread().setContextClassLoader(currentClassLoader);
                    }
                } else {
                    // Verify the configuration, using the component's classloader
                    try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, processor.getClass(), getIdentifier())) {
                        results.addAll(verifiable.verify(context, logger, attributes));
                    }
                }

                final long validationNanos = validationComplete - startNanos;
                final long verificationNanos = System.nanoTime() - validationComplete;
                LOG.debug("{} completed full configuration validation in {} plus {} for validation",
                    this, FormatUtils.formatNanos(verificationNanos, false), FormatUtils.formatNanos(validationNanos, false));
            } else {
                LOG.debug("{} is not a VerifiableProcessor, so will not perform full verification of configuration. Validation took {}", this,
                    FormatUtils.formatNanos(validationComplete - startNanos, false));
            }
        } catch (final Throwable t) {
            LOG.error("Failed to perform verification of processor's configuration for {}", this, t);

            results.add(new ConfigVerificationResult.Builder()
                .outcome(Outcome.FAILED)
                .verificationStepName("Perform Verification")
                .explanation("Encountered unexpected failure when attempting to perform verification: " + t)
                .build());
        }

        return results;
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
            if (validationContext.isValidateConnections()) {
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
            }

            // Ensure that execution node will not be misused
            if (getExecutionNode() == ExecutionNode.PRIMARY && hasIncomingConnection()) {
                results.add(new ValidationResult.Builder()
                        .explanation("Processors with incoming connections cannot be scheduled for Primary Node Only.")
                        .subject("Execution Node").valid(false).build());
            }
        } catch (final Throwable t) {
            LOG.error("Failed to perform validation", t);
            results.add(new ValidationResult.Builder().explanation("Failed to run validation due to " + t.toString())
                    .valid(false).build());
        }

        return results;
    }

    @Override
    public List<ValidationResult> validateConfig() {

        final List<ValidationResult> results = new ArrayList<>();
        final ParameterContext parameterContext = getParameterContext();

        if (parameterContext == null && !this.parameterReferences.isEmpty()) {
            results.add(new ValidationResult.Builder()
                    .subject(RUN_SCHEDULE)
                    .input("Parameter Context")
                    .valid(false)
                    .explanation("Processor configuration references one or more Parameters but no Parameter Context is currently set on the Process Group.")
                    .build());
        } else {
            for (final ParameterReference paramRef : parameterReferences) {
                final Optional<Parameter> parameterRef = parameterContext.getParameter(paramRef.getParameterName());
                if (!parameterRef.isPresent() ) {
                    results.add(new ValidationResult.Builder()
                            .subject(RUN_SCHEDULE)
                            .input(paramRef.getParameterName())
                            .valid(false)
                            .explanation("Processor configuration references Parameter '" + paramRef.getParameterName() +
                                    "' but the currently selected Parameter Context does not have a Parameter with that name")
                            .build());
                } else {
                    final ParameterDescriptor parameterDescriptor = parameterRef.get().getDescriptor();
                    if (parameterDescriptor.isSensitive()) {
                        results.add(new ValidationResult.Builder()
                                .subject(RUN_SCHEDULE)
                                .input(parameterDescriptor.getName())
                                .valid(false)
                                .explanation("Processor configuration cannot reference sensitive parameters")
                                .build());
                    }
                }
            }

            final String schedulingPeriod = getSchedulingPeriod();
            final String evaluatedSchedulingPeriod = evaluateParameters(schedulingPeriod);

            if (evaluatedSchedulingPeriod != null) {
                switch (schedulingStrategy) {
                    case CRON_DRIVEN: {
                        try {
                            CronExpression.parse(evaluatedSchedulingPeriod);
                        } catch (final Exception e) {
                            results.add(new ValidationResult.Builder()
                                    .subject(RUN_SCHEDULE)
                                    .input(schedulingPeriod)
                                    .valid(false)
                                    .explanation("Scheduling Period is not a valid cron expression")
                                    .build());
                        }
                    }
                    break;
                    case TIMER_DRIVEN: {
                        try {
                            final long schedulingNanos = FormatUtils.getTimeDuration(Objects.requireNonNull(evaluatedSchedulingPeriod),
                                    TimeUnit.NANOSECONDS);

                            if (schedulingNanos < 0) {
                                results.add(new ValidationResult.Builder()
                                        .subject(RUN_SCHEDULE)
                                        .input(schedulingPeriod)
                                        .valid(false)
                                        .explanation("Scheduling Period must be positive")
                                        .build());
                            }

                            this.schedulingNanos.set(Math.max(MINIMUM_SCHEDULING_NANOS, schedulingNanos));

                        } catch (final Exception e) {
                            results.add(new ValidationResult.Builder()
                                    .subject(RUN_SCHEDULE)
                                    .input(schedulingPeriod)
                                    .valid(false)
                                    .explanation("Scheduling Period is not a valid time duration")
                                    .build());
                        }
                    }
                    break;
                    default:
                        return results;
                }
            }
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
        return Objects.equals(identifier, on.getIdentifier());
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public Collection<Relationship> getRelationships() {
        final Processor processor = processorRef.get().getProcessor();
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(getExtensionManager(), processor.getClass(), processor.getIdentifier())) {
            return getProcessor().getRelationships();
        }
    }

    @Override
    public String toString() {
        final Processor processor = processorRef.get().getProcessor();
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(getExtensionManager(), processor.getClass(), processor.getIdentifier())) {
            return getProcessor().toString();
        }
    }

    @Override
    public ProcessGroup getProcessGroup() {
        return processGroup.get();
    }

    @Override
    protected ParameterContext getParameterContext() {
        final ProcessGroup processGroup = getProcessGroup();
        return processGroup == null ? null : processGroup.getParameterContext();
    }

    @Override
    public ParameterLookup getParameterLookup() {
        return getParameterContext();
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
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(getExtensionManager(), processor.getClass(), processor.getIdentifier())) {
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
        if (isRunning()) {
            throw new IllegalStateException("Cannot set AnnotationData on " + this + " while processor is running");
        }
        super.setAnnotationData(data);
    }

    @Override
    public void verifyCanDelete() throws IllegalStateException {
        verifyCanDelete(false);
    }

    @Override
    public void verifyCanDelete(final boolean ignoreConnections) {
        if (isRunning()) {
            throw new IllegalStateException("Cannot delete " + this + " because Processor is running");
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
                    throw new IllegalStateException("Cannot delete " + this + " because it is the destination of another component");
                }
            }
        }
    }

    @Override
    public void verifyCanStart() {
        final ScheduledState currentState = getPhysicalScheduledState();
        if (currentState != ScheduledState.STOPPED && currentState != ScheduledState.DISABLED) {
            throw new IllegalStateException(this + " cannot be started because it is not stopped. Current state is " + currentState.name());
        }

        verifyNoActiveThreads();
    }

    @Override
    public void verifyCanStop() {
        if (getScheduledState() != ScheduledState.RUNNING) {
            throw new IllegalStateException(this + " cannot be stopped because is not scheduled to run");
        }
    }

    @Override
    public void verifyCanUpdate() {
        if (isRunning()) {
            throw new IllegalStateException(this + " cannot be updated because it is not stopped");
        }
    }

    @Override
    public void verifyCanEnable() {
        if (getScheduledState() != ScheduledState.DISABLED) {
            throw new IllegalStateException(this + " cannot be enabled because is not disabled");
        }

        verifyNoActiveThreads();
    }

    @Override
    public void verifyCanDisable() {
        if (getScheduledState() != ScheduledState.STOPPED) {
            throw new IllegalStateException(this + " cannot be disabled because is not stopped");
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
                throw new IllegalStateException(this + " has " + threadCount + " threads still active");
            }
        }
    }

    @Override
    public void verifyModifiable() throws IllegalStateException {
        if (isRunning()) {
            throw new IllegalStateException("Cannot modify configuration of " + this +  " while the Processor is running");
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
    public void start(final ScheduledExecutorService taskScheduler, final long administrativeYieldMillis, final long timeoutMillis, final Supplier<ProcessContext> processContextFactory,
            final SchedulingAgentCallback schedulingAgentCallback, final boolean failIfStopping, final boolean triggerLifecycleMethods) {

        run(taskScheduler, administrativeYieldMillis, timeoutMillis, processContextFactory, schedulingAgentCallback, failIfStopping,
            ScheduledState.RUNNING, ScheduledState.STARTING, triggerLifecycleMethods);
    }

    /**
     * Similar to {@link #start(ScheduledExecutorService, long, long, Supplier, SchedulingAgentCallback, boolean, boolean)}, except for the following:
     * <ul>
     *     <li>
     *         Once the {@link Processor#onTrigger(ProcessContext, ProcessSessionFactory)} method has been invoked successfully, the processor is scehduled to be stopped immediately.
     *         All appropriate lifecycle methods will be executed as well.
     *     </li>
     *     <li>
     *         The processor's desired state is going to be set to STOPPED right away. This usually doesn't prevent the processor to run once, unless NiFi is restarted before it can finish.
     *         In that case the processor will stay STOPPED after the restart.
     *     </li>
     * </ul>
     */
    @Override
    public void runOnce(final ScheduledExecutorService taskScheduler, final long administrativeYieldMillis, final long timeoutMillis, final Supplier<ProcessContext> processContextFactory,
                        final SchedulingAgentCallback schedulingAgentCallback) {

        run(taskScheduler, administrativeYieldMillis, timeoutMillis, processContextFactory, schedulingAgentCallback, true, ScheduledState.RUN_ONCE, ScheduledState.RUN_ONCE, true);
    }

    private void run(final ScheduledExecutorService taskScheduler, final long administrativeYieldMillis, final long timeoutMillis, final Supplier<ProcessContext> processContextFactory,
                     final SchedulingAgentCallback schedulingAgentCallback, final boolean failIfStopping, final ScheduledState desiredState,
                     final ScheduledState scheduledState, final boolean triggerLifecycleMethods) {

        final Processor processor = processorRef.get().getProcessor();
        final ComponentLog procLog = new SimpleProcessLogger(StandardProcessorNode.this.getIdentifier(), processor, new StandardLoggingContext(StandardProcessorNode.this));
        LOG.debug("Starting {}", this);

        ScheduledState currentState;
        boolean starting;
        synchronized (this) {
            currentState = this.scheduledState.get();

            if (currentState == ScheduledState.STOPPED) {
                starting = this.scheduledState.compareAndSet(ScheduledState.STOPPED, scheduledState);
                if (starting) {
                    this.desiredState = desiredState;
                }
            } else if (currentState == ScheduledState.STOPPING && !failIfStopping) {
                this.desiredState = desiredState;
                return;
            } else {
                starting = false;
            }
        }

        if (starting) { // will ensure that the Processor represented by this node can only be started once
            initiateStart(taskScheduler, administrativeYieldMillis, timeoutMillis, new AtomicLong(0), processContextFactory, schedulingAgentCallback, triggerLifecycleMethods);
        } else {
            final String procName = processorRef.get().getProcessor().toString();
            procLog.warn("Cannot start {} because it is not currently stopped. Current state is {}", procName, currentState);
        }
    }

    @Override
    public void notifyPrimaryNodeChanged(final PrimaryNodeState nodeState, final LifecycleState lifecycleState) {
        final Class<?> implClass = getProcessor().getClass();
        final List<Method> methods = ReflectionUtils.findMethodsWithAnnotations(implClass, new Class[] {OnPrimaryNodeStateChange.class});
        if (methods.isEmpty()) {
            return;
        }

        lifecycleState.incrementActiveThreadCount(null);
        activateThread();
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(getExtensionManager(), implClass, getIdentifier())) {
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnPrimaryNodeStateChange.class, getProcessor(), nodeState);
        } finally {
            deactivateThread();
            lifecycleState.decrementActiveThreadCount();
        }
    }

    private void activateThread() {
        final Thread thread = Thread.currentThread();
        final long timestamp = System.currentTimeMillis();
        activeThreads.put(thread, new ActiveTask(timestamp));
    }

    private void deactivateThread() {
        activeThreads.remove(Thread.currentThread());
    }

    @Override
    public List<ActiveThreadInfo> getActiveThreads(final ThreadDetails threadDetails) {
        final long now = System.currentTimeMillis();

        final Map<Long, ThreadInfo> threadInfoMap = Stream.of(threadDetails.getThreadInfos())
            .collect(Collectors.toMap(ThreadInfo::getThreadId, Function.identity(), (a, b) -> a));

        final List<ActiveThreadInfo> threadList = new ArrayList<>(activeThreads.size());
        for (final Map.Entry<Thread, ActiveTask> entry : activeThreads.entrySet()) {
            final Thread thread = entry.getKey();
            final ActiveTask activeTask = entry.getValue();
            final long timestamp = activeTask.getStartTime();
            final long activeMillis = now - timestamp;
            final ThreadInfo threadInfo = threadInfoMap.get(thread.threadId());

            final String stackTrace = ThreadUtils.createStackTrace(threadInfo, threadDetails.getDeadlockedThreadIds(), threadDetails.getMonitorDeadlockThreadIds());

            final ActiveThreadInfo activeThreadInfo = new ActiveThreadInfo(thread.getName(), stackTrace, activeMillis, activeTask.isTerminated());
            threadList.add(activeThreadInfo);
        }

        return threadList;
    }

    @Override
    public int getTerminatedThreadCount() {
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
        LOG.info("Terminating {}", this);

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

        LOG.info("Terminated {} threads for {}", count, this);
        getLogger().terminate();
        completeStopAction();

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
        final ScheduledState state = getScheduledState();
        if (state != ScheduledState.STOPPED && state != ScheduledState.RUN_ONCE) {
            throw new IllegalStateException("Cannot terminate " + this + " because Processor is not stopped");
        }
    }


    private void initiateStart(final ScheduledExecutorService taskScheduler, final long administrativeYieldMillis, final long timeoutMillis,
            final AtomicLong startupAttemptCount, final Supplier<ProcessContext> processContextFactory, final SchedulingAgentCallback schedulingAgentCallback,
            final boolean triggerLifecycleMethods) {

        final Processor processor = getProcessor();
        final ComponentLog procLog = new SimpleProcessLogger(StandardProcessorNode.this.getIdentifier(), processor, new StandardLoggingContext(StandardProcessorNode.this));

        // Completion Timestamp is set to MAX_VALUE because we don't want to timeout until the task has a chance to run.
        final AtomicLong completionTimestampRef = new AtomicLong(Long.MAX_VALUE);

        // Create a task to invoke the @OnScheduled annotation of the processor
        final Callable<Void> startupTask = () -> {
            final ScheduledState currentScheduleState = scheduledState.get();
            if (currentScheduleState == ScheduledState.STOPPING || currentScheduleState == ScheduledState.STOPPED || getDesiredState() == ScheduledState.STOPPED) {
                LOG.debug("{} is stopped. Will not call @OnScheduled lifecycle methods or begin trigger onTrigger() method", StandardProcessorNode.this);
                schedulingAgentCallback.onTaskComplete();
                completeStopAction();
                return null;
            }

            final ValidationStatus validationStatus = getValidationStatus();
            if (validationStatus != ValidationStatus.VALID) {
                LOG.debug("Cannot start {} because Processor is currently not valid; will try again after 5 seconds", StandardProcessorNode.this);

                startupAttemptCount.incrementAndGet();
                if (startupAttemptCount.get() == 240 || startupAttemptCount.get() % 7200 == 0) {
                    final ValidationState validationState = getValidationState();
                    procLog.error("Encountering difficulty starting. (Validation State is {}: {}). Will continue trying to start.",
                            validationState, validationState.getValidationErrors());
                }

                // re-initiate the entire process
                final Runnable initiateStartTask = () -> initiateStart(taskScheduler, administrativeYieldMillis, timeoutMillis, startupAttemptCount,
                    processContextFactory, schedulingAgentCallback, triggerLifecycleMethods);

                taskScheduler.schedule(initiateStartTask, 500, TimeUnit.MILLISECONDS);

                schedulingAgentCallback.onTaskComplete();
                return null;
            }

            // Now that the task has been scheduled, set the timeout
            completionTimestampRef.set(System.currentTimeMillis() + timeoutMillis);

            final ProcessContext processContext = processContextFactory.get();

            try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(getExtensionManager(), processor.getClass(), processor.getIdentifier())) {
                try {
                    hasActiveThreads = true;

                    if (triggerLifecycleMethods) {
                        LOG.debug("Invoking @OnScheduled methods of {}", processor);
                        activateThread();
                        try {
                            ReflectionUtils.invokeMethodsWithAnnotation(OnScheduled.class, processor, processContext);
                        } finally {
                            deactivateThread();
                        }
                    } else {
                        LOG.debug("Will not invoke @OnScheduled methods of {} because triggerLifecycleMethods = false", processor);
                    }

                    if (
                        (desiredState == ScheduledState.RUNNING && scheduledState.compareAndSet(ScheduledState.STARTING, ScheduledState.RUNNING))
                            || (desiredState == ScheduledState.RUN_ONCE && scheduledState.compareAndSet(ScheduledState.RUN_ONCE, ScheduledState.RUN_ONCE))
                    ) {
                        LOG.debug("Successfully completed the @OnScheduled methods of {}; will now start triggering processor to run", processor);
                        schedulingAgentCallback.trigger(); // callback provided by StandardProcessScheduler to essentially initiate component's onTrigger() cycle
                    } else {
                        LOG.info("Successfully invoked @OnScheduled methods of {} but scheduled state is no longer STARTING so will stop processor now; current state = {}, desired state = {}",
                            processor, scheduledState.get(), desiredState);

                        if (triggerLifecycleMethods) {
                            // can only happen if stopProcessor was called before service was transitioned to RUNNING state
                            activateThread();
                            try {
                                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnUnscheduled.class, processor, processContext);
                                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnStopped.class, processor, processContext);
                                hasActiveThreads = false;
                            } finally {
                                deactivateThread();
                            }
                        } else {
                            LOG.debug("Will not trigger @OnUnscheduled / @OnStopped methods on {} because triggerLifecycleMethods = false", processor);
                        }

                        completeStopAction();

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
            } catch (Exception e) {
                final Throwable cause = (e instanceof InvocationTargetException) ? e.getCause() : e;
                procLog.error("Failed to properly initialize Processor. If still scheduled to run, NiFi will attempt to "
                    + "initialize and run the Processor again after the 'Administrative Yield Duration' has elapsed. Failure is due to " + cause, cause);

                // If processor's task completed Exceptionally, then we want to retry initiating the start (if Processor is still scheduled to run).
                try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(getExtensionManager(), processor.getClass(), processor.getIdentifier())) {
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
                if (scheduledState.get() != ScheduledState.STOPPING && scheduledState.get() != ScheduledState.RUN_ONCE) {
                    // re-initiate the entire process
                    final Runnable initiateStartTask = () -> initiateStart(taskScheduler, administrativeYieldMillis, timeoutMillis, startupAttemptCount,
                        processContextFactory, schedulingAgentCallback, triggerLifecycleMethods);

                    taskScheduler.schedule(initiateStartTask, administrativeYieldMillis, TimeUnit.MILLISECONDS);
                } else {
                    completeStopAction();
                }
            }

            return null;
        };

        final Future<?> taskFuture;
        try {
            // Trigger the task in a background thread.
            taskFuture = schedulingAgentCallback.scheduleTask(startupTask);
        } catch (RejectedExecutionException rejectedExecutionException) {
            final ValidationState validationState = getValidationState();
            LOG.error("Unable to start {}.  Last known validation state was {} : {}", this, validationState, validationState.getValidationErrors(), rejectedExecutionException);
            return;
        }

        // Trigger a task periodically to check if @OnScheduled task completed. Once it has,
        // this task will call SchedulingAgentCallback#onTaskComplete.
        // However, if the task times out, we need to be able to cancel the monitoring. So, in order
        // to do this, we use #scheduleWithFixedDelay and then make that Future available to the task
        // itself by placing it into an AtomicReference.
        final AtomicReference<Future<?>> futureRef = new AtomicReference<>();
        final Runnable monitoringTask = () -> {
            Future<?> monitoringFuture = futureRef.get();
            if (monitoringFuture == null) { // Future is not yet available. Just return and wait for the next invocation.
                return;
            }

           monitorAsyncTask(taskFuture, monitoringFuture, completionTimestampRef.get());
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
     * {@link #start(ScheduledExecutorService, long, long, Supplier, SchedulingAgentCallback, boolean, boolean)}
     * method to initiate processor's shutdown upon exiting @OnScheduled
     * operation, otherwise the processor's scheduled state will remain
     * unchanged ensuring that multiple calls to this method are idempotent.
     * </p>
     */
    @Override
    public CompletableFuture<Void> stop(final ProcessScheduler processScheduler, final ScheduledExecutorService executor, final ProcessContext processContext,
            final SchedulingAgent schedulingAgent, final LifecycleState lifecycleState, final boolean triggerLifecycleMethods) {

        final Processor processor = processorRef.get().getProcessor();
        LOG.debug("Stopping processor: {}", this);
        desiredState = ScheduledState.STOPPED;

        final CompletableFuture<Void> future = new CompletableFuture<>();

        addStopFuture(future);

        // will ensure that the Processor represented by this node can only be stopped once
        if (this.scheduledState.compareAndSet(ScheduledState.RUNNING, ScheduledState.STOPPING) || this.scheduledState.compareAndSet(ScheduledState.RUN_ONCE, ScheduledState.STOPPING)) {
            lifecycleState.incrementActiveThreadCount(null);

            // will continue to monitor active threads, invoking OnStopped once there are no
            // active threads (with the exception of the thread performing shutdown operations)
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (lifecycleState.isScheduled()) {
                            schedulingAgent.unschedule(StandardProcessorNode.this, lifecycleState);

                            if (triggerLifecycleMethods) {
                                LOG.debug("Triggering @OnUnscheduled methods of {}", this);

                                activateThread();
                                try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(getExtensionManager(), processor.getClass(), processor.getIdentifier())) {
                                    ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnUnscheduled.class, processor, processContext);
                                } finally {
                                    deactivateThread();
                                }
                            } else {
                                LOG.debug("Will not trigger @OnUnscheduled methods of {} because triggerLifecycleState = false", this);
                            }
                        }

                        // all threads are complete if the active thread count is 1. This is because this thread that is
                        // performing the lifecycle actions counts as 1 thread.
                        final boolean allThreadsComplete = lifecycleState.getActiveThreadCount() == 1;
                        if (allThreadsComplete) {
                            if (triggerLifecycleMethods) {
                                LOG.debug("Triggering @OnStopped methods of {}", this);

                                activateThread();
                                try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(getExtensionManager(), processor.getClass(), processor.getIdentifier())) {
                                    ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnStopped.class, processor, processContext);
                                } finally {
                                    deactivateThread();
                                }
                            } else {
                                LOG.debug("Will not trigger @OnStopped methods of {} because triggerLifecycleState = false", this);
                            }

                            lifecycleState.decrementActiveThreadCount();
                            completeStopAction();

                            // This can happen only when we join a cluster. In such a case, we can inherit a flow from the cluster that says that
                            // the Processor is to be running. However, if the Processor is already in the process of stopping, we cannot immediately
                            // start running the Processor. As a result, we check here, since the Processor is stopped, and then immediately start the
                            // Processor if need be.
                            final ScheduledState desired = StandardProcessorNode.this.desiredState;
                            if (desired == ScheduledState.RUNNING) {
                                LOG.info("Finished stopping {} but desired state is now RUNNING so will start processor", this);
                                getProcessGroup().startProcessor(StandardProcessorNode.this, true);
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
                        LOG.warn("Failed while shutting down processor {}", processor, e);
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
            final boolean updated = this.scheduledState.compareAndSet(ScheduledState.STARTING, ScheduledState.STOPPING);
            if (updated) {
                LOG.debug("Transitioned state of {} from STARTING to STOPPING", this);
            }
        }

        return future;
    }

    /**
     * Marks the processor as fully stopped, and completes any futures that are to be completed as a result
     */
    private void completeStopAction() {
        synchronized (this.stopFutures) {
            LOG.info("{} has completely stopped. Completing any associated Futures.", this);
            this.hasActiveThreads = false;
            this.scheduledState.set(ScheduledState.STOPPED);

            final List<CompletableFuture<Void>> futures = this.stopFutures.getAndSet(new ArrayList<>());
            futures.forEach(f -> f.complete(null));
        }
    }

    /**
     * Adds the given CompletableFuture to the list of those that will completed whenever the processor has fully stopped
     * @param future the future to add
     */
    private void addStopFuture(final CompletableFuture<Void> future) {
        synchronized (this.stopFutures) {
            if (scheduledState.get() == ScheduledState.STOPPED) {
                future.complete(null);
            } else {
                stopFutures.get().add(future);
            }
        }
    }

    @Override
    public ScheduledState getDesiredState() {
        return desiredState;
    }

    @Override
    public int getRetryCount() {
        return retryCount;
    }

    @Override
    public void setRetryCount(Integer retryCount) {
        if (isRunning()) {
            throw new IllegalStateException("Cannot modify configuration of " + this + " while the Processor is running");
        }
        this.retryCount = (retryCount == null) ? 0 : retryCount;
    }

    @Override
    public Set<String> getRetriedRelationships() {
        return retriedRelationships;
    }

    @Override
    public void setRetriedRelationships(Set<String> retriedRelationships) {
        if (isRunning()) {
            throw new IllegalStateException("Cannot modify configuration of " + this + " while the Processor is running");
        }
        this.retriedRelationships = (retriedRelationships == null) ? Collections.emptySet() : new HashSet<>(retriedRelationships);
    }

    @Override
    public boolean isRelationshipRetried(final Relationship relationship) {
        if (relationship == null) {
            return false;
        } else {
            return this.retriedRelationships.contains(relationship.getName());
        }
    }

    @Override
    public BackoffMechanism getBackoffMechanism() {
        return backoffMechanism;
    }

    @Override
    public void setBackoffMechanism(BackoffMechanism backoffMechanism) {
        if (isRunning()) {
            throw new IllegalStateException("Cannot modify configuration of " + this + " while the Processor is running");
        }
        this.backoffMechanism = (backoffMechanism == null) ? BackoffMechanism.PENALIZE_FLOWFILE : backoffMechanism;
    }

    @Override
    public String getMaxBackoffPeriod() {
        return maxBackoffPeriod;
    }

    @Override
    public void setMaxBackoffPeriod(String maxBackoffPeriod) {
        if (isRunning()) {
            throw new IllegalStateException("Cannot modify configuration of " + this + " while the Processor is running");
        }
        if (maxBackoffPeriod == null) {
            maxBackoffPeriod = DEFAULT_MAX_BACKOFF_PERIOD;
        }
        final long backoffNanos = FormatUtils.getTimeDuration(maxBackoffPeriod, TimeUnit.NANOSECONDS);
        if (backoffNanos < 0) {
            throw new IllegalArgumentException("Cannot set Max Backoff Period of " + this + " to negative value: " + backoffNanos + " nanos");
        }
        this.maxBackoffPeriod = maxBackoffPeriod;
    }

    @Override
    public String evaluateParameters(final String value) {
        final ParameterContext parameterContext = getParameterContext();
        final ParameterParser parameterParser = new ExpressionLanguageAgnosticParameterParser();
        final ParameterTokenList parameterTokenList = parameterParser.parseTokens(value);

        return parameterTokenList.substitute(parameterContext);
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

    @Override
    public void onConfigurationRestored(final ProcessContext context) {
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(getExtensionManager(), getProcessor().getClass(), getProcessor().getIdentifier())) {
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, getProcessor(), context);
        }

        // Now that the configuration has been fully restored, it's possible that some components (in particular, scripted components)
        // may change the Property Descriptor definitions because they've now reloaded the configured scripts. As a result,
        // the PropertyDescriptor may now indicate that it references a Controller Service when it previously had not.
        // In order to account for this, we need to refresh our controller service references by removing an previously existing
        // references and establishing new references.
        updateControllerServiceReferences();
    }

    @Override
    public void migrateConfiguration(final Map<String, String> rawPropertyValues, final ControllerServiceFactory serviceFactory) {
        try {
            migrateProperties(rawPropertyValues, serviceFactory);
        } catch (final Exception e) {
            LOG.error("Failed to migrate Property Configuration for {}.", this, e);
        }

        try {
            migrateRelationships();
        } catch (final Exception e) {
            LOG.error("Failed to migrate Relationship Configuration for {}.", this, e);
        }
    }

    private void migrateProperties(final Map<String, String> originalPropertyValues, final ControllerServiceFactory serviceFactory) {
        final Processor processor = getProcessor();

        final Map<String, String> effectiveValues = new HashMap<>();
        originalPropertyValues.forEach((key, value) -> effectiveValues.put(key, mapRawValueToEffectiveValue(value)));

        final StandardPropertyConfiguration propertyConfig = new StandardPropertyConfiguration(effectiveValues,
                originalPropertyValues, this::mapRawValueToEffectiveValue, toString(), serviceFactory);
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(getExtensionManager(), processor.getClass(), getIdentifier())) {
            processor.migrateProperties(propertyConfig);
        }

        if (propertyConfig.isModified()) {
            // Create any necessary Controller Services. It is important that we create the services
            // before updating the processor's properties, as it's necessary in order to properly account
            // for the Controller Service References.
            final List<ControllerServiceCreationDetails> servicesCreated = propertyConfig.getCreatedServices();
            servicesCreated.forEach(serviceFactory::create);

            overwriteProperties(propertyConfig.getRawProperties());
        }
    }


    private void migrateRelationships() {
        final Processor processor = getProcessor();

        final StandardRelationshipConfiguration relationshipConfig = new StandardRelationshipConfiguration(this);
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(getExtensionManager(), getProcessor().getClass(), getProcessor().getIdentifier())) {
            processor.migrateRelationships(relationshipConfig);
        }
    }


    private void updateControllerServiceReferences() {
        for (final Map.Entry<PropertyDescriptor, PropertyConfiguration> entry : getProperties().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            final PropertyConfiguration propertyConfiguration = entry.getValue();
            if (descriptor.getControllerServiceDefinition() == null || propertyConfiguration == null) {
                continue;
            }

            final String propertyValue = propertyConfiguration.getEffectiveValue(getParameterLookup());
            if (propertyValue == null) {
                continue;
            }

            final ControllerServiceNode serviceNode = getControllerServiceProvider().getControllerServiceNode(propertyValue);
            if (serviceNode == null) {
                continue;
            }

            serviceNode.updateReference(this, descriptor);
        }
    }
}
