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
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.FormatUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.Objects.requireNonNull;

public class StandardFunnel implements Funnel {

    public static final long MINIMUM_PENALIZATION_MILLIS = 0L;
    public static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;
    public static final long MINIMUM_YIELD_MILLIS = 0L;
    public static final long DEFAULT_YIELD_PERIOD = 1000L;
    public static final TimeUnit DEFAULT_YIELD_TIME_UNIT = TimeUnit.MILLISECONDS;

    private final String identifier;
    private final Set<Connection> outgoingConnections;
    private final List<Connection> incomingConnections;
    private final List<Relationship> relationships;

    private final AtomicReference<ProcessGroup> processGroupRef;
    private final AtomicReference<Position> position;
    private final AtomicReference<String> penalizationPeriod;
    private final AtomicReference<String> yieldPeriod;
    private final AtomicReference<String> schedulingPeriod;
    private final AtomicReference<String> name;
    private final AtomicLong schedulingNanos;
    private final AtomicBoolean lossTolerant;
    private final AtomicReference<ScheduledState> scheduledState;
    private final AtomicLong yieldExpiration;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    public StandardFunnel(final String identifier, final ProcessGroup processGroup, final ProcessScheduler scheduler) {
        this.identifier = identifier;
        this.processGroupRef = new AtomicReference<>(processGroup);

        outgoingConnections = new HashSet<>();
        incomingConnections = new ArrayList<>();

        final List<Relationship> relationships = new ArrayList<>();
        relationships.add(Relationship.ANONYMOUS);
        this.relationships = Collections.unmodifiableList(relationships);

        lossTolerant = new AtomicBoolean(false);
        position = new AtomicReference<>(new Position(0D, 0D));
        scheduledState = new AtomicReference<>(ScheduledState.STOPPED);
        penalizationPeriod = new AtomicReference<>("30 sec");
        yieldPeriod = new AtomicReference<>("250 millis");
        yieldExpiration = new AtomicLong(0L);
        schedulingPeriod = new AtomicReference<>("0 millis");
        schedulingNanos = new AtomicLong(MINIMUM_SCHEDULING_NANOS);
        name = new AtomicReference<>("Funnel");
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public String getProcessGroupIdentifier() {
        final ProcessGroup procGroup = getProcessGroup();
        return procGroup == null ? null : procGroup.getIdentifier();
    }

    @Override
    public Collection<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public Relationship getRelationship(final String relationshipName) {
        return (Relationship.ANONYMOUS.getName().equals(relationshipName)) ? Relationship.ANONYMOUS : null;
    }

    @Override
    public Authorizable getParentAuthorizable() {
        return getProcessGroup();
    }

    @Override
    public Resource getResource() {
        return ResourceFactory.getComponentResource(ResourceType.Funnel, getIdentifier(), getName());
    }

    @Override
    public void addConnection(final Connection connection) throws IllegalArgumentException {
        writeLock.lock();
        try {
            if (!requireNonNull(connection).getSource().equals(this) && !connection.getDestination().equals(this)) {
                throw new IllegalArgumentException("Cannot add a connection to a Funnel for which the Funnel is neither the Source nor the Destination");
            }
            if (connection.getSource().equals(this) && connection.getDestination().equals(this)) {
                throw new IllegalArgumentException("Cannot add a connection from a Funnel back to itself");
            }

            if (connection.getDestination().equals(this)) {
                // don't add the connection twice. This may occur if we have a self-loop because we will be told
                // to add the connection once because we are the source and again because we are the destination.
                if (!incomingConnections.contains(connection)) {
                    incomingConnections.add(connection);
                }
            }

            if (connection.getSource().equals(this)) {
                // don't add the connection twice. This may occur if we have a self-loop because we will be told
                // to add the connection once because we are the source and again because we are the destination.
                if (!outgoingConnections.contains(connection)) {
                    for (final Relationship relationship : connection.getRelationships()) {
                        if (!relationship.equals(Relationship.ANONYMOUS)) {
                            throw new IllegalArgumentException("No relationship with name " + relationship + " exists for Funnels");
                        }
                    }

                    outgoingConnections.add(connection);
                }
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
        }

        if (connection.getDestination().equals(this)) {
            writeLock.lock();
            try {
                if (!incomingConnections.remove(connection)) {
                    throw new IllegalStateException("No Connection with ID " + connection.getIdentifier() + " is currently registered with this Port");
                }
                incomingConnections.add(connection);
            } finally {
                writeLock.unlock();
            }
        }
    }

    @Override
    public void removeConnection(final Connection connection) throws IllegalArgumentException, IllegalStateException {
        writeLock.lock();
        try {
            if (!requireNonNull(connection).getSource().equals(this)) {
                final boolean existed = incomingConnections.remove(connection);
                if (!existed) {
                    throw new IllegalStateException("The given connection is not currently registered for this ProcessorNode");
                }
                return;
            }

            final boolean removed = outgoingConnections.remove(connection);
            if (!removed) {
                throw new IllegalStateException(connection.getIdentifier() + " is not registered with " + this.getIdentifier());
            }
        } finally {
            writeLock.unlock();
        }
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
            if (relationship.equals(Relationship.ANONYMOUS)) {
                return Collections.unmodifiableSet(outgoingConnections);
            }

            throw new IllegalArgumentException("No relationship with name " + relationship.getName() + " exists for Funnels");
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public List<Connection> getIncomingConnections() {
        readLock.lock();
        try {
            return new ArrayList<>(incomingConnections);
        } finally {
            readLock.unlock();
        }
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
    public String getName() {
        return name.get();
    }

    /**
     * Throws {@link UnsupportedOperationException}
     *
     * @param name new name
     */
    @Override
    public void setName(final String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getComments() {
        return "";
    }

    @Override
    public void setComments(final String comments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ProcessGroup getProcessGroup() {
        return processGroupRef.get();
    }

    @Override
    public void setProcessGroup(final ProcessGroup group) {
        processGroupRef.set(group);
    }

    @Override
    public boolean isAutoTerminated(Relationship relationship) {
        return false;
    }

    @Override
    public boolean isRunning() {
        return isRunning(this);
    }

    private boolean isRunning(final Connectable source) {
        return getScheduledState() == ScheduledState.RUNNING;
    }

    @Override
    public boolean isTriggerWhenEmpty() {
        return false;
    }

    @Override
    public ScheduledState getScheduledState() {
        return scheduledState.get();
    }

    @Override
    public boolean isLossTolerant() {
        return lossTolerant.get();
    }

    @Override
    public void setLossTolerant(final boolean lossTolerant) {
        this.lossTolerant.set(lossTolerant);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("id", getIdentifier()).toString();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();

        try {
            onTrigger(context, session);
            session.commit();
        } catch (final ProcessException e) {
            session.rollback();
            throw e;
        } catch (final Throwable t) {
            session.rollback();
            throw new RuntimeException(t);
        }
    }

    private void onTrigger(final ProcessContext context, final ProcessSession session) {
        readLock.lock();
        try {
            Set<Relationship> available = context.getAvailableRelationships();
            while (!available.isEmpty()) {
                final List<FlowFile> flowFiles = session.get(100);
                if (flowFiles.isEmpty()) {
                    break;
                }

                session.transfer(flowFiles, Relationship.ANONYMOUS);
                session.commit();
                available = context.getAvailableRelationships();
            }
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Has no effect
     */
    @Override
    public void setMaxConcurrentTasks(int taskCount) {
    }

    @Override
    public int getMaxConcurrentTasks() {
        return 1;
    }

    @Override
    public void setScheduledState(final ScheduledState scheduledState) {
        this.scheduledState.set(scheduledState);
    }

    @Override
    public ConnectableType getConnectableType() {
        return ConnectableType.FUNNEL;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<ValidationResult> getValidationErrors() {
        return Collections.EMPTY_LIST;
    }

    /**
     * Updates the amount of time that this processor should avoid being
     * scheduled when the processor calls
     * {@link org.apache.nifi.processor.ProcessContext#yield() ProcessContext.yield()}
     *
     * @param yieldPeriod new period
     */
    @Override
    public void setYieldPeriod(final String yieldPeriod) {
        final long yieldMillis = FormatUtils.getTimeDuration(requireNonNull(yieldPeriod), TimeUnit.MILLISECONDS);
        if (yieldMillis < 0) {
            throw new IllegalArgumentException("Yield duration must be positive");
        }
        this.yieldPeriod.set(yieldPeriod);
    }

    @Override
    public void setScheduldingPeriod(final String schedulingPeriod) {
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

    /**
     * Causes the processor not to be scheduled for some period of time. This
     * duration can be obtained and set via the
     * {@link #getYieldPeriod(TimeUnit)} and
     * {@link #setYieldPeriod(String)} methods.
     */
    @Override
    public void yield() {
        final long yieldMillis = getYieldPeriod(TimeUnit.MILLISECONDS);
        yieldExpiration.set(Math.max(yieldExpiration.get(), System.currentTimeMillis() + yieldMillis));
    }

    @Override
    public long getYieldExpiration() {
        return yieldExpiration.get();
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
    public long getSchedulingPeriod(final TimeUnit timeUnit) {
        return timeUnit.convert(schedulingNanos.get(), TimeUnit.NANOSECONDS);
    }

    @Override
    public boolean isSideEffectFree() {
        return true;
    }

    @Override
    public void verifyCanDelete(boolean ignoreConnections) throws IllegalStateException {
        if (ignoreConnections) {
            return;
        }

        readLock.lock();
        try {
            for (final Connection connection : outgoingConnections) {
                connection.verifyCanDelete();
            }

            for (final Connection connection : incomingConnections) {
                if (connection.getSource().equals(this)) {
                    connection.verifyCanDelete();
                } else {
                    throw new IllegalStateException("Funnel " + this.getIdentifier() + " is the destination of another component");
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void verifyCanDelete() {
        verifyCanDelete(false);
    }

    @Override
    public void verifyCanStart() {
    }

    @Override
    public void verifyCanStop() {
    }

    @Override
    public void verifyCanUpdate() {
    }

    @Override
    public void verifyCanEnable() {
    }

    @Override
    public void verifyCanDisable() {
    }

    @Override
    public void verifyCanClearState() {
    }

    @Override
    public SchedulingStrategy getSchedulingStrategy() {
        return SchedulingStrategy.TIMER_DRIVEN;
    }

    @Override
    public String getComponentType() {
        return "Funnel";
    }
}
