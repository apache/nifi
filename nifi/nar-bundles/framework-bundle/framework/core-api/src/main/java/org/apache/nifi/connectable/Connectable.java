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
package org.apache.nifi.connectable;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.Triggerable;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.scheduling.SchedulingStrategy;

/**
 * Represents a connectable component to which or from which data can flow.
 */
public interface Connectable extends Triggerable {

    /**
     * @return the unique identifier for this <code>Connectable</code>
     */
    String getIdentifier();

    /**
     * @return a Collection of all relationships for this Connectable
     */
    Collection<Relationship> getRelationships();

    /**
     * Returns the ProcessorRelationship whose name is given
     *
     * @param relationshipName
     * @return a ProcessorRelationship whose name is given, or <code>null</code>
     * if none exists
     */
    Relationship getRelationship(String relationshipName);

    /**
     * Adds the given connection to this Connectable.
     *
     * @param connection the connection to add
     * @throws NullPointerException if the argument is null
     * @throws IllegalArgumentException if the given Connection is not valid
     */
    void addConnection(Connection connection) throws IllegalArgumentException;

    /**
     * @return true if the Connectable is the destination of any other
     * Connectable, false otherwise.
     */
    boolean hasIncomingConnection();

    /**
     *
     * @param connection
     * @throws IllegalStateException if the given Connection is not registered
     * to <code>this</code>.
     */
    void removeConnection(Connection connection) throws IllegalStateException;

    /**
     * Updates any internal state that depends on the given connection. The
     * given connection will share the same ID as the old connection.
     *
     * @param newConnection
     * @throws IllegalStateException
     */
    void updateConnection(Connection newConnection) throws IllegalStateException;

    /**
     * @return a <code>Set</code> of all <code>Connection</code>s for which this
     * <code>Connectable</code> is the destination
     */
    List<Connection> getIncomingConnections();

    /**
     * @return a <code>Set</code> of all <code>Connection</code>s for which this
     * <code>Connectable</code> is the source; if no connections exist, will
     * return an empty Collection. Guaranteed not null.
     */
    Set<Connection> getConnections();

    /**
     * @param relationship
     * @return a <code>Set</code> of all <code>Connection</code>s that contain
     * the given relationship for which this <code>Connectable</code> is the
     * source
     */
    Set<Connection> getConnections(Relationship relationship);

    /**
     * Returns the position on the graph where this Connectable is located
     *
     * @return
     */
    Position getPosition();

    /**
     * Updates this component's position on the graph
     *
     * @param position
     */
    void setPosition(Position position);

    /**
     * @return the name of this Connectable
     */
    String getName();

    /**
     * Sets the name of this Connectable so that its name will be visible on the
     * UI
     * @param name
     */
    void setName(String name);

    /**
     * @return the comments of this Connectable
     */
    String getComments();

    /**
     * Sets the comments of this Connectable.
     * @param comments
     */
    void setComments(String comments);

    /**
     * If true,
     * {@link #onTrigger(nifi.processor.ProcessContext, nifi.processor.ProcessSessionFactory)}
     * should be called even when this Connectable has no FlowFiles queued for
     * processing
     *
     * @return
     */
    boolean isTriggerWhenEmpty();

    /**
     * Returns the ProcessGroup to which this <code>Connectable</code> belongs
     *
     * @return
     */
    ProcessGroup getProcessGroup();

    /**
     * Sets the new ProcessGroup to which this <code>Connectable</code> belongs
     *
     * @param group
     */
    void setProcessGroup(ProcessGroup group);

    /**
     *
     * @param relationship the relationship
     * @return true indicates flow files transferred to the given relationship
     * should be terminated if the relationship is not connected to another
     * FlowFileConsumer; false indicates they will not be terminated and the
     * processor will not be valid until specified
     */
    boolean isAutoTerminated(Relationship relationship);

    /**
     * Indicates whether flow file content made by this connectable must be
     * persisted
     *
     * @return 
     */
    boolean isLossTolerant();

    /**
     * @param lossTolerant
     */
    void setLossTolerant(boolean lossTolerant);

    /**
     * @return the type of the Connectable
     */
    ConnectableType getConnectableType();

    /**
     * Returns the any validation errors for this connectable.
     *
     * @return
     */
    Collection<ValidationResult> getValidationErrors();

    /**
     * Returns the amount of time for which a FlowFile should be penalized when
     * {@link ProcessSession#penalize(nifi.flowfile.FlowFile)} is called
     *
     * @param timeUnit
     * @return
     */
    long getPenalizationPeriod(final TimeUnit timeUnit);

    /**
     * Returns a string representation for which a FlowFile should be penalized
     * when {@link ProcessSession#penalize(nifi.flowfile.FlowFile)} is called
     *
     * @return
     */
    String getPenalizationPeriod();

    /**
     * @param timeUnit determines the unit of time to represent the yield
     * period.
     * @return
     */
    long getYieldPeriod(TimeUnit timeUnit);

    /**
     * returns the string representation for this Connectable's configured yield
     * period
     *
     * @return
     */
    String getYieldPeriod();

    /**
     * Updates the amount of time that this Connectable should avoid being
     * scheduled when the processor calls
     * {@link nifi.processor.ProcessContext#yield() ProcessContext.yield()}
     *
     * @param yieldPeriod
     */
    void setYieldPeriod(String yieldPeriod);

    /**
     * Updates the amount of time that this Connectable will penalize FlowFiles
     * when {@link ProcessSession#penalize(nifi.flowfile.FlowFile)} is called
     * @param penalizationPeriod
     */
    void setPenalizationPeriod(String penalizationPeriod);

    /**
     * Causes the processor not to be scheduled for some period of time. This
     * duration can be obtained and set via the
     * {@link #getYieldPeriod(TimeUnit)} and
     * {@link #setYieldPeriod(long, TimeUnit)} methods.
     */
    void yield();

    /**
     * Returns the time in milliseconds since Epoch at which this Connectable
     * should no longer yield its threads
     *
     * @return
     */
    long getYieldExpiration();

    /**
     * Specifies whether or not this component is considered side-effect free,
     * with respect to external systems.
     *
     * @return
     */
    boolean isSideEffectFree();

    void verifyCanDelete() throws IllegalStateException;

    void verifyCanDelete(boolean ignoreConnections) throws IllegalStateException;

    void verifyCanStart() throws IllegalStateException;

    void verifyCanStop() throws IllegalStateException;

    void verifyCanUpdate() throws IllegalStateException;

    void verifyCanEnable() throws IllegalStateException;

    void verifyCanDisable() throws IllegalStateException;

    SchedulingStrategy getSchedulingStrategy();
}
