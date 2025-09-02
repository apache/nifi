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

package org.apache.nifi.components.connector;

import java.util.concurrent.CompletableFuture;

/**
 * Manages the state transitions and lifecycle of a ConnectorNode, including tracking current and desired states,
 * handling asynchronous state transition completion, and coordinating update resume states.
 */
public interface ConnectorStateTransition {

    /**
     * Returns the current state of the connector.
     *
     * @return the current ConnectorState
     */
    ConnectorState getCurrentState();

    /**
     * Returns the desired state of the connector.
     *
     * @return the desired ConnectorState
     */
    ConnectorState getDesiredState();

    /**
     * Sets the desired state for the connector. This operation is used to indicate what state the connector
     * should be in, and the framework will work to transition to that state.
     *
     * @param desiredState the target state for the connector
     */
    void setDesiredState(ConnectorState desiredState);

    /**
     * Attempts to atomically update the current state from the expected state to the new state.
     *
     * @param expectedState the expected current state
     * @param newState the new state to transition to
     * @return true if the state was successfully updated, false if the current state did not match the expected state
     */
    boolean trySetCurrentState(ConnectorState expectedState, ConnectorState newState);

    /**
     * Sets the current state to the specified state, regardless of the previous state.
     *
     * @param newState the new current state
     */
    void setCurrentState(ConnectorState newState);

    /**
     * Registers a future to be completed when the connector transitions to the RUNNING state.
     * This method is thread-safe and handles internal synchronization.
     *
     * @param future the CompletableFuture to complete when the connector starts
     */
    void addPendingStartFuture(CompletableFuture<Void> future);

    /**
     * Registers a future to be completed when the connector transitions to the STOPPED state.
     * This method is thread-safe and handles internal synchronization.
     *
     * @param future the CompletableFuture to complete when the connector stops
     */
    void addPendingStopFuture(CompletableFuture<Void> future);
}

