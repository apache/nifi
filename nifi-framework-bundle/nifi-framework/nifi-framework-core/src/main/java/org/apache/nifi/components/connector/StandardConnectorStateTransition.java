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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

public class StandardConnectorStateTransition implements ConnectorStateTransition {
    private static final Logger logger = LoggerFactory.getLogger(StandardConnectorStateTransition.class);

    private final String componentDescription;
    private final AtomicReference<ConnectorState> currentState = new AtomicReference<>(ConnectorState.STOPPED);
    private final AtomicReference<ConnectorState> desiredState = new AtomicReference<>(ConnectorState.STOPPED);
    private final List<CompletableFuture<Void>> pendingStartFutures = new ArrayList<>();
    private final List<CompletableFuture<Void>> pendingStopFutures = new ArrayList<>();

    public StandardConnectorStateTransition(final String componentDescription) {
        this.componentDescription = componentDescription;
    }

    @Override
    public ConnectorState getCurrentState() {
        return currentState.get();
    }

    @Override
    public ConnectorState getDesiredState() {
        return desiredState.get();
    }

    @Override
    public void setDesiredState(final ConnectorState desiredState) {
        this.desiredState.set(desiredState);
        logger.info("Desired State for {} set to {}", componentDescription, desiredState);
    }

    @Override
    public boolean trySetCurrentState(final ConnectorState expectedState, final ConnectorState newState) {
        final boolean changed = currentState.compareAndSet(expectedState, newState);
        if (changed) {
            logger.info("Transitioned current state for {} from {} to {}", componentDescription, expectedState, newState);
            completeFuturesForStateTransition(newState);
        }

        return changed;
    }

    @Override
    public void setCurrentState(final ConnectorState newState) {
        final ConnectorState oldState = currentState.getAndSet(newState);
        logger.info("Transitioned current state for {} from {} to {}", componentDescription, oldState, newState);
        completeFuturesForStateTransition(newState);
    }

    @Override
    public synchronized void addPendingStartFuture(final CompletableFuture<Void> future) {
        pendingStartFutures.add(future);
    }

    @Override
    public synchronized void addPendingStopFuture(final CompletableFuture<Void> future) {
        pendingStopFutures.add(future);
    }

    private synchronized void completeFuturesForStateTransition(final ConnectorState newState) {
        if (newState == ConnectorState.RUNNING) {
            final List<CompletableFuture<Void>> futuresToComplete = new ArrayList<>(pendingStartFutures);
            pendingStartFutures.clear();

            for (final CompletableFuture<Void> future : futuresToComplete) {
                future.complete(null);
            }

            if (!futuresToComplete.isEmpty()) {
                logger.debug("Completed {} pending start futures for {}", futuresToComplete.size(), componentDescription);
            }
        }

        if (newState == ConnectorState.STOPPED) {
            final List<CompletableFuture<Void>> futuresToComplete = new ArrayList<>(pendingStopFutures);
            pendingStopFutures.clear();

            for (final CompletableFuture<Void> future : futuresToComplete) {
                future.complete(null);
            }

            if (!futuresToComplete.isEmpty()) {
                logger.debug("Completed {} pending stop futures for {}", futuresToComplete.size(), componentDescription);
            }
        }
    }
}

