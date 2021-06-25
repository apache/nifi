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

package org.apache.nifi.controller.service;

import org.apache.nifi.controller.ComponentNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ServiceStateTransition {
    private static final Logger logger = LoggerFactory.getLogger(ServiceStateTransition.class);
    private ControllerServiceState state = ControllerServiceState.DISABLED;
    private final List<CompletableFuture<?>> enabledFutures = new ArrayList<>();
    private final List<CompletableFuture<?>> disabledFutures = new ArrayList<>();
    private final ControllerServiceNode controllerServiceNode;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock writeLock = rwLock.writeLock();
    private final Lock readLock = rwLock.readLock();
    private final Condition stateChangeCondition = writeLock.newCondition();

    public ServiceStateTransition(final ControllerServiceNode controllerServiceNode) {
        this.controllerServiceNode = controllerServiceNode;
    }

    public boolean transitionToEnabling(final ControllerServiceState expectedState, final CompletableFuture<?> enabledFuture) {
        writeLock.lock();
        try {
            if (expectedState != state) {
                return false;
            }

            state = ControllerServiceState.ENABLING;
            logger.debug("{} transitioned to ENABLING", controllerServiceNode);

            stateChangeCondition.signalAll();
            enabledFutures.add(enabledFuture);
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    public boolean enable(final ControllerServiceReference controllerServiceReference) {
        writeLock.lock();
        try {
            if (state != ControllerServiceState.ENABLING) {
                logger.debug("{} cannot be transitioned to enabled because it's not currently ENABLING but rather {}", controllerServiceNode, state);
                return false;
            }

            state = ControllerServiceState.ENABLED;
            logger.debug("{} transitioned to ENABLED", controllerServiceNode);

            enabledFutures.forEach(future -> future.complete(null));
        } finally {
            writeLock.unlock();
        }

        // We want to perform validation against other components outside of the write lock. Component validation could be expensive
        // and more importantly could reference other controller services, which could result in a dead lock if we run the validation
        // while holding this.writeLock.
        final List<ComponentNode> referencingComponents = controllerServiceReference.findRecursiveReferences(ComponentNode.class);
        for (final ComponentNode component : referencingComponents) {
            component.performValidation();
        }

        // Now that the write lock was relinquished in order to perform validation on referencing components, we must re-acquire it
        // in order to signal that a state change has completed.
        writeLock.lock();
        try {
            stateChangeCondition.signalAll();
        } finally {
            writeLock.unlock();
        }

        return true;
    }

    public boolean transitionToDisabling(final ControllerServiceState expectedState, final CompletableFuture<?> disabledFuture) {
        writeLock.lock();
        try {
            if (expectedState != state) {
                logger.debug("{} cannot be transitioned to DISABLING because its state is {}, not the expected {}", controllerServiceNode, state, expectedState);
                return false;
            }

            state = ControllerServiceState.DISABLING;
            stateChangeCondition.signalAll();
            disabledFutures.add(disabledFuture);
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    public void disable() {
        writeLock.lock();
        try {
            state = ControllerServiceState.DISABLED;
            logger.debug("{} transitioned to DISABLED", controllerServiceNode);

            stateChangeCondition.signalAll();
            disabledFutures.forEach(future -> future.complete(null));
        } finally {
            writeLock.unlock();
        }
    }

    public ControllerServiceState getState() {
        readLock.lock();
        try {
            return state;
        } finally {
            readLock.unlock();
        }
    }

    public boolean awaitState(final ControllerServiceState desiredState, final long timePeriod, final TimeUnit timeUnit) throws InterruptedException {
        Objects.requireNonNull(timeUnit);
        final long timeout = System.currentTimeMillis() + timeUnit.toMillis(timePeriod);

        writeLock.lock();
        try {
            while (desiredState != state) {
                final long millisLeft = timeout - System.currentTimeMillis();
                if (millisLeft <= 0) {
                    return false;
                }

                logger.debug("State of {} is currently {}. Will wait up to {} milliseconds for state to transition to {}", controllerServiceNode, state, millisLeft, desiredState);

                stateChangeCondition.await(millisLeft, TimeUnit.MILLISECONDS);
            }

            return true;
        } finally {
            writeLock.unlock();
        }
    }
}
