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

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock writeLock = rwLock.writeLock();
    private final Lock readLock = rwLock.readLock();
    private final Condition stateChangeCondition = writeLock.newCondition();

    public boolean transitionToEnabling(final ControllerServiceState expectedState, final CompletableFuture<?> enabledFuture) {
        writeLock.lock();
        try {
            if (expectedState != state) {
                return false;
            }

            state = ControllerServiceState.ENABLING;
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
                return false;
            }

            state = ControllerServiceState.ENABLED;

            enabledFutures.forEach(future -> future.complete(null));

            final List<ComponentNode> referencingComponents = controllerServiceReference.findRecursiveReferences(ComponentNode.class);
            for (final ComponentNode component : referencingComponents) {
                component.performValidation();
            }

            stateChangeCondition.signalAll();

            return true;
        } finally {
            writeLock.unlock();
        }
    }

    public boolean transitionToDisabling(final ControllerServiceState expectedState, final CompletableFuture<?> disabledFuture) {
        writeLock.lock();
        try {
            if (expectedState != state) {
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

                stateChangeCondition.await(millisLeft, TimeUnit.MILLISECONDS);
            }

            return true;
        } finally {
            writeLock.unlock();
        }
    }
}
