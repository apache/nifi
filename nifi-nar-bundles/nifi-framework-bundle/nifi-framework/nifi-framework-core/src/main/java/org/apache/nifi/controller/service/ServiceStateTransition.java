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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ServiceStateTransition {
    private ControllerServiceState state = ControllerServiceState.DISABLED;
    private final List<CompletableFuture<?>> enabledFutures = new ArrayList<>();
    private final List<CompletableFuture<?>> disabledFutures = new ArrayList<>();

    private final ControllerServiceNode serviceNode;
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock writeLock = rwLock.writeLock();
    private final Lock readLock = rwLock.readLock();

    public ServiceStateTransition(final ControllerServiceNode serviceNode) {
        this.serviceNode = serviceNode;
    }

    public boolean transitionToEnabling(final ControllerServiceState expectedState, final CompletableFuture<?> enabledFuture) {
        writeLock.lock();
        try {
            if (expectedState != state) {
                return false;
            }

            state = ControllerServiceState.ENABLING;
            enabledFutures.add(enabledFuture);
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    public boolean enable() {
        writeLock.lock();
        try {
            if (state != ControllerServiceState.ENABLING) {
                return false;
            }

            state = ControllerServiceState.ENABLED;

            validateReferences(serviceNode);

            enabledFutures.stream().forEach(future -> future.complete(null));
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    private void validateReferences(final ControllerServiceNode service) {
        final List<ComponentNode> referencingComponents = service.getReferences().findRecursiveReferences(ComponentNode.class);
        for (final ComponentNode component : referencingComponents) {
            component.performValidation();
        }
    }

    public boolean transitionToDisabling(final ControllerServiceState expectedState, final CompletableFuture<?> disabledFuture) {
        writeLock.lock();
        try {
            if (expectedState != state) {
                return false;
            }

            state = ControllerServiceState.DISABLING;
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
            disabledFutures.stream().forEach(future -> future.complete(null));
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
}
