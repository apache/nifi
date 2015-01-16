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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.nifi.controller.AbstractConfiguredComponent;
import org.apache.nifi.controller.Availability;
import org.apache.nifi.controller.ConfiguredComponent;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ValidationContextFactory;

public class StandardControllerServiceNode extends AbstractConfiguredComponent implements ControllerServiceNode {

    private final ControllerService controllerService;

    private final AtomicReference<Availability> availability = new AtomicReference<>(Availability.NODE_ONLY);
    private final AtomicBoolean disabled = new AtomicBoolean(true);

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    private final Set<ConfiguredComponent> referencingComponents = new HashSet<>();

    public StandardControllerServiceNode(final ControllerService controllerService, final String id,
            final ValidationContextFactory validationContextFactory, final ControllerServiceProvider serviceProvider) {
        super(controllerService, id, validationContextFactory, serviceProvider);
        this.controllerService = controllerService;
    }

    @Override
    public boolean isDisabled() {
        return disabled.get();
    }

    @Override
    public void setDisabled(final boolean disabled) {
        if (!disabled && !isValid()) {
            throw new IllegalStateException("Cannot enable Controller Service " + controllerService + " because it is not valid");
        }

        if (disabled) {
            // do not allow a Controller Service to be disabled if it's currently being used.
            final Set<ConfiguredComponent> runningRefs = getReferences().getRunningReferences();
            if (!runningRefs.isEmpty()) {
                throw new IllegalStateException("Cannot disable Controller Service because it is referenced (either directly or indirectly) by " + runningRefs.size() + " different components that are currently running");
            }
        }

        this.disabled.set(disabled);
    }

    @Override
    public Availability getAvailability() {
        return availability.get();
    }

    @Override
    public void setAvailability(final Availability availability) {
        this.availability.set(availability);
    }

    @Override
    public ControllerService getControllerService() {
        return controllerService;
    }

    @Override
    public ControllerServiceReference getReferences() {
        readLock.lock();
        try {
            return new StandardControllerServiceReference(this, referencingComponents);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void addReference(final ConfiguredComponent referencingComponent) {
        writeLock.lock();
        try {
            referencingComponents.add(referencingComponent);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void removeReference(final ConfiguredComponent referencingComponent) {
        writeLock.lock();
        try {
            referencingComponents.remove(referencingComponent);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void verifyModifiable() throws IllegalStateException {
        if (!isDisabled()) {
            throw new IllegalStateException("Cannot modify Controller Service configuration because it is currently enabled. Please disable the Controller Service first.");
        }
    }
}
