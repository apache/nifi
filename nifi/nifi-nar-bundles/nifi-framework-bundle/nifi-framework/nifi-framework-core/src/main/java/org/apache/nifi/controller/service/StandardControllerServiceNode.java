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
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ConfiguredComponent;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.controller.annotation.OnConfigured;
import org.apache.nifi.controller.exception.ProcessorLifeCycleException;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.util.ReflectionUtils;

public class StandardControllerServiceNode extends AbstractConfiguredComponent implements ControllerServiceNode {

    private final ControllerService proxedControllerService;
    private final ControllerService implementation;
    private final ControllerServiceProvider serviceProvider;

    private final AtomicReference<Availability> availability = new AtomicReference<>(Availability.NODE_ONLY);
    private final AtomicBoolean disabled = new AtomicBoolean(true);

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    private final Set<ConfiguredComponent> referencingComponents = new HashSet<>();

    public StandardControllerServiceNode(final ControllerService proxiedControllerService, final ControllerService implementation, final String id,
            final ValidationContextFactory validationContextFactory, final ControllerServiceProvider serviceProvider) {
        super(proxiedControllerService, id, validationContextFactory, serviceProvider);
        this.proxedControllerService = proxiedControllerService;
        this.implementation = implementation;
        this.serviceProvider = serviceProvider;
    }

    @Override
    public boolean isDisabled() {
        return disabled.get();
    }

    @Override
    public void setDisabled(final boolean disabled) {
        if (!disabled && !isValid()) {
            throw new IllegalStateException("Cannot enable Controller Service " + implementation + " because it is not valid");
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
    public ControllerService getProxiedControllerService() {
        return proxedControllerService;
    }
    
    @Override
    public ControllerService getControllerServiceImplementation() {
        return implementation;
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
    
    @Override
    public void setProperty(final String name, final String value) {
        super.setProperty(name, value);
        
        onConfigured();
    }
    
    @Override
    public boolean removeProperty(String name) {
        final boolean removed = super.removeProperty(name);
        if ( removed ) {
            onConfigured();
        }
        
        return removed;
    }
    
    private void onConfigured() {
        try (final NarCloseable x = NarCloseable.withNarLoader()) {
            final ConfigurationContext configContext = new StandardConfigurationContext(this, serviceProvider);
            ReflectionUtils.invokeMethodsWithAnnotation(OnConfigured.class, implementation, configContext);
        } catch (final Exception e) {
            throw new ProcessorLifeCycleException("Failed to invoke On-Configured Lifecycle methods of " + implementation, e);
        }
    }
    
    @Override
    public void verifyCanDelete() {
        if ( !isDisabled() ) {
            throw new IllegalStateException(implementation + " cannot be deleted because it is not disabled");
        }
    }
    
    @Override
    public void verifyCanDisable() {
        final ControllerServiceReference references = getReferences();
        final int numRunning = references.getRunningReferences().size();
        if ( numRunning > 0 ) {
            throw new IllegalStateException(implementation + " cannot be disabled because it is referenced by " + numRunning + " components that are currently running");
        }
    }
    
    @Override
    public void verifyCanEnable() {
        if ( !isDisabled() ) {
            throw new IllegalStateException(implementation + " cannot be enabled because it is not disabled");
        }
    }
    
    @Override
    public void verifyCanUpdate() {
        if ( !isDisabled() ) {
            throw new IllegalStateException(implementation + " cannot be updated because it is not disabled");
        }
    }
}
