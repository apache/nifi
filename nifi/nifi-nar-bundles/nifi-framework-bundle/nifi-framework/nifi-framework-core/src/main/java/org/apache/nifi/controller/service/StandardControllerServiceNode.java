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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractConfiguredComponent;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ConfiguredComponent;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.controller.annotation.OnConfigured;
import org.apache.nifi.controller.exception.ComponentLifeCycleException;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.util.ReflectionUtils;

public class StandardControllerServiceNode extends AbstractConfiguredComponent implements ControllerServiceNode {

    private final ControllerService proxedControllerService;
    private final ControllerService implementation;
    private final ControllerServiceProvider serviceProvider;

    private final AtomicReference<ControllerServiceState> stateRef = new AtomicReference<>(ControllerServiceState.DISABLED);

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    private final Set<ConfiguredComponent> referencingComponents = new HashSet<>();
    private String comment;

    public StandardControllerServiceNode(final ControllerService proxiedControllerService, final ControllerService implementation, final String id,
            final ValidationContextFactory validationContextFactory, final ControllerServiceProvider serviceProvider) {
        super(implementation, id, validationContextFactory, serviceProvider);
        this.proxedControllerService = proxiedControllerService;
        this.implementation = implementation;
        this.serviceProvider = serviceProvider;
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
        if (getState() != ControllerServiceState.DISABLED) {
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
        if (removed) {
            onConfigured();
        }

        return removed;
    }

    @SuppressWarnings("deprecation")
    private void onConfigured() {
        try (final NarCloseable x = NarCloseable.withNarLoader()) {
            final ConfigurationContext configContext = new StandardConfigurationContext(this, serviceProvider);
            ReflectionUtils.invokeMethodsWithAnnotation(OnConfigured.class, implementation, configContext);
        } catch (final Exception e) {
            throw new ComponentLifeCycleException("Failed to invoke On-Configured Lifecycle methods of " + implementation, e);
        }
    }

    @Override
    public void verifyCanDelete() {
        if (getState() != ControllerServiceState.DISABLED) {
            throw new IllegalStateException(implementation + " cannot be deleted because it is not disabled");
        }
    }

    @Override
    public void verifyCanDisable() {
        verifyCanDisable(Collections.<ControllerServiceNode>emptySet());
    }

    @Override
    public void verifyCanDisable(final Set<ControllerServiceNode> ignoreReferences) {
        final ControllerServiceState state = getState();
        if (state != ControllerServiceState.ENABLED && state != ControllerServiceState.ENABLING) {
            throw new IllegalStateException("Cannot disable " + getControllerServiceImplementation() + " because it is not enabled");
        }

        final ControllerServiceReference references = getReferences();

        for (final ConfiguredComponent activeReference : references.getActiveReferences()) {
            if (!ignoreReferences.contains(activeReference)) {
                throw new IllegalStateException(implementation + " cannot be disabled because it is referenced by at least one component that is currently running");
            }
        }
    }

    @Override
    public void verifyCanEnable() {
        if (getState() != ControllerServiceState.DISABLED) {
            throw new IllegalStateException(implementation + " cannot be enabled because it is not disabled");
        }

        if (!isValid()) {
            throw new IllegalStateException(implementation + " cannot be enabled because it is not valid: " + getValidationErrors());
        }
    }

    @Override
    public void verifyCanEnable(final Set<ControllerServiceNode> ignoredReferences) {
        if (getState() != ControllerServiceState.DISABLED) {
            throw new IllegalStateException(implementation + " cannot be enabled because it is not disabled");
        }

        final Set<String> ids = new HashSet<>();
        for (final ControllerServiceNode node : ignoredReferences) {
            ids.add(node.getIdentifier());
        }

        final Collection<ValidationResult> validationResults = getValidationErrors(ids);
        for (final ValidationResult result : validationResults) {
            if (!result.isValid()) {
                throw new IllegalStateException(implementation + " cannot be enabled because it is not valid: " + result);
            }
        }
    }

    @Override
    public void verifyCanUpdate() {
        if (getState() != ControllerServiceState.DISABLED) {
            throw new IllegalStateException(implementation + " cannot be updated because it is not disabled");
        }
    }

    @Override
    public String getComments() {
        readLock.lock();
        try {
            return comment;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void setComments(final String comment) {
        writeLock.lock();
        try {
            this.comment = comment;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public ControllerServiceState getState() {
        return stateRef.get();
    }

    @Override
    public void setState(final ControllerServiceState state) {
        this.stateRef.set(state);
    }
}
