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

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractConfiguredComponent;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ConfiguredComponent;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.SimpleProcessLogger;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandardControllerServiceNode extends AbstractConfiguredComponent implements ControllerServiceNode {

    private static final Logger LOG = LoggerFactory.getLogger(StandardControllerServiceNode.class);

    private final ControllerService proxedControllerService;
    private final ControllerService implementation;
    private final ControllerServiceProvider serviceProvider;
    private final VariableRegistry variableRegistry;
    private final AtomicReference<ControllerServiceState> stateRef = new AtomicReference<>(ControllerServiceState.DISABLED);

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    private final Set<ConfiguredComponent> referencingComponents = new HashSet<>();
    private String comment;
    private ProcessGroup processGroup;

    private final AtomicBoolean active;

    public StandardControllerServiceNode(final ControllerService proxiedControllerService, final ControllerService implementation, final String id,
                                         final ValidationContextFactory validationContextFactory, final ControllerServiceProvider serviceProvider,
                                         final VariableRegistry variableRegistry) {

        this(proxiedControllerService, implementation, id, validationContextFactory, serviceProvider,
            implementation.getClass().getSimpleName(), implementation.getClass().getCanonicalName(), variableRegistry);
    }

    public StandardControllerServiceNode(final ControllerService proxiedControllerService, final ControllerService implementation, final String id,
                                         final ValidationContextFactory validationContextFactory, final ControllerServiceProvider serviceProvider,
                                         final String componentType, final String componentCanonicalClass, VariableRegistry variableRegistry) {

        super(implementation, id, validationContextFactory, serviceProvider, componentType, componentCanonicalClass);
        this.proxedControllerService = proxiedControllerService;
        this.implementation = implementation;
        this.serviceProvider = serviceProvider;
        this.active = new AtomicBoolean();
        this.variableRegistry = variableRegistry;

    }

    @Override
    public Authorizable getParentAuthorizable() {
        final ProcessGroup processGroup = getProcessGroup();
        if (processGroup == null) {
            return new Authorizable() {
                @Override
                public Authorizable getParentAuthorizable() {
                    return null;
                }

                @Override
                public Resource getResource() {
                    return ResourceFactory.getControllerResource();
                }
            };
        } else {
            return processGroup;
        }
    }

    @Override
    public Resource getResource() {
        return ResourceFactory.getComponentResource(ResourceType.ControllerService, getIdentifier(), getName());
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
    public ProcessGroup getProcessGroup() {
        readLock.lock();
        try {
            return processGroup;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void setProcessGroup(final ProcessGroup group) {
        writeLock.lock();
        try {
            this.processGroup = group;
        } finally {
            writeLock.unlock();
        }
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
    public List<ControllerServiceNode> getRequiredControllerServices() {
        Set<ControllerServiceNode> requiredServices = new HashSet<>();
        for (Entry<PropertyDescriptor, String> entry : getProperties().entrySet()) {
            PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.getControllerServiceDefinition() != null && entry.getValue() != null) {
                ControllerServiceNode requiredNode = serviceProvider.getControllerServiceNode(entry.getValue());
                requiredServices.add(requiredNode);
                requiredServices.addAll(requiredNode.getRequiredControllerServices());
            }
        }
        return new ArrayList<>(requiredServices);
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
    }

    @Override
    public boolean removeProperty(String name) {
        return super.removeProperty(name);
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
        if (!this.isActive()) {
            throw new IllegalStateException("Cannot disable " + getControllerServiceImplementation() + " because it is not enabled");
        }

        final ControllerServiceReference references = getReferences();

        final Set<ConfiguredComponent> activeReferences = new HashSet<>();
        for (final ConfiguredComponent activeReference : references.getActiveReferences()) {
            if (!ignoreReferences.contains(activeReference)) {
                activeReferences.add(activeReference);
            }
        }

        if (!activeReferences.isEmpty()) {
            throw new IllegalStateException(implementation + " cannot be disabled because it is referenced by " + activeReferences.size() +
                " components that are currently running: " + activeReferences);
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
    public void verifyCanClearState() {
        verifyCanUpdate();
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
    public boolean isActive() {
        return this.active.get();
    }

    /**
     * Will atomically enable this service by invoking its @OnEnabled operation.
     * It uses CAS operation on {@link #stateRef} to transition this service
     * from DISABLED to ENABLING state. If such transition succeeds the service
     * will be marked as 'active' (see {@link ControllerServiceNode#isActive()}).
     * If such transition doesn't succeed then no enabling logic will be
     * performed and the method will exit. In other words it is safe to invoke
     * this operation multiple times and from multiple threads.
     * <br>
     * This operation will also perform re-try of service enabling in the event
     * of exception being thrown by previous invocation of @OnEnabled.
     * <br>
     * Upon successful invocation of @OnEnabled this service will be transitioned to
     * ENABLED state.
     * <br>
     * In the event where enabling took longer then expected by the user and such user
     * initiated disable operation, this service will be automatically disabled as soon
     * as it reached ENABLED state.
     */
    @Override
    public void enable(final ScheduledExecutorService scheduler, final long administrativeYieldMillis) {
        if (this.stateRef.compareAndSet(ControllerServiceState.DISABLED, ControllerServiceState.ENABLING)) {
            this.active.set(true);
            final ConfigurationContext configContext = new StandardConfigurationContext(this, this.serviceProvider, null, variableRegistry);
            scheduler.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        ReflectionUtils.invokeMethodsWithAnnotation(OnEnabled.class, getControllerServiceImplementation(), configContext);
                        boolean shouldEnable = false;
                        synchronized (active) {
                            shouldEnable = active.get() && stateRef.compareAndSet(ControllerServiceState.ENABLING, ControllerServiceState.ENABLED);
                        }
                        if (!shouldEnable) {
                            LOG.debug("Disabling service " + this + " after it has been enabled due to disable action being initiated.");
                            // Can only happen if user initiated DISABLE operation before service finished enabling. It's state will be
                            // set to DISABLING (see disable() operation)
                            invokeDisable(configContext);
                            stateRef.set(ControllerServiceState.DISABLED);
                        }
                    } catch (Exception e) {
                        final Throwable cause = e instanceof InvocationTargetException ? e.getCause() : e;
                        final ComponentLog componentLog = new SimpleProcessLogger(getIdentifier(), StandardControllerServiceNode.this);
                        componentLog.error("Failed to invoke @OnEnabled method due to {}", cause);
                        LOG.error("Failed to invoke @OnEnabled method of {} due to {}", getControllerServiceImplementation(), cause.toString());
                        invokeDisable(configContext);

                        if (isActive()) {
                            scheduler.schedule(this, administrativeYieldMillis, TimeUnit.MILLISECONDS);
                        } else {
                            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnDisabled.class, getControllerServiceImplementation(), configContext);
                            stateRef.set(ControllerServiceState.DISABLED);
                        }
                    }
                }
            });
        }
    }

    /**
     * Will atomically disable this service by invoking its @OnDisabled operation.
     * It uses CAS operation on {@link #stateRef} to transition this service
     * from ENABLED to DISABLING state. If such transition succeeds the service
     * will be de-activated (see {@link ControllerServiceNode#isActive()}).
     * If such transition doesn't succeed (the service is still in ENABLING state)
     * then the service will still be transitioned to DISABLING state to ensure that
     * no other transition could happen on this service. However in such event
     * (e.g., its @OnEnabled finally succeeded), the {@link #enable(ScheduledExecutorService, long)}
     * operation will initiate service disabling javadoc for (see {@link #enable(ScheduledExecutorService, long)}
     * <br>
     * Upon successful invocation of @OnDisabled this service will be transitioned to
     * DISABLED state.
     */
    @Override
    public void disable(ScheduledExecutorService scheduler) {
        /*
         * The reason for synchronization is to ensure consistency of the
         * service state when another thread is in the middle of enabling this
         * service since it will attempt to transition service state from
         * ENABLING to ENABLED but only if it's active.
         */
        synchronized (this.active) {
            this.active.set(false);
        }

        if (this.stateRef.compareAndSet(ControllerServiceState.ENABLED, ControllerServiceState.DISABLING)) {
            final ConfigurationContext configContext = new StandardConfigurationContext(this, this.serviceProvider, null, variableRegistry);
            scheduler.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        invokeDisable(configContext);
                    } finally {
                        stateRef.set(ControllerServiceState.DISABLED);
                    }
                }
            });
        } else {
            this.stateRef.compareAndSet(ControllerServiceState.ENABLING, ControllerServiceState.DISABLING);
        }
    }

    /**
     *
     */
    private void invokeDisable(ConfigurationContext configContext) {
        try {
            ReflectionUtils.invokeMethodsWithAnnotation(OnDisabled.class, StandardControllerServiceNode.this.getControllerServiceImplementation(), configContext);
        } catch (Exception e) {
            final Throwable cause = e instanceof InvocationTargetException ? e.getCause() : e;
            final ComponentLog componentLog = new SimpleProcessLogger(getIdentifier(), StandardControllerServiceNode.this);
            componentLog.error("Failed to invoke @OnDisabled method due to {}", cause);
            LOG.error("Failed to invoke @OnDisabled method of {} due to {}", getControllerServiceImplementation(), cause.toString());
        }
    }

    @Override
    protected String getProcessGroupIdentifier() {
        final ProcessGroup procGroup = getProcessGroup();
        return procGroup == null ? null : procGroup.getIdentifier();
    }
}
