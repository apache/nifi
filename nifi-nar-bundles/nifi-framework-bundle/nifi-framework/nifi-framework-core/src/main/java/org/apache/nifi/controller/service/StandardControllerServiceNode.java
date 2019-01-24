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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.documentation.DeprecationNotice;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.controller.AbstractComponentNode;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.LoggableComponent;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.TerminationAwareLogger;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.controller.exception.ControllerServiceInstantiationException;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.processor.SimpleProcessLogger;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.apache.nifi.util.CharacterFilterUtils;
import org.apache.nifi.util.ReflectionUtils;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StandardControllerServiceNode extends AbstractComponentNode implements ControllerServiceNode {

    private static final Logger LOG = LoggerFactory.getLogger(StandardControllerServiceNode.class);

    private final AtomicReference<ControllerServiceDetails> controllerServiceHolder = new AtomicReference<>(null);
    private final ControllerServiceProvider serviceProvider;
    private final ServiceStateTransition stateTransition;
    private final AtomicReference<String> versionedComponentId = new AtomicReference<>();

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    private final Set<ComponentNode> referencingComponents = new HashSet<>();
    private String comment;
    private ProcessGroup processGroup;

    private final AtomicBoolean active;

    public StandardControllerServiceNode(final LoggableComponent<ControllerService> implementation, final LoggableComponent<ControllerService> proxiedControllerService,
                                         final ControllerServiceInvocationHandler invocationHandler, final String id, final ValidationContextFactory validationContextFactory,
                                         final ControllerServiceProvider serviceProvider, final ComponentVariableRegistry variableRegistry, final ReloadComponent reloadComponent,
                                         final ExtensionManager extensionManager, final ValidationTrigger validationTrigger) {

        this(implementation, proxiedControllerService, invocationHandler, id, validationContextFactory, serviceProvider, implementation.getComponent().getClass().getSimpleName(),
            implementation.getComponent().getClass().getCanonicalName(), variableRegistry, reloadComponent, extensionManager, validationTrigger, false);
    }

    public StandardControllerServiceNode(final LoggableComponent<ControllerService> implementation, final LoggableComponent<ControllerService> proxiedControllerService,
                                         final ControllerServiceInvocationHandler invocationHandler, final String id, final ValidationContextFactory validationContextFactory,
                                         final ControllerServiceProvider serviceProvider, final String componentType, final String componentCanonicalClass,
                                         final ComponentVariableRegistry variableRegistry, final ReloadComponent reloadComponent, final ExtensionManager extensionManager,
                                         final ValidationTrigger validationTrigger, final boolean isExtensionMissing) {

        super(id, validationContextFactory, serviceProvider, componentType, componentCanonicalClass, variableRegistry, reloadComponent, extensionManager, validationTrigger, isExtensionMissing);
        this.serviceProvider = serviceProvider;
        this.active = new AtomicBoolean();
        setControllerServiceAndProxy(implementation, proxiedControllerService, invocationHandler);
        stateTransition = new ServiceStateTransition(this);
    }

    @Override
    public ConfigurableComponent getComponent() {
        return controllerServiceHolder.get().getImplementation();
    }

    @Override
    public TerminationAwareLogger getLogger() {
        return controllerServiceHolder.get().getComponentLog();
    }

    @Override
    public BundleCoordinate getBundleCoordinate() {
        return controllerServiceHolder.get().getBundleCoordinate();
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
    public boolean isRestricted() {
        return getControllerServiceImplementation().getClass().isAnnotationPresent(Restricted.class);
    }

    @Override
    public Class<?> getComponentClass() {
        return getControllerServiceImplementation().getClass();
    }

    @Override
    public boolean isDeprecated() {
        return getControllerServiceImplementation().getClass().isAnnotationPresent(DeprecationNotice.class);
    }

    @Override
    public ControllerService getControllerServiceImplementation() {
        return controllerServiceHolder.get().getImplementation();
    }

    @Override
    public ControllerService getProxiedControllerService() {
        return controllerServiceHolder.get().getProxiedControllerService();
    }

    @Override
    public ControllerServiceInvocationHandler getInvocationHandler() {
        return controllerServiceHolder.get().getInvocationHandler();
    }

    @Override
    public void setControllerServiceAndProxy(final LoggableComponent<ControllerService> implementation,
                                             final LoggableComponent<ControllerService> proxiedControllerService,
                                             final ControllerServiceInvocationHandler invocationHandler) {
        synchronized (this.active) {
            if (isActive()) {
                throw new IllegalStateException("Cannot modify Controller Service configuration while service is active");
            }

            final ControllerServiceDetails controllerServiceDetails = new ControllerServiceDetails(implementation, proxiedControllerService, invocationHandler);
            this.controllerServiceHolder.set(controllerServiceDetails);
        }
    }

    @Override
    public void reload(final Set<URL> additionalUrls) throws ControllerServiceInstantiationException {
        synchronized (this.active) {
            if (isActive()) {
                throw new IllegalStateException("Cannot reload Controller Service while service is active");
            }
            String additionalResourcesFingerprint = ClassLoaderUtils.generateAdditionalUrlsFingerprint(additionalUrls);
            setAdditionalResourcesFingerprint(additionalResourcesFingerprint);
            getReloadComponent().reload(this, getCanonicalClassName(), getBundleCoordinate(), additionalUrls);
        }
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
            LOG.debug("Resetting Validation State of {} due to setting process group", this);
            resetValidationState();
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
    public void addReference(final ComponentNode referencingComponent) {
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
            }
        }
        return new ArrayList<>(requiredServices);
    }


    @Override
    public void removeReference(final ComponentNode referencingComponent) {
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
    public void verifyCanDelete() {
        if (getState() != ControllerServiceState.DISABLED) {
            throw new IllegalStateException("Controller Service " + getControllerServiceImplementation().getIdentifier() + " cannot be deleted because it is not disabled");
        }
    }

    @Override
    public void verifyCanDisable() {
        verifyCanDisable(Collections.<ControllerServiceNode>emptySet());
    }

    @Override
    public void verifyCanDisable(final Set<ControllerServiceNode> ignoreReferences) {
        if (!this.isActive()) {
            throw new IllegalStateException("Cannot disable " + getControllerServiceImplementation().getIdentifier() + " because it is not enabled");
        }

        final ControllerServiceReference references = getReferences();

        final Set<String> activeReferencesIdentifiers = new HashSet<>();
        for (final ComponentNode activeReference : references.getActiveReferences()) {
            if (!ignoreReferences.contains(activeReference)) {
                activeReferencesIdentifiers.add(activeReference.getIdentifier());
            }
        }

        if (!activeReferencesIdentifiers.isEmpty()) {
            throw new IllegalStateException(getControllerServiceImplementation().getIdentifier() + " cannot be disabled because it is referenced by " + activeReferencesIdentifiers.size() +
                " components that are currently running: [" + StringUtils.join(activeReferencesIdentifiers, ", ") + "]");
        }
    }

    @Override
    public void verifyCanEnable() {
        if (getState() != ControllerServiceState.DISABLED) {
            throw new IllegalStateException(getControllerServiceImplementation().getIdentifier() + " cannot be enabled because it is not disabled");
        }
    }

    @Override
    public void verifyCanEnable(final Set<ControllerServiceNode> ignoredReferences) {
        if (getState() != ControllerServiceState.DISABLED) {
            throw new IllegalStateException(getControllerServiceImplementation().getIdentifier() + " cannot be enabled because it is not disabled");
        }
    }

    @Override
    public void verifyCanUpdate() {
        if (getState() != ControllerServiceState.DISABLED) {
            throw new IllegalStateException(getControllerServiceImplementation().getIdentifier() + " cannot be updated because it is not disabled");
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
            this.comment = CharacterFilterUtils.filterInvalidXmlCharacters(comment);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public ControllerServiceState getState() {
        return stateTransition.getState();
    }

    @Override
    public boolean isActive() {
        return this.active.get();
    }

    @Override
    public boolean isValidationNecessary() {
        switch (getState()) {
            case DISABLED:
            case DISABLING:
                return true;
            case ENABLING:
                // If enabling and currently not valid, then we must trigger validation to occur. This allows the #enable method
                // to continue running in the background and complete enabling when the service becomes valid.
                return getValidationStatus() != ValidationStatus.VALID;
            case ENABLED:
            default:
                return false;
        }
    }

    /**
     * Will atomically enable this service by invoking its @OnEnabled operation.
     * It uses CAS operation on {@link #stateTransition} to transition this service
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
    public CompletableFuture<Void> enable(final ScheduledExecutorService scheduler, final long administrativeYieldMillis) {
        final CompletableFuture<Void> future = new CompletableFuture<>();

        if (this.stateTransition.transitionToEnabling(ControllerServiceState.DISABLED, future)) {
            synchronized (active) {
                this.active.set(true);
            }

            final StandardControllerServiceNode service = this;
            final ConfigurationContext configContext = new StandardConfigurationContext(this, this.serviceProvider, null, getVariableRegistry());
            scheduler.execute(new Runnable() {
                @Override
                public void run() {
                    if (!isActive()) {
                        LOG.debug("{} is no longer active so will not attempt to enable it", StandardControllerServiceNode.this);
                        stateTransition.disable();
                        return;
                    }

                    final ValidationStatus validationStatus = getValidationStatus();
                    if (validationStatus != ValidationStatus.VALID) {
                        LOG.debug("Cannot enable {} because it is not currently valid. Will try again in 5 seconds", StandardControllerServiceNode.this);
                        scheduler.schedule(this, 5, TimeUnit.SECONDS);
                        future.completeExceptionally(new RuntimeException(this + " cannot be enabled because it is not currently valid. Will try again in 5 seconds."));
                        return;
                    }

                    try {
                        try (final NarCloseable nc = NarCloseable.withComponentNarLoader(getExtensionManager(), getControllerServiceImplementation().getClass(), getIdentifier())) {
                            ReflectionUtils.invokeMethodsWithAnnotation(OnEnabled.class, getControllerServiceImplementation(), configContext);
                        }

                        boolean shouldEnable;
                        synchronized (active) {
                            shouldEnable = active.get() && stateTransition.enable();
                        }

                        if (!shouldEnable) {
                            LOG.debug("Disabling service {} after it has been enabled due to disable action being initiated.", service);
                            // Can only happen if user initiated DISABLE operation before service finished enabling. It's state will be
                            // set to DISABLING (see disable() operation)
                            invokeDisable(configContext);
                            stateTransition.disable();
                        } else {
                            LOG.info("Successfully enabled {}", service);
                        }
                    } catch (Exception e) {
                        future.completeExceptionally(e);

                        final Throwable cause = e instanceof InvocationTargetException ? e.getCause() : e;
                        final ComponentLog componentLog = new SimpleProcessLogger(getIdentifier(), StandardControllerServiceNode.this);
                        componentLog.error("Failed to invoke @OnEnabled method due to {}", cause);
                        LOG.error("Failed to invoke @OnEnabled method of {} due to {}", getControllerServiceImplementation(), cause.toString());
                        invokeDisable(configContext);

                        if (isActive()) {
                            scheduler.schedule(this, administrativeYieldMillis, TimeUnit.MILLISECONDS);
                        } else {
                            try (final NarCloseable nc = NarCloseable.withComponentNarLoader(getExtensionManager(), getControllerServiceImplementation().getClass(), getIdentifier())) {
                                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnDisabled.class, getControllerServiceImplementation(), configContext);
                            }
                            stateTransition.disable();
                        }
                    }
                }
            });
        } else {
            future.complete(null);
        }

        return future;
    }


    /**
     * Will atomically disable this service by invoking its @OnDisabled operation.
     * It uses CAS operation on {@link #stateTransition} to transition this service
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
    public CompletableFuture<Void> disable(ScheduledExecutorService scheduler) {
        /*
         * The reason for synchronization is to ensure consistency of the
         * service state when another thread is in the middle of enabling this
         * service since it will attempt to transition service state from
         * ENABLING to ENABLED but only if it's active.
         */
        synchronized (this.active) {
            this.active.set(false);
        }

        final CompletableFuture<Void> future = new CompletableFuture<>();
        if (this.stateTransition.transitionToDisabling(ControllerServiceState.ENABLED, future)) {
            final ConfigurationContext configContext = new StandardConfigurationContext(this, this.serviceProvider, null, getVariableRegistry());
            scheduler.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        invokeDisable(configContext);
                    } finally {
                        stateTransition.disable();

                        // Now all components that reference this service will be invalid. Trigger validation to occur so that
                        // this is reflected in any response that may go back to a user/client.
                        for (final ComponentNode component : getReferences().getReferencingComponents()) {
                            component.performValidation();
                        }
                    }
                }
            });
        } else {
            this.stateTransition.transitionToDisabling(ControllerServiceState.ENABLING, future);
        }

        return future;
    }


    private void invokeDisable(ConfigurationContext configContext) {
        try (final NarCloseable nc = NarCloseable.withComponentNarLoader(getExtensionManager(), getControllerServiceImplementation().getClass(), getIdentifier())) {
            ReflectionUtils.invokeMethodsWithAnnotation(OnDisabled.class, StandardControllerServiceNode.this.getControllerServiceImplementation(), configContext);
            LOG.debug("Successfully disabled {}", this);
        } catch (Exception e) {
            final Throwable cause = e instanceof InvocationTargetException ? e.getCause() : e;
            final ComponentLog componentLog = new SimpleProcessLogger(getIdentifier(), StandardControllerServiceNode.this);
            componentLog.error("Failed to invoke @OnDisabled method due to {}", cause);
            LOG.error("Failed to invoke @OnDisabled method of {} due to {}", getControllerServiceImplementation(), cause.toString());
        }
    }

    @Override
    public String getProcessGroupIdentifier() {
        final ProcessGroup procGroup = getProcessGroup();
        return procGroup == null ? null : procGroup.getIdentifier();
    }

    @Override
    public Optional<String> getVersionedComponentId() {
        return Optional.ofNullable(versionedComponentId.get());
    }

    @Override
    public void setVersionedComponentId(final String versionedComponentId) {
        boolean updated = false;
        while (!updated) {
            final String currentId = this.versionedComponentId.get();

            if (currentId == null) {
                updated = this.versionedComponentId.compareAndSet(null, versionedComponentId);
            } else if (currentId.equals(versionedComponentId)) {
                return;
            } else if (versionedComponentId == null) {
                updated = this.versionedComponentId.compareAndSet(currentId, null);
            } else {
                throw new IllegalStateException(this + " is already under version control");
            }
        }
    }
}
