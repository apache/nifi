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

import org.apache.nifi.controller.ConfiguredComponent;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.LoggableComponent;
import org.apache.nifi.groups.ProcessGroup;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

public interface ControllerServiceNode extends ConfiguredComponent {

    /**
     * @return the Process Group that this Controller Service belongs to, or <code>null</code> if the Controller Service
     *         does not belong to any Process Group
     */
    ProcessGroup getProcessGroup();

    /**
     * Sets the Process Group for this Controller Service
     *
     * @param group the group that the service belongs to
     */
    void setProcessGroup(ProcessGroup group);

    /**
     * <p>
     * Returns a proxy implementation of the Controller Service that this ControllerServiceNode
     * encapsulates. The object returned by this method may be passed to other components, as
     * required. Invocations of methods on the object returned will be intercepted and the service's
     * state will be verified before the underlying implementation's method is called.
     * </p>
     *
     * @return a proxied ControllerService that can be addressed outside of the framework.
     */
    ControllerService getProxiedControllerService();

    /**
     * @return the invocation handler being used by the proxy
     */
    ControllerServiceInvocationHandler getInvocationHandler();

    /**
     * Returns the list of services that are required to be enabled before this
     * service is enabled. The returned list is flattened and contains both
     * immediate and transient dependencies.
     *
     * @return list of services required to be enabled before this service is
     *         enabled
     */
    List<ControllerServiceNode> getRequiredControllerServices();

    /**
     * <p>
     * Returns the actual implementation of the Controller Service that this ControllerServiceNode
     * encapsulates. This direct implementation should <strong>NEVER</strong> be passed to another
     * pluggable component. This implementation should be addressed only by the framework itself.
     * If providing the controller service to another pluggable component, provide it with the
     * proxied entity obtained via {@link #getProxiedControllerService()}
     * </p>
     *
     * @return the actual implementation of the Controller Service
     */
    ControllerService getControllerServiceImplementation();

    /**
     * @return the current state of the Controller Service
     */
    ControllerServiceState getState();

     /**
     * Will enable this service. Enabling of the service typically means
     * invoking it's operation that is annotated with @OnEnabled.
     *
     * @param scheduler
     *            implementation of {@link ScheduledExecutorService} used to
     *            initiate service enabling task as well as its re-tries
     * @param administrativeYieldMillis
     *            the amount of milliseconds to wait for administrative yield
     *
     * @return a CompletableFuture that can be used to wait for the service to finish enabling
     */
    CompletableFuture<Void> enable(ScheduledExecutorService scheduler, long administrativeYieldMillis);

    /**
     * Will disable this service. Disabling of the service typically means
     * invoking it's operation that is annotated with @OnDisabled.
     *
     * @param scheduler
     *            implementation of {@link ScheduledExecutorService} used to
     *            initiate service disabling task
     */
    CompletableFuture<Void> disable(ScheduledExecutorService scheduler);

    /**
     * @return the ControllerServiceReference that describes which components are referencing this Controller Service
     */
    ControllerServiceReference getReferences();

    /**
     * Indicates that the given component is now referencing this Controller Service
     * @param referringComponent the component referencing this service
     */
    void addReference(ConfiguredComponent referringComponent);

    /**
     * Indicates that the given component is no longer referencing this Controller Service
     * @param referringComponent the component that is no longer referencing this service
     */
    void removeReference(ConfiguredComponent referringComponent);

    void setComments(String comment);

    String getComments();

    void verifyCanEnable();

    void verifyCanDisable();

    /**
     * Verifies that this Controller Service can be disabled if the provided set
     * of services are also disabled. This is introduced because we can have an
     * instance where A references B, which references C, which references A and
     * we want to disable service C. In this case, the cycle needs to not cause
     * us to fail, so we want to verify that C can be disabled if A and B also
     * are.
     *
     * @param ignoredReferences references to ignore
     */
    void verifyCanDisable(Set<ControllerServiceNode> ignoredReferences);

    /**
     * Verifies that this Controller Service can be enabled if the provided set
     * of services are also enabled. This is introduced because we can have an
     * instance where A reference B, which references C, which references A and
     * we want to enable Service A. In this case, the cycle needs to not cause
     * us to fail, so we want to verify that A can be enabled if A and B also
     * are.
     *
     * @param ignoredReferences to ignore
     */
    void verifyCanEnable(Set<ControllerServiceNode> ignoredReferences);

    void verifyCanDelete();

    void verifyCanUpdate();

    void verifyCanClearState();

    /**
     * Returns 'true' if this service is active. The service is considered to be
     * active if and only if it's
     * {@link #enable(ScheduledExecutorService, long)} operation
     * has been invoked and the service has been transitioned to ENABLING state.
     * The service will also remain 'active' after its been transitioned to
     * ENABLED state. <br>
     * The service will be de-activated upon invocation of
     * {@link #disable(ScheduledExecutorService)}.
     */
    boolean isActive();

    /**
     * Sets a new proxy and implementation for this node.
     *
     * @param implementation the actual implementation controller service
     * @param proxiedControllerService the proxied controller service
     * @param invocationHandler the invocation handler being used by the proxy
     */
    void setControllerServiceAndProxy(final LoggableComponent<ControllerService> implementation,
                                      final LoggableComponent<ControllerService> proxiedControllerService,
                                      final ControllerServiceInvocationHandler invocationHandler);

}
