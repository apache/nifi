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
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.nar.ExtensionManager;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 *
 */
public interface ControllerServiceProvider extends ControllerServiceLookup {

    /**
     * Notifies the ControllerServiceProvider that the given Controller Service has been added to the flow
     * @param serviceNode the Controller Service Node
     */
    void onControllerServiceAdded(ControllerServiceNode serviceNode);

    /**
     * @param id of the service
     * @return the controller service node for the specified identifier. Returns
     * <code>null</code> if the identifier does not match a known service
     */
    ControllerServiceNode getControllerServiceNode(String id);

    /**
     * Removes the given Controller Service from the flow. This will call all
     * appropriate methods that have the @OnRemoved annotation.
     *
     * @param serviceNode the controller service to remove
     *
     * @throws IllegalStateException if the controller service is not disabled
     * or is not a part of this flow
     */
    void removeControllerService(ControllerServiceNode serviceNode);

    /**
     * Enables the given controller service that it can be used by other
     * components. This method will asynchronously enable the service, returning
     * immediately.
     *
     * @param serviceNode the service node
     * @return a Future that can be used to wait for the service to finish being enabled.
     */
    CompletableFuture<Void> enableControllerService(ControllerServiceNode serviceNode);

    /**
     * Enables the collection of services. If a service in this collection
     * depends on another service, the service being depended on must either
     * already be enabled or must be in the collection as well.
     *
     * @param serviceNodes the nodes
     */
    void enableControllerServices(Collection<ControllerServiceNode> serviceNodes);

    /**
     * Enables the collection of services in the background. If a service in this collection
     * depends on another service, the service being depended on must either already be enabled
     * or must be in the collection as well.
     *
     * @param serviceNodes the nodes
     * @return a Future that can be used to cancel the task or wait until it is completed
     */
    Future<Void> enableControllerServicesAsync(Collection<ControllerServiceNode> serviceNodes);

    /**
     * Disables the given controller service so that it cannot be used by other
     * components. This allows configuration to be updated or allows service to
     * be removed.
     *
     * @param serviceNode the node
     */
    CompletableFuture<Void> disableControllerService(ControllerServiceNode serviceNode);

    /**
     * Disables the collection of services in the background. If any of the services given is referenced
     * by another service, then that other service must either be disabled or be in the given collection.
     *
     * @param serviceNodes the nodes the disable
     * @return a Future that can be used to cancel the task or wait until it is completed
     */
    Future<Void> disableControllerServicesAsync(Collection<ControllerServiceNode> serviceNodes);

    /**
     * @return a Set of all Controller Services that exist for this service provider
     */
    Collection<ControllerServiceNode> getNonRootControllerServices();

    /**
     * Verifies that all running Processors and Reporting Tasks referencing the
     * Controller Service (or a service that depends on the provided service)
     * can be stopped.
     *
     * @param serviceNode the node
     *
     * @throws IllegalStateException if any referencing component cannot be
     * stopped
     */
    void verifyCanStopReferencingComponents(ControllerServiceNode serviceNode);

    /**
     * Recursively unschedules all schedulable components (Processors and
     * Reporting Tasks) that reference the given Controller Service. For any
     * Controller services that reference this one, its schedulable referencing
     * components will also be unscheduled.
     *
     * @param serviceNode the node
     */
    Set<ComponentNode> unscheduleReferencingComponents(ControllerServiceNode serviceNode);

    /**
     * Verifies that all Controller Services referencing the provided Controller
     * Service can be disabled.
     *
     * @param serviceNode the node
     *
     * @throws IllegalStateException if any referencing service cannot be
     * disabled
     */
    void verifyCanDisableReferencingServices(ControllerServiceNode serviceNode);

    /**
     * Disables any Controller Service that references the provided Controller
     * Service. This action is performed recursively so that if service A
     * references B and B references C, disabling references for C will first
     * disable A, then B.
     *
     * @param serviceNode the node
     */
    Set<ComponentNode> disableReferencingServices(ControllerServiceNode serviceNode);

    /**
     * Verifies that all Controller Services referencing the provided
     * ControllerService can be enabled.
     *
     * @param serviceNode the node
     *
     * @throws IllegalStateException if any referencing component cannot be
     * enabled
     */
    void verifyCanEnableReferencingServices(ControllerServiceNode serviceNode);

    /**
     * Enables all Controller Services that are referencing the given service.
     * If Service A references Service B and Service B references serviceNode,
     * Service A and B will both be enabled.
     *
     * @param serviceNode the node
     *
     * @return the set of all components that were updated as a result of this action
     */
    Set<ComponentNode> enableReferencingServices(ControllerServiceNode serviceNode);

    /**
     * Verifies that all enabled Processors referencing the ControllerService
     * (or a service that depends on the provided service) can be scheduled to
     * run.
     *
     * @param serviceNode the node
     *
     * @throws IllegalStateException if any referencing component cannot be
     * scheduled
     */
    void verifyCanScheduleReferencingComponents(ControllerServiceNode serviceNode);

    /**
     * Schedules any schedulable component (Processor, ReportingTask) that is
     * referencing the given Controller Service to run. This is performed
     * recursively, so if a Processor is referencing Service A, which is
     * referencing serviceNode, then the Processor will also be started.
     *
     * @param serviceNode the node
     */
    Set<ComponentNode> scheduleReferencingComponents(ControllerServiceNode serviceNode);

    /**
     *
     * @param serviceType type of service to get identifiers for
     * @param groupId the ID of the Process Group to look in for Controller Services
     *
     * @return the set of all Controller Service Identifiers whose Controller
     *         Service is of the given type.
     * @throws IllegalArgumentException if the given class is not an interface
     */
    Set<String> getControllerServiceIdentifiers(Class<? extends ControllerService> serviceType, String groupId) throws IllegalArgumentException;

    /**
     * @param serviceIdentifier the identifier of the controller service
     * @param componentId the identifier of the component that is referencing the service.
     * @return the Controller Service that is registered with the given identifier or <code>null</code> if that
     *         identifier does not exist for any controller service or if the controller service with that identifier is
     *         not accessible from the component with the given componentId, or if no component exists with the given
     *         identifier
     */
    ControllerService getControllerServiceForComponent(String serviceIdentifier, String componentId);

    /**
     * @return the ExtensionManager used by this provider
     */
    ExtensionManager getExtensionManager();

}
