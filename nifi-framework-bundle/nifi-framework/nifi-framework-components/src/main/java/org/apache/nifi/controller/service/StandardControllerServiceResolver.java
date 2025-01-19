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

import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.flow.ControllerServiceAPI;
import org.apache.nifi.flow.ExternalControllerServiceReference;
import org.apache.nifi.flow.VersionedConfigurableExtension;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedPropertyDescriptor;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.registry.flow.FlowSnapshotContainer;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.registry.flow.mapping.NiFiRegistryFlowMapper;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

public class StandardControllerServiceResolver implements ControllerServiceResolver {

    private final Authorizer authorizer;
    private final FlowManager flowManager;
    private final NiFiRegistryFlowMapper flowMapper;
    private final ControllerServiceProvider controllerServiceProvider;
    private final ControllerServiceApiLookup controllerServiceApiLookup;

    public StandardControllerServiceResolver(final Authorizer authorizer,
                                             final FlowManager flowManager,
                                             final NiFiRegistryFlowMapper flowMapper,
                                             final ControllerServiceProvider controllerServiceProvider,
                                             final ControllerServiceApiLookup controllerServiceApiLookup) {
        this.authorizer = authorizer;
        this.flowManager = flowManager;
        this.flowMapper = flowMapper;
        this.controllerServiceProvider = controllerServiceProvider;
        this.controllerServiceApiLookup = controllerServiceApiLookup;
    }

    @Override
    public Set<String> resolveInheritedControllerServices(final FlowSnapshotContainer flowSnapshotContainer, final String parentGroupId, final NiFiUser user) {
        final RegisteredFlowSnapshot topLevelSnapshot = flowSnapshotContainer.getFlowSnapshot();
        final VersionedProcessGroup versionedGroup = topLevelSnapshot.getFlowContents();
        final Map<String, ExternalControllerServiceReference> externalControllerServiceReferences = topLevelSnapshot.getExternalControllerServices();

        final ProcessGroup parentGroup = flowManager.getGroup(parentGroupId);

        final Set<VersionedControllerService> ancestorServices = parentGroup.getControllerServices(true).stream()
                .filter(serviceNode -> serviceNode.isAuthorized(authorizer, RequestAction.READ, user))
                .map(serviceNode -> flowMapper.mapControllerService(serviceNode, controllerServiceProvider, new HashSet<>(), new HashMap<>()))
                .collect(Collectors.toSet());

        final Stack<Set<VersionedControllerService>> serviceHierarchyStack = new Stack<>();
        serviceHierarchyStack.push(ancestorServices);

        final Set<String> unresolvedServices = new HashSet<>();
        resolveInheritedControllerServices(flowSnapshotContainer, versionedGroup, externalControllerServiceReferences, serviceHierarchyStack, unresolvedServices);
        return unresolvedServices;
    }

    private void resolveInheritedControllerServices(final FlowSnapshotContainer flowSnapshotContainer, final VersionedProcessGroup versionedGroup,
                                                    final Map<String, ExternalControllerServiceReference> externalControllerServiceReferences,
                                                    final Stack<Set<VersionedControllerService>> serviceHierarchyStack,
                                                    final Set<String> unresolvedServices) {

        final Set<VersionedControllerService> currentGroupServices = versionedGroup.getControllerServices() == null ? Collections.emptySet() : versionedGroup.getControllerServices();
        serviceHierarchyStack.push(currentGroupServices);

        final Set<VersionedControllerService> availableControllerServices = serviceHierarchyStack.stream()
                .flatMap(Set::stream)
                .collect(Collectors.toSet());

        for (final VersionedProcessor processor : versionedGroup.getProcessors()) {
            resolveInheritedControllerServices(processor, availableControllerServices, externalControllerServiceReferences, unresolvedServices);
        }

        for (final VersionedControllerService service : versionedGroup.getControllerServices()) {
            resolveInheritedControllerServices(service, availableControllerServices, externalControllerServiceReferences, unresolvedServices);
        }

        // If the child group is under version, the external service references need to come from the snapshot of the
        // child instead of what was passed into this method which was for the parent group
        for (final VersionedProcessGroup child : versionedGroup.getProcessGroups()) {
            final Map<String, ExternalControllerServiceReference> childExternalServices;
            if (child.getVersionedFlowCoordinates() == null) {
                childExternalServices = externalControllerServiceReferences;
            } else {
                final RegisteredFlowSnapshot childSnapshot = flowSnapshotContainer.getChildSnapshot(child.getIdentifier());
                if (childSnapshot == null) {
                    childExternalServices = Collections.emptyMap();
                } else {
                    childExternalServices = childSnapshot.getExternalControllerServices();
                }
            }
            resolveInheritedControllerServices(flowSnapshotContainer, child, childExternalServices, serviceHierarchyStack, unresolvedServices);
        }

        serviceHierarchyStack.pop();
    }

    private void resolveInheritedControllerServices(final VersionedConfigurableExtension component, final Set<VersionedControllerService> availableControllerServices,
                                                    final Map<String, ExternalControllerServiceReference> externalControllerServiceReferences,
                                                    final Set<String> unresolvedServices) {

        final Map<String, ControllerServiceAPI> componentRequiredApis = controllerServiceApiLookup.getRequiredServiceApis(component.getType(), component.getBundle());
        if (componentRequiredApis.isEmpty()) {
            return;
        }

        final Map<String, VersionedPropertyDescriptor> propertyDescriptors = component.getPropertyDescriptors();
        final Map<String, String> componentProperties = component.getProperties();

        for (final Map.Entry<String, String> entry : componentProperties.entrySet()) {
            final String propertyName = entry.getKey();
            final String propertyValue = entry.getValue();

            // if the property isn't set there is nothing to resolve
            if (propertyValue == null) {
                continue;
            }

            final VersionedPropertyDescriptor propertyDescriptor = propertyDescriptors.get(propertyName);
            if (propertyDescriptor == null) {
                continue;
            }

            if (!propertyDescriptor.getIdentifiesControllerService()) {
                continue;
            }

            final Set<String> availableControllerServiceIds = availableControllerServices.stream()
                    .map(VersionedControllerService::getIdentifier)
                    .collect(Collectors.toSet());

            // If the referenced Controller Service is available, there is nothing to resolve.
            if (availableControllerServiceIds.contains(propertyValue)) {
                unresolvedServices.add(propertyValue);
                continue;
            }

            final ExternalControllerServiceReference externalServiceReference = externalControllerServiceReferences == null ? null : externalControllerServiceReferences.get(propertyValue);
            if (externalServiceReference == null) {
                unresolvedServices.add(propertyValue);
                continue;
            }

            final ControllerServiceAPI descriptorRequiredApi = componentRequiredApis.get(propertyName);
            if (descriptorRequiredApi == null) {
                unresolvedServices.add(propertyValue);
                continue;
            }

            final String externalControllerServiceName = externalServiceReference.getName();
            final List<VersionedControllerService> matchingControllerServices = availableControllerServices.stream()
                    .filter(service -> service.getName().equals(externalControllerServiceName))
                    .filter(service -> implementsApi(descriptorRequiredApi, service))
                    .collect(Collectors.toList());

            if (matchingControllerServices.size() != 1) {
                unresolvedServices.add(propertyValue);
                continue;
            }

            final VersionedControllerService matchingService = matchingControllerServices.get(0);
            final String resolvedId = matchingService.getIdentifier();
            componentProperties.put(propertyName, resolvedId);
        }
    }

    private boolean implementsApi(final ControllerServiceAPI requiredServiceApi, final VersionedControllerService versionedControllerService) {
        if (versionedControllerService.getControllerServiceApis() == null) {
            return false;
        }

        for (final ControllerServiceAPI implementedApi : versionedControllerService.getControllerServiceApis()) {
            if (implementedApi.getType().equals(requiredServiceApi.getType())
                    && implementedApi.getBundle().getGroup().equals(requiredServiceApi.getBundle().getGroup())
                    && implementedApi.getBundle().getArtifact().equals(requiredServiceApi.getBundle().getArtifact())) {
                return true;
            }
        }

        return false;
    }

}
