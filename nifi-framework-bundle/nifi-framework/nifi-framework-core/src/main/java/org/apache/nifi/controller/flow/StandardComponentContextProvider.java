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

package org.apache.nifi.controller.flow;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.connector.facades.standalone.ComponentContextProvider;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.lifecycle.TaskTermination;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.StandardConfigurationContext;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.StandardProcessContext;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class StandardComponentContextProvider implements ComponentContextProvider {
    private final FlowController flowController;

    public StandardComponentContextProvider(final FlowController flowController) {
        this.flowController = flowController;
    }

    @Override
    public ProcessContext createProcessContext(final ProcessorNode processorNode, final ParameterLookup parameterLookup) {
        return createProcessContext(processorNode, Map.of(), parameterLookup);
    }

    @Override
    public ProcessContext createProcessContext(final ProcessorNode processorNode, final Map<String, String> propertiesOverride, final ParameterLookup parameterLookup) {
        final StateManager stateManager = flowController.getStateManagerProvider().getStateManager(processorNode.getIdentifier());
        final TaskTermination taskTermination = () -> false;
        final Map<String, String> serviceReferencedProperties = resolveServiceReferences(processorNode, processorNode.getProcessGroup(), propertiesOverride);
        return new StandardProcessContext(processorNode, serviceReferencedProperties, null, parameterLookup,
            flowController.getControllerServiceProvider(), stateManager, taskTermination, flowController);
    }

    @Override
    public ValidationContext createValidationContext(final ProcessorNode processorNode, final Map<String, String> properties, final ParameterLookup parameterLookup) {
        final Map<String, String> serviceReferencedProperties = resolveServiceReferences(processorNode, processorNode.getProcessGroup(), properties);
        return processorNode.createValidationContext(serviceReferencedProperties, processorNode.getAnnotationData(), parameterLookup, true);
    }

    private Map<String, String> resolveServiceReferences(final ComponentNode processorNode, final ProcessGroup processGroup, final Map<String, String> propertiesOverride) {
        final Map<String, String> versionedToInstanceIds = processGroup.findAllControllerServices().stream()
            .filter(cs -> cs.getVersionedComponentId().isPresent())
            .collect(Collectors.toMap(cs -> cs.getVersionedComponentId().get(), ComponentNode::getIdentifier));

        final Map<String, String> resolved = new HashMap<>(propertiesOverride);
        for (final PropertyDescriptor descriptor : processorNode.getPropertyDescriptors()) {
            if (descriptor.getControllerServiceDefinition() == null) {
                continue;
            }

            final String overriddenValue = propertiesOverride.get(descriptor.getName());
            final String currentValue = overriddenValue == null ? processorNode.getRawPropertyValue(descriptor) : overriddenValue;
            final String instanceId = versionedToInstanceIds.get(currentValue);
            if (instanceId != null) {
                resolved.put(descriptor.getName(), instanceId);
            }
        }

        return resolved;
    }

    @Override
    public ConfigurationContext createConfigurationContext(final ControllerServiceNode serviceNode, final ParameterLookup parameterLookup) {
        return createConfigurationContext(serviceNode, Map.of(), parameterLookup);
    }

    @Override
    public ConfigurationContext createConfigurationContext(final ControllerServiceNode serviceNode, final Map<String, String> propertiesOverride, final ParameterLookup parameterLookup) {
        final Map<String, String> resolvedProperties = resolveServiceReferences(serviceNode, serviceNode.getProcessGroup(), propertiesOverride);
        return new StandardConfigurationContext(serviceNode, resolvedProperties, null, parameterLookup, flowController.getControllerServiceProvider(), null);
    }

    @Override
    public ValidationContext createValidationContext(final ControllerServiceNode serviceNode, final Map<String, String> properties, final ParameterLookup parameterLookup) {
        final Map<String, String> resolvedProperties = resolveServiceReferences(serviceNode, serviceNode.getProcessGroup(), properties);
        return serviceNode.createValidationContext(resolvedProperties, serviceNode.getAnnotationData(), parameterLookup, true);
    }
}
