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
package org.apache.nifi.controller.state;

import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.state.ComponentStateCapabilitiesResolver;
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.registry.flow.FlowRegistryClientNode;

import java.util.Objects;

/**
 * Component state capability resolver backed by FlowManager lookups.
 */
public class FlowManagerStateCapabilitiesResolver implements ComponentStateCapabilitiesResolver {
    private final FlowManager flowManager;

    public FlowManagerStateCapabilitiesResolver(final FlowManager flowManager) {
        this.flowManager = Objects.requireNonNull(flowManager);
    }

    @Override
    public boolean isDropStateKeySupported(final String componentId) {
        final Class<?> componentClass = findComponentClass(componentId);
        if (componentClass == null) {
            return false;
        }

        final Stateful stateful = componentClass.getAnnotation(Stateful.class);
        return stateful != null && stateful.dropStateKeySupported();
    }

    private Class<?> findComponentClass(final String componentId) {
        // Processor
        final ProcessorNode processorNode = flowManager.getProcessorNode(componentId);
        if (processorNode != null) {
            return processorNode.getComponent().getClass();
        }

        // Reporting Task
        final ReportingTaskNode reportingTaskNode = flowManager.getReportingTaskNode(componentId);
        if (reportingTaskNode != null) {
            return reportingTaskNode.getComponent().getClass();
        }

        // Controller Service
        final ControllerServiceNode controllerServiceNode = flowManager.getControllerServiceNode(componentId);
        if (controllerServiceNode != null) {
            return controllerServiceNode.getComponent().getClass();
        }

        // Flow Analysis Rule
        final FlowAnalysisRuleNode flowAnalysisRuleNode = flowManager.getFlowAnalysisRuleNode(componentId);
        if (flowAnalysisRuleNode != null) {
            return flowAnalysisRuleNode.getComponent().getClass();
        }

        // Flow Registry Client
        final FlowRegistryClientNode flowRegistryClientNode = flowManager.getFlowRegistryClient(componentId);
        if (flowRegistryClientNode != null) {
            return flowRegistryClientNode.getComponent().getClass();
        }

        // Parameter Provider – no direct lookup by id; iterate
        for (final ParameterProviderNode parameterProviderNode : flowManager.getAllParameterProviders()) {
            final ConfigurableComponent component = parameterProviderNode.getComponent();
            if (component != null && componentId.equals(component.getIdentifier())) {
                return component.getClass();
            }
        }

        return null;
    }
}

