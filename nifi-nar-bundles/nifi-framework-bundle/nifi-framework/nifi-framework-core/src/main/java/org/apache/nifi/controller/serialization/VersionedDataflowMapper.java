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

package org.apache.nifi.controller.serialization;

import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.Template;
import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.controller.flow.VersionedFlowEncodingVersion;
import org.apache.nifi.controller.flow.VersionedRegistry;
import org.apache.nifi.controller.flow.VersionedTemplate;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedReportingTask;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.registry.flow.FlowRegistry;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.registry.flow.mapping.ComponentIdLookup;
import org.apache.nifi.registry.flow.mapping.FlowMappingOptions;
import org.apache.nifi.registry.flow.mapping.NiFiRegistryFlowMapper;
import org.apache.nifi.registry.flow.mapping.SensitiveValueEncryptor;
import org.apache.nifi.registry.flow.mapping.VersionedComponentStateLookup;
import org.apache.nifi.web.api.dto.TemplateDTO;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class VersionedDataflowMapper {
    private static final VersionedFlowEncodingVersion ENCODING_VERSION = new VersionedFlowEncodingVersion(2, 0);

    private final FlowController flowController;
    private final NiFiRegistryFlowMapper flowMapper;
    private final ScheduledStateLookup stateLookup;

    public VersionedDataflowMapper(final FlowController flowController, final ExtensionManager extensionManager, final SensitiveValueEncryptor encryptor, final ScheduledStateLookup stateLookup) {
        this.flowController = flowController;
        this.stateLookup = stateLookup;

        final VersionedComponentStateLookup versionedComponentStateLookup = createStateLookup();

        final FlowMappingOptions mappingOptions = new FlowMappingOptions.Builder()
            .mapSensitiveConfiguration(true)
            .mapPropertyDescriptors(false)
            .stateLookup(versionedComponentStateLookup)
            .sensitiveValueEncryptor(encryptor)
            .componentIdLookup(ComponentIdLookup.VERSIONED_OR_GENERATE)
            .mapInstanceIdentifiers(true)
            .mapControllerServiceReferencesToVersionedId(false)
            .build();

        flowMapper = new NiFiRegistryFlowMapper(extensionManager, mappingOptions);
    }

    public VersionedDataflow createMapping() {
        final VersionedDataflow dataflow = new VersionedDataflow();
        dataflow.setEncodingVersion(ENCODING_VERSION);
        dataflow.setMaxTimerDrivenThreadCount(flowController.getMaxTimerDrivenThreadCount());
        dataflow.setControllerServices(mapControllerServices());
        dataflow.setParameterContexts(mapParameterContexts());
        dataflow.setRegistries(mapRegistries());
        dataflow.setReportingTasks(mapReportingTasks());
        dataflow.setRootGroup(mapRootGroup());
        dataflow.setTemplates(mapTemplates());

        return dataflow;
    }

    private List<VersionedControllerService> mapControllerServices() {
        final List<VersionedControllerService> controllerServices = new ArrayList<>();

        for (final ControllerServiceNode serviceNode : flowController.getFlowManager().getRootControllerServices()) {
            final VersionedControllerService versionedControllerService = flowMapper.mapControllerService(
                serviceNode, flowController.getControllerServiceProvider(), Collections.emptySet(), Collections.emptyMap());
            controllerServices.add(versionedControllerService);
        }

        return controllerServices;
    }

    private List<VersionedParameterContext> mapParameterContexts() {
        final List<VersionedParameterContext> parameterContexts = new ArrayList<>();

        for (final ParameterContext parameterContext : flowController.getFlowManager().getParameterContextManager().getParameterContexts()) {
            final VersionedParameterContext versionedParameterContext = flowMapper.mapParameterContext(parameterContext);
            parameterContexts.add(versionedParameterContext);
        }

        return parameterContexts;
    }

    private List<VersionedRegistry> mapRegistries() {
        final List<VersionedRegistry> registries = new ArrayList<>();

        final FlowRegistryClient flowRegistryClient = flowController.getFlowRegistryClient();
        for (final String registryId : flowRegistryClient.getRegistryIdentifiers()) {
            final FlowRegistry flowRegistry = flowRegistryClient.getFlowRegistry(registryId);
            final VersionedRegistry versionedRegistry = mapRegistry(flowRegistry);
            registries.add(versionedRegistry);
        }

        return registries;
    }

    private VersionedRegistry mapRegistry(final FlowRegistry flowRegistry) {
        final VersionedRegistry versionedRegistry = new VersionedRegistry();
        versionedRegistry.setDescription(flowRegistry.getDescription());
        versionedRegistry.setId(flowRegistry.getIdentifier());
        versionedRegistry.setName(flowRegistry.getName());
        versionedRegistry.setUrl(flowRegistry.getURL());

        return versionedRegistry;
    }

    private List<VersionedReportingTask> mapReportingTasks() {
        final List<VersionedReportingTask> reportingTasks = new ArrayList<>();

        for (final ReportingTaskNode taskNode : flowController.getAllReportingTasks()) {
            final VersionedReportingTask versionedReportingTask = flowMapper.mapReportingTask(taskNode, flowController.getControllerServiceProvider());
            reportingTasks.add(versionedReportingTask);
        }

        return reportingTasks;
    }

    private VersionedProcessGroup mapRootGroup() {
        final FlowRegistryClient registryClient = flowController.getFlowRegistryClient();
        final ProcessGroup rootGroup = flowController.getFlowManager().getRootGroup();
        final VersionedProcessGroup versionedRootGroup = flowMapper.mapProcessGroup(rootGroup, flowController.getControllerServiceProvider(), registryClient, true);
        return versionedRootGroup;
    }

    private Set<VersionedTemplate> mapTemplates() {
        return flowController.getFlowManager().getRootGroup().findAllTemplates().stream()
            .map(this::mapTemplate)
            .collect(Collectors.toSet());
    }

    private VersionedTemplate mapTemplate(final Template template) {
        final TemplateDTO dto = template.getDetails();

        final VersionedTemplate versionedTemplate = new VersionedTemplate();
        versionedTemplate.setIdentifier(template.getIdentifier());
        versionedTemplate.setInstanceIdentifier(template.getIdentifier());
        versionedTemplate.setGroupIdentifier(template.getProcessGroupIdentifier());
        versionedTemplate.setName(dto.getName());
        versionedTemplate.setTemplateDto(dto);
        return versionedTemplate;
    }

    private VersionedComponentStateLookup createStateLookup() {
        return new VersionedComponentStateLookup() {
            @Override
            public ScheduledState getState(final ProcessorNode processorNode) {
                return map(stateLookup.getScheduledState(processorNode));
            }

            private ScheduledState map(final org.apache.nifi.controller.ScheduledState currentState) {
                switch (currentState) {
                    case DISABLED:
                        return ScheduledState.DISABLED;
                    case RUNNING:
                    case STARTING:
                        return ScheduledState.RUNNING;
                    case STOPPED:
                    case STOPPING:
                    case RUN_ONCE:
                    default:
                        return ScheduledState.ENABLED;
                }
            }

            @Override
            public ScheduledState getState(final Port port) {
                return map(stateLookup.getScheduledState(port));
            }

            @Override
            public ScheduledState getState(final ReportingTaskNode taskNode) {
                return map(taskNode.getScheduledState());
            }

            @Override
            public ScheduledState getState(final ControllerServiceNode serviceNode) {
                switch (serviceNode.getState()) {
                    case ENABLED:
                    case ENABLING:
                        return ScheduledState.ENABLED;
                    case DISABLED:
                    case DISABLING:
                    default:
                        return ScheduledState.DISABLED;
                }
            }
        };
    }
}
