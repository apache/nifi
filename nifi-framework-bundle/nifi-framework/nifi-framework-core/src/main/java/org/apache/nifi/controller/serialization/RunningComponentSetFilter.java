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
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.flow.ExecutionEngine;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedFlowAnalysisRule;
import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedRemoteGroupPort;
import org.apache.nifi.flow.VersionedRemoteProcessGroup;
import org.apache.nifi.flow.VersionedReportingTask;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.registry.flow.FlowRegistryClientNode;
import org.apache.nifi.remote.RemoteGroupPort;

import java.util.HashMap;
import java.util.Map;
public class RunningComponentSetFilter implements ComponentSetFilter {
    private final Map<String, VersionedControllerService> controllerServices = new HashMap<>();
    private final Map<String, VersionedProcessor> processors = new HashMap<>();
    private final Map<String, VersionedReportingTask> reportingTasks = new HashMap<>();
    private final Map<String, VersionedFlowAnalysisRule> flowAnalysisRules = new HashMap<>();
    private final Map<String, VersionedPort> inputPorts = new HashMap<>();
    private final Map<String, VersionedPort> outputPorts = new HashMap<>();
    private final Map<String, VersionedRemoteGroupPort> remoteInputPorts = new HashMap<>();
    private final Map<String, VersionedRemoteGroupPort> remoteOutputPorts = new HashMap<>();
    private final Map<String, VersionedProcessGroup> statelessGroups = new HashMap<>();

    public RunningComponentSetFilter(final VersionedDataflow dataflow) {
        dataflow.getControllerServices().forEach(service -> controllerServices.put(service.getInstanceIdentifier(), service));
        dataflow.getReportingTasks().forEach(task -> reportingTasks.put(task.getInstanceIdentifier(), task));
        if (dataflow.getFlowAnalysisRules() != null) {
            dataflow.getFlowAnalysisRules().forEach(rule -> flowAnalysisRules.put(rule.getInstanceIdentifier(), rule));
        }
        flatten(dataflow.getRootGroup());
    }

    private void flatten(final VersionedProcessGroup group) {
        group.getInputPorts().forEach(port -> inputPorts.put(port.getInstanceIdentifier(), port));
        group.getOutputPorts().forEach(port -> outputPorts.put(port.getInstanceIdentifier(), port));
        group.getControllerServices().forEach(service -> controllerServices.put(service.getInstanceIdentifier(), service));
        group.getProcessors().forEach(processor -> processors.put(processor.getInstanceIdentifier(), processor));
        if (group.getExecutionEngine() == ExecutionEngine.STATELESS) {
            statelessGroups.put(group.getIdentifier(), group);
        }

        for (final VersionedRemoteProcessGroup rpg : group.getRemoteProcessGroups()) {
            rpg.getInputPorts().forEach(port -> {
                if (port.getInstanceIdentifier() != null) {
                    remoteInputPorts.put(port.getInstanceIdentifier(), port);
                }
            });

            rpg.getOutputPorts().forEach(port -> {
                if (port.getInstanceIdentifier() != null) {
                    remoteOutputPorts.put(port.getInstanceIdentifier(), port);
                }
            });
        }

        group.getProcessGroups().forEach(this::flatten);
    }

    @Override
    public boolean testProcessor(final ProcessorNode processor) {
        final VersionedProcessor versionedProcessor = processors.get(processor.getIdentifier());
        return versionedProcessor != null && versionedProcessor.getScheduledState() == ScheduledState.RUNNING;
    }

    @Override
    public boolean testReportingTask(final ReportingTaskNode reportingTask) {
        final VersionedReportingTask versionedReportingTask = reportingTasks.get(reportingTask.getIdentifier());
        return versionedReportingTask != null && versionedReportingTask.getScheduledState() == ScheduledState.RUNNING;
    }

    @Override
    public boolean testFlowAnalysisRule(FlowAnalysisRuleNode flowAnalysisRule) {
        final VersionedFlowAnalysisRule versionedFlowAnalysisRule = flowAnalysisRules.get(flowAnalysisRule.getIdentifier());
        return versionedFlowAnalysisRule != null && versionedFlowAnalysisRule.getScheduledState() == ScheduledState.ENABLED;
    }

    @Override
    public boolean testControllerService(final ControllerServiceNode controllerService) {
        final VersionedControllerService versionedService = controllerServices.get(controllerService.getIdentifier());
        return versionedService != null && versionedService.getScheduledState() == ScheduledState.ENABLED;
    }

    @Override
    public boolean testInputPort(final Port port) {
        final VersionedPort versionedPort = inputPorts.get(port.getIdentifier());
        return versionedPort != null && versionedPort.getScheduledState() == ScheduledState.RUNNING;
    }

    @Override
    public boolean testOutputPort(final Port port) {
        final VersionedPort versionedPort = outputPorts.get(port.getIdentifier());
        return versionedPort != null && versionedPort.getScheduledState() == ScheduledState.RUNNING;
    }

    @Override
    public boolean testRemoteInputPort(final RemoteGroupPort port) {
        final VersionedRemoteGroupPort versionedPort = remoteInputPorts.get(port.getIdentifier());
        return versionedPort != null && versionedPort.getScheduledState() == ScheduledState.RUNNING;
    }

    @Override
    public boolean testRemoteOutputPort(final RemoteGroupPort port) {
        final VersionedRemoteGroupPort versionedPort = remoteOutputPorts.get(port.getIdentifier());
        return versionedPort != null && versionedPort.getScheduledState() == ScheduledState.RUNNING;
    }

    @Override
    public boolean testFlowRegistryClient(final FlowRegistryClientNode flowRegistryClient) {
       return false;
    }

    @Override
    public boolean testStatelessGroup(final ProcessGroup group) {
        final VersionedProcessGroup versionedGroup = statelessGroups.get(group.getIdentifier());
        return versionedGroup != null && versionedGroup.getScheduledState() == ScheduledState.RUNNING;
    }
}
