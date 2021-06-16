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
package org.apache.nifi.diagnostics.bootstrap.tasks;

import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.diagnostics.DiagnosticTask;
import org.apache.nifi.diagnostics.DiagnosticsDumpElement;
import org.apache.nifi.diagnostics.StandardDiagnosticsDumpElement;
import org.apache.nifi.groups.ProcessGroup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ComponentCountTask implements DiagnosticTask {
    private final FlowController flowController;

    public ComponentCountTask(final FlowController flowController) {
        this.flowController = flowController;
    }

    @Override
    public DiagnosticsDumpElement captureDump(final boolean verbose) {
        final List<String> details = new ArrayList<>();

        final ProcessGroup processGroup = flowController.getFlowManager().getRootGroup();
        details.add("Processors:");
        countProcessors(processGroup.findAllProcessors(), details);

        details.add("\n\nController Services:");
        countControllerServices(processGroup.findAllControllerServices(), details);

        return new StandardDiagnosticsDumpElement("Component Counts", details);
    }

    private void countProcessors(final Collection<ProcessorNode> processors, final List<String> details) {
        final Map<String, Map<ScheduledState, Integer>> typeMap = new HashMap<>();

        for (final ProcessorNode procNode : processors) {
            final String componentType = procNode.getComponentType();

            final ScheduledState scheduledState = procNode.getScheduledState();
            final Map<ScheduledState, Integer> stateCounts = typeMap.computeIfAbsent(componentType, key -> new HashMap<>());
            final Integer count = stateCounts.computeIfAbsent(scheduledState, key -> 0);
            stateCounts.put(scheduledState, count + 1);
        }

        for (final Map.Entry<String, Map<ScheduledState, Integer>> typeEntry : typeMap.entrySet()) {
            final String type = typeEntry.getKey();
            final Map<ScheduledState, Integer> stateMap = typeEntry.getValue();

            final int total = stateMap.values().stream().mapToInt(Integer::intValue).sum();
            details.add(type + " : " + total + " Total, " + stateMap.toString().toLowerCase());
        }

        if (typeMap.isEmpty()) {
            details.add("No Processors");
        }
    }

    private void countControllerServices(final Collection<ControllerServiceNode> services, final List<String> details) {
        final Map<String, Map<ControllerServiceState, Integer>> typeMap = new HashMap<>();

        for (final ControllerServiceNode serviceNode : services) {
            final String componentType = serviceNode.getComponentType();

            final ControllerServiceState serviceState = serviceNode.getState();
            final Map<ControllerServiceState, Integer> stateCounts = typeMap.computeIfAbsent(componentType, key -> new HashMap<>());
            final Integer count = stateCounts.computeIfAbsent(serviceState, key -> 0);
            stateCounts.put(serviceState, count + 1);
        }

        for (final Map.Entry<String, Map<ControllerServiceState, Integer>> typeEntry : typeMap.entrySet()) {
            final String type = typeEntry.getKey();
            final Map<ControllerServiceState, Integer> stateMap = typeEntry.getValue();

            final int total = stateMap.values().stream().mapToInt(Integer::intValue).sum();
            details.add(type + " : " + total + " total, " + stateMap.toString().toLowerCase());
        }

        if (typeMap.isEmpty()) {
            details.add("No Controller Services");
        }
    }
}
