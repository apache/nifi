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
package org.apache.nifi.atlas;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class NiFiFlowAnalyzer {

    private static final Logger logger = LoggerFactory.getLogger(NiFiFlowAnalyzer.class);

    public void analyzeProcessGroup(NiFiFlow nifiFlow, ProcessGroupStatus rootProcessGroup) {
        analyzeProcessGroup(rootProcessGroup, nifiFlow);
        analyzeRootGroupPorts(nifiFlow, rootProcessGroup);
    }

    private void analyzeRootGroupPorts(NiFiFlow nifiFlow, ProcessGroupStatus rootProcessGroup) {
        rootProcessGroup.getInputPortStatus().forEach(port -> nifiFlow.addRootInputPort(port));
        rootProcessGroup.getOutputPortStatus().forEach(port -> nifiFlow.addRootOutputPort(port));
    }

    private void analyzeProcessGroup(final ProcessGroupStatus processGroupStatus, final NiFiFlow nifiFlow) {

        processGroupStatus.getConnectionStatus().forEach(c -> nifiFlow.addConnection(c));
        processGroupStatus.getProcessorStatus().forEach(p -> nifiFlow.addProcessor(p));
        processGroupStatus.getRemoteProcessGroupStatus().forEach(r -> nifiFlow.addRemoteProcessGroup(r));
        processGroupStatus.getInputPortStatus().forEach(p -> nifiFlow.addInputPort(p));
        processGroupStatus.getOutputPortStatus().forEach(p -> nifiFlow.addOutputPort(p));

        // Analyze child ProcessGroups recursively.
        for (ProcessGroupStatus child : processGroupStatus.getProcessGroupStatus()) {
            analyzeProcessGroup(child, nifiFlow);
        }

    }

    private List<String> getIncomingProcessorsIds(NiFiFlow nifiFlow, List<ConnectionStatus> incomingConnections) {
        if (incomingConnections == null) {
            return Collections.emptyList();
        }

        final List<String> ids = new ArrayList<>();

        incomingConnections.forEach(c -> {
            // Ignore self relationship.
            final String sourceId = c.getSourceId();
            if (!sourceId.equals(c.getDestinationId())) {
                if (nifiFlow.isProcessor(sourceId)) {
                    ids.add(sourceId);
                } else {
                    ids.addAll(getIncomingProcessorsIds(nifiFlow, nifiFlow.getIncomingConnections(sourceId)));
                }
            }
        });

        return ids;
    }

    private List<String> getNextProcessComponent(NiFiFlow nifiFlow, NiFiFlowPath path, String componentId) {
        final List<ConnectionStatus> outs = nifiFlow.getOutgoingConnections(componentId);
        if (outs == null || outs.isEmpty()) {
            return Collections.emptyList();
        }

        final List<String> nextProcessComponent = new ArrayList<>();
        for (ConnectionStatus out : outs) {
            final String destinationId = out.getDestinationId();
            if (path.getProcessComponentIds().contains(destinationId)) {
                // If the connection is pointing back to current path, then skip it to avoid loop.
                continue;
            }

            if (nifiFlow.isProcessComponent(destinationId)) {
                nextProcessComponent.add(destinationId);
            } else {
                nextProcessComponent.addAll(getNextProcessComponent(nifiFlow, path, destinationId));
            }
        }
        return nextProcessComponent;
    }

    private void traverse(NiFiFlow nifiFlow, NiFiFlowPath path, String componentId) {

        // If the pid is RootInputPort of the same NiFi instance, then stop traversing to create separate self S2S path.
        // E.g InputPort -> MergeContent, GenerateFlowFile -> InputPort.
        if (path.getProcessComponentIds().size() > 0 && nifiFlow.isRootInputPort(componentId)) {
            return;
        }

        // Add known inputs/outputs to/from this processor, such as RootGroupIn/Output port
        if (nifiFlow.isProcessComponent(componentId)) {
            path.addProcessor(componentId);
        }

        final List<ConnectionStatus> outs = nifiFlow.getOutgoingConnections(componentId);
        if (outs == null || outs.isEmpty()) {
            return;
        }

        // Analyze destination process components.
        final List<String> nextProcessComponents = getNextProcessComponent(nifiFlow, path, componentId);
        nextProcessComponents.forEach(destPid -> {
            if (path.getProcessComponentIds().contains(destPid)) {
                // Avoid loop to it self.
                return;
            }

            // If the destination has more than one inputs, or there are multiple destinations,
            // then it should be treated as a separate flow path.
            final boolean createJointPoint = nextProcessComponents.size() > 1
                    || getIncomingProcessorsIds(nifiFlow, nifiFlow.getIncomingConnections(destPid)).size() > 1;

            if (createJointPoint) {

                final boolean alreadyTraversed = nifiFlow.isTraversedPath(destPid);

                // Create an input queue DataSet because Atlas doesn't show lineage if it doesn't have in and out.
                // This DataSet is also useful to link flowPaths together on Atlas lineage graph.
                final Tuple<AtlasObjectId, AtlasEntity> queueTuple = nifiFlow.getOrCreateQueue(destPid);

                final AtlasObjectId queueId = queueTuple.getKey();
                path.getOutputs().add(queueId);

                // If the destination is already traversed once, it doesn't have to be visited again.
                if (alreadyTraversed) {
                    return;
                }

                // Get existing or create new one.
                final NiFiFlowPath jointPoint = nifiFlow.getOrCreateFlowPath(destPid);
                jointPoint.getInputs().add(queueId);

                // Start traversing as a new joint point.
                traverse(nifiFlow, jointPoint, destPid);

            } else {
                // Normal relation, continue digging.
                traverse(nifiFlow, path, destPid);
            }

        });
    }

    private boolean isHeadProcessor(NiFiFlow nifiFlow, List<ConnectionStatus> ins) {
        if (ins == null || ins.isEmpty()) {
            return true;
        }
        return ins.stream().allMatch(
                in -> {
                    // If it has incoming relationship from other process components, then return false.
                    final String sourceId = in.getSourceId();
                    if (nifiFlow.isProcessComponent(sourceId)) {
                        return false;
                    }
                    // Check next level.
                    final List<ConnectionStatus> incomingConnections = nifiFlow.getIncomingConnections(sourceId);
                    return isHeadProcessor(nifiFlow, incomingConnections);
                }
        );
    }

    public void analyzePaths(NiFiFlow nifiFlow) {
        final String rootProcessGroupId = nifiFlow.getRootProcessGroupId();

        // Now let's break it into flow paths.
        final Map<String, ProcessorStatus> processors = nifiFlow.getProcessors();
        final Set<String> headProcessComponents = processors.keySet().stream()
                .filter(pid -> {
                    final List<ConnectionStatus> ins = nifiFlow.getIncomingConnections(pid);
                    return isHeadProcessor(nifiFlow, ins);
                })
                .collect(Collectors.toSet());

        // Use RootInputPorts as headProcessors.
        headProcessComponents.addAll(nifiFlow.getRootInputPorts().keySet());

        headProcessComponents.forEach(startPid -> {
            // By using the startPid as its qualifiedName, it's guaranteed that
            // the same path will end up being the same Atlas entity.
            // However, if the first processor is replaced by another,
            // the flow path will have a different id, and the old path is logically deleted.
            final NiFiFlowPath path = nifiFlow.getOrCreateFlowPath(startPid);
            traverse(nifiFlow, path, startPid);
        });

        nifiFlow.getFlowPaths().values().forEach(path -> {
            if (processors.containsKey(path.getId())) {
                final ProcessorStatus processor = processors.get(path.getId());
                path.setGroupId(processor.getGroupId());
            } else {
                path.setGroupId(rootProcessGroupId);
            }
        });
    }

}
