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
package org.apache.nifi.atlas.provenance.lineage;

import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.NiFiFlow;
import org.apache.nifi.atlas.NiFiFlowPath;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.lineage.ComputeLineageResult;
import org.apache.nifi.provenance.lineage.LineageEdge;
import org.apache.nifi.provenance.lineage.LineageNode;
import org.apache.nifi.provenance.lineage.LineageNodeType;

import java.util.List;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_QUEUE;

public class SimpleFlowPathLineage extends AbstractLineageStrategy {

    @Override
    public void processEvent(AnalysisContext analysisContext, NiFiFlow nifiFlow, ProvenanceEventRecord event) {
        final DataSetRefs refs = executeAnalyzer(analysisContext, event);
        if (refs == null || (refs.isEmpty())) {
            return;
        }

        if ("Remote Input Port".equals(event.getComponentType()) || "Remote Output Port".equals(event.getComponentType())) {
            processRemotePortEvent(analysisContext, nifiFlow, event, refs);
        } else {
            addDataSetRefs(nifiFlow, refs);
        }

    }

    /**
     * Create a flow_path entity corresponding to the target RemoteGroupPort when a SEND/RECEIVE event are received.
     * Because such entity can not be created in advance while analyzing flow statically,
     * as ReportingTask can not determine whether a component id is a RemoteGroupPort,
     * since connectionStatus is the only available information in ReportingContext.
     * ConnectionStatus only knows component id, component type is unknown.
     * For example, there is no difference to tell if a connected component is a funnel or a RemoteGroupPort.
     */
    private void processRemotePortEvent(AnalysisContext analysisContext, NiFiFlow nifiFlow, ProvenanceEventRecord event, DataSetRefs analyzedRefs) {

        final boolean isRemoteInputPort = "Remote Input Port".equals(event.getComponentType());

        // Create a RemoteInputPort Process.
        // event.getComponentId returns UUID for RemoteGroupPort as a client of S2S, and it's different from a remote port UUID (portDataSetid).
        // See NIFI-4571 for detail.
        final Referenceable remotePortDataSet = isRemoteInputPort ? analyzedRefs.getOutputs().iterator().next() :  analyzedRefs.getInputs().iterator().next();
        final String portProcessId = event.getComponentId();

        final NiFiFlowPath remotePortProcess = new NiFiFlowPath(portProcessId);
        remotePortProcess.setName(event.getComponentType());
        remotePortProcess.addProcessor(portProcessId);

        // For RemoteInputPort, need to find the previous component connected to this port,
        // which passed this particular FlowFile.
        // That is only possible by calling lineage API.
        if (isRemoteInputPort) {
            final ProvenanceEventRecord previousEvent = findPreviousProvenanceEvent(analysisContext, event);
            if (previousEvent == null) {
                logger.warn("Previous event was not found: {}", new Object[]{event});
                return;
            }

            // Set groupId from incoming connection if available.
            final List<ConnectionStatus> incomingConnections = nifiFlow.getIncomingConnections(portProcessId);
            if (incomingConnections == null || incomingConnections.isEmpty()) {
                logger.warn("Incoming relationship was not found: {}", new Object[]{event});
                return;
            }

            final ConnectionStatus connection = incomingConnections.get(0);
            remotePortProcess.setGroupId(connection.getGroupId());

            final Referenceable remotePortProcessRef = toReferenceable(remotePortProcess, nifiFlow);
            createEntity(remotePortProcessRef);

            // Create a queue.
            Referenceable queueFromStaticFlowPathToRemotePortProcess = new Referenceable(TYPE_NIFI_QUEUE);
            queueFromStaticFlowPathToRemotePortProcess.set(ATTR_NAME, "queue");
            queueFromStaticFlowPathToRemotePortProcess.set(ATTR_QUALIFIED_NAME, nifiFlow.toQualifiedName(portProcessId));

            // Create lineage: Static flow_path -> queue
            DataSetRefs staticFlowPathRefs = new DataSetRefs(previousEvent.getComponentId());
            staticFlowPathRefs.addOutput(queueFromStaticFlowPathToRemotePortProcess);
            addDataSetRefs(nifiFlow, staticFlowPathRefs);


            // Create lineage: Queue -> RemoteInputPort process -> RemoteInputPort dataSet
            DataSetRefs remotePortRefs = new DataSetRefs(portProcessId);
            remotePortRefs.addInput(queueFromStaticFlowPathToRemotePortProcess);
            remotePortRefs.addOutput(remotePortDataSet);
            addDataSetRefs(remotePortRefs, remotePortProcessRef);

        } else {
            // For RemoteOutputPort, it's possible that multiple processors are connected.
            // In that case, the received FlowFile is cloned and passed to each connection.
            // So we need to create multiple DataSetRefs.
            final List<ConnectionStatus> connections = nifiFlow.getOutgoingConnections(portProcessId);
            if (connections == null || connections.isEmpty()) {
                logger.warn("Incoming connection was not found: {}", new Object[]{event});
                return;
            }

            // Set group id from outgoing connection if available.
            remotePortProcess.setGroupId(connections.get(0).getGroupId());

            final Referenceable remotePortProcessRef = toReferenceable(remotePortProcess, nifiFlow);
            createEntity(remotePortProcessRef);

            // Create lineage: RemoteOutputPort dataSet -> RemoteOutputPort process
            DataSetRefs remotePortRefs = new DataSetRefs(portProcessId);
            remotePortRefs.addInput(remotePortDataSet);
            addDataSetRefs(remotePortRefs, remotePortProcessRef);

            for (ConnectionStatus connection : connections) {
                final String destinationId = connection.getDestinationId();
                final NiFiFlowPath destFlowPath = nifiFlow.findPath(destinationId);
                if (destFlowPath == null) {
                    // If the destination of a connection is a Remote Input Port,
                    // then its corresponding flow path may not be created yet.
                    // In such direct RemoteOutputPort to RemoteInputPort case, do not add a queue from this RemoteOutputPort
                    // as a queue will be created by the connected RemoteInputPort to connect this RemoteOutputPort.
                    continue;
                }

                // Create a queue.
                Referenceable queueFromRemotePortProcessToStaticFlowPath = new Referenceable(TYPE_NIFI_QUEUE);
                queueFromRemotePortProcessToStaticFlowPath.set(ATTR_NAME, "queue");
                queueFromRemotePortProcessToStaticFlowPath.set(ATTR_QUALIFIED_NAME, nifiFlow.toQualifiedName(destinationId));

                // Create lineage: Queue -> Static flow_path
                DataSetRefs staticFlowPathRefs = new DataSetRefs(destinationId);
                staticFlowPathRefs.addInput(queueFromRemotePortProcessToStaticFlowPath);
                addDataSetRefs(nifiFlow, staticFlowPathRefs);

                // Create lineage: RemoteOutputPort dataSet -> RemoteOutputPort process -> Queue
                remotePortRefs.addOutput(queueFromRemotePortProcessToStaticFlowPath);
                addDataSetRefs(remotePortRefs, remotePortProcessRef);
            }

            // Add RemoteOutputPort process, so that it can be found even if it is connected to RemoteInputPort directory without any processor in between.
            nifiFlow.getFlowPaths().put(remotePortProcess.getId(), remotePortProcess);

        }

    }

    private ProvenanceEventRecord findPreviousProvenanceEvent(AnalysisContext context, ProvenanceEventRecord event) {
        final ComputeLineageResult lineage = context.queryLineage(event.getEventId());
        if (lineage == null) {
            logger.warn("Lineage was not found: {}", new Object[]{event});
            return null;
        }

        // If no previous provenance node found due to expired or other reasons, just log a warning msg and do nothing.
        final LineageNode previousProvenanceNode = traverseLineage(lineage, String.valueOf(event.getEventId()));
        if (previousProvenanceNode == null) {
            logger.warn("Traverse lineage could not find any preceding provenance event node: {}", new Object[]{event});
            return null;
        }

        final long previousEventId = Long.parseLong(previousProvenanceNode.getIdentifier());
        return context.getProvenanceEvent(previousEventId);
    }

    /**
     * Recursively traverse lineage graph until a preceding provenance event is found.
     */
    private LineageNode traverseLineage(ComputeLineageResult lineage, String eventId) {
        final LineageNode previousNode = lineage.getEdges().stream()
                .filter(edge -> edge.getDestination().getIdentifier().equals(String.valueOf(eventId)))
                .findFirst().map(LineageEdge::getSource).orElse(null);
        if (previousNode == null) {
            return null;
        }
        if (previousNode.getNodeType().equals(LineageNodeType.PROVENANCE_EVENT_NODE)) {
            return previousNode;
        }
        return traverseLineage(lineage, previousNode.getIdentifier());
    }


}
