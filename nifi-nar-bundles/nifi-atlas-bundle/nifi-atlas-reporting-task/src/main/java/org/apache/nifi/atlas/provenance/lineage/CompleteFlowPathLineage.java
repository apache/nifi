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
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.lineage.ComputeLineageResult;
import org.apache.nifi.provenance.lineage.LineageNode;
import org.apache.nifi.provenance.lineage.LineageNodeType;
import org.apache.nifi.util.Tuple;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.CRC32;

import static org.apache.nifi.atlas.AtlasUtils.toQualifiedName;
import static org.apache.nifi.atlas.AtlasUtils.toStr;
import static org.apache.nifi.atlas.AtlasUtils.toTypedQualifiedName;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_INPUTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_OUTPUTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_QUEUE;
import static org.apache.nifi.provenance.ProvenanceEventType.DROP;

public class CompleteFlowPathLineage extends AbstractLineageStrategy {

    @Override
    public ProvenanceEventType[] getTargetEventTypes() {
        return new ProvenanceEventType[]{DROP};
    }

    @Override
    public void processEvent(AnalysisContext analysisContext, NiFiFlow nifiFlow, ProvenanceEventRecord event) {
        if (!ProvenanceEventType.DROP.equals(event.getEventType())) {
            return;
        }
        final ComputeLineageResult lineage = analysisContext.queryLineage(event.getEventId());

        // Construct a tree model to traverse backwards.
        final Map<String, List<LineageNode>> lineageTree = new HashMap<>();
        analyzeLineageTree(lineage, lineageTree);

        final LineagePath lineagePath = new LineagePath();
        extractLineagePaths(analysisContext, lineageTree, lineagePath, event);

        analyzeLineagePath(analysisContext, lineagePath);

        // Input and output data set are both required to report lineage.
        List<Tuple<NiFiFlowPath, DataSetRefs>> createdFlowPaths = new ArrayList<>();
        if (lineagePath.isComplete()) {
            createCompleteFlowPath(nifiFlow, lineagePath, createdFlowPaths);
            for (Tuple<NiFiFlowPath, DataSetRefs> createdFlowPath : createdFlowPaths) {
                final NiFiFlowPath flowPath = createdFlowPath.getKey();
                // NOTE 1: FlowPath creation and DataSet references should be reported separately
                // ------------------------------------------------------------------------------
                // For example, with following provenance event inputs:
                //   CREATE(F1), FORK (F1 -> F2, F3), DROP(F1), SEND (F2), SEND(F3), DROP(F2), DROP(F3),
                // there is no guarantee that DROP(F2) and DROP(F3) are processed within the same cycle.
                // If DROP(F3) is processed in different cycle, it needs to be added to the existing FlowPath
                // that contains F1 -> F2, to be F1 -> F2, F3.
                // Execution cycle 1: Path1 (source of F1 -> ForkA), ForkA_queue (F1 -> F2), Path2 (ForkA -> dest of F2)
                // Execution cycle 2: Path1 (source of F1 -> ForkB), ForkB_queue (F1 -> F3), Path3 (ForkB -> dest of F3)

                // NOTE 2: Both FlowPath creation and FlowPath update messages are required
                // ------------------------------------------------------------------------
                // For the 1st time when a lineage is found, nifi_flow_path and referred DataSets are created.
                // If we notify these entities by a create 3 entities message (Path1, DataSet1, DataSet2)
                // followed by 1 partial update message to add lineage (DataSet1 -> Path1 -> DataSet2), then
                // the update message may arrive at Atlas earlier than the create message gets processed.
                // If that happens, lineage among these entities will be missed.
                // But as written in NOTE1, there is a case where existing nifi_flow_paths need to be updated.
                // Also, we don't know if this is the 1st time or 2nd or later.
                // So, we need to notify entity creation and also partial update messages.

                // Create flow path entity with DataSet refs.
                final Referenceable flowPathRef = toReferenceable(flowPath, nifiFlow);
                final DataSetRefs dataSetRefs = createdFlowPath.getValue();
                addDataSetRefs(dataSetRefs.getInputs(), flowPathRef, ATTR_INPUTS);
                addDataSetRefs(dataSetRefs.getOutputs(), flowPathRef, ATTR_OUTPUTS);
                createEntity(flowPathRef);
                // Also, sending partial update message to update existing flow_path.
                addDataSetRefs(nifiFlow, Collections.singleton(flowPath), createdFlowPath.getValue());

            }
            createdFlowPaths.clear();
        }
    }

    private List<LineageNode> findParentEvents(Map<String, List<LineageNode>> lineageTree, ProvenanceEventRecord event) {
        List<LineageNode> parentNodes = lineageTree.get(String.valueOf(event.getEventId()));
        return parentNodes == null || parentNodes.isEmpty() ? null : parentNodes.stream()
                // In case it's not a provenance event (i.e. FLOWFILE_NODE), get one level higher parents.
                .flatMap(n -> !LineageNodeType.PROVENANCE_EVENT_NODE.equals(n.getNodeType())
                        ? lineageTree.get(n.getIdentifier()).stream() : Stream.of(n))
                .collect(Collectors.toList());
    }

    private void extractLineagePaths(AnalysisContext context, Map<String, List<LineageNode>> lineageTree,
                                     LineagePath lineagePath, ProvenanceEventRecord lastEvent) {

        lineagePath.getEvents().add(lastEvent);
        List<LineageNode> parentEvents = findParentEvents(lineageTree, lastEvent);

        final boolean createSeparateParentPath = lineagePath.shouldCreateSeparatePath(lastEvent.getEventType());

        if (createSeparateParentPath && (parentEvents == null || parentEvents.isEmpty())) {
            // Try expanding the lineage.
            // This is for the FlowFiles those are FORKed (or JOINed ... etc) other FlowFile(s).
            // FlowFiles routed to 'original' may have these event types, too, however they have parents fetched together.

            // For example, with these inputs: CREATE(F1), FORK (F1 -> F2, F3), DROP(F1), SEND (F2), SEND(F3), DROP(F2), DROP(F3)
            // Then when DROP(F1) is queried, FORK(F1) and CREATE(F1) are returned.
            // For DROP(F2), SEND(F2) and FORK(F2) are returned.
            // For DROP(F3), SEND(F3) and FORK(F3) are returned.
            // In this case, FORK(F2) and FORK(F3) have to query their parents again, to get CREATE(F1).
            final ComputeLineageResult joinedParents = context.findParents(lastEvent.getEventId());
            analyzeLineageTree(joinedParents, lineageTree);

            parentEvents = findParentEvents(lineageTree, lastEvent);
        }

        if (parentEvents == null || parentEvents.isEmpty()) {
            logger.debug("{} does not have any parent, stop extracting lineage path.", lastEvent);
            return;
        }

        if (createSeparateParentPath) {
            // Treat those as separated lineage_path
            parentEvents.stream()
                    .map(parentEvent -> context.getProvenanceEvent(Long.parseLong(parentEvent.getIdentifier())))
                    .filter(Objects::nonNull)
                    .forEach(parent -> {
                        final LineagePath parentPath = new LineagePath();
                        lineagePath.getParents().add(parentPath);
                        extractLineagePaths(context, lineageTree, parentPath, parent);
                    });
        } else {
            // Simply traverse upwards.
            if (parentEvents.size() > 1) {
                throw new IllegalStateException(String.format("Having more than 1 parents for event type %s" +
                                " is not expected. Should ask NiFi developer for investigation. %s",
                        lastEvent.getEventType(), lastEvent));
            }
            final ProvenanceEventRecord parentEvent = context.getProvenanceEvent(Long.parseLong(parentEvents.get(0).getIdentifier()));
            if (parentEvent != null) {
                extractLineagePaths(context, lineageTree, lineagePath, parentEvent);
            }
        }
    }

    private void analyzeLineagePath(AnalysisContext analysisContext, LineagePath lineagePath) {
        final List<ProvenanceEventRecord> events = lineagePath.getEvents();

        final DataSetRefs parentRefs = new DataSetRefs(events.get(0).getComponentId());
        events.forEach(event -> {
            final DataSetRefs refs = executeAnalyzer(analysisContext, event);
            if (refs == null || refs.isEmpty()) {
                return;
            }
            refs.getInputs().forEach(parentRefs::addInput);
            refs.getOutputs().forEach(parentRefs::addOutput);
        });

        lineagePath.setRefs(parentRefs);

        // Analyse parents.
        lineagePath.getParents().forEach(parent -> analyzeLineagePath(analysisContext, parent));
    }

    private void analyzeLineageTree(ComputeLineageResult lineage, Map<String, List<LineageNode>> lineageTree) {
        lineage.getEdges().forEach(edge -> lineageTree
                        .computeIfAbsent(edge.getDestination().getIdentifier(), k -> new ArrayList<>())
                        .add(edge.getSource()));
    }

    /**
     * Create a new FlowPath from a LineagePath. FlowPaths created by this method will have a hash in its qualified name.
     *
     * <p>This method processes parents first to generate a hash, as parent LineagePath hashes contribute child hash
     * in order to distinguish FlowPaths based on the complete path for a given FlowFile.
     * For example, even if two lineagePaths have identical componentIds/inputs/outputs,
     * if those parents have different inputs, those should be treated as different paths.</p>
     *
     * @param nifiFlow A reference to current NiFiFlow
     * @param lineagePath LineagePath from which NiFiFlowPath and DataSet refs are created and added to the {@code createdFlowPaths}.
     * @param createdFlowPaths A list to buffer created NiFiFlowPaths,
     *                         in order to defer sending notification to Kafka until all parent FlowPath get analyzed.
     */
    private void createCompleteFlowPath(NiFiFlow nifiFlow, LineagePath lineagePath, List<Tuple<NiFiFlowPath, DataSetRefs>> createdFlowPaths) {

        final List<ProvenanceEventRecord> events = lineagePath.getEvents();
        Collections.reverse(events);

        final List<String> componentIds = events.stream().map(ProvenanceEventRecord::getComponentId).collect(Collectors.toList());
        final String firstComponentId = events.get(0).getComponentId();
        final DataSetRefs dataSetRefs = lineagePath.getRefs();

        // Process parents first.
        Referenceable queueBetweenParent = null;
        if (!lineagePath.getParents().isEmpty()) {
            // Add queue between this lineage path and parent.
            queueBetweenParent = new Referenceable(TYPE_NIFI_QUEUE);
            // The first event knows why this lineage has parents, e.g. FORK or JOIN.
            final String firstEventType = events.get(0).getEventType().name();
            queueBetweenParent.set(ATTR_NAME, firstEventType);
            dataSetRefs.addInput(queueBetweenParent);

            for (LineagePath parent : lineagePath.getParents()) {
                parent.getRefs().addOutput(queueBetweenParent);
                createCompleteFlowPath(nifiFlow, parent, createdFlowPaths);
            }
        }

        // Create a variant path.
        // Calculate a hash from component_ids and input and output resource ids.
        final Stream<String> ioIds = Stream.concat(dataSetRefs.getInputs().stream(), dataSetRefs.getOutputs()
                .stream()).map(ref -> toTypedQualifiedName(ref.getTypeName(), toStr(ref.get(ATTR_QUALIFIED_NAME))));

        final Stream<String> parentHashes = lineagePath.getParents().stream().map(p -> String.valueOf(p.getLineagePathHash()));
        final CRC32 crc32 = new CRC32();
        crc32.update(Stream.of(componentIds.stream(), ioIds, parentHashes).reduce(Stream::concat).orElseGet(Stream::empty)
                .sorted().distinct()
                .collect(Collectors.joining(",")).getBytes(StandardCharsets.UTF_8));

        final long hash = crc32.getValue();
        lineagePath.setLineagePathHash(hash);
        final NiFiFlowPath flowPath = new NiFiFlowPath(firstComponentId, hash);

        // In order to differentiate a queue between parents and this flow_path, add the hash into the queue qname.
        // E.g, FF1 and FF2 read from dirA were merged, vs FF3 and FF4 read from dirB were merged then passed here, these two should be different queue.
        if (queueBetweenParent != null) {
            queueBetweenParent.set(ATTR_QUALIFIED_NAME, toQualifiedName(nifiFlow.getClusterName(), firstComponentId + "::" + hash));
        }

        // If the same components emitted multiple provenance events consecutively, merge it to come up with a simpler name.
        String previousComponentId = null;
        List<ProvenanceEventRecord> uniqueEventsForName = new ArrayList<>();
        for (ProvenanceEventRecord event : events) {
            if (!event.getComponentId().equals(previousComponentId)) {
                uniqueEventsForName.add(event);
            }
            previousComponentId = event.getComponentId();
        }

        final String pathName = uniqueEventsForName.stream()
                // Processor name can be configured by user and more meaningful if available.
                // If the component is already removed, it may not be available here.
                .map(event -> nifiFlow.getProcessComponentName(event.getComponentId(), event::getComponentType))
                .collect(Collectors.joining(", "));

        flowPath.setName(pathName);
        final NiFiFlowPath staticFlowPath = nifiFlow.findPath(firstComponentId);
        flowPath.setGroupId(staticFlowPath != null ? staticFlowPath.getGroupId() : nifiFlow.getRootProcessGroupId());

        // To defer send notification until entire lineagePath analysis gets finished, just add the instance into a buffer.
        createdFlowPaths.add(new Tuple<>(flowPath, dataSetRefs));
    }
}
