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

package org.apache.nifi.registry.flow.diff;

import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedLabel;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedPropertyDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRebaseEngine {

    private RebaseEngine engine;

    @BeforeEach
    public void setup() {
        engine = new RebaseEngine();
    }

    @Test
    public void testCompatiblePositionChangeNoUpstreamConflict() {
        final VersionedProcessor processorA = createProcessor("proc-a", "ProcessorA");
        processorA.setPosition(new Position(100.0, 200.0));

        final VersionedProcessor localProcessorA = createProcessor("proc-a", "ProcessorA");
        localProcessorA.setPosition(new Position(300.0, 400.0));

        final VersionedProcessor upstreamProcessorB = createProcessor("proc-b", "ProcessorB");

        final Set<FlowDifference> localDifferences = new HashSet<>();
        localDifferences.add(new StandardFlowDifference(DifferenceType.POSITION_CHANGED, processorA, localProcessorA,
                new Position(100.0, 200.0), new Position(300.0, 400.0), "Position changed on ProcessorA"));

        final Set<FlowDifference> upstreamDifferences = new HashSet<>();
        upstreamDifferences.add(new StandardFlowDifference(DifferenceType.COMPONENT_ADDED, null, upstreamProcessorB,
                null, upstreamProcessorB, "ProcessorB added upstream"));

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier("root");
        targetSnapshot.getProcessors().add(createProcessor("proc-a", "ProcessorA"));
        targetSnapshot.getProcessors().add(createProcessor("proc-b", "ProcessorB"));

        final RebaseAnalysis analysis = engine.analyze(localDifferences, upstreamDifferences, targetSnapshot);

        assertTrue(analysis.isRebaseAllowed());
        assertEquals(1, analysis.getClassifiedLocalChanges().size());
        assertEquals(RebaseClassification.COMPATIBLE, analysis.getClassifiedLocalChanges().get(0).getClassification());

        final VersionedProcessGroup merged = analysis.getMergedSnapshot();
        assertNotNull(merged);
        final VersionedProcessor mergedProcA = findProcessorById(merged, "proc-a");
        assertNotNull(mergedProcA);
        assertEquals(300.0, mergedProcA.getPosition().getX());
        assertEquals(400.0, mergedProcA.getPosition().getY());
    }

    @Test
    public void testCompatiblePropertyChangeOnDifferentProperties() {
        final VersionedProcessor processorA = createProcessorWithProperty("proc-a", "ProcessorA", "propX", "oldValueX");
        final VersionedProcessor localProcessorA = createProcessorWithProperty("proc-a", "ProcessorA", "propX", "newValueX");

        final VersionedProcessor upstreamProcessorB = createProcessorWithProperty("proc-b", "ProcessorB", "propY", "oldValueY");
        final VersionedProcessor upstreamProcessorBNew = createProcessorWithProperty("proc-b", "ProcessorB", "propY", "newValueY");

        final Set<FlowDifference> localDifferences = new HashSet<>();
        localDifferences.add(new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processorA, localProcessorA, "propX",
                "oldValueX", "newValueX", "Property propX changed on ProcessorA"));

        final Set<FlowDifference> upstreamDifferences = new HashSet<>();
        upstreamDifferences.add(new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, upstreamProcessorB, upstreamProcessorBNew, "propY",
                "oldValueY", "newValueY", "Property propY changed on ProcessorB"));

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier("root");
        targetSnapshot.getProcessors().add(createProcessorWithProperty("proc-a", "ProcessorA", "propX", "oldValueX"));
        targetSnapshot.getProcessors().add(createProcessorWithProperty("proc-b", "ProcessorB", "propY", "newValueY"));

        final RebaseAnalysis analysis = engine.analyze(localDifferences, upstreamDifferences, targetSnapshot);

        assertTrue(analysis.isRebaseAllowed());
        assertEquals(1, analysis.getClassifiedLocalChanges().size());
        assertEquals(RebaseClassification.COMPATIBLE, analysis.getClassifiedLocalChanges().get(0).getClassification());
    }

    @Test
    public void testConflictingPropertyChangeOnSamePropertyAndComponent() {
        final VersionedProcessor processor = createProcessorWithProperty("proc-a", "ProcessorA", "propX", "original");
        final VersionedProcessor localProcessor = createProcessorWithProperty("proc-a", "ProcessorA", "propX", "localValue");
        final VersionedProcessor upstreamProcessor = createProcessorWithProperty("proc-a", "ProcessorA", "propX", "upstreamValue");

        final Set<FlowDifference> localDifferences = new HashSet<>();
        localDifferences.add(new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processor, localProcessor, "propX",
                "original", "localValue", "Property propX changed locally"));

        final Set<FlowDifference> upstreamDifferences = new HashSet<>();
        upstreamDifferences.add(new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processor, upstreamProcessor, "propX",
                "original", "upstreamValue", "Property propX changed upstream"));

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier("root");
        targetSnapshot.getProcessors().add(createProcessorWithProperty("proc-a", "ProcessorA", "propX", "upstreamValue"));

        final RebaseAnalysis analysis = engine.analyze(localDifferences, upstreamDifferences, targetSnapshot);

        assertFalse(analysis.isRebaseAllowed());
        assertEquals(1, analysis.getClassifiedLocalChanges().size());
        assertEquals(RebaseClassification.CONFLICTING, analysis.getClassifiedLocalChanges().get(0).getClassification());
        assertNull(analysis.getMergedSnapshot());
    }

    @Test
    public void testUnsupportedDifferenceTypeNoHandler() {
        final VersionedProcessor processor = createProcessor("proc-a", "ProcessorA");

        final Set<FlowDifference> localDifferences = new HashSet<>();
        localDifferences.add(new StandardFlowDifference(DifferenceType.COMPONENT_ADDED, null, processor,
                null, processor, "Component added locally"));

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier("root");

        final RebaseAnalysis analysis = engine.analyze(localDifferences, Collections.emptySet(), targetSnapshot);

        assertFalse(analysis.isRebaseAllowed());
        assertEquals(1, analysis.getClassifiedLocalChanges().size());

        final RebaseAnalysis.ClassifiedDifference classified = analysis.getClassifiedLocalChanges().get(0);
        assertEquals(RebaseClassification.UNSUPPORTED, classified.getClassification());
        assertEquals("NO_HANDLER", classified.getConflictCode());
        assertNull(analysis.getMergedSnapshot());
    }

    @Test
    public void testMixedCompatibleAndUnsupported() {
        final VersionedProcessor processorA = createProcessor("proc-a", "ProcessorA");
        processorA.setPosition(new Position(10.0, 20.0));
        final VersionedProcessor localProcessorA = createProcessor("proc-a", "ProcessorA");
        localProcessorA.setPosition(new Position(50.0, 60.0));

        final VersionedProcessor processorB = createProcessor("proc-b", "ProcessorB");

        final Set<FlowDifference> localDifferences = new HashSet<>();
        localDifferences.add(new StandardFlowDifference(DifferenceType.POSITION_CHANGED, processorA, localProcessorA,
                new Position(10.0, 20.0), new Position(50.0, 60.0), "Position changed"));
        localDifferences.add(new StandardFlowDifference(DifferenceType.COMPONENT_ADDED, null, processorB,
                null, processorB, "Component added"));

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier("root");
        targetSnapshot.getProcessors().add(createProcessor("proc-a", "ProcessorA"));

        final RebaseAnalysis analysis = engine.analyze(localDifferences, Collections.emptySet(), targetSnapshot);

        assertFalse(analysis.isRebaseAllowed());
        assertNull(analysis.getMergedSnapshot());
    }

    @Test
    public void testMixedCompatibleAndConflicting() {
        final VersionedProcessor processorA = createProcessor("proc-a", "ProcessorA");
        processorA.setPosition(new Position(10.0, 20.0));
        final VersionedProcessor localProcessorA = createProcessor("proc-a", "ProcessorA");
        localProcessorA.setPosition(new Position(50.0, 60.0));

        final VersionedProcessor processorB = createProcessorWithProperty("proc-b", "ProcessorB", "propX", "original");
        final VersionedProcessor localProcessorB = createProcessorWithProperty("proc-b", "ProcessorB", "propX", "localVal");
        final VersionedProcessor upstreamProcessorB = createProcessorWithProperty("proc-b", "ProcessorB", "propX", "upstreamVal");

        final Set<FlowDifference> localDifferences = new HashSet<>();
        localDifferences.add(new StandardFlowDifference(DifferenceType.POSITION_CHANGED, processorA, localProcessorA,
                new Position(10.0, 20.0), new Position(50.0, 60.0), "Position changed"));
        localDifferences.add(new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processorB, localProcessorB, "propX",
                "original", "localVal", "Property changed locally"));

        final Set<FlowDifference> upstreamDifferences = new HashSet<>();
        upstreamDifferences.add(new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processorB, upstreamProcessorB, "propX",
                "original", "upstreamVal", "Property changed upstream"));

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier("root");

        final RebaseAnalysis analysis = engine.analyze(localDifferences, upstreamDifferences, targetSnapshot);

        assertFalse(analysis.isRebaseAllowed());
        assertNull(analysis.getMergedSnapshot());
    }

    @Test
    public void testAllCompatibleMultipleLocalChanges() {
        final VersionedProcessor processorA = createProcessorWithProperty("proc-a", "ProcessorA", "propX", "oldVal");
        processorA.setPosition(new Position(10.0, 20.0));
        processorA.setComments("old comments");

        final VersionedProcessor localProcessorA = createProcessorWithProperty("proc-a", "ProcessorA", "propX", "newVal");
        localProcessorA.setPosition(new Position(50.0, 60.0));
        localProcessorA.setComments("new comments");

        final Set<FlowDifference> localDifferences = new HashSet<>();
        localDifferences.add(new StandardFlowDifference(DifferenceType.POSITION_CHANGED, processorA, localProcessorA,
                new Position(10.0, 20.0), new Position(50.0, 60.0), "Position changed"));
        localDifferences.add(new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processorA, localProcessorA, "propX",
                "oldVal", "newVal", "Property propX changed"));
        localDifferences.add(new StandardFlowDifference(DifferenceType.COMMENTS_CHANGED, processorA, localProcessorA,
                "old comments", "new comments", "Comments changed"));

        final VersionedProcessor targetProcessor = createProcessorWithProperty("proc-a", "ProcessorA", "propX", "oldVal");
        targetProcessor.setPosition(new Position(10.0, 20.0));
        targetProcessor.setComments("old comments");

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier("root");
        targetSnapshot.getProcessors().add(targetProcessor);

        final RebaseAnalysis analysis = engine.analyze(localDifferences, Collections.emptySet(), targetSnapshot);

        assertTrue(analysis.isRebaseAllowed());
        assertEquals(3, analysis.getClassifiedLocalChanges().size());
        for (final RebaseAnalysis.ClassifiedDifference classified : analysis.getClassifiedLocalChanges()) {
            assertEquals(RebaseClassification.COMPATIBLE, classified.getClassification());
        }

        final VersionedProcessGroup merged = analysis.getMergedSnapshot();
        assertNotNull(merged);
        final VersionedProcessor mergedProc = findProcessorById(merged, "proc-a");
        assertNotNull(mergedProc);
        assertEquals(50.0, mergedProc.getPosition().getX());
        assertEquals(60.0, mergedProc.getPosition().getY());
        assertEquals("newVal", mergedProc.getProperties().get("propX"));
        assertEquals("new comments", mergedProc.getComments());
    }

    @Test
    public void testEmptyLocalChanges() {
        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier("root");
        targetSnapshot.getProcessors().add(createProcessor("proc-a", "ProcessorA"));

        final RebaseAnalysis analysis = engine.analyze(Collections.emptySet(), Collections.emptySet(), targetSnapshot);

        assertTrue(analysis.isRebaseAllowed());
        assertTrue(analysis.getClassifiedLocalChanges().isEmpty());
        assertNotNull(analysis.getMergedSnapshot());
        assertEquals("root", analysis.getMergedSnapshot().getIdentifier());
    }

    @Test
    public void testCanonicalConflictKeySameComponentDifferentProperties() {
        final VersionedProcessor processor = createProcessor("proc-a", "ProcessorA");

        final FlowDifference diffPropX = new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processor, processor, "propX",
                "old", "new", "Property propX changed");
        final FlowDifference diffPropY = new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processor, processor, "propY",
                "old", "new", "Property propY changed");

        final Set<FlowDifference> localDifferences = new HashSet<>();
        localDifferences.add(diffPropX);
        localDifferences.add(diffPropY);

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier("root");
        targetSnapshot.getProcessors().add(createProcessorWithProperty("proc-a", "ProcessorA", "propX", "old"));

        final RebaseAnalysis analysis = engine.analyze(localDifferences, Collections.emptySet(), targetSnapshot);

        assertTrue(analysis.isRebaseAllowed());
        assertEquals(2, analysis.getClassifiedLocalChanges().size());
        for (final RebaseAnalysis.ClassifiedDifference classified : analysis.getClassifiedLocalChanges()) {
            assertEquals(RebaseClassification.COMPATIBLE, classified.getClassification());
        }
    }

    @Test
    public void testCanonicalConflictKeySamePropertyDifferentComponents() {
        final VersionedProcessor processorA = createProcessor("proc-a", "ProcessorA");
        final VersionedProcessor processorB = createProcessor("proc-b", "ProcessorB");

        final FlowDifference diffA = new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processorA, processorA, "propX",
                "old", "new", "Property propX changed on A");
        final FlowDifference diffB = new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processorB, processorB, "propX",
                "old", "new", "Property propX changed on B");

        final String keyA = RebaseEngine.computeConflictKey(diffA);
        final String keyB = RebaseEngine.computeConflictKey(diffB);
        assertNotEquals(keyA, keyB);

        final Set<FlowDifference> localDifferences = new HashSet<>();
        localDifferences.add(diffA);
        localDifferences.add(diffB);

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier("root");
        targetSnapshot.getProcessors().add(createProcessorWithProperty("proc-a", "ProcessorA", "propX", "old"));
        targetSnapshot.getProcessors().add(createProcessorWithProperty("proc-b", "ProcessorB", "propX", "old"));

        final RebaseAnalysis analysis = engine.analyze(localDifferences, Collections.emptySet(), targetSnapshot);

        assertTrue(analysis.isRebaseAllowed());
        assertEquals(2, analysis.getClassifiedLocalChanges().size());
        for (final RebaseAnalysis.ClassifiedDifference classified : analysis.getClassifiedLocalChanges()) {
            assertEquals(RebaseClassification.COMPATIBLE, classified.getClassification());
        }
    }

    @Test
    public void testAnalysisFingerprintDeterminism() {
        final VersionedProcessor processor = createProcessor("proc-a", "ProcessorA");
        processor.setPosition(new Position(10.0, 20.0));
        final VersionedProcessor localProcessor = createProcessor("proc-a", "ProcessorA");
        localProcessor.setPosition(new Position(50.0, 60.0));

        final Set<FlowDifference> localDifferences = new HashSet<>();
        localDifferences.add(new StandardFlowDifference(DifferenceType.POSITION_CHANGED, processor, localProcessor,
                new Position(10.0, 20.0), new Position(50.0, 60.0), "Position changed"));

        final Set<FlowDifference> upstreamDifferences = new HashSet<>();
        upstreamDifferences.add(new StandardFlowDifference(DifferenceType.COMPONENT_ADDED, null, createProcessor("proc-b", "ProcessorB"),
                null, null, "Upstream added proc-b"));

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier("root");
        targetSnapshot.getProcessors().add(createProcessor("proc-a", "ProcessorA"));

        final RebaseAnalysis analysis1 = engine.analyze(localDifferences, upstreamDifferences, targetSnapshot);
        final RebaseAnalysis analysis2 = engine.analyze(localDifferences, upstreamDifferences, targetSnapshot);

        assertNotNull(analysis1.getAnalysisFingerprint());
        assertNotNull(analysis2.getAnalysisFingerprint());
        assertEquals(analysis1.getAnalysisFingerprint(), analysis2.getAnalysisFingerprint());
    }

    @Test
    public void testAnalysisFingerprintChangesWithDifferentInputs() {
        final VersionedProcessor processor = createProcessor("proc-a", "ProcessorA");
        processor.setPosition(new Position(10.0, 20.0));
        final VersionedProcessor localProcessor = createProcessor("proc-a", "ProcessorA");
        localProcessor.setPosition(new Position(50.0, 60.0));

        final Set<FlowDifference> localDifferences1 = new HashSet<>();
        localDifferences1.add(new StandardFlowDifference(DifferenceType.POSITION_CHANGED, processor, localProcessor,
                new Position(10.0, 20.0), new Position(50.0, 60.0), "Position changed"));

        final Set<FlowDifference> localDifferences2 = new HashSet<>();
        localDifferences2.add(new StandardFlowDifference(DifferenceType.COMMENTS_CHANGED, processor, localProcessor,
                "old", "new", "Comments changed"));

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier("root");
        targetSnapshot.getProcessors().add(createProcessor("proc-a", "ProcessorA"));

        final RebaseAnalysis analysis1 = engine.analyze(localDifferences1, Collections.emptySet(), targetSnapshot);
        final RebaseAnalysis analysis2 = engine.analyze(localDifferences2, Collections.emptySet(), targetSnapshot);

        assertNotEquals(analysis1.getAnalysisFingerprint(), analysis2.getAnalysisFingerprint());
    }

    @Test
    public void testDeepCloneIndependence() {
        final VersionedProcessGroup original = new VersionedProcessGroup();
        original.setIdentifier("root");
        original.setComments("original comments");

        final VersionedProcessor processor = createProcessor("proc-a", "ProcessorA");
        processor.setPosition(new Position(10.0, 20.0));
        original.getProcessors().add(processor);

        final VersionedProcessGroup cloned = engine.deepClone(original);

        cloned.setComments("modified comments");
        final VersionedProcessor clonedProcessor = cloned.getProcessors().iterator().next();
        clonedProcessor.setPosition(new Position(999.0, 999.0));

        assertEquals("original comments", original.getComments());
        final VersionedProcessor originalProcessor = original.getProcessors().iterator().next();
        assertEquals(10.0, originalProcessor.getPosition().getX());
        assertEquals(20.0, originalProcessor.getPosition().getY());
    }

    @Test
    public void testNestedComponentResolutionPositionChange() {
        final VersionedProcessor processor = createProcessor("nested-proc", "NestedProcessor");
        processor.setPosition(new Position(10.0, 20.0));
        final VersionedProcessor localProcessor = createProcessor("nested-proc", "NestedProcessor");
        localProcessor.setPosition(new Position(100.0, 200.0));

        final Set<FlowDifference> localDifferences = new HashSet<>();
        localDifferences.add(new StandardFlowDifference(DifferenceType.POSITION_CHANGED, processor, localProcessor,
                new Position(10.0, 20.0), new Position(100.0, 200.0), "Position changed on nested processor"));

        final VersionedProcessGroup childGroup = new VersionedProcessGroup();
        childGroup.setIdentifier("child-pg");
        final VersionedProcessor targetNestedProcessor = createProcessor("nested-proc", "NestedProcessor");
        targetNestedProcessor.setPosition(new Position(10.0, 20.0));
        childGroup.getProcessors().add(targetNestedProcessor);

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier("root");
        targetSnapshot.getProcessGroups().add(childGroup);

        final RebaseAnalysis analysis = engine.analyze(localDifferences, Collections.emptySet(), targetSnapshot);

        assertTrue(analysis.isRebaseAllowed());
        assertNotNull(analysis.getMergedSnapshot());

        final VersionedProcessGroup mergedChild = analysis.getMergedSnapshot().getProcessGroups().iterator().next();
        final VersionedProcessor mergedNestedProc = mergedChild.getProcessors().iterator().next();
        assertEquals(100.0, mergedNestedProc.getPosition().getX());
        assertEquals(200.0, mergedNestedProc.getPosition().getY());
    }

    @Test
    public void testSizeChangeCompatibility() {
        final VersionedLabel originalLabel = createLabel("label-a", 100.0, 50.0);
        final VersionedLabel localLabel = createLabel("label-a", 200.0, 100.0);

        final Set<FlowDifference> localDifferences = new HashSet<>();
        localDifferences.add(new StandardFlowDifference(DifferenceType.SIZE_CHANGED, originalLabel, localLabel,
                null, null, "Size changed on label"));

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier("root");
        targetSnapshot.getLabels().add(createLabel("label-a", 100.0, 50.0));

        final RebaseAnalysis analysis = engine.analyze(localDifferences, Collections.emptySet(), targetSnapshot);

        assertTrue(analysis.isRebaseAllowed());
        assertEquals(RebaseClassification.COMPATIBLE, analysis.getClassifiedLocalChanges().get(0).getClassification());
        assertNotNull(analysis.getMergedSnapshot());

        final VersionedLabel mergedLabel = analysis.getMergedSnapshot().getLabels().iterator().next();
        assertEquals(200.0, mergedLabel.getWidth());
        assertEquals(100.0, mergedLabel.getHeight());
    }

    @Test
    public void testBendpointsChangeCompatibility() {
        final VersionedConnection originalConn = createConnection("conn-a");
        originalConn.setBends(List.of(new Position(10.0, 10.0)));

        final VersionedConnection localConn = createConnection("conn-a");
        final List<Position> localBends = List.of(new Position(50.0, 50.0), new Position(75.0, 75.0));
        localConn.setBends(localBends);

        final Set<FlowDifference> localDifferences = new HashSet<>();
        localDifferences.add(new StandardFlowDifference(DifferenceType.BENDPOINTS_CHANGED, originalConn, localConn,
                originalConn.getBends(), localConn.getBends(), "Bendpoints changed"));

        final VersionedConnection targetConn = createConnection("conn-a");
        targetConn.setBends(List.of(new Position(10.0, 10.0)));

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier("root");
        targetSnapshot.getConnections().add(targetConn);

        final RebaseAnalysis analysis = engine.analyze(localDifferences, Collections.emptySet(), targetSnapshot);

        assertTrue(analysis.isRebaseAllowed());
        assertEquals(RebaseClassification.COMPATIBLE, analysis.getClassifiedLocalChanges().get(0).getClassification());
        assertNotNull(analysis.getMergedSnapshot());

        final VersionedConnection mergedConn = analysis.getMergedSnapshot().getConnections().iterator().next();
        assertEquals(2, mergedConn.getBends().size());
    }

    @Test
    public void testCommentsChangeCompatibility() {
        final VersionedProcessor processor = createProcessor("proc-a", "ProcessorA");
        processor.setComments("old comments");
        final VersionedProcessor localProcessor = createProcessor("proc-a", "ProcessorA");
        localProcessor.setComments("new comments");

        final Set<FlowDifference> localDifferences = new HashSet<>();
        localDifferences.add(new StandardFlowDifference(DifferenceType.COMMENTS_CHANGED, processor, localProcessor,
                "old comments", "new comments", "Comments changed"));

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier("root");
        final VersionedProcessor targetProcessor = createProcessor("proc-a", "ProcessorA");
        targetProcessor.setComments("old comments");
        targetSnapshot.getProcessors().add(targetProcessor);

        final RebaseAnalysis analysis = engine.analyze(localDifferences, Collections.emptySet(), targetSnapshot);

        assertTrue(analysis.isRebaseAllowed());
        assertEquals(RebaseClassification.COMPATIBLE, analysis.getClassifiedLocalChanges().get(0).getClassification());
        assertNotNull(analysis.getMergedSnapshot());

        final VersionedProcessor mergedProc = findProcessorById(analysis.getMergedSnapshot(), "proc-a");
        assertNotNull(mergedProc);
        assertEquals("new comments", mergedProc.getComments());
    }

    @Test
    public void testCommentsChangeConflict() {
        final VersionedProcessor processor = createProcessor("proc-a", "ProcessorA");
        processor.setComments("original");
        final VersionedProcessor localProcessor = createProcessor("proc-a", "ProcessorA");
        localProcessor.setComments("local comments");
        final VersionedProcessor upstreamProcessor = createProcessor("proc-a", "ProcessorA");
        upstreamProcessor.setComments("upstream comments");

        final Set<FlowDifference> localDifferences = new HashSet<>();
        localDifferences.add(new StandardFlowDifference(DifferenceType.COMMENTS_CHANGED, processor, localProcessor,
                "original", "local comments", "Comments changed locally"));

        final Set<FlowDifference> upstreamDifferences = new HashSet<>();
        upstreamDifferences.add(new StandardFlowDifference(DifferenceType.COMMENTS_CHANGED, processor, upstreamProcessor,
                "original", "upstream comments", "Comments changed upstream"));

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier("root");

        final RebaseAnalysis analysis = engine.analyze(localDifferences, upstreamDifferences, targetSnapshot);

        assertFalse(analysis.isRebaseAllowed());
        assertEquals(1, analysis.getClassifiedLocalChanges().size());
        assertEquals(RebaseClassification.CONFLICTING, analysis.getClassifiedLocalChanges().get(0).getClassification());
        assertNull(analysis.getMergedSnapshot());
    }

    private VersionedProcessor createProcessor(final String identifier, final String name) {
        final VersionedProcessor processor = new VersionedProcessor();
        processor.setIdentifier(identifier);
        processor.setName(name);
        processor.setProperties(Collections.emptyMap());
        processor.setPropertyDescriptors(Collections.emptyMap());
        return processor;
    }

    private VersionedProcessor createProcessorWithProperty(final String identifier, final String name, final String propertyName, final String propertyValue) {
        final VersionedProcessor processor = new VersionedProcessor();
        processor.setIdentifier(identifier);
        processor.setName(name);

        final Map<String, String> properties = new HashMap<>();
        properties.put(propertyName, propertyValue);
        processor.setProperties(properties);

        final VersionedPropertyDescriptor descriptor = new VersionedPropertyDescriptor();
        descriptor.setName(propertyName);
        descriptor.setSensitive(false);

        final Map<String, VersionedPropertyDescriptor> descriptors = new HashMap<>();
        descriptors.put(propertyName, descriptor);
        processor.setPropertyDescriptors(descriptors);

        return processor;
    }

    private VersionedLabel createLabel(final String identifier, final double width, final double height) {
        final VersionedLabel label = new VersionedLabel();
        label.setIdentifier(identifier);
        label.setWidth(width);
        label.setHeight(height);
        return label;
    }

    private VersionedConnection createConnection(final String identifier) {
        final VersionedConnection connection = new VersionedConnection();
        connection.setIdentifier(identifier);
        return connection;
    }

    private VersionedProcessor findProcessorById(final VersionedProcessGroup group, final String identifier) {
        for (final VersionedProcessor processor : group.getProcessors()) {
            if (identifier.equals(processor.getIdentifier())) {
                return processor;
            }
        }
        for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
            final VersionedProcessor result = findProcessorById(childGroup, identifier);
            if (result != null) {
                return result;
            }
        }
        return null;
    }
}
