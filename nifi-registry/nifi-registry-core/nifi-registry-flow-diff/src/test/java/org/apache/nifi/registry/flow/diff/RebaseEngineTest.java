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

import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedControllerService;
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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RebaseEngineTest {

    private static final String ROOT_ID = "root";
    private static final String VERSION_N_ROOT_ID = "root-n";
    private static final String TARGET_ROOT_ID = "root-n-plus-one";
    private static final String PROCESSOR_ID = "proc-a";
    private static final String PROCESSOR_NAME = "ProcessorA";
    private static final String SERVICE_A_ID = "service-a";
    private static final String SERVICE_X_ID = "service-x";
    private static final String SERVICE_Y_ID = "service-y";
    private static final String SERVICE_Z_ID = "service-z";
    private static final String SERVICE_A_NAME = "Service A";
    private static final String SERVICE_X_NAME = "Service X";
    private static final String SERVICE_Y_NAME = "Service Y";
    private static final String SERVICE_Z_NAME = "Service Z";
    private static final String CONTROLLER_SERVICE_PROPERTY = "controller.service";
    private static final String DYNAMIC_Y_PROPERTY = "dynamic.y";
    private static final String DYNAMIC_Z_PROPERTY = "dynamic.z";
    private static final String CONTROLLER_SERVICE_TYPE = "org.apache.nifi.services.LocalControllerService";
    private static final String BUNDLE_GROUP = "group";
    private static final String BUNDLE_ARTIFACT = "artifact";
    private static final String BUNDLE_VERSION = "1.0.0";

    private RebaseEngine engine;

    @BeforeEach
    void setup() {
        engine = new StandardRebaseEngine();
    }

    @Test
    void testCompatiblePositionChangeNoUpstreamConflict() {
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
    void testCompatiblePropertyChangeOnDifferentProperties() {
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
    void testConflictingPropertyChangeOnSamePropertyAndComponent() {
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
    void testUnsupportedLocalProcessorAdditionUsesRegisteredComponentAddedHandler() {
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
        assertEquals(RebaseConflictCode.UNSUPPORTED_COMPONENT_TYPE, classified.getConflictCode());
        assertNull(analysis.getMergedSnapshot());
    }

    @Test
    void testAnalyzeScenario1PreservesAddedControllerServiceAndProcessorReference() {
        final VersionedControllerService versionNService = createControllerService(SERVICE_X_ID, SERVICE_X_NAME, ROOT_ID);
        final VersionedControllerService localAddedService = createControllerService(SERVICE_Y_ID, SERVICE_Y_NAME, ROOT_ID);

        final VersionedProcessor versionNProcessor = createProcessorWithProperty(PROCESSOR_ID, PROCESSOR_NAME, CONTROLLER_SERVICE_PROPERTY, SERVICE_X_ID);
        final VersionedProcessor localProcessor = createProcessorWithProperty(PROCESSOR_ID, PROCESSOR_NAME, CONTROLLER_SERVICE_PROPERTY, SERVICE_Y_ID);

        final Set<FlowDifference> localDifferences = new HashSet<>();
        localDifferences.add(new StandardFlowDifference(DifferenceType.COMPONENT_ADDED, null, localAddedService,
                null, localAddedService, "Controller service added locally"));
        localDifferences.add(new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, versionNProcessor, localProcessor, CONTROLLER_SERVICE_PROPERTY,
                SERVICE_X_ID, SERVICE_Y_ID, "Processor property changed locally"));

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier(ROOT_ID);
        targetSnapshot.getControllerServices().add(versionNService);
        targetSnapshot.getProcessors().add(createProcessorWithProperty(PROCESSOR_ID, PROCESSOR_NAME, CONTROLLER_SERVICE_PROPERTY, SERVICE_X_ID));

        final RebaseAnalysis analysis = engine.analyze(localDifferences, Collections.emptySet(), targetSnapshot);

        assertTrue(analysis.isRebaseAllowed());
        assertEquals(2, analysis.getClassifiedLocalChanges().size());
        assertAllCompatible(analysis);
        assertClassification(analysis, DifferenceType.COMPONENT_ADDED, SERVICE_Y_ID, RebaseClassification.COMPATIBLE, null);
        assertClassification(analysis, DifferenceType.PROPERTY_CHANGED, PROCESSOR_ID, RebaseClassification.COMPATIBLE, null);

        final VersionedProcessGroup merged = analysis.getMergedSnapshot();
        assertNotNull(merged);
        final VersionedControllerService mergedService = findControllerServiceById(merged, SERVICE_Y_ID);
        assertSame(localAddedService, mergedService);

        final VersionedProcessor mergedProcessor = findProcessorById(merged, PROCESSOR_ID);
        assertNotNull(mergedProcessor);
        assertEquals(SERVICE_Y_ID, mergedProcessor.getProperties().get(CONTROLLER_SERVICE_PROPERTY));
    }

    @Test
    void testAnalyzePreservesRootAdditionWhenTargetRootIdentifierChangedUpstream() {
        final VersionedControllerService localAddedService = createControllerService(SERVICE_Y_ID, SERVICE_Y_NAME, VERSION_N_ROOT_ID);
        final Set<FlowDifference> localDifferences = Set.of(new StandardFlowDifference(DifferenceType.COMPONENT_ADDED, null, localAddedService,
                null, localAddedService, "Controller service added locally"));

        final VersionedProcessGroup targetRoot = new VersionedProcessGroup();
        targetRoot.setIdentifier(TARGET_ROOT_ID);
        targetRoot.setInstanceIdentifier(VERSION_N_ROOT_ID);
        targetRoot.setName("Root");

        final RebaseAnalysis analysis = engine.analyze(localDifferences, Collections.emptySet(), targetRoot);

        assertTrue(analysis.isRebaseAllowed());
        assertSame(localAddedService, findControllerServiceById(targetRoot, SERVICE_Y_ID));
        assertEquals(TARGET_ROOT_ID, localAddedService.getGroupIdentifier());
    }

    @Test
    void testAnalyzeRejectsRootAdditionWhenParentRemovedUpstream() {
        final VersionedControllerService localAddedService = createControllerService(SERVICE_Y_ID, SERVICE_Y_NAME, VERSION_N_ROOT_ID);
        final Set<FlowDifference> localDifferences = Set.of(new StandardFlowDifference(DifferenceType.COMPONENT_ADDED, null, localAddedService,
                null, localAddedService, "Controller service added locally"));

        final VersionedProcessGroup removedParent = new VersionedProcessGroup();
        removedParent.setIdentifier(VERSION_N_ROOT_ID);

        final VersionedProcessGroup targetRoot = new VersionedProcessGroup();
        targetRoot.setIdentifier("replacement-root");

        final Set<FlowDifference> upstreamDifferences = Set.of(new StandardFlowDifference(DifferenceType.COMPONENT_REMOVED, removedParent, null,
                removedParent, null, "Parent removed upstream"));

        final RebaseAnalysis analysis = engine.analyze(localDifferences, upstreamDifferences, targetRoot);

        assertFalse(analysis.isRebaseAllowed());
        assertClassification(analysis, DifferenceType.COMPONENT_ADDED, SERVICE_Y_ID, RebaseClassification.UNSUPPORTED,
                RebaseConflictCode.COMPONENT_NOT_FOUND);
    }

    @Test
    void testAnalyzeScenario2PreservesMultipleAddedControllerServicesAndDynamicReferences() {
        final VersionedControllerService versionNServiceA = createControllerService(SERVICE_A_ID, SERVICE_A_NAME, ROOT_ID);
        versionNServiceA.setProperties(Collections.emptyMap());
        versionNServiceA.setPropertyDescriptors(Collections.emptyMap());

        final VersionedControllerService localServiceA = createControllerService(SERVICE_A_ID, SERVICE_A_NAME, ROOT_ID);
        localServiceA.setProperties(Map.of(DYNAMIC_Y_PROPERTY, SERVICE_Y_ID, DYNAMIC_Z_PROPERTY, SERVICE_Z_ID));
        localServiceA.setPropertyDescriptors(Map.of(
                DYNAMIC_Y_PROPERTY, createPropertyDescriptor(DYNAMIC_Y_PROPERTY, true, false),
                DYNAMIC_Z_PROPERTY, createPropertyDescriptor(DYNAMIC_Z_PROPERTY, true, false)));

        final VersionedControllerService localServiceY = createControllerService(SERVICE_Y_ID, SERVICE_Y_NAME, ROOT_ID);
        final VersionedControllerService localServiceZ = createControllerService(SERVICE_Z_ID, SERVICE_Z_NAME, ROOT_ID);

        final Set<FlowDifference> localDifferences = new HashSet<>();
        localDifferences.add(new StandardFlowDifference(DifferenceType.COMPONENT_ADDED, null, localServiceY,
                null, localServiceY, "Controller service Y added locally"));
        localDifferences.add(new StandardFlowDifference(DifferenceType.COMPONENT_ADDED, null, localServiceZ,
                null, localServiceZ, "Controller service Z added locally"));
        localDifferences.add(new StandardFlowDifference(DifferenceType.PROPERTY_ADDED, versionNServiceA, localServiceA, DYNAMIC_Y_PROPERTY,
                null, SERVICE_Y_ID, "Dynamic property added for service Y"));
        localDifferences.add(new StandardFlowDifference(DifferenceType.PROPERTY_ADDED, versionNServiceA, localServiceA, DYNAMIC_Z_PROPERTY,
                null, SERVICE_Z_ID, "Dynamic property added for service Z"));

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier(ROOT_ID);
        targetSnapshot.getControllerServices().add(versionNServiceA);

        final RebaseAnalysis analysis = engine.analyze(localDifferences, Collections.emptySet(), targetSnapshot);

        assertTrue(analysis.isRebaseAllowed());
        assertEquals(4, analysis.getClassifiedLocalChanges().size());
        assertAllCompatible(analysis);
        assertClassification(analysis, DifferenceType.COMPONENT_ADDED, SERVICE_Y_ID, RebaseClassification.COMPATIBLE, null);
        assertClassification(analysis, DifferenceType.COMPONENT_ADDED, SERVICE_Z_ID, RebaseClassification.COMPATIBLE, null);

        final VersionedProcessGroup merged = analysis.getMergedSnapshot();
        assertNotNull(merged);
        assertSame(localServiceY, findControllerServiceById(merged, SERVICE_Y_ID));
        assertSame(localServiceZ, findControllerServiceById(merged, SERVICE_Z_ID));

        final VersionedControllerService mergedServiceA = findControllerServiceById(merged, SERVICE_A_ID);
        assertNotNull(mergedServiceA);
        assertEquals(SERVICE_Y_ID, mergedServiceA.getProperties().get(DYNAMIC_Y_PROPERTY));
        assertEquals(SERVICE_Z_ID, mergedServiceA.getProperties().get(DYNAMIC_Z_PROPERTY));
    }

    @Test
    void testAnalyzeCollisionBlocksRebaseAndDoesNotMutateTargetSnapshot() {
        final VersionedControllerService collidingTargetService = createControllerService(SERVICE_Y_ID, "Target Service Y", ROOT_ID);
        final VersionedControllerService localAddedService = createControllerService(SERVICE_Y_ID, "Local Service Y", ROOT_ID);

        final VersionedProcessor versionNProcessor = createProcessorWithProperty(PROCESSOR_ID, PROCESSOR_NAME, CONTROLLER_SERVICE_PROPERTY, SERVICE_X_ID);
        final VersionedProcessor localProcessor = createProcessorWithProperty(PROCESSOR_ID, PROCESSOR_NAME, CONTROLLER_SERVICE_PROPERTY, SERVICE_Y_ID);

        final Set<FlowDifference> localDifferences = new HashSet<>();
        localDifferences.add(new StandardFlowDifference(DifferenceType.COMPONENT_ADDED, null, localAddedService,
                null, localAddedService, "Controller service added with collision"));
        localDifferences.add(new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, versionNProcessor, localProcessor, CONTROLLER_SERVICE_PROPERTY,
                SERVICE_X_ID, SERVICE_Y_ID, "Processor property changed locally"));

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier(ROOT_ID);
        targetSnapshot.getControllerServices().add(collidingTargetService);
        targetSnapshot.getProcessors().add(createProcessorWithProperty(PROCESSOR_ID, PROCESSOR_NAME, CONTROLLER_SERVICE_PROPERTY, SERVICE_X_ID));

        final RebaseAnalysis analysis = engine.analyze(localDifferences, Collections.emptySet(), targetSnapshot);

        assertFalse(analysis.isRebaseAllowed());
        assertNull(analysis.getMergedSnapshot());
        assertClassification(analysis, DifferenceType.COMPONENT_ADDED, SERVICE_Y_ID, RebaseClassification.CONFLICTING,
                RebaseConflictCode.COMPONENT_IDENTIFIER_COLLISION);
        assertEquals(1, countComponentsById(targetSnapshot, SERVICE_Y_ID));

        final VersionedProcessor unchangedProcessor = findProcessorById(targetSnapshot, PROCESSOR_ID);
        assertNotNull(unchangedProcessor);
        assertEquals(SERVICE_X_ID, unchangedProcessor.getProperties().get(CONTROLLER_SERVICE_PROPERTY));
    }

    @Test
    void testMixedCompatibleAndUnsupported() {
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
    void testMixedCompatibleAndConflicting() {
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
    void testAllCompatibleMultipleLocalChanges() {
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
    void testEmptyLocalChanges() {
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
    void testCanonicalConflictKeySameComponentDifferentProperties() {
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
    void testCanonicalConflictKeySamePropertyDifferentComponents() {
        final VersionedProcessor processorA = createProcessor("proc-a", "ProcessorA");
        final VersionedProcessor processorB = createProcessor("proc-b", "ProcessorB");

        final FlowDifference diffA = new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processorA, processorA, "propX",
                "old", "new", "Property propX changed on A");
        final FlowDifference diffB = new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processorB, processorB, "propX",
                "old", "new", "Property propX changed on B");

        final String keyA = StandardRebaseEngine.computeConflictKey(diffA);
        final String keyB = StandardRebaseEngine.computeConflictKey(diffB);
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
    void testAnalysisFingerprintDeterminism() {
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
    void testAnalysisFingerprintChangesWithDifferentInputs() {
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
    void testMergedSnapshotAppliesLocalChangesToTargetInPlace() {
        final VersionedProcessor targetProcessor = createProcessor("proc-a", "ProcessorA");
        targetProcessor.setPosition(new Position(10.0, 20.0));

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier("root");
        targetSnapshot.getProcessors().add(targetProcessor);

        final VersionedProcessor localProcessor = createProcessor("proc-a", "ProcessorA");
        localProcessor.setPosition(new Position(100.0, 200.0));

        final Set<FlowDifference> localDifferences = new HashSet<>();
        localDifferences.add(new StandardFlowDifference(DifferenceType.POSITION_CHANGED, targetProcessor, localProcessor,
                new Position(10.0, 20.0), new Position(100.0, 200.0), "Position changed on proc-a"));

        final RebaseAnalysis analysis = engine.analyze(localDifferences, Collections.emptySet(), targetSnapshot);

        // The engine mutates the provided target snapshot in place to build the merged snapshot
        assertTrue(analysis.isRebaseAllowed());
        assertSame(targetSnapshot, analysis.getMergedSnapshot());
        final VersionedProcessor mergedProcessor = analysis.getMergedSnapshot().getProcessors().iterator().next();
        assertEquals(100.0, mergedProcessor.getPosition().getX());
        assertEquals(200.0, mergedProcessor.getPosition().getY());
    }

    @Test
    void testNestedComponentResolutionPositionChange() {
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
    void testSizeChangeCompatibility() {
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
    void testBendpointsChangeCompatibility() {
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
    void testCommentsChangeCompatibility() {
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
    void testCommentsChangeConflict() {
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

    @Test
    void testUpstreamRemovedComponentIsUnsupported() {
        final VersionedProcessor processor = createProcessor("proc-a", "ProcessorA");
        processor.setPosition(new Position(10.0, 20.0));
        final VersionedProcessor localProcessor = createProcessor("proc-a", "ProcessorA");
        localProcessor.setPosition(new Position(100.0, 200.0));

        final Set<FlowDifference> localDifferences = new HashSet<>();
        localDifferences.add(new StandardFlowDifference(DifferenceType.POSITION_CHANGED, processor, localProcessor,
                new Position(10.0, 20.0), new Position(100.0, 200.0), "Position changed locally"));

        // Upstream removed the same component in the target version
        final Set<FlowDifference> upstreamDifferences = new HashSet<>();
        upstreamDifferences.add(new StandardFlowDifference(DifferenceType.COMPONENT_REMOVED, processor, null,
                null, null, "Component removed upstream"));

        // Target no longer contains the component
        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier("root");

        final RebaseAnalysis analysis = engine.analyze(localDifferences, upstreamDifferences, targetSnapshot);

        assertFalse(analysis.isRebaseAllowed());
        assertNull(analysis.getMergedSnapshot());
        assertEquals(1, analysis.getClassifiedLocalChanges().size());
        final RebaseAnalysis.ClassifiedDifference classified = analysis.getClassifiedLocalChanges().get(0);
        assertEquals(RebaseClassification.UNSUPPORTED, classified.getClassification());
        assertEquals(RebaseConflictCode.COMPONENT_NOT_FOUND, classified.getConflictCode());
    }

    @Test
    void testClassifyDoesNotBuildMergedSnapshotOrMutateTarget() {
        final VersionedProcessor targetProcessor = createProcessor("proc-a", "ProcessorA");
        targetProcessor.setPosition(new Position(10.0, 20.0));
        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier("root");
        targetSnapshot.getProcessors().add(targetProcessor);

        final VersionedProcessor localProcessor = createProcessor("proc-a", "ProcessorA");
        localProcessor.setPosition(new Position(100.0, 200.0));

        final Set<FlowDifference> localDifferences = new HashSet<>();
        localDifferences.add(new StandardFlowDifference(DifferenceType.POSITION_CHANGED, targetProcessor, localProcessor,
                new Position(10.0, 20.0), new Position(100.0, 200.0), "Position changed locally"));

        final RebaseAnalysis analysis = engine.classify(localDifferences, Collections.emptySet(), targetSnapshot);

        // classify() reports the classification but never builds a merged snapshot or mutates the target
        assertTrue(analysis.isRebaseAllowed());
        assertNull(analysis.getMergedSnapshot());
        final VersionedProcessor unchanged = targetSnapshot.getProcessors().iterator().next();
        assertEquals(10.0, unchanged.getPosition().getX());
        assertEquals(20.0, unchanged.getPosition().getY());
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

    private VersionedControllerService createControllerService(final String identifier, final String name, final String groupIdentifier) {
        final VersionedControllerService service = new VersionedControllerService();
        service.setIdentifier(identifier);
        service.setName(name);
        service.setGroupIdentifier(groupIdentifier);
        service.setType(CONTROLLER_SERVICE_TYPE);
        service.setBundle(new Bundle(BUNDLE_GROUP, BUNDLE_ARTIFACT, BUNDLE_VERSION));
        service.setScheduledState(ScheduledState.DISABLED);
        service.setComments(name + " comments");
        service.setProperties(Collections.emptyMap());
        service.setPropertyDescriptors(Collections.emptyMap());
        return service;
    }

    private VersionedPropertyDescriptor createPropertyDescriptor(final String propertyName, final boolean dynamic, final boolean sensitive) {
        final VersionedPropertyDescriptor descriptor = new VersionedPropertyDescriptor();
        descriptor.setName(propertyName);
        descriptor.setDynamic(dynamic);
        descriptor.setSensitive(sensitive);
        return descriptor;
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

    private VersionedControllerService findControllerServiceById(final VersionedProcessGroup group, final String identifier) {
        for (final VersionedControllerService service : group.getControllerServices()) {
            if (identifier.equals(service.getIdentifier())) {
                return service;
            }
        }
        for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
            final VersionedControllerService result = findControllerServiceById(childGroup, identifier);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    private int countComponentsById(final VersionedProcessGroup group, final String identifier) {
        int count = identifier.equals(group.getIdentifier()) ? 1 : 0;

        for (final VersionedProcessor processor : group.getProcessors()) {
            if (identifier.equals(processor.getIdentifier())) {
                count++;
            }
        }
        for (final VersionedControllerService service : group.getControllerServices()) {
            if (identifier.equals(service.getIdentifier())) {
                count++;
            }
        }
        for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
            count += countComponentsById(childGroup, identifier);
        }

        return count;
    }

    private void assertAllCompatible(final RebaseAnalysis analysis) {
        for (final RebaseAnalysis.ClassifiedDifference classified : analysis.getClassifiedLocalChanges()) {
            assertEquals(RebaseClassification.COMPATIBLE, classified.getClassification());
        }
    }

    private void assertClassification(final RebaseAnalysis analysis, final DifferenceType differenceType, final String componentIdentifier,
                                      final RebaseClassification expectedClassification, final RebaseConflictCode expectedConflictCode) {
        RebaseAnalysis.ClassifiedDifference matchingDifference = null;
        for (final RebaseAnalysis.ClassifiedDifference classified : analysis.getClassifiedLocalChanges()) {
            final FlowDifference difference = classified.getDifference();
            final String differenceComponentId = difference.getComponentB() != null
                    ? difference.getComponentB().getIdentifier()
                    : difference.getComponentA().getIdentifier();
            if (difference.getDifferenceType() == differenceType && componentIdentifier.equals(differenceComponentId)) {
                matchingDifference = classified;
                break;
            }
        }

        assertNotNull(matchingDifference);
        assertEquals(expectedClassification, matchingDifference.getClassification());
        assertEquals(expectedConflictCode, matchingDifference.getConflictCode());
    }
}
