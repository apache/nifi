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

import org.apache.nifi.flow.ComponentType;
import org.apache.nifi.flow.ExecutionEngine;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedAsset;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedFlowCoordinates;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestStandardFlowComparator {
    private Map<String, String> decryptedToEncrypted;
    private Map<String, String> encryptedToDecrypted;
    private StandardFlowComparator comparator;

    @BeforeEach
    public void setup() {
        decryptedToEncrypted = new HashMap<>();
        decryptedToEncrypted.put("XYZ", "Hello");
        decryptedToEncrypted.put("xyz", "hola");

        encryptedToDecrypted = new HashMap<>();
        encryptedToDecrypted.put("Hello", "XYZ");
        encryptedToDecrypted.put("hola", "xyz");

        final Function<String, String> decryptor = encryptedToDecrypted::get;
        final ComparableDataFlow flowA = new StandardComparableDataFlow("Flow A", new VersionedProcessGroup());
        final ComparableDataFlow flowB = new StandardComparableDataFlow("Flow B", new VersionedProcessGroup());
        comparator = new StandardFlowComparator(flowA, flowB, Collections.emptySet(),
            new StaticDifferenceDescriptor(), decryptor, VersionedComponent::getInstanceIdentifier, FlowComparatorVersionedStrategy.SHALLOW);
    }

    // Ensure that when we are comparing parameter values that we compare the decrypted values, but we don't include any
    // decrypted values in the descriptions of the Flow Difference.
    @Test
    public void testSensitiveParametersDecryptedBeforeCompare() {
        final Set<FlowDifference> differences = new HashSet<>();

        final Set<VersionedParameter> parametersA = new HashSet<>();
        parametersA.add(createParameter("Param 1", "xyz", false));
        parametersA.add(createParameter("Param 2", "XYZ", false));
        parametersA.add(createParameter("Param 3", "Hi there", false));
        parametersA.add(createParameter("Param 4", "xyz", true));
        parametersA.add(createParameter("Param 5", "XYZ", true));

        // Now that we've created the parameters, change the mapping of decrypted to encrypted so that we encrypt the values
        // differently in each context but have the same decrypted value
        decryptedToEncrypted.put("xyz", "bonjour");
        encryptedToDecrypted.put("bonjour", "xyz");

        final Set<VersionedParameter> parametersB = new HashSet<>();
        parametersB.add(createParameter("Param 1", "xyz", false));
        parametersB.add(createParameter("Param 2", "XYZ", false));
        parametersB.add(createParameter("Param 3", "Hey", false));
        parametersB.add(createParameter("Param 4", "xyz", true));
        parametersB.add(createParameter("Param 5", "xyz", true));

        final VersionedParameterContext contextA = new VersionedParameterContext();
        contextA.setIdentifier("id");
        contextA.setInstanceIdentifier("instanceId");
        contextA.setParameters(parametersA);

        final VersionedParameterContext contextB = new VersionedParameterContext();
        contextB.setIdentifier("id");
        contextB.setInstanceIdentifier("instanceId");
        contextB.setParameters(parametersB);

        comparator.compare(contextA, contextB, differences);

        assertEquals(2, differences.size());
        for (final FlowDifference difference : differences) {
            assertSame(DifferenceType.PARAMETER_VALUE_CHANGED, difference.getDifferenceType());

            // Ensure that the sensitive values are not contained in the description
            assertFalse(difference.getDescription().contains("Hello"));
            assertFalse(difference.getDescription().contains("Hola"));
            assertFalse(difference.getDescription().contains("bonjour"));
        }

        final long numContainingValue = differences.stream()
            .filter(diff -> diff.getDescription().contains("Hey") && diff.getDescription().contains("Hi there"))
            .count();
        assertEquals(1, numContainingValue);
    }

    @Test
    public void testAssetReferencesChanged() {
        final VersionedAsset assetA = createAsset("assetA", "assetA-file.txt");
        final VersionedAsset assetB = createAsset("assetB", "assetB-file.txt");
        final VersionedAsset assetC = createAsset("assetC", "assetC-file.txt");

        final Set<VersionedParameter> parametersA = new HashSet<>();
        parametersA.add(createParameter("Param 1", null, false, List.of(assetA, assetB, assetC)));
        parametersA.add(createParameter("Param 2", "Param 2 Value", false));

        final Set<VersionedParameter> parametersB = new HashSet<>();
        parametersB.add(createParameter("Param 1", null, false, List.of(assetA, assetC)));
        parametersB.add(createParameter("Param 2", "Param 2 Value", false));

        final VersionedParameterContext contextA = new VersionedParameterContext();
        contextA.setIdentifier("contextA");
        contextA.setInstanceIdentifier("contextAInstanceId");
        contextA.setParameters(parametersA);

        final VersionedParameterContext contextB = new VersionedParameterContext();
        contextB.setIdentifier("contextB");
        contextB.setInstanceIdentifier("contextBInstanceId");
        contextB.setParameters(parametersB);

        final Set<FlowDifference> differences = new HashSet<>();
        comparator.compare(contextA, contextB, differences);

        assertEquals(1, differences.size());

        final FlowDifference difference = differences.iterator().next();
        assertNotNull(difference);
        assertEquals(DifferenceType.PARAMETER_ASSET_REFERENCES_CHANGED, difference.getDifferenceType());
        assertEquals(contextA.getIdentifier(), difference.getComponentA().getIdentifier());
        assertEquals(contextB.getIdentifier(), difference.getComponentB().getIdentifier());
    }

    @Test
    public void testMultipleParametersNamesChanged() {
        final Set<FlowDifference> differences = new HashSet<>();

        final Set<VersionedParameter> parametersA = new HashSet<>();
        parametersA.add(createParameter("Param 1", "ABC", true));
        parametersA.add(createParameter("Param 2", "XYZ", true));

        final Set<VersionedParameter> parametersB = new HashSet<>();
        parametersB.add(createParameter("New Param 1", "ABC", true));
        parametersB.add(createParameter("New Param 2", "XYZ", true));

        final VersionedParameterContext contextA = new VersionedParameterContext();
        contextA.setIdentifier("id");
        contextA.setInstanceIdentifier("instanceId");
        contextA.setParameters(parametersA);

        final VersionedParameterContext contextB = new VersionedParameterContext();
        contextB.setIdentifier("id");
        contextB.setInstanceIdentifier("instanceId");
        contextB.setParameters(parametersB);

        comparator.compare(contextA, contextB, differences);

        assertEquals(4, differences.size());
    }

    @Test
    public void testDeepStrategyWithChildPGs() {
        final Function<String, String> decryptor = encryptedToDecrypted::get;

        final VersionedProcessGroup rootPGA = new VersionedProcessGroup();
        rootPGA.setIdentifier("rootPG");

        final VersionedProcessGroup rootPGB = new VersionedProcessGroup();
        rootPGB.setIdentifier("rootPG");
        final VersionedProcessGroup childPG = new VersionedProcessGroup();
        childPG.setIdentifier("childPG");
        rootPGB.getProcessGroups().add(childPG);
        final VersionedProcessGroup subChildPG = new VersionedProcessGroup();
        subChildPG.setIdentifier("subChildPG");
        childPG.getProcessGroups().add(subChildPG);
        final VersionedProcessor processor = new VersionedProcessor();
        processor.setIdentifier("processor");
        childPG.getProcessors().add(processor);
        final VersionedControllerService controllerService = new VersionedControllerService();
        controllerService.setIdentifier("controllerService");
        subChildPG.getControllerServices().add(controllerService);

        // change all configuration of PG to check diff on PG configuration
        subChildPG.setExecutionEngine(ExecutionEngine.STATELESS);
        subChildPG.setFlowFileConcurrency("SINGLE_BATCH_PER_NODE");
        subChildPG.setFlowFileOutboundPolicy("BATCH_OUTPUT");
        subChildPG.setDefaultBackPressureDataSizeThreshold("1B");
        subChildPG.setDefaultBackPressureObjectThreshold(1L);
        subChildPG.setDefaultFlowFileExpiration("10 sec");
        subChildPG.setParameterContextName("paramContextName");
        subChildPG.setLogFileSuffix("logSuffix");
        subChildPG.setScheduledState(ScheduledState.DISABLED);
        subChildPG.setMaxConcurrentTasks(4);
        subChildPG.setStatelessFlowTimeout("30 sec");

        final ComparableDataFlow flowA = new StandardComparableDataFlow("Flow A", rootPGA);
        final ComparableDataFlow flowB = new StandardComparableDataFlow("Flow B", rootPGB);

        // Testing when a child PG is added and the child PG contains components

        comparator = new StandardFlowComparator(flowA, flowB, Collections.emptySet(),
                new StaticDifferenceDescriptor(), decryptor, VersionedComponent::getIdentifier, FlowComparatorVersionedStrategy.SHALLOW);

        final Set<FlowDifference> diffShallowChildPgAdded = comparator.compare().getDifferences();
        assertEquals(1, diffShallowChildPgAdded.size());
        assertTrue(diffShallowChildPgAdded.stream()
                .anyMatch(difference -> difference.getDifferenceType() == DifferenceType.COMPONENT_ADDED
                        && difference.getComponentB().getComponentType() == ComponentType.PROCESS_GROUP));

        comparator = new StandardFlowComparator(flowA, flowB, Collections.emptySet(),
                new StaticDifferenceDescriptor(), decryptor, VersionedComponent::getIdentifier, FlowComparatorVersionedStrategy.DEEP);
        final Set<FlowDifference> diffDeepChildPgAdded = comparator.compare().getDifferences();
        assertEquals(15, diffDeepChildPgAdded.size());
        assertTrue(diffDeepChildPgAdded.stream()
                .anyMatch(difference -> difference.getDifferenceType() == DifferenceType.COMPONENT_ADDED
                        && difference.getComponentB().getComponentType() == ComponentType.PROCESS_GROUP
                        && difference.getComponentB().getIdentifier().equals("childPG")));
        assertTrue(diffDeepChildPgAdded.stream()
                .anyMatch(difference -> difference.getDifferenceType() == DifferenceType.COMPONENT_ADDED
                        && difference.getComponentB().getComponentType() == ComponentType.PROCESS_GROUP
                        && difference.getComponentB().getIdentifier().equals("subChildPG")));
        assertTrue(diffDeepChildPgAdded.stream()
                .anyMatch(difference -> difference.getDifferenceType() == DifferenceType.COMPONENT_ADDED
                        && difference.getComponentB().getComponentType() == ComponentType.PROCESSOR));
        assertTrue(diffDeepChildPgAdded.stream()
                .anyMatch(difference -> difference.getDifferenceType() == DifferenceType.COMPONENT_ADDED
                        && difference.getComponentB().getComponentType() == ComponentType.CONTROLLER_SERVICE));
        assertTrue(diffDeepChildPgAdded.stream()
                .anyMatch(difference -> difference.getDifferenceType() == DifferenceType.EXECUTION_ENGINE_CHANGED
                        && difference.getComponentB().getComponentType() == ComponentType.PROCESS_GROUP
                        && difference.getComponentB().getIdentifier().equals("subChildPG")));

        // Testing when a child PG is removed and the child PG contains components

        comparator = new StandardFlowComparator(flowB, flowA, Collections.emptySet(),
                new StaticDifferenceDescriptor(), decryptor, VersionedComponent::getIdentifier, FlowComparatorVersionedStrategy.SHALLOW);

        final Set<FlowDifference> diffShallowChildPgRemoved = comparator.compare().getDifferences();
        assertEquals(1, diffShallowChildPgRemoved.size());
        assertTrue(diffShallowChildPgRemoved.stream()
                .anyMatch(difference -> difference.getDifferenceType() == DifferenceType.COMPONENT_REMOVED
                        && difference.getComponentA().getComponentType() == ComponentType.PROCESS_GROUP));

        comparator = new StandardFlowComparator(flowB, flowA, Collections.emptySet(),
                new StaticDifferenceDescriptor(), decryptor, VersionedComponent::getIdentifier, FlowComparatorVersionedStrategy.DEEP);
        final Set<FlowDifference> diffDeepChildPgRemoved = comparator.compare().getDifferences();
        assertEquals(4, diffDeepChildPgRemoved.size());
        assertTrue(diffDeepChildPgRemoved.stream()
                .anyMatch(difference -> difference.getDifferenceType() == DifferenceType.COMPONENT_REMOVED
                        && difference.getComponentA().getComponentType() == ComponentType.PROCESS_GROUP
                        && difference.getComponentA().getIdentifier().equals("childPG")));
        assertTrue(diffDeepChildPgRemoved.stream()
                .anyMatch(difference -> difference.getDifferenceType() == DifferenceType.COMPONENT_REMOVED
                        && difference.getComponentA().getComponentType() == ComponentType.PROCESS_GROUP
                        && difference.getComponentA().getIdentifier().equals("subChildPG")));
        assertTrue(diffDeepChildPgRemoved.stream()
                .anyMatch(difference -> difference.getDifferenceType() == DifferenceType.COMPONENT_REMOVED
                        && difference.getComponentA().getComponentType() == ComponentType.PROCESSOR));
        assertTrue(diffDeepChildPgRemoved.stream()
                .anyMatch(difference -> difference.getDifferenceType() == DifferenceType.COMPONENT_REMOVED
                        && difference.getComponentA().getComponentType() == ComponentType.CONTROLLER_SERVICE));
    }

    /**
     * NIFI-15016: Test that scheduled state changes are detected for processors inside a regular
     * nested process group (NOT separately version-controlled).
     *
     * Scenario: A versioned PG contains a regular nested PG. User disables a processor inside
     * the nested PG. When comparing for upgrade, we should detect the scheduled state change.
     *
     * Note: The nested PG does NOT have VersionedFlowCoordinates because it's just a regular
     * nested PG within the flow, not a separately versioned child flow.
     */
    @Test
    public void testScheduledStateChangeDetectedForProcessorInRegularNestedGroup() {
        final String rootPgIdentifier = "rootPG";
        final String nestedPgIdentifier = "nestedPG";
        final String procIdentifier = "processorZ";

        // Registry version
        final VersionedProcessGroup registryRoot = new VersionedProcessGroup();
        registryRoot.setIdentifier(rootPgIdentifier);

        final VersionedProcessGroup registryNested = new VersionedProcessGroup();
        registryNested.setIdentifier(nestedPgIdentifier);
        // NO setVersionedFlowCoordinates - this is a regular nested PG, not separately versioned
        registryRoot.getProcessGroups().add(registryNested);

        final VersionedProcessor registryProcessor = new VersionedProcessor();
        registryProcessor.setIdentifier(procIdentifier);
        registryProcessor.setScheduledState(ScheduledState.ENABLED);
        registryProcessor.setProperties(Collections.emptyMap());
        registryProcessor.setPropertyDescriptors(Collections.emptyMap());
        registryNested.getProcessors().add(registryProcessor);

        // Local version - user has disabled the processor
        final VersionedProcessGroup localRoot = new VersionedProcessGroup();
        localRoot.setIdentifier(rootPgIdentifier);

        final VersionedProcessGroup localNested = new VersionedProcessGroup();
        localNested.setIdentifier(nestedPgIdentifier);
        // NO setVersionedFlowCoordinates - this is a regular nested PG
        localRoot.getProcessGroups().add(localNested);

        final VersionedProcessor localProcessor = new VersionedProcessor();
        localProcessor.setIdentifier(procIdentifier);
        localProcessor.setScheduledState(ScheduledState.DISABLED); // User disabled this
        localProcessor.setProperties(Collections.emptyMap());
        localProcessor.setPropertyDescriptors(Collections.emptyMap());
        localNested.getProcessors().add(localProcessor);

        final ComparableDataFlow registryFlow = new StandardComparableDataFlow("registry", registryRoot);
        final ComparableDataFlow localFlow = new StandardComparableDataFlow("local", localRoot);

        final StandardFlowComparator testComparator = new StandardFlowComparator(
                registryFlow,
                localFlow,
                Collections.emptySet(),
                new StaticDifferenceDescriptor(),
                Function.identity(),
                VersionedComponent::getIdentifier,
                FlowComparatorVersionedStrategy.SHALLOW);

        final Set<FlowDifference> differences = testComparator.compare().getDifferences();

        final boolean scheduledStateDiffFound = differences.stream()
                .anyMatch(diff -> diff.getDifferenceType() == DifferenceType.SCHEDULED_STATE_CHANGED
                        && diff.getComponentB() != null
                        && procIdentifier.equals(diff.getComponentB().getIdentifier()));

        assertTrue(scheduledStateDiffFound,
                "Expected scheduled state change for processor inside regular nested process group to be detected");
    }

    /**
     * NIFI-15366: Test that when a nested VERSIONED PG (separately version-controlled) has the
     * SAME version coordinates (is "up to date"), its contents are NOT compared.
     *
     * This is the correct behavior because:
     * 1. When both PGs have the same version coordinates, the child PG is considered "up to date"
     * 2. Any content differences should be viewed via the child PG's own "Show Local Changes"
     * 3. The parent PG's local changes should only show changes made directly to the parent
     */
    @Test
    public void testNoChangesDetectedForSeparatelyVersionedNestedGroupWhenVersionsMatch() {
        final String rootPgIdentifier = "rootPG";
        final String nestedPgIdentifier = "nestedPG";
        final String procIdentifier = "processorZ";
        final VersionedProcessGroup registryRoot = new VersionedProcessGroup();
        registryRoot.setIdentifier(rootPgIdentifier);

        final VersionedProcessGroup localRoot = new VersionedProcessGroup();
        localRoot.setIdentifier(rootPgIdentifier);

        final VersionedProcessGroup registryNested = new VersionedProcessGroup();
        registryNested.setIdentifier(nestedPgIdentifier);
        registryNested.setVersionedFlowCoordinates(createVersionedFlowCoordinates()); // Separately versioned!
        registryRoot.getProcessGroups().add(registryNested);

        final VersionedProcessGroup localNested = new VersionedProcessGroup();
        localNested.setIdentifier(nestedPgIdentifier);
        localNested.setVersionedFlowCoordinates(createVersionedFlowCoordinates()); // Same version = up-to-date
        localRoot.getProcessGroups().add(localNested);

        final VersionedProcessor registryProcessor = new VersionedProcessor();
        registryProcessor.setIdentifier(procIdentifier);
        registryProcessor.setScheduledState(ScheduledState.ENABLED);
        registryProcessor.setProperties(Collections.emptyMap());
        registryProcessor.setPropertyDescriptors(Collections.emptyMap());
        registryNested.getProcessors().add(registryProcessor);

        final VersionedProcessor localProcessor = new VersionedProcessor();
        localProcessor.setIdentifier(procIdentifier);
        localProcessor.setScheduledState(ScheduledState.DISABLED); // Different state - but should NOT be reported
        localProcessor.setProperties(Collections.emptyMap());
        localProcessor.setPropertyDescriptors(Collections.emptyMap());
        localNested.getProcessors().add(localProcessor);

        final ComparableDataFlow registryFlow = new StandardComparableDataFlow("registry", registryRoot);
        final ComparableDataFlow localFlow = new StandardComparableDataFlow("local", localRoot);

        final StandardFlowComparator testComparator = new StandardFlowComparator(
                registryFlow,
                localFlow,
                Collections.emptySet(),
                new StaticDifferenceDescriptor(),
                Function.identity(),
                VersionedComponent::getIdentifier,
                FlowComparatorVersionedStrategy.SHALLOW);

        final Set<FlowDifference> differences = testComparator.compare().getDifferences();

        // When version coordinates match for a separately versioned nested PG,
        // we should NOT report any differences - they should be viewed via the child's own local changes
        assertEquals(0, differences.size(),
                "When separately versioned nested PG has same version coordinates, should NOT detect content differences. " +
                        "Differences found: " + differences);
    }

    /**
     * Test that when a separately versioned nested PG has DIFFERENT version coordinates,
     * scheduled state changes inside it ARE detected.
     */
    @Test
    public void testChangesDetectedForSeparatelyVersionedNestedGroupWhenVersionsDiffer() {
        final String rootPgIdentifier = "rootPG";
        final String nestedPgIdentifier = "nestedPG";
        final String procIdentifier = "processorZ";
        final VersionedProcessGroup registryRoot = new VersionedProcessGroup();
        registryRoot.setIdentifier(rootPgIdentifier);

        final VersionedProcessGroup localRoot = new VersionedProcessGroup();
        localRoot.setIdentifier(rootPgIdentifier);

        final VersionedProcessGroup registryNested = new VersionedProcessGroup();
        registryNested.setIdentifier(nestedPgIdentifier);
        final VersionedFlowCoordinates registryCoords = createVersionedFlowCoordinates();
        registryCoords.setVersion("1");
        registryNested.setVersionedFlowCoordinates(registryCoords);
        registryRoot.getProcessGroups().add(registryNested);

        final VersionedProcessGroup localNested = new VersionedProcessGroup();
        localNested.setIdentifier(nestedPgIdentifier);
        final VersionedFlowCoordinates localCoords = createVersionedFlowCoordinates();
        localCoords.setVersion("2"); // Different version
        localNested.setVersionedFlowCoordinates(localCoords);
        localRoot.getProcessGroups().add(localNested);

        final VersionedProcessor registryProcessor = new VersionedProcessor();
        registryProcessor.setIdentifier(procIdentifier);
        registryProcessor.setScheduledState(ScheduledState.ENABLED);
        registryProcessor.setProperties(Collections.emptyMap());
        registryProcessor.setPropertyDescriptors(Collections.emptyMap());
        registryNested.getProcessors().add(registryProcessor);

        final VersionedProcessor localProcessor = new VersionedProcessor();
        localProcessor.setIdentifier(procIdentifier);
        localProcessor.setScheduledState(ScheduledState.DISABLED);
        localProcessor.setProperties(Collections.emptyMap());
        localProcessor.setPropertyDescriptors(Collections.emptyMap());
        localNested.getProcessors().add(localProcessor);

        final ComparableDataFlow registryFlow = new StandardComparableDataFlow("registry", registryRoot);
        final ComparableDataFlow localFlow = new StandardComparableDataFlow("local", localRoot);

        final StandardFlowComparator testComparator = new StandardFlowComparator(
                registryFlow,
                localFlow,
                Collections.emptySet(),
                new StaticDifferenceDescriptor(),
                Function.identity(),
                VersionedComponent::getIdentifier,
                FlowComparatorVersionedStrategy.SHALLOW);

        final Set<FlowDifference> differences = testComparator.compare().getDifferences();

        final boolean scheduledStateDiffFound = differences.stream()
                .anyMatch(diff -> diff.getDifferenceType() == DifferenceType.SCHEDULED_STATE_CHANGED
                        && diff.getComponentB() != null
                        && procIdentifier.equals(diff.getComponentB().getIdentifier()));

        assertTrue(scheduledStateDiffFound,
                "When nested versioned PG has different version coordinates, scheduled state change should be detected");
    }

    /**
     * Test for NIFI-15366: Simulates the actual bug scenario where:
     * - Child PG B was committed separately to registry
     * - Parent PG A was committed containing B as a versioned reference
     * - When fetching A's snapshot with nested contents, B's contents come from B's own registry entry
     * - B's registry entry uses different identifiers than what's in the local canvas
     *
     * This test verifies that when identifiers differ but version coordinates are the same,
     * we should NOT report the child's contents as changes.
     */
    @Test
    public void testNestedVersionedPGWithDifferentIdentifiersButSameVersion() {
        final String rootPgIdentifier = "rootPG";
        final String nestedPgIdentifier = "nestedPG";

        // Registry version of root PG - child B has contents with B's internal identifiers
        final VersionedProcessGroup registryRoot = new VersionedProcessGroup();
        registryRoot.setIdentifier(rootPgIdentifier);

        final VersionedProcessGroup registryNested = new VersionedProcessGroup();
        registryNested.setIdentifier(nestedPgIdentifier);
        registryNested.setVersionedFlowCoordinates(createVersionedFlowCoordinates());
        registryRoot.getProcessGroups().add(registryNested);

        // Processor in registry nested PG has its own identifier (from B's registry snapshot)
        final VersionedProcessor registryNestedProcessor = new VersionedProcessor();
        registryNestedProcessor.setIdentifier("proc-id-from-B-registry"); // B's internal ID
        registryNestedProcessor.setScheduledState(ScheduledState.ENABLED);
        registryNestedProcessor.setProperties(Collections.emptyMap());
        registryNestedProcessor.setPropertyDescriptors(Collections.emptyMap());
        registryNested.getProcessors().add(registryNestedProcessor);

        // Local version - child B has contents with local NiFi identifiers
        final VersionedProcessGroup localRoot = new VersionedProcessGroup();
        localRoot.setIdentifier(rootPgIdentifier);

        final VersionedProcessGroup localNested = new VersionedProcessGroup();
        localNested.setIdentifier(nestedPgIdentifier);
        localNested.setVersionedFlowCoordinates(createVersionedFlowCoordinates()); // Same coordinates - B is up-to-date
        localRoot.getProcessGroups().add(localNested);

        // Same processor but with different identifier (local NiFi instance ID)
        final VersionedProcessor localNestedProcessor = new VersionedProcessor();
        localNestedProcessor.setIdentifier("proc-id-from-local-nifi"); // Different ID!
        localNestedProcessor.setScheduledState(ScheduledState.ENABLED); // Same state
        localNestedProcessor.setProperties(Collections.emptyMap());
        localNestedProcessor.setPropertyDescriptors(Collections.emptyMap());
        localNested.getProcessors().add(localNestedProcessor);

        final ComparableDataFlow registryFlow = new StandardComparableDataFlow("Versioned Flow", registryRoot);
        final ComparableDataFlow localFlow = new StandardComparableDataFlow("Local Flow", localRoot);

        // Test with SHALLOW strategy
        final StandardFlowComparator shallowComparator = new StandardFlowComparator(
                registryFlow,
                localFlow,
                Collections.emptySet(),
                new StaticDifferenceDescriptor(),
                Function.identity(),
                VersionedComponent::getIdentifier,
                FlowComparatorVersionedStrategy.SHALLOW);

        final Set<FlowDifference> shallowDifferences = shallowComparator.compare().getDifferences();

        assertEquals(0, shallowDifferences.size(),
                "When nested versioned PG has same version coordinates, should not report any content differences " +
                        "even if component identifiers differ. Differences found: " + shallowDifferences);
    }

    /**
     * Versioned PG A contains versioned PG B (up-to-date).
     * Add a processor to A only (not in B).
     * Listing local changes on A should return a single entry for the added processor.
     */
    @Test
    public void testAddProcessorToParentOnly() {
        final String rootPgIdentifier = "rootPG";
        final String nestedPgIdentifier = "nestedPG";
        final String addedProcessorInA = "addedProcessorInA";
        final String existingProcessorInB = "existingProcessorInB";

        // Registry version: A contains B with one processor
        final VersionedProcessGroup registryRoot = new VersionedProcessGroup();
        registryRoot.setIdentifier(rootPgIdentifier);

        final VersionedProcessGroup registryNested = new VersionedProcessGroup();
        registryNested.setIdentifier(nestedPgIdentifier);
        registryNested.setVersionedFlowCoordinates(createVersionedFlowCoordinates());
        registryRoot.getProcessGroups().add(registryNested);

        final VersionedProcessor registryNestedProcessor = new VersionedProcessor();
        registryNestedProcessor.setIdentifier(existingProcessorInB);
        registryNestedProcessor.setScheduledState(ScheduledState.ENABLED);
        registryNestedProcessor.setProperties(Collections.emptyMap());
        registryNestedProcessor.setPropertyDescriptors(Collections.emptyMap());
        registryNested.getProcessors().add(registryNestedProcessor);

        // Local version: A has a new processor, B is unchanged (up-to-date)
        final VersionedProcessGroup localRoot = new VersionedProcessGroup();
        localRoot.setIdentifier(rootPgIdentifier);

        // New processor added to A
        final VersionedProcessor processorAddedToA = new VersionedProcessor();
        processorAddedToA.setIdentifier(addedProcessorInA);
        processorAddedToA.setScheduledState(ScheduledState.ENABLED);
        processorAddedToA.setProperties(Collections.emptyMap());
        processorAddedToA.setPropertyDescriptors(Collections.emptyMap());
        localRoot.getProcessors().add(processorAddedToA);

        // B is unchanged - same version coordinates
        final VersionedProcessGroup localNested = new VersionedProcessGroup();
        localNested.setIdentifier(nestedPgIdentifier);
        localNested.setVersionedFlowCoordinates(createVersionedFlowCoordinates()); // Same coordinates = up-to-date
        localRoot.getProcessGroups().add(localNested);

        // Same processor in B (unchanged)
        final VersionedProcessor localNestedProcessor = new VersionedProcessor();
        localNestedProcessor.setIdentifier(existingProcessorInB);
        localNestedProcessor.setScheduledState(ScheduledState.ENABLED);
        localNestedProcessor.setProperties(Collections.emptyMap());
        localNestedProcessor.setPropertyDescriptors(Collections.emptyMap());
        localNested.getProcessors().add(localNestedProcessor);

        final ComparableDataFlow registryFlow = new StandardComparableDataFlow("Versioned Flow", registryRoot);
        final ComparableDataFlow localFlow = new StandardComparableDataFlow("Local Flow", localRoot);

        final StandardFlowComparator comparator = new StandardFlowComparator(
                registryFlow,
                localFlow,
                Collections.emptySet(),
                new StaticDifferenceDescriptor(),
                Function.identity(),
                VersionedComponent::getIdentifier,
                FlowComparatorVersionedStrategy.SHALLOW);

        final Set<FlowDifference> differences = comparator.compare().getDifferences();

        // Should only show the added processor in A
        assertEquals(1, differences.size(), "Should only have 1 difference (added processor in A)");
        final FlowDifference diff = differences.iterator().next();
        assertEquals(DifferenceType.COMPONENT_ADDED, diff.getDifferenceType());
        assertEquals(addedProcessorInA, diff.getComponentB().getIdentifier());
        assertEquals(ComponentType.PROCESSOR, diff.getComponentB().getComponentType());
    }

    private VersionedParameter createParameter(final String name, final String value, final boolean sensitive) {
        return createParameter(name, value, sensitive, null);
    }

    private VersionedParameter createParameter(final String name, final String value, final boolean sensitive, final List<VersionedAsset> referencedAssets) {
        final VersionedParameter parameter = new VersionedParameter();
        parameter.setName(name);
        parameter.setValue(sensitive ? "enc{" + decryptedToEncrypted.get(value) + "}" : value);
        parameter.setSensitive(sensitive);
        parameter.setReferencedAssets(referencedAssets);
        return parameter;
    }

    private VersionedAsset createAsset(final String id, final String name) {
        final VersionedAsset asset = new VersionedAsset();
        asset.setIdentifier(id);
        asset.setName(name);
        return asset;
    }

    private VersionedFlowCoordinates createVersionedFlowCoordinates() {
        final VersionedFlowCoordinates coordinates = new VersionedFlowCoordinates();
        coordinates.setRegistryId("registry");
        coordinates.setBucketId("bucketId");
        coordinates.setFlowId("flowId");
        coordinates.setVersion("1");
        return coordinates;
    }
}
