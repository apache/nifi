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
}
