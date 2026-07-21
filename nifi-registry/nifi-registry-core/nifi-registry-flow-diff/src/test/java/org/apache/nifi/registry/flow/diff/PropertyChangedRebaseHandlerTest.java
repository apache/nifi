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

import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedPropertyDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class PropertyChangedRebaseHandlerTest {

    private static final String ROOT = "root";
    private static final String PROC_A = "proc-a";
    private static final String PROC_B = "proc-b";
    private static final String PROC_C = "proc-c";
    private static final String PROP_X = "propX";
    private static final String PROP_Y = "propY";
    private static final String PROP_Z = "propZ";
    private static final String OLD_VALUE = "oldVal";
    private static final String NEW_VALUE = "newVal";
    private static final String UPSTREAM_VALUE = "upstreamVal";
    private static final String SECRET_PROP = "secretProp";

    private PropertyChangedRebaseHandler handler;

    @BeforeEach
    void setup() {
        handler = new PropertyChangedRebaseHandler();
    }

    @Test
    void testNoUpstreamConflict() {
        final VersionedProcessor processor = createProcessorWithProperty(PROC_A, PROP_X, OLD_VALUE);
        final VersionedProcessor localProcessor = createProcessorWithProperty(PROC_A, PROP_X, NEW_VALUE);

        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processor, localProcessor, PROP_X,
                OLD_VALUE, NEW_VALUE, "Property propX changed");

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier(ROOT);
        targetSnapshot.getProcessors().add(createProcessorWithProperty(PROC_A, PROP_X, OLD_VALUE));

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Collections.emptySet(), targetSnapshot);

        assertEquals(RebaseClassification.COMPATIBLE, result.getClassification());
    }

    @Test
    void testDifferentPropertyOnSameComponent() {
        final VersionedProcessor processor = createProcessorWithProperty(PROC_A, PROP_X, OLD_VALUE);
        final VersionedProcessor localProcessor = createProcessorWithProperty(PROC_A, PROP_X, NEW_VALUE);

        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processor, localProcessor, PROP_X,
                OLD_VALUE, NEW_VALUE, "Property propX changed locally");

        final Set<FlowDifference> upstreamDifferences = new HashSet<>();
        upstreamDifferences.add(new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processor, processor, PROP_Y,
                "oldY", "newY", "Property propY changed upstream"));

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier(ROOT);
        targetSnapshot.getProcessors().add(createProcessorWithProperty(PROC_A, PROP_X, OLD_VALUE));

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, upstreamDifferences, targetSnapshot);

        assertEquals(RebaseClassification.COMPATIBLE, result.getClassification());
    }

    @Test
    void testSamePropertyOnSameComponent() {
        final VersionedProcessor processor = createProcessorWithProperty(PROC_A, PROP_X, "original");
        final VersionedProcessor localProcessor = createProcessorWithProperty(PROC_A, PROP_X, "localVal");

        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processor, localProcessor, PROP_X,
                "original", "localVal", "Property propX changed locally");

        final Set<FlowDifference> upstreamDifferences = new HashSet<>();
        upstreamDifferences.add(new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processor, processor, PROP_X,
                "original", UPSTREAM_VALUE, "Property propX changed upstream"));

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier(ROOT);

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, upstreamDifferences, targetSnapshot);

        assertEquals(RebaseClassification.CONFLICTING, result.getClassification());
        assertEquals(RebaseConflictCode.SAME_PROPERTY, result.getConflictCode());
    }

    @Test
    void testSamePropertyOnDifferentComponent() {
        final VersionedProcessor processorA = createProcessorWithProperty(PROC_A, PROP_X, OLD_VALUE);
        final VersionedProcessor localProcessorA = createProcessorWithProperty(PROC_A, PROP_X, NEW_VALUE);

        final VersionedProcessor processorB = createProcessorWithProperty(PROC_B, PROP_X, OLD_VALUE);

        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processorA, localProcessorA, PROP_X,
                OLD_VALUE, NEW_VALUE, "Property propX changed on proc-a");

        final Set<FlowDifference> upstreamDifferences = new HashSet<>();
        upstreamDifferences.add(new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processorB, processorB, PROP_X,
                OLD_VALUE, UPSTREAM_VALUE, "Property propX changed on proc-b"));

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier(ROOT);
        targetSnapshot.getProcessors().add(createProcessorWithProperty(PROC_A, PROP_X, OLD_VALUE));

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, upstreamDifferences, targetSnapshot);

        assertEquals(RebaseClassification.COMPATIBLE, result.getClassification());
    }

    @Test
    void testSensitivePropertyNullRegistryValueWithDescriptorPresent() {
        final VersionedProcessor processor = createProcessorWithSensitiveProperty(PROC_A, SECRET_PROP, true);
        final VersionedProcessor localProcessor = createProcessorWithSensitiveProperty(PROC_A, SECRET_PROP, true);

        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processor, localProcessor, SECRET_PROP,
                null, "newSecret", "Sensitive property secretProp changed");

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier(ROOT);
        targetSnapshot.getProcessors().add(createProcessorWithSensitiveProperty(PROC_A, SECRET_PROP, true));

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Collections.emptySet(), targetSnapshot);

        assertEquals(RebaseClassification.COMPATIBLE, result.getClassification());
    }

    @Test
    void testSensitivePropertyDescriptorRemovedInTarget() {
        final VersionedProcessor processor = createProcessorWithSensitiveProperty(PROC_A, SECRET_PROP, true);
        final VersionedProcessor localProcessor = createProcessorWithSensitiveProperty(PROC_A, SECRET_PROP, true);

        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processor, localProcessor, SECRET_PROP,
                null, "newSecret", "Sensitive property secretProp changed");

        final VersionedProcessor targetProcessor = new VersionedProcessor();
        targetProcessor.setIdentifier(PROC_A);
        targetProcessor.setProperties(Collections.emptyMap());
        targetProcessor.setPropertyDescriptors(Collections.emptyMap());

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier(ROOT);
        targetSnapshot.getProcessors().add(targetProcessor);

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Collections.emptySet(), targetSnapshot);

        assertEquals(RebaseClassification.UNSUPPORTED, result.getClassification());
        assertEquals(RebaseConflictCode.DESCRIPTOR_CHANGED, result.getConflictCode());
    }

    @Test
    void testApplySetsPropertyValueCorrectly() {
        final VersionedProcessor processor = createProcessorWithProperty(PROC_A, PROP_X, OLD_VALUE);
        final VersionedProcessor localProcessor = createProcessorWithProperty(PROC_A, PROP_X, NEW_VALUE);

        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processor, localProcessor, PROP_X,
                OLD_VALUE, NEW_VALUE, "Property propX changed");

        final VersionedProcessGroup mergedFlow = new VersionedProcessGroup();
        mergedFlow.setIdentifier(ROOT);
        mergedFlow.getProcessors().add(createProcessorWithProperty(PROC_A, PROP_X, OLD_VALUE));

        handler.apply(localDifference, mergedFlow);

        final VersionedProcessor mergedProcessor = mergedFlow.getProcessors().iterator().next();
        assertNotNull(mergedProcessor);
        assertEquals(NEW_VALUE, mergedProcessor.getProperties().get(PROP_X));
    }

    @Test
    void testMultipleUpstreamChangesNoneMatching() {
        final VersionedProcessor processorA = createProcessorWithProperty(PROC_A, PROP_X, OLD_VALUE);
        final VersionedProcessor localProcessorA = createProcessorWithProperty(PROC_A, PROP_X, NEW_VALUE);

        final VersionedProcessor processorB = createProcessorWithProperty(PROC_B, PROP_Y, "oldY");
        final VersionedProcessor processorC = createProcessorWithProperty(PROC_C, PROP_Z, "oldZ");

        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processorA, localProcessorA, PROP_X,
                OLD_VALUE, NEW_VALUE, "Property propX changed on proc-a");

        final Set<FlowDifference> upstreamDifferences = new HashSet<>();
        upstreamDifferences.add(new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processorB, processorB, PROP_Y,
                "oldY", "newY", "Property propY changed on proc-b"));
        upstreamDifferences.add(new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processorC, processorC, PROP_Z,
                "oldZ", "newZ", "Property propZ changed on proc-c"));
        upstreamDifferences.add(new StandardFlowDifference(DifferenceType.POSITION_CHANGED,
                processorA, processorA, null, null, "Position changed on proc-a"));

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier(ROOT);
        targetSnapshot.getProcessors().add(createProcessorWithProperty(PROC_A, PROP_X, OLD_VALUE));

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, upstreamDifferences, targetSnapshot);

        assertEquals(RebaseClassification.COMPATIBLE, result.getClassification());
    }

    private VersionedProcessor createProcessorWithProperty(final String identifier, final String propertyName, final String propertyValue) {
        final VersionedProcessor processor = new VersionedProcessor();
        processor.setIdentifier(identifier);

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

    private VersionedProcessor createProcessorWithSensitiveProperty(final String identifier, final String propertyName, final boolean sensitive) {
        final VersionedProcessor processor = new VersionedProcessor();
        processor.setIdentifier(identifier);

        final Map<String, String> properties = new HashMap<>();
        properties.put(propertyName, null);
        processor.setProperties(properties);

        final VersionedPropertyDescriptor descriptor = new VersionedPropertyDescriptor();
        descriptor.setName(propertyName);
        descriptor.setSensitive(sensitive);

        final Map<String, VersionedPropertyDescriptor> descriptors = new HashMap<>();
        descriptors.put(propertyName, descriptor);
        processor.setPropertyDescriptors(descriptors);

        return processor;
    }
}
