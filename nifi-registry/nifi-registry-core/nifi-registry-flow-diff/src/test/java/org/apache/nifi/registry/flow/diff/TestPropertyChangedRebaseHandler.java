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

public class TestPropertyChangedRebaseHandler {

    private PropertyChangedRebaseHandler handler;

    @BeforeEach
    public void setup() {
        handler = new PropertyChangedRebaseHandler();
    }

    @Test
    public void testNoUpstreamConflict() {
        final VersionedProcessor processor = createProcessorWithProperty("proc-a", "propX", "oldVal");
        final VersionedProcessor localProcessor = createProcessorWithProperty("proc-a", "propX", "newVal");

        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processor, localProcessor, "propX",
                "oldVal", "newVal", "Property propX changed");

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier("root");
        targetSnapshot.getProcessors().add(createProcessorWithProperty("proc-a", "propX", "oldVal"));

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Collections.emptySet(), targetSnapshot);

        assertEquals(RebaseClassification.COMPATIBLE, result.getClassification());
    }

    @Test
    public void testDifferentPropertyOnSameComponent() {
        final VersionedProcessor processor = createProcessorWithProperty("proc-a", "propX", "oldVal");
        final VersionedProcessor localProcessor = createProcessorWithProperty("proc-a", "propX", "newVal");

        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processor, localProcessor, "propX",
                "oldVal", "newVal", "Property propX changed locally");

        final Set<FlowDifference> upstreamDifferences = new HashSet<>();
        upstreamDifferences.add(new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processor, processor, "propY",
                "oldY", "newY", "Property propY changed upstream"));

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier("root");
        targetSnapshot.getProcessors().add(createProcessorWithProperty("proc-a", "propX", "oldVal"));

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, upstreamDifferences, targetSnapshot);

        assertEquals(RebaseClassification.COMPATIBLE, result.getClassification());
    }

    @Test
    public void testSamePropertyOnSameComponent() {
        final VersionedProcessor processor = createProcessorWithProperty("proc-a", "propX", "original");
        final VersionedProcessor localProcessor = createProcessorWithProperty("proc-a", "propX", "localVal");

        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processor, localProcessor, "propX",
                "original", "localVal", "Property propX changed locally");

        final Set<FlowDifference> upstreamDifferences = new HashSet<>();
        upstreamDifferences.add(new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processor, processor, "propX",
                "original", "upstreamVal", "Property propX changed upstream"));

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier("root");

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, upstreamDifferences, targetSnapshot);

        assertEquals(RebaseClassification.CONFLICTING, result.getClassification());
        assertEquals("SAME_PROPERTY", result.getConflictCode());
    }

    @Test
    public void testSamePropertyOnDifferentComponent() {
        final VersionedProcessor processorA = createProcessorWithProperty("proc-a", "propX", "oldVal");
        final VersionedProcessor localProcessorA = createProcessorWithProperty("proc-a", "propX", "newVal");

        final VersionedProcessor processorB = createProcessorWithProperty("proc-b", "propX", "oldVal");

        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processorA, localProcessorA, "propX",
                "oldVal", "newVal", "Property propX changed on proc-a");

        final Set<FlowDifference> upstreamDifferences = new HashSet<>();
        upstreamDifferences.add(new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processorB, processorB, "propX",
                "oldVal", "upstreamVal", "Property propX changed on proc-b"));

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier("root");
        targetSnapshot.getProcessors().add(createProcessorWithProperty("proc-a", "propX", "oldVal"));

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, upstreamDifferences, targetSnapshot);

        assertEquals(RebaseClassification.COMPATIBLE, result.getClassification());
    }

    @Test
    public void testSensitivePropertyNullRegistryValueWithDescriptorPresent() {
        final VersionedProcessor processor = createProcessorWithSensitiveProperty("proc-a", "secretProp", true);
        final VersionedProcessor localProcessor = createProcessorWithSensitiveProperty("proc-a", "secretProp", true);

        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processor, localProcessor, "secretProp",
                null, "newSecret", "Sensitive property secretProp changed");

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier("root");
        targetSnapshot.getProcessors().add(createProcessorWithSensitiveProperty("proc-a", "secretProp", true));

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Collections.emptySet(), targetSnapshot);

        assertEquals(RebaseClassification.COMPATIBLE, result.getClassification());
    }

    @Test
    public void testSensitivePropertyDescriptorRemovedInTarget() {
        final VersionedProcessor processor = createProcessorWithSensitiveProperty("proc-a", "secretProp", true);
        final VersionedProcessor localProcessor = createProcessorWithSensitiveProperty("proc-a", "secretProp", true);

        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processor, localProcessor, "secretProp",
                null, "newSecret", "Sensitive property secretProp changed");

        final VersionedProcessor targetProcessor = new VersionedProcessor();
        targetProcessor.setIdentifier("proc-a");
        targetProcessor.setProperties(Collections.emptyMap());
        targetProcessor.setPropertyDescriptors(Collections.emptyMap());

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier("root");
        targetSnapshot.getProcessors().add(targetProcessor);

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Collections.emptySet(), targetSnapshot);

        assertEquals(RebaseClassification.UNSUPPORTED, result.getClassification());
        assertEquals("DESCRIPTOR_CHANGED", result.getConflictCode());
    }

    @Test
    public void testApplySetsPropertyValueCorrectly() {
        final VersionedProcessor processor = createProcessorWithProperty("proc-a", "propX", "oldVal");
        final VersionedProcessor localProcessor = createProcessorWithProperty("proc-a", "propX", "newVal");

        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processor, localProcessor, "propX",
                "oldVal", "newVal", "Property propX changed");

        final VersionedProcessGroup mergedFlow = new VersionedProcessGroup();
        mergedFlow.setIdentifier("root");
        mergedFlow.getProcessors().add(createProcessorWithProperty("proc-a", "propX", "oldVal"));

        handler.apply(localDifference, mergedFlow);

        final VersionedProcessor mergedProcessor = mergedFlow.getProcessors().iterator().next();
        assertNotNull(mergedProcessor);
        assertEquals("newVal", mergedProcessor.getProperties().get("propX"));
    }

    @Test
    public void testMultipleUpstreamChangesNoneMatching() {
        final VersionedProcessor processorA = createProcessorWithProperty("proc-a", "propX", "oldVal");
        final VersionedProcessor localProcessorA = createProcessorWithProperty("proc-a", "propX", "newVal");

        final VersionedProcessor processorB = createProcessorWithProperty("proc-b", "propY", "oldY");
        final VersionedProcessor processorC = createProcessorWithProperty("proc-c", "propZ", "oldZ");

        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processorA, localProcessorA, "propX",
                "oldVal", "newVal", "Property propX changed on proc-a");

        final Set<FlowDifference> upstreamDifferences = new HashSet<>();
        upstreamDifferences.add(new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processorB, processorB, "propY",
                "oldY", "newY", "Property propY changed on proc-b"));
        upstreamDifferences.add(new StandardFlowDifference(DifferenceType.PROPERTY_CHANGED, processorC, processorC, "propZ",
                "oldZ", "newZ", "Property propZ changed on proc-c"));
        upstreamDifferences.add(new StandardFlowDifference(DifferenceType.POSITION_CHANGED,
                processorA, processorA, null, null, "Position changed on proc-a"));

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier("root");
        targetSnapshot.getProcessors().add(createProcessorWithProperty("proc-a", "propX", "oldVal"));

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
