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

import org.apache.nifi.flow.VersionedControllerService;
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
import static org.junit.jupiter.api.Assertions.assertNull;

class PropertyAddedRebaseHandlerTest {

    private static final String ROOT = "root";
    private static final String PROC_A = "proc-a";
    private static final String SERVICE_A = "service-a";
    private static final String DYNAMIC_PROP = "my.dynamic.key";
    private static final String STATIC_PROP = "Timeout";
    private static final String LOCAL_VALUE = "localVal";
    private static final String UPSTREAM_VALUE = "upstreamVal";

    private PropertyAddedRebaseHandler handler;

    @BeforeEach
    void setup() {
        handler = new PropertyAddedRebaseHandler();
    }

    // --- processor: dynamic property ---

    @Test
    void testDynamicPropertyOnProcessorNoUpstreamConflict_isCompatible() {
        final VersionedProcessor versionNProcessor = createProcessorNoProperties(PROC_A);
        final VersionedProcessor localProcessor = createProcessorWithDynamicProperty(PROC_A, DYNAMIC_PROP, LOCAL_VALUE);

        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.PROPERTY_ADDED,
                versionNProcessor, localProcessor, DYNAMIC_PROP, null, LOCAL_VALUE, "Dynamic property added");

        // target version: processor exists but does not have the dynamic property descriptor
        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier(ROOT);
        targetSnapshot.getProcessors().add(createProcessorNoProperties(PROC_A));

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Collections.emptySet(), targetSnapshot);

        assertEquals(RebaseClassification.COMPATIBLE, result.getClassification());
    }

    @Test
    void testNonDynamicPropertyDescriptorNotInTarget_isUnsupported() {
        final VersionedProcessor versionNProcessor = createProcessorNoProperties(PROC_A);
        final VersionedProcessor localProcessor = createProcessorWithStaticProperty(PROC_A, STATIC_PROP, LOCAL_VALUE);

        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.PROPERTY_ADDED,
                versionNProcessor, localProcessor, STATIC_PROP, null, LOCAL_VALUE, "Static property added");

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier(ROOT);
        targetSnapshot.getProcessors().add(createProcessorNoProperties(PROC_A));

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Collections.emptySet(), targetSnapshot);

        assertEquals(RebaseClassification.UNSUPPORTED, result.getClassification());
        assertEquals(RebaseConflictCode.DESCRIPTOR_NOT_FOUND, result.getConflictCode());
    }

    @Test
    void testDynamicPropertyConflictOnProcessor_isConflicting() {
        final VersionedProcessor versionNProcessor = createProcessorNoProperties(PROC_A);
        final VersionedProcessor localProcessor = createProcessorWithDynamicProperty(PROC_A, DYNAMIC_PROP, LOCAL_VALUE);

        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.PROPERTY_ADDED,
                versionNProcessor, localProcessor, DYNAMIC_PROP, null, LOCAL_VALUE, "Dynamic property added locally");

        // upstream also added the same dynamic property but with a different value
        final Set<FlowDifference> upstreamDifferences = new HashSet<>();
        upstreamDifferences.add(new StandardFlowDifference(DifferenceType.PROPERTY_ADDED,
                versionNProcessor, versionNProcessor, DYNAMIC_PROP, null, UPSTREAM_VALUE, "Dynamic property added upstream"));

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier(ROOT);

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, upstreamDifferences, targetSnapshot);

        assertEquals(RebaseClassification.CONFLICTING, result.getClassification());
        assertEquals(RebaseConflictCode.SAME_PROPERTY, result.getConflictCode());
    }

    @Test
    void testDynamicPropertySameValueUpstream_isCompatible() {
        final VersionedProcessor versionNProcessor = createProcessorNoProperties(PROC_A);
        final VersionedProcessor localProcessor = createProcessorWithDynamicProperty(PROC_A, DYNAMIC_PROP, LOCAL_VALUE);

        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.PROPERTY_ADDED,
                versionNProcessor, localProcessor, DYNAMIC_PROP, null, LOCAL_VALUE, "Dynamic property added locally");

        // upstream added the same property with the same value — idempotent
        final Set<FlowDifference> upstreamDifferences = new HashSet<>();
        upstreamDifferences.add(new StandardFlowDifference(DifferenceType.PROPERTY_ADDED,
                versionNProcessor, versionNProcessor, DYNAMIC_PROP, null, LOCAL_VALUE, "Dynamic property added upstream"));

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier(ROOT);

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, upstreamDifferences, targetSnapshot);

        assertEquals(RebaseClassification.COMPATIBLE, result.getClassification());
    }

    @Test
    void testDynamicPropertyDescriptorMissingFromLocalComponent_isUnsupported() {
        // componentB has an empty descriptor map — cannot determine dynamic flag → UNSUPPORTED
        final VersionedProcessor versionNProcessor = createProcessorNoProperties(PROC_A);
        final VersionedProcessor localProcessor = new VersionedProcessor();
        localProcessor.setIdentifier(PROC_A);
        localProcessor.setProperties(Map.of(DYNAMIC_PROP, LOCAL_VALUE));
        localProcessor.setPropertyDescriptors(Collections.emptyMap());

        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.PROPERTY_ADDED,
                versionNProcessor, localProcessor, DYNAMIC_PROP, null, LOCAL_VALUE, "Property added, no descriptor");

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier(ROOT);
        targetSnapshot.getProcessors().add(createProcessorNoProperties(PROC_A));

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Collections.emptySet(), targetSnapshot);

        assertEquals(RebaseClassification.UNSUPPORTED, result.getClassification());
        assertEquals(RebaseConflictCode.DESCRIPTOR_NOT_FOUND, result.getConflictCode());
    }

    @Test
    void testApplyAddsDynamicPropertyToProcessor() {
        final VersionedProcessor versionNProcessor = createProcessorNoProperties(PROC_A);
        final VersionedProcessor localProcessor = createProcessorWithDynamicProperty(PROC_A, DYNAMIC_PROP, LOCAL_VALUE);

        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.PROPERTY_ADDED,
                versionNProcessor, localProcessor, DYNAMIC_PROP, null, LOCAL_VALUE, "Dynamic property added");

        final VersionedProcessGroup mergedFlow = new VersionedProcessGroup();
        mergedFlow.setIdentifier(ROOT);
        mergedFlow.getProcessors().add(createProcessorNoProperties(PROC_A));

        handler.apply(localDifference, mergedFlow);

        final VersionedProcessor mergedProcessor = mergedFlow.getProcessors().iterator().next();
        assertNotNull(mergedProcessor);
        assertEquals(LOCAL_VALUE, mergedProcessor.getProperties().get(DYNAMIC_PROP));
    }

    // --- controller service: dynamic property ---

    @Test
    void testDynamicPropertyOnControllerServiceNoUpstreamConflict_isCompatible() {
        final VersionedControllerService versionNService = createServiceNoProperties(SERVICE_A);
        final VersionedControllerService localService = createServiceWithDynamicProperty(SERVICE_A, DYNAMIC_PROP, LOCAL_VALUE);

        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.PROPERTY_ADDED,
                versionNService, localService, DYNAMIC_PROP, null, LOCAL_VALUE, "Dynamic property added on CS");

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier(ROOT);
        targetSnapshot.getControllerServices().add(createServiceNoProperties(SERVICE_A));

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Collections.emptySet(), targetSnapshot);

        assertEquals(RebaseClassification.COMPATIBLE, result.getClassification());
    }

    @Test
    void testApplyAddsDynamicPropertyToControllerService() {
        final VersionedControllerService versionNService = createServiceNoProperties(SERVICE_A);
        final VersionedControllerService localService = createServiceWithDynamicProperty(SERVICE_A, DYNAMIC_PROP, LOCAL_VALUE);

        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.PROPERTY_ADDED,
                versionNService, localService, DYNAMIC_PROP, null, LOCAL_VALUE, "Dynamic property added on CS");

        final VersionedProcessGroup mergedFlow = new VersionedProcessGroup();
        mergedFlow.setIdentifier(ROOT);
        mergedFlow.getControllerServices().add(createServiceNoProperties(SERVICE_A));

        handler.apply(localDifference, mergedFlow);

        final VersionedControllerService mergedService = mergedFlow.getControllerServices().iterator().next();
        assertNotNull(mergedService);
        assertEquals(LOCAL_VALUE, mergedService.getProperties().get(DYNAMIC_PROP));
    }

    @Test
    void testNonDynamicPropertyOnControllerServiceDescriptorNotInTarget_isUnsupported() {
        final VersionedControllerService versionNService = createServiceNoProperties(SERVICE_A);
        final VersionedControllerService localService = createServiceWithStaticProperty(SERVICE_A, STATIC_PROP, LOCAL_VALUE);

        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.PROPERTY_ADDED,
                versionNService, localService, STATIC_PROP, null, LOCAL_VALUE, "Static property added on CS");

        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier(ROOT);
        targetSnapshot.getControllerServices().add(createServiceNoProperties(SERVICE_A));

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Collections.emptySet(), targetSnapshot);

        assertEquals(RebaseClassification.UNSUPPORTED, result.getClassification());
        assertEquals(RebaseConflictCode.DESCRIPTOR_NOT_FOUND, result.getConflictCode());
    }

    // --- static property present in target: still COMPATIBLE (existing behaviour) ---

    @Test
    void testStaticPropertyDescriptorPresentInTarget_isCompatible() {
        final VersionedProcessor versionNProcessor = createProcessorNoProperties(PROC_A);
        final VersionedProcessor localProcessor = createProcessorWithStaticProperty(PROC_A, STATIC_PROP, LOCAL_VALUE);

        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.PROPERTY_ADDED,
                versionNProcessor, localProcessor, STATIC_PROP, null, LOCAL_VALUE, "Static property added");

        // target has the descriptor for the property
        final VersionedProcessGroup targetSnapshot = new VersionedProcessGroup();
        targetSnapshot.setIdentifier(ROOT);
        targetSnapshot.getProcessors().add(createProcessorWithStaticProperty(PROC_A, STATIC_PROP, "defaultVal"));

        final RebaseAnalysis.ClassifiedDifference result = handler.classify(localDifference, Collections.emptySet(), targetSnapshot);

        assertEquals(RebaseClassification.COMPATIBLE, result.getClassification());
        assertNull(result.getConflictCode());
    }

    // --- helpers ---

    private VersionedProcessor createProcessorNoProperties(final String identifier) {
        final VersionedProcessor processor = new VersionedProcessor();
        processor.setIdentifier(identifier);
        processor.setProperties(Collections.emptyMap());
        processor.setPropertyDescriptors(Collections.emptyMap());
        return processor;
    }

    private VersionedProcessor createProcessorWithDynamicProperty(final String identifier, final String propName, final String propValue) {
        final VersionedProcessor processor = new VersionedProcessor();
        processor.setIdentifier(identifier);

        final Map<String, String> properties = new HashMap<>();
        properties.put(propName, propValue);
        processor.setProperties(properties);

        final VersionedPropertyDescriptor descriptor = new VersionedPropertyDescriptor();
        descriptor.setName(propName);
        descriptor.setDynamic(true);

        final Map<String, VersionedPropertyDescriptor> descriptors = new HashMap<>();
        descriptors.put(propName, descriptor);
        processor.setPropertyDescriptors(descriptors);

        return processor;
    }

    private VersionedProcessor createProcessorWithStaticProperty(final String identifier, final String propName, final String propValue) {
        final VersionedProcessor processor = new VersionedProcessor();
        processor.setIdentifier(identifier);

        final Map<String, String> properties = new HashMap<>();
        properties.put(propName, propValue);
        processor.setProperties(properties);

        final VersionedPropertyDescriptor descriptor = new VersionedPropertyDescriptor();
        descriptor.setName(propName);
        descriptor.setDynamic(false);

        final Map<String, VersionedPropertyDescriptor> descriptors = new HashMap<>();
        descriptors.put(propName, descriptor);
        processor.setPropertyDescriptors(descriptors);

        return processor;
    }

    private VersionedControllerService createServiceNoProperties(final String identifier) {
        final VersionedControllerService service = new VersionedControllerService();
        service.setIdentifier(identifier);
        service.setProperties(Collections.emptyMap());
        service.setPropertyDescriptors(Collections.emptyMap());
        return service;
    }

    private VersionedControllerService createServiceWithDynamicProperty(final String identifier, final String propName, final String propValue) {
        final VersionedControllerService service = new VersionedControllerService();
        service.setIdentifier(identifier);

        final Map<String, String> properties = new HashMap<>();
        properties.put(propName, propValue);
        service.setProperties(properties);

        final VersionedPropertyDescriptor descriptor = new VersionedPropertyDescriptor();
        descriptor.setName(propName);
        descriptor.setDynamic(true);

        final Map<String, VersionedPropertyDescriptor> descriptors = new HashMap<>();
        descriptors.put(propName, descriptor);
        service.setPropertyDescriptors(descriptors);

        return service;
    }

    private VersionedControllerService createServiceWithStaticProperty(final String identifier, final String propName, final String propValue) {
        final VersionedControllerService service = new VersionedControllerService();
        service.setIdentifier(identifier);

        final Map<String, String> properties = new HashMap<>();
        properties.put(propName, propValue);
        service.setProperties(properties);

        final VersionedPropertyDescriptor descriptor = new VersionedPropertyDescriptor();
        descriptor.setName(propName);
        descriptor.setDynamic(false);

        final Map<String, VersionedPropertyDescriptor> descriptors = new HashMap<>();
        descriptors.put(propName, descriptor);
        service.setPropertyDescriptors(descriptors);

        return service;
    }
}
