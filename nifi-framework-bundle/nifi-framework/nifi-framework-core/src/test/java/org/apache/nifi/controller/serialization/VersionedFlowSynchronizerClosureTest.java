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
package org.apache.nifi.controller.serialization;

import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedParameterProvider;
import org.apache.nifi.flow.VersionedPropertyDescriptor;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Focused unit tests for the pure-data closure helper used by the pre-pass that enables
 * Controller Services backing Parameter Providers before connector synchronization.
 */
class VersionedFlowSynchronizerClosureTest {

    private static final String PP_TYPE = "org.apache.nifi.parameter.TestParameterProvider";
    private static final String CS_TYPE = "org.apache.nifi.controller.TestControllerService";

    private static final String CS_A_ID = "cs-a";
    private static final String CS_B_ID = "cs-b";
    private static final String CS_C_ID = "cs-c";

    @Test
    void closureIsEmptyWhenDataflowIsNull() {
        final Set<String> closure = VersionedFlowSynchronizer.computeParameterProviderReferencedControllerServiceClosure(null);
        assertTrue(closure.isEmpty());
    }

    @Test
    void closureIsEmptyWhenNoParameterProviders() {
        final VersionedDataflow dataflow = new VersionedDataflow();
        dataflow.setParameterProviders(List.of());
        dataflow.setControllerServices(List.of(controllerServiceWithoutRefs(CS_A_ID)));

        final Set<String> closure = VersionedFlowSynchronizer.computeParameterProviderReferencedControllerServiceClosure(dataflow);
        assertTrue(closure.isEmpty());
    }

    @Test
    void closureIncludesDirectlyReferencedControllerService() {
        final VersionedParameterProvider pp = parameterProvider(Map.of("Service", CS_A_ID), Map.of("Service", csIdentifyingDescriptor()));
        final VersionedDataflow dataflow = new VersionedDataflow();
        dataflow.setParameterProviders(List.of(pp));
        dataflow.setControllerServices(List.of(controllerServiceWithoutRefs(CS_A_ID)));

        final Set<String> closure = VersionedFlowSynchronizer.computeParameterProviderReferencedControllerServiceClosure(dataflow);
        assertEquals(Set.of(CS_A_ID), closure);
    }

    @Test
    void closureExpandsTransitivelyAcrossControllerServiceReferences() {
        final VersionedParameterProvider pp = parameterProvider(Map.of("Service", CS_A_ID), Map.of("Service", csIdentifyingDescriptor()));
        final VersionedControllerService csA = controllerService(CS_A_ID, Map.of("Delegate", CS_B_ID), Map.of("Delegate", csIdentifyingDescriptor()));
        final VersionedControllerService csB = controllerService(CS_B_ID, Map.of("Delegate", CS_C_ID), Map.of("Delegate", csIdentifyingDescriptor()));
        final VersionedControllerService csC = controllerServiceWithoutRefs(CS_C_ID);

        final VersionedDataflow dataflow = new VersionedDataflow();
        dataflow.setParameterProviders(List.of(pp));
        dataflow.setControllerServices(List.of(csA, csB, csC));

        final Set<String> closure = VersionedFlowSynchronizer.computeParameterProviderReferencedControllerServiceClosure(dataflow);
        assertEquals(Set.of(CS_A_ID, CS_B_ID, CS_C_ID), closure);
    }

    @Test
    void closureTerminatesWhenControllerServicesReferenceEachOther() {
        final VersionedParameterProvider pp = parameterProvider(Map.of("Service", CS_A_ID), Map.of("Service", csIdentifyingDescriptor()));
        final VersionedControllerService csA = controllerService(CS_A_ID, Map.of("Partner", CS_B_ID), Map.of("Partner", csIdentifyingDescriptor()));
        final VersionedControllerService csB = controllerService(CS_B_ID, Map.of("Partner", CS_A_ID), Map.of("Partner", csIdentifyingDescriptor()));

        final VersionedDataflow dataflow = new VersionedDataflow();
        dataflow.setParameterProviders(List.of(pp));
        dataflow.setControllerServices(List.of(csA, csB));

        final Set<String> closure = VersionedFlowSynchronizer.computeParameterProviderReferencedControllerServiceClosure(dataflow);
        assertEquals(Set.of(CS_A_ID, CS_B_ID), closure);
    }

    @Test
    void closureIgnoresPropertyDescriptorsThatDoNotIdentifyControllerServices() {
        // The "Endpoint" property value happens to coincide with a CS instance identifier, but its
        // descriptor's identifiesControllerService flag is false, so it must not appear in the closure.
        final VersionedPropertyDescriptor plainDescriptor = new VersionedPropertyDescriptor();
        plainDescriptor.setName("Endpoint");
        plainDescriptor.setIdentifiesControllerService(false);

        final VersionedParameterProvider pp = parameterProvider(Map.of("Endpoint", CS_A_ID), Map.of("Endpoint", plainDescriptor));
        final VersionedDataflow dataflow = new VersionedDataflow();
        dataflow.setParameterProviders(List.of(pp));
        dataflow.setControllerServices(List.of(controllerServiceWithoutRefs(CS_A_ID)));

        final Set<String> closure = VersionedFlowSynchronizer.computeParameterProviderReferencedControllerServiceClosure(dataflow);
        assertTrue(closure.isEmpty());
    }

    @Test
    void closureOmitsDanglingReferenceNotPresentInControllerServices() {
        final VersionedParameterProvider pp = parameterProvider(
                Map.of("Service", CS_A_ID),
                Map.of("Service", csIdentifyingDescriptor()));

        final VersionedDataflow dataflow = new VersionedDataflow();
        dataflow.setParameterProviders(List.of(pp));
        dataflow.setControllerServices(List.of());

        final Set<String> closure = VersionedFlowSynchronizer.computeParameterProviderReferencedControllerServiceClosure(dataflow);
        assertTrue(closure.isEmpty());
    }

    @Test
    void closureSkipsNullOrEmptyControllerServiceReferenceValues() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("Service", null);
        properties.put("Other", "");

        final Map<String, VersionedPropertyDescriptor> descriptors = Map.of(
                "Service", csIdentifyingDescriptor(),
                "Other", csIdentifyingDescriptor());

        final VersionedParameterProvider pp = parameterProvider(properties, descriptors);
        final VersionedDataflow dataflow = new VersionedDataflow();
        dataflow.setParameterProviders(List.of(pp));
        dataflow.setControllerServices(List.of());

        final Set<String> closure = VersionedFlowSynchronizer.computeParameterProviderReferencedControllerServiceClosure(dataflow);
        assertTrue(closure.isEmpty());
    }

    private VersionedParameterProvider parameterProvider(final Map<String, String> properties,
                                                         final Map<String, VersionedPropertyDescriptor> descriptors) {
        final VersionedParameterProvider pp = new VersionedParameterProvider();
        pp.setInstanceIdentifier("pp-" + properties.hashCode());
        pp.setType(PP_TYPE);
        pp.setProperties(properties);
        pp.setPropertyDescriptors(descriptors);
        return pp;
    }

    private VersionedControllerService controllerService(final String instanceIdentifier, final Map<String, String> properties,
                                                         final Map<String, VersionedPropertyDescriptor> descriptors) {
        final VersionedControllerService cs = new VersionedControllerService();
        cs.setInstanceIdentifier(instanceIdentifier);
        cs.setType(CS_TYPE);
        cs.setProperties(properties);
        cs.setPropertyDescriptors(descriptors);
        return cs;
    }

    private VersionedControllerService controllerServiceWithoutRefs(final String instanceIdentifier) {
        return controllerService(instanceIdentifier, Map.of(), Map.of());
    }

    private VersionedPropertyDescriptor csIdentifyingDescriptor() {
        final VersionedPropertyDescriptor descriptor = new VersionedPropertyDescriptor();
        descriptor.setIdentifiesControllerService(true);
        return descriptor;
    }
}
