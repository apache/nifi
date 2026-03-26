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

package org.apache.nifi.components.connector.facades.standalone;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.connector.components.StatelessGroupLifecycle;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.parameter.ParameterLookup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestStandaloneProcessGroupLifecycle {

    private static final String SERVICE_ID = "service-1";
    private static final String SERVICE_ID_2 = "service-2";

    private static final AllowableValue VALUE_A = new AllowableValue("A", "Value A");
    private static final AllowableValue VALUE_B = new AllowableValue("B", "Value B");

    private static final PropertyDescriptor MODE_PROPERTY = new PropertyDescriptor.Builder()
        .name("mode")
        .displayName("Mode")
        .allowableValues(VALUE_A, VALUE_B)
        .defaultValue(VALUE_A.getValue())
        .required(true)
        .build();

    private static final PropertyDescriptor SERVICE_PROPERTY_NO_DEPENDENCY = new PropertyDescriptor.Builder()
        .name("service-no-dep")
        .displayName("Service (No Dependency)")
        .identifiesControllerService(ControllerService.class)
        .build();

    private static final PropertyDescriptor SERVICE_PROPERTY_WITH_DEPENDENCY = new PropertyDescriptor.Builder()
        .name("service-with-dep")
        .displayName("Service (With Dependency)")
        .identifiesControllerService(ControllerService.class)
        .dependsOn(MODE_PROPERTY, VALUE_B)
        .build();

    @Mock
    private ProcessGroup processGroup;

    @Mock
    private ControllerServiceProvider controllerServiceProvider;

    @Mock
    private StatelessGroupLifecycle statelessGroupLifecycle;

    private StandaloneProcessGroupLifecycle lifecycle;

    @BeforeEach
    void setUp() {
        lifecycle = new StandaloneProcessGroupLifecycle(processGroup, controllerServiceProvider, statelessGroupLifecycle, id -> null);
        lenient().when(processGroup.getProcessGroups()).thenReturn(Collections.emptySet());
    }

    @Test
    void testFindReferencedServicesIncludesServiceWithNoDependency() {
        final ControllerServiceNode serviceNode = mock(ControllerServiceNode.class);
        when(serviceNode.getRequiredControllerServices()).thenReturn(Collections.emptyList());

        final ProcessorNode processor = createProcessorNode(
            List.of(SERVICE_PROPERTY_NO_DEPENDENCY),
            Map.of(SERVICE_PROPERTY_NO_DEPENDENCY, SERVICE_ID)
        );

        when(processGroup.getProcessors()).thenReturn(List.of(processor));
        when(controllerServiceProvider.getControllerServiceNode(SERVICE_ID)).thenReturn(serviceNode);

        final Set<ControllerServiceNode> result = lifecycle.findReferencedServices(false);

        assertTrue(result.contains(serviceNode), "Service with no property dependency should be included");
        assertEquals(1, result.size());
    }

    @Test
    void testFindReferencedServicesExcludesServiceWhenDependencyNotSatisfied() {
        final ProcessorNode processor = createProcessorNode(
            List.of(MODE_PROPERTY, SERVICE_PROPERTY_WITH_DEPENDENCY),
            Map.of(MODE_PROPERTY, VALUE_A.getValue(), SERVICE_PROPERTY_WITH_DEPENDENCY, SERVICE_ID)
        );

        when(processGroup.getProcessors()).thenReturn(List.of(processor));

        final Set<ControllerServiceNode> result = lifecycle.findReferencedServices(false);

        assertTrue(result.isEmpty(), "Service should not be included when property dependency is not satisfied");
    }

    @Test
    void testFindReferencedServicesIncludesServiceWhenDependencySatisfied() {
        final ControllerServiceNode serviceNode = mock(ControllerServiceNode.class);
        when(serviceNode.getRequiredControllerServices()).thenReturn(Collections.emptyList());

        final ProcessorNode processor = createProcessorNode(
            List.of(MODE_PROPERTY, SERVICE_PROPERTY_WITH_DEPENDENCY),
            Map.of(MODE_PROPERTY, VALUE_B.getValue(), SERVICE_PROPERTY_WITH_DEPENDENCY, SERVICE_ID)
        );

        when(processGroup.getProcessors()).thenReturn(List.of(processor));
        when(controllerServiceProvider.getControllerServiceNode(SERVICE_ID)).thenReturn(serviceNode);

        final Set<ControllerServiceNode> result = lifecycle.findReferencedServices(false);

        assertTrue(result.contains(serviceNode), "Service should be included when property dependency is satisfied");
        assertEquals(1, result.size());
    }

    @Test
    void testFindReferencedServicesUsesDefaultValueForDependencyCheck() {
        // A property that depends on MODE_PROPERTY having VALUE_A (which is the default)
        final PropertyDescriptor servicePropertyDependsOnDefault = new PropertyDescriptor.Builder()
            .name("service-dep-default")
            .displayName("Service (Depends on Default)")
            .identifiesControllerService(ControllerService.class)
            .dependsOn(MODE_PROPERTY, VALUE_A)
            .build();

        final ControllerServiceNode serviceNode = mock(ControllerServiceNode.class);
        when(serviceNode.getRequiredControllerServices()).thenReturn(Collections.emptyList());

        // Mode property not explicitly set; the default value "A" should satisfy the dependency
        final ProcessorNode processor = createProcessorNode(
            List.of(MODE_PROPERTY, servicePropertyDependsOnDefault),
            Map.of(servicePropertyDependsOnDefault, SERVICE_ID)
        );

        when(processGroup.getProcessors()).thenReturn(List.of(processor));
        when(controllerServiceProvider.getControllerServiceNode(SERVICE_ID)).thenReturn(serviceNode);

        final Set<ControllerServiceNode> result = lifecycle.findReferencedServices(false);

        assertTrue(result.contains(serviceNode), "Service should be included when dependency property uses default value that satisfies dependency");
        assertEquals(1, result.size());
    }

    @Test
    void testFindReferencedServicesTransitiveServiceExcludedWhenDependencyNotSatisfied() {
        // Service 1 is directly referenced by the processor (no dependency on service property)
        final ControllerServiceNode serviceNode1 = mock(ControllerServiceNode.class);
        // getRequiredControllerServices() already filters based on dependencies, so it returns empty
        when(serviceNode1.getRequiredControllerServices()).thenReturn(Collections.emptyList());

        final ProcessorNode processor = createProcessorNode(
            List.of(SERVICE_PROPERTY_NO_DEPENDENCY),
            Map.of(SERVICE_PROPERTY_NO_DEPENDENCY, SERVICE_ID)
        );

        when(processGroup.getProcessors()).thenReturn(List.of(processor));
        when(controllerServiceProvider.getControllerServiceNode(SERVICE_ID)).thenReturn(serviceNode1);

        final Set<ControllerServiceNode> result = lifecycle.findReferencedServices(false);

        assertTrue(result.contains(serviceNode1), "Directly referenced service should be included");
        assertEquals(1, result.size(), "Only the directly referenced service should be in the result");
    }

    @Test
    void testFindReferencedServicesTransitiveServiceIncludedWhenDependencySatisfied() {
        final ControllerServiceNode serviceNode1 = mock(ControllerServiceNode.class);
        final ControllerServiceNode serviceNode2 = mock(ControllerServiceNode.class);
        // Service 1 requires Service 2 (dependency is satisfied)
        when(serviceNode1.getRequiredControllerServices()).thenReturn(List.of(serviceNode2));
        when(serviceNode2.getRequiredControllerServices()).thenReturn(Collections.emptyList());

        final ProcessorNode processor = createProcessorNode(
            List.of(SERVICE_PROPERTY_NO_DEPENDENCY),
            Map.of(SERVICE_PROPERTY_NO_DEPENDENCY, SERVICE_ID)
        );

        when(processGroup.getProcessors()).thenReturn(List.of(processor));
        when(controllerServiceProvider.getControllerServiceNode(SERVICE_ID)).thenReturn(serviceNode1);

        final Set<ControllerServiceNode> result = lifecycle.findReferencedServices(false);

        assertTrue(result.contains(serviceNode1), "Directly referenced service should be included");
        assertTrue(result.contains(serviceNode2), "Transitively required service should be included");
        assertEquals(2, result.size());
    }

    @Test
    void testFindReferencedServicesMultiplePropertiesMixedDependencies() {
        final ControllerServiceNode serviceNode1 = mock(ControllerServiceNode.class);
        when(serviceNode1.getRequiredControllerServices()).thenReturn(Collections.emptyList());
        final ControllerServiceNode serviceNode2 = mock(ControllerServiceNode.class);

        // Processor with two service properties: one with no dependency (service1) and one with unsatisfied dependency (service2)
        // Mode is "A", which does NOT satisfy the dependency on VALUE_B for SERVICE_PROPERTY_WITH_DEPENDENCY
        final ProcessorNode processor = createProcessorNode(
            List.of(MODE_PROPERTY, SERVICE_PROPERTY_NO_DEPENDENCY, SERVICE_PROPERTY_WITH_DEPENDENCY),
            Map.of(MODE_PROPERTY, VALUE_A.getValue(), SERVICE_PROPERTY_NO_DEPENDENCY, SERVICE_ID, SERVICE_PROPERTY_WITH_DEPENDENCY, SERVICE_ID_2)
        );

        when(processGroup.getProcessors()).thenReturn(List.of(processor));
        when(controllerServiceProvider.getControllerServiceNode(SERVICE_ID)).thenReturn(serviceNode1);

        final Set<ControllerServiceNode> result = lifecycle.findReferencedServices(false);

        assertTrue(result.contains(serviceNode1), "Service from property with no dependency should be included");
        assertFalse(result.contains(serviceNode2), "Service from property with unsatisfied dependency should not be included");
        assertEquals(1, result.size());
    }

    /**
     * Creates a mock ProcessorNode with the given property descriptors and effective property values.
     * The mock is configured with a ValidationContext whose isDependencySatisfied calls the real default
     * implementation, using property values derived from the provided effective values.
     */
    private ProcessorNode createProcessorNode(final List<PropertyDescriptor> descriptors,
            final Map<PropertyDescriptor, String> effectiveValues) {
        final ProcessorNode processor = mock(ProcessorNode.class);
        when(processor.getPropertyDescriptors()).thenReturn(descriptors);

        for (final PropertyDescriptor descriptor : descriptors) {
            lenient().when(processor.getPropertyDescriptor(descriptor.getName())).thenReturn(descriptor);
            lenient().when(processor.getEffectivePropertyValue(descriptor)).thenReturn(effectiveValues.get(descriptor));
        }

        // Build the effective property value map for creating the validation context
        final Map<PropertyDescriptor, String> effectivePropertyValues = new LinkedHashMap<>();
        for (final PropertyDescriptor descriptor : descriptors) {
            final String value = effectiveValues.get(descriptor);
            if (value != null) {
                effectivePropertyValues.put(descriptor, value);
            }
        }
        when(processor.getEffectivePropertyValues()).thenReturn(effectivePropertyValues);

        // Set up a ValidationContext that delegates isDependencySatisfied to the real default implementation
        final ValidationContext validationContext = mock(ValidationContext.class);
        when(validationContext.isDependencySatisfied(any(PropertyDescriptor.class), any(Function.class))).thenCallRealMethod();

        // Mock getProperty() so the real isDependencySatisfied default method can resolve property values
        for (final PropertyDescriptor descriptor : descriptors) {
            final String value = effectiveValues.get(descriptor);
            if (value != null) {
                final PropertyValue propertyValue = mock(PropertyValue.class);
                lenient().when(propertyValue.getValue()).thenReturn(value);
                lenient().when(validationContext.getProperty(descriptor)).thenReturn(propertyValue);
            }
        }

        lenient().when(processor.getAnnotationData()).thenReturn(null);
        lenient().when(processor.getParameterLookup()).thenReturn(ParameterLookup.EMPTY);
        when(processor.createValidationContext(any(Map.class), any(), any(ParameterLookup.class), anyBoolean())).thenReturn(validationContext);

        return processor;
    }
}
