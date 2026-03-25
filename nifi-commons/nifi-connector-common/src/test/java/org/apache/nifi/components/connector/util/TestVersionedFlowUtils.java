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

package org.apache.nifi.components.connector.util;

import org.apache.nifi.components.connector.ComponentBundleLookup;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestVersionedFlowUtils {

    @Nested
    class UpdateToLatestBundles {
        private static final String PROCESSOR_TYPE = "org.apache.nifi.processors.TestProcessor";
        private static final String SERVICE_TYPE = "org.apache.nifi.services.TestService";

        @Test
        void testLookupReturnsNewerBundle() {
            final VersionedProcessGroup group = createProcessGroup();
            final VersionedProcessor processor = VersionedFlowUtils.addProcessor(group, PROCESSOR_TYPE, new Bundle("group", "artifact", "2.0.0"), "Test Processor", new Position(0, 0));

            final ComponentBundleLookup lookup = mock(ComponentBundleLookup.class);
            when(lookup.getLatestBundle(PROCESSOR_TYPE)).thenReturn(Optional.of(new Bundle("group", "artifact", "3.0.0")));

            VersionedFlowUtils.updateToLatestBundles(group, lookup);

            assertEquals("3.0.0", processor.getBundle().getVersion());
        }

        @Test
        void testLookupReturnsLatestBundle() {
            final VersionedProcessGroup group = createProcessGroup();
            final VersionedProcessor processor = VersionedFlowUtils.addProcessor(group, PROCESSOR_TYPE, new Bundle("group", "artifact", "2.0.0"), "Test Processor", new Position(0, 0));

            final ComponentBundleLookup lookup = mock(ComponentBundleLookup.class);
            when(lookup.getLatestBundle(PROCESSOR_TYPE)).thenReturn(Optional.of(new Bundle("group", "artifact", "4.0.0")));

            VersionedFlowUtils.updateToLatestBundles(group, lookup);

            assertEquals("4.0.0", processor.getBundle().getVersion());
        }

        @Test
        void testControllerServiceUpdatedToNewerBundle() {
            final VersionedProcessGroup group = createProcessGroup();
            final VersionedControllerService service = VersionedFlowUtils.addControllerService(group, SERVICE_TYPE, new Bundle("group", "artifact", "1.5.0"), "Test Service");

            final ComponentBundleLookup lookup = mock(ComponentBundleLookup.class);
            when(lookup.getLatestBundle(SERVICE_TYPE)).thenReturn(Optional.of(new Bundle("group", "artifact", "2.5.0")));

            VersionedFlowUtils.updateToLatestBundles(group, lookup);

            assertEquals("2.5.0", service.getBundle().getVersion());
        }

        @Test
        void testNestedProcessorUpdatedToNewerBundle() {
            final VersionedProcessGroup rootGroup = createProcessGroup();
            final VersionedProcessGroup childGroup = createChildProcessGroup(rootGroup, "child-group-id");
            final VersionedProcessor nestedProcessor = VersionedFlowUtils.addProcessor(childGroup, PROCESSOR_TYPE, new Bundle("group", "artifact", "1.0.0"), "Nested Processor", new Position(0, 0));

            final ComponentBundleLookup lookup = mock(ComponentBundleLookup.class);
            when(lookup.getLatestBundle(PROCESSOR_TYPE)).thenReturn(Optional.of(new Bundle("group", "artifact", "5.0.0")));

            VersionedFlowUtils.updateToLatestBundles(rootGroup, lookup);

            assertEquals("5.0.0", nestedProcessor.getBundle().getVersion());
        }

        @Test
        void testEmptyOptionalDoesNotChangeBundle() {
            final VersionedProcessGroup group = createProcessGroup();
            final VersionedProcessor processor = VersionedFlowUtils.addProcessor(group, PROCESSOR_TYPE, new Bundle("group", "artifact", "2.0.0"), "Test Processor", new Position(0, 0));

            final ComponentBundleLookup lookup = mock(ComponentBundleLookup.class);
            when(lookup.getLatestBundle(PROCESSOR_TYPE)).thenReturn(Optional.empty());

            VersionedFlowUtils.updateToLatestBundles(group, lookup);

            assertEquals("2.0.0", processor.getBundle().getVersion());
        }

        @Test
        void testUpdateToLatestBundleReturnsTrueWhenUpdated() {
            final VersionedProcessGroup group = createProcessGroup();
            final VersionedProcessor processor = VersionedFlowUtils.addProcessor(group, PROCESSOR_TYPE, new Bundle("group", "artifact", "2.0.0"), "Test Processor", new Position(0, 0));

            final ComponentBundleLookup lookup = mock(ComponentBundleLookup.class);
            when(lookup.getLatestBundle(PROCESSOR_TYPE)).thenReturn(Optional.of(new Bundle("group", "artifact", "3.0.0")));

            final boolean updated = VersionedFlowUtils.updateToLatestBundle(processor, lookup);

            assertTrue(updated);
            assertEquals("3.0.0", processor.getBundle().getVersion());
        }

        @Test
        void testUpdateToLatestBundleReturnsFalseWhenNotUpdated() {
            final VersionedProcessGroup group = createProcessGroup();
            final VersionedProcessor processor = VersionedFlowUtils.addProcessor(group, PROCESSOR_TYPE, new Bundle("group", "artifact", "2.0.0"), "Test Processor", new Position(0, 0));

            final ComponentBundleLookup lookup = mock(ComponentBundleLookup.class);
            when(lookup.getLatestBundle(PROCESSOR_TYPE)).thenReturn(Optional.empty());

            final boolean updated = VersionedFlowUtils.updateToLatestBundle(processor, lookup);

            assertFalse(updated);
            assertEquals("2.0.0", processor.getBundle().getVersion());
        }

    }

    @Nested
    class GetReferencedControllerServices {
        private static final String PROCESSOR_TYPE = "org.apache.nifi.processors.TestProcessor";
        private static final String SERVICE_TYPE = "org.apache.nifi.services.TestService";
        private static final Bundle TEST_BUNDLE = new Bundle("group", "artifact", "1.0.0");

        @Test
        void testProcessorWithNoReferences() {
            final VersionedProcessGroup group = createProcessGroup();
            VersionedFlowUtils.addControllerService(group, SERVICE_TYPE, TEST_BUNDLE, "Unreferenced Service");
            final VersionedProcessor processor = VersionedFlowUtils.addProcessor(group, PROCESSOR_TYPE, TEST_BUNDLE, "Processor", new Position(0, 0));
            processor.getProperties().put("some-property", "not-a-service-id");

            final Set<VersionedControllerService> result = VersionedFlowUtils.getReferencedControllerServices(group, processor);

            assertTrue(result.isEmpty());
        }

        @Test
        void testProcessorWithDirectReferences() {
            final VersionedProcessGroup group = createProcessGroup();
            final VersionedControllerService service1 = VersionedFlowUtils.addControllerService(group, SERVICE_TYPE, TEST_BUNDLE, "Service 1");
            final VersionedControllerService service2 = VersionedFlowUtils.addControllerService(group, SERVICE_TYPE, TEST_BUNDLE, "Service 2");
            final VersionedControllerService unreferencedService = VersionedFlowUtils.addControllerService(group, SERVICE_TYPE, TEST_BUNDLE, "Unreferenced Service");

            final VersionedProcessor processor = VersionedFlowUtils.addProcessor(group, PROCESSOR_TYPE, TEST_BUNDLE, "Processor", new Position(0, 0));
            processor.getProperties().put("service-prop-1", service1.getIdentifier());
            processor.getProperties().put("service-prop-2", service2.getIdentifier());
            processor.getProperties().put("non-service-prop", "some-value");

            final Set<VersionedControllerService> result = VersionedFlowUtils.getReferencedControllerServices(group, processor);

            assertEquals(2, result.size());
            assertTrue(result.contains(service1));
            assertTrue(result.contains(service2));
            assertFalse(result.contains(unreferencedService));
        }

        @Test
        void testProcessorWithTransitiveReferences() {
            final VersionedProcessGroup group = createProcessGroup();
            final VersionedControllerService serviceC = VersionedFlowUtils.addControllerService(group, SERVICE_TYPE, TEST_BUNDLE, "Service C");
            final VersionedControllerService serviceB = VersionedFlowUtils.addControllerService(group, SERVICE_TYPE, TEST_BUNDLE, "Service B");
            serviceB.getProperties().put("nested-service", serviceC.getIdentifier());
            final VersionedControllerService serviceA = VersionedFlowUtils.addControllerService(group, SERVICE_TYPE, TEST_BUNDLE, "Service A");
            serviceA.getProperties().put("nested-service", serviceB.getIdentifier());

            final VersionedProcessor processor = VersionedFlowUtils.addProcessor(group, PROCESSOR_TYPE, TEST_BUNDLE, "Processor", new Position(0, 0));
            processor.getProperties().put("service-prop", serviceA.getIdentifier());

            final Set<VersionedControllerService> result = VersionedFlowUtils.getReferencedControllerServices(group, processor);

            assertEquals(3, result.size());
            assertTrue(result.contains(serviceA));
            assertTrue(result.contains(serviceB));
            assertTrue(result.contains(serviceC));
        }

        @Test
        void testProcessorReferencingServiceInAncestorGroup() {
            final VersionedProcessGroup rootGroup = createProcessGroup();
            final VersionedControllerService parentService = VersionedFlowUtils.addControllerService(rootGroup, SERVICE_TYPE, TEST_BUNDLE, "Parent Service");
            final VersionedProcessGroup childGroup = createChildProcessGroup(rootGroup, "child-group-id");
            final VersionedProcessor processor = VersionedFlowUtils.addProcessor(childGroup, PROCESSOR_TYPE, TEST_BUNDLE, "Processor", new Position(0, 0));
            processor.getProperties().put("service-prop", parentService.getIdentifier());

            final Set<VersionedControllerService> result = VersionedFlowUtils.getReferencedControllerServices(rootGroup, processor);

            assertEquals(1, result.size());
            assertTrue(result.contains(parentService));
        }

        @Test
        void testProcessorDoesNotFindServiceInDescendantGroup() {
            final VersionedProcessGroup rootGroup = createProcessGroup();
            final VersionedProcessGroup childGroup = createChildProcessGroup(rootGroup, "child-group-id");
            final VersionedControllerService childService = VersionedFlowUtils.addControllerService(childGroup, SERVICE_TYPE, TEST_BUNDLE, "Child Service");

            final VersionedProcessor processor = VersionedFlowUtils.addProcessor(rootGroup, PROCESSOR_TYPE, TEST_BUNDLE, "Processor", new Position(0, 0));
            processor.getProperties().put("service-prop", childService.getIdentifier());

            final Set<VersionedControllerService> result = VersionedFlowUtils.getReferencedControllerServices(rootGroup, processor);

            assertTrue(result.isEmpty());
        }

        @Test
        void testProcessorWithCircularServiceReferences() {
            final VersionedProcessGroup group = createProcessGroup();
            final VersionedControllerService serviceA = VersionedFlowUtils.addControllerService(group, SERVICE_TYPE, TEST_BUNDLE, "Service A");
            final VersionedControllerService serviceB = VersionedFlowUtils.addControllerService(group, SERVICE_TYPE, TEST_BUNDLE, "Service B");
            serviceA.getProperties().put("nested-service", serviceB.getIdentifier());
            serviceB.getProperties().put("nested-service", serviceA.getIdentifier());

            final VersionedProcessor processor = VersionedFlowUtils.addProcessor(group, PROCESSOR_TYPE, TEST_BUNDLE, "Processor", new Position(0, 0));
            processor.getProperties().put("service-prop", serviceA.getIdentifier());

            final Set<VersionedControllerService> result = VersionedFlowUtils.getReferencedControllerServices(group, processor);

            assertEquals(2, result.size());
            assertTrue(result.contains(serviceA));
            assertTrue(result.contains(serviceB));
        }

        @Test
        void testControllerServiceWithNoReferences() {
            final VersionedProcessGroup group = createProcessGroup();
            final VersionedControllerService service = VersionedFlowUtils.addControllerService(group, SERVICE_TYPE, TEST_BUNDLE, "Service");
            service.getProperties().put("some-property", "not-a-service-id");

            final Set<VersionedControllerService> result = VersionedFlowUtils.getReferencedControllerServices(group, service);

            assertTrue(result.isEmpty());
        }

        @Test
        void testControllerServiceWithDirectAndTransitiveReferences() {
            final VersionedProcessGroup group = createProcessGroup();
            final VersionedControllerService serviceC = VersionedFlowUtils.addControllerService(group, SERVICE_TYPE, TEST_BUNDLE, "Service C");
            final VersionedControllerService serviceB = VersionedFlowUtils.addControllerService(group, SERVICE_TYPE, TEST_BUNDLE, "Service B");
            serviceB.getProperties().put("nested-service", serviceC.getIdentifier());
            final VersionedControllerService serviceA = VersionedFlowUtils.addControllerService(group, SERVICE_TYPE, TEST_BUNDLE, "Service A");
            serviceA.getProperties().put("nested-service", serviceB.getIdentifier());

            final Set<VersionedControllerService> result = VersionedFlowUtils.getReferencedControllerServices(group, serviceA);

            assertEquals(2, result.size());
            assertTrue(result.contains(serviceB));
            assertTrue(result.contains(serviceC));
            assertFalse(result.contains(serviceA));
        }
    }

    private static VersionedProcessGroup createProcessGroup() {
        return VersionedFlowUtils.createProcessGroup("test-group-id", "Test Process Group");
    }

    private static VersionedProcessGroup createChildProcessGroup(final VersionedProcessGroup parent, final String identifier) {
        final VersionedProcessGroup childGroup = VersionedFlowUtils.createProcessGroup(identifier, "Child Process Group");
        childGroup.setGroupIdentifier(parent.getIdentifier());
        parent.getProcessGroups().add(childGroup);
        return childGroup;
    }

}
