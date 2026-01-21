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
import org.apache.nifi.flow.ComponentType;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestVersionedFlowUtils {

    @Nested
    class GetLatest {
        @Test
        void testMinorVersion() {
            final List<Bundle> bundles = List.of(
                new Bundle("group", "artifact", "1.0.0"),
                new Bundle("group", "artifact", "1.2.0")
            );

            final Bundle latestBundle = VersionedFlowUtils.getLatest(bundles);
            assertNotNull(latestBundle);
            assertEquals("1.2.0", latestBundle.getVersion());
        }

        @Test
        void testMajorVersion() {
            final List<Bundle> bundles = List.of(
                new Bundle("group", "artifact", "1.0.0"),
                new Bundle("group", "artifact", "2.0.0")
            );

            final Bundle latestBundle = VersionedFlowUtils.getLatest(bundles);
            assertNotNull(latestBundle);
            assertEquals("2.0.0", latestBundle.getVersion());
        }

        @Test
        void testMoreDigitsLater() {
            final List<Bundle> bundles = List.of(
                new Bundle("group", "artifact", "1.0"),
                new Bundle("group", "artifact", "1.0.1")
            );

            final Bundle latestBundle = VersionedFlowUtils.getLatest(bundles);
            assertNotNull(latestBundle);
            assertEquals("1.0.1", latestBundle.getVersion());
        }

        @Test
        void testMoreDigitsFirst() {
            final List<Bundle> bundles = List.of(
                new Bundle("group", "artifact", "1.0.1"),
                new Bundle("group", "artifact", "1.0")
            );

            final Bundle latestBundle = VersionedFlowUtils.getLatest(bundles);
            assertNotNull(latestBundle);
            assertEquals("1.0.1", latestBundle.getVersion());
        }

        @Test
        void testWithSnapshotAndSameVersion() {
            final List<Bundle> bundles = List.of(
                new Bundle("group", "artifact", "1.0.0-SNAPSHOT"),
                new Bundle("group", "artifact", "1.0.0")
            );

            final Bundle latestBundle = VersionedFlowUtils.getLatest(bundles);
            assertNotNull(latestBundle);
            assertEquals("1.0.0", latestBundle.getVersion());
        }

        @Test
        void testWithSnapshotAndDifferentVersion() {
            final List<Bundle> bundles = List.of(
                new Bundle("group", "artifact", "1.0.1-SNAPSHOT"),
                new Bundle("group", "artifact", "1.0.0")
            );

            final Bundle latestBundle = VersionedFlowUtils.getLatest(bundles);
            assertNotNull(latestBundle);
            assertEquals("1.0.1-SNAPSHOT", latestBundle.getVersion());
        }

        @Test
        void testEmptyList() {
            final List<Bundle> bundles = List.of();
            assertNull(VersionedFlowUtils.getLatest(bundles));
        }

        @Test
        void testNonNumericVersionPart() {
            final List<Bundle> bundles = List.of(
                new Bundle("group", "artifact", "4.0.0"),
                new Bundle("group", "artifact", "4.0.next")
            );

            final Bundle latestBundle = VersionedFlowUtils.getLatest(bundles);
            assertNotNull(latestBundle);
            assertEquals("4.0.0", latestBundle.getVersion());
        }

        @Test
        void testFullyNonNumericVersionVsNumeric() {
            final List<Bundle> bundles = List.of(
                new Bundle("group", "artifact", "1.0.0"),
                new Bundle("group", "artifact", "undefined")
            );

            final Bundle latestBundle = VersionedFlowUtils.getLatest(bundles);
            assertNotNull(latestBundle);
            assertEquals("1.0.0", latestBundle.getVersion());
        }

        @Test
        void testTwoFullyNonNumericVersions() {
            final List<Bundle> bundles = List.of(
                new Bundle("group", "artifact", "undefined"),
                new Bundle("group", "artifact", "unknown")
            );

            final Bundle latestBundle = VersionedFlowUtils.getLatest(bundles);
            assertNotNull(latestBundle);
            assertEquals("unknown", latestBundle.getVersion());
        }

        @Test
        void testQualifierOrdering() {
            final List<Bundle> bundles = List.of(
                new Bundle("group", "artifact", "2.0.0-SNAPSHOT"),
                new Bundle("group", "artifact", "2.0.0-M1"),
                new Bundle("group", "artifact", "2.0.0-M4"),
                new Bundle("group", "artifact", "2.0.0-RC1"),
                new Bundle("group", "artifact", "2.0.0-RC2"),
                new Bundle("group", "artifact", "2.0.0")
            );

            final Bundle latestBundle = VersionedFlowUtils.getLatest(bundles);
            assertNotNull(latestBundle);
            assertEquals("2.0.0", latestBundle.getVersion());
        }

        @Test
        void testQualifierOrderingWithoutRelease() {
            final List<Bundle> bundles = List.of(
                new Bundle("group", "artifact", "2.0.0-SNAPSHOT"),
                new Bundle("group", "artifact", "2.0.0-M1"),
                new Bundle("group", "artifact", "2.0.0-M4"),
                new Bundle("group", "artifact", "2.0.0-RC1"),
                new Bundle("group", "artifact", "2.0.0-RC2")
            );

            final Bundle latestBundle = VersionedFlowUtils.getLatest(bundles);
            assertNotNull(latestBundle);
            assertEquals("2.0.0-RC2", latestBundle.getVersion());
        }

        @Test
        void testMilestoneVersionsOrdering() {
            final List<Bundle> bundles = List.of(
                new Bundle("group", "artifact", "2.0.0-M1"),
                new Bundle("group", "artifact", "2.0.0-M4"),
                new Bundle("group", "artifact", "2.0.0-SNAPSHOT")
            );

            final Bundle latestBundle = VersionedFlowUtils.getLatest(bundles);
            assertNotNull(latestBundle);
            assertEquals("2.0.0-M4", latestBundle.getVersion());
        }

        @Test
        void testCalendarDateFormat() {
            final List<Bundle> bundles = List.of(
                new Bundle("group", "artifact", "2025.12.31"),
                new Bundle("group", "artifact", "2026.01.01")
            );

            final Bundle latestBundle = VersionedFlowUtils.getLatest(bundles);
            assertNotNull(latestBundle);
            assertEquals("2026.01.01", latestBundle.getVersion());
        }

        @Test
        void testCalendarDateFormatWithBuildNumber() {
            final List<Bundle> bundles = List.of(
                new Bundle("group", "artifact", "2025.12.31.999"),
                new Bundle("group", "artifact", "2026.01.01.451")
            );

            final Bundle latestBundle = VersionedFlowUtils.getLatest(bundles);
            assertNotNull(latestBundle);
            assertEquals("2026.01.01.451", latestBundle.getVersion());
        }
    }

    @Nested
    class UpdateToLatestBundles {
        private static final String PROCESSOR_TYPE = "org.apache.nifi.processors.TestProcessor";
        private static final String SERVICE_TYPE = "org.apache.nifi.services.TestService";

        @Test
        void testLookupReturnsOneOlderBundle() {
            final VersionedProcessGroup group = createProcessGroup();
            final VersionedProcessor processor = VersionedFlowUtils.addProcessor(group, PROCESSOR_TYPE, new Bundle("group", "artifact", "2.0.0"), "Test Processor", new Position(0, 0));

            final ComponentBundleLookup lookup = mock(ComponentBundleLookup.class);
            when(lookup.getAvailableBundles(PROCESSOR_TYPE)).thenReturn(List.of(new Bundle("group", "artifact", "1.0.0")));

            VersionedFlowUtils.updateToLatestBundles(group, lookup);

            assertEquals("2.0.0", processor.getBundle().getVersion());
        }

        @Test
        void testLookupReturnsOneNewerBundle() {
            final VersionedProcessGroup group = createProcessGroup();
            final VersionedProcessor processor = VersionedFlowUtils.addProcessor(group, PROCESSOR_TYPE, new Bundle("group", "artifact", "2.0.0"), "Test Processor", new Position(0, 0));

            final ComponentBundleLookup lookup = mock(ComponentBundleLookup.class);
            when(lookup.getAvailableBundles(PROCESSOR_TYPE)).thenReturn(List.of(new Bundle("group", "artifact", "3.0.0")));

            VersionedFlowUtils.updateToLatestBundles(group, lookup);

            assertEquals("3.0.0", processor.getBundle().getVersion());
        }

        @Test
        void testLookupReturnsMultipleBundlesIncludingSameOlderAndNewer() {
            final VersionedProcessGroup group = createProcessGroup();
            final VersionedProcessor processor = VersionedFlowUtils.addProcessor(group, PROCESSOR_TYPE, new Bundle("group", "artifact", "2.0.0"), "Test Processor", new Position(0, 0));

            final ComponentBundleLookup lookup = mock(ComponentBundleLookup.class);
            when(lookup.getAvailableBundles(PROCESSOR_TYPE)).thenReturn(List.of(
                new Bundle("group", "artifact", "1.0.0"),
                new Bundle("group", "artifact", "2.0.0"),
                new Bundle("group", "artifact", "3.0.0"),
                new Bundle("group", "artifact", "4.0.0")
            ));

            VersionedFlowUtils.updateToLatestBundles(group, lookup);

            assertEquals("4.0.0", processor.getBundle().getVersion());
        }

        @Test
        void testLookupReturnsNoBundles() {
            final VersionedProcessGroup group = createProcessGroup();
            final VersionedProcessor processor = VersionedFlowUtils.addProcessor(group, PROCESSOR_TYPE, new Bundle("group", "artifact", "2.0.0"), "Test Processor", new Position(0, 0));

            final ComponentBundleLookup lookup = mock(ComponentBundleLookup.class);
            when(lookup.getAvailableBundles(PROCESSOR_TYPE)).thenReturn(List.of());

            VersionedFlowUtils.updateToLatestBundles(group, lookup);

            assertEquals("2.0.0", processor.getBundle().getVersion());
        }

        @Test
        void testControllerServiceUpdatedToNewerBundle() {
            final VersionedProcessGroup group = createProcessGroup();
            final VersionedControllerService service = VersionedFlowUtils.addControllerService(group, SERVICE_TYPE, new Bundle("group", "artifact", "1.5.0"), "Test Service");

            final ComponentBundleLookup lookup = mock(ComponentBundleLookup.class);
            when(lookup.getAvailableBundles(SERVICE_TYPE)).thenReturn(List.of(new Bundle("group", "artifact", "2.5.0")));

            VersionedFlowUtils.updateToLatestBundles(group, lookup);

            assertEquals("2.5.0", service.getBundle().getVersion());
        }

        @Test
        void testNestedProcessorUpdatedToNewerBundle() {
            final VersionedProcessGroup rootGroup = createProcessGroup();
            final VersionedProcessGroup childGroup = createChildProcessGroup(rootGroup, "child-group-id");
            final VersionedProcessor nestedProcessor = VersionedFlowUtils.addProcessor(childGroup, PROCESSOR_TYPE, new Bundle("group", "artifact", "1.0.0"), "Nested Processor", new Position(0, 0));

            final ComponentBundleLookup lookup = mock(ComponentBundleLookup.class);
            when(lookup.getAvailableBundles(PROCESSOR_TYPE)).thenReturn(List.of(new Bundle("group", "artifact", "5.0.0")));

            VersionedFlowUtils.updateToLatestBundles(rootGroup, lookup);

            assertEquals("5.0.0", nestedProcessor.getBundle().getVersion());
        }

        @Test
        void testLookupReturnsVersionsWithQualifiers() {
            final VersionedProcessGroup group = createProcessGroup();
            final VersionedProcessor processor = VersionedFlowUtils.addProcessor(group, PROCESSOR_TYPE, new Bundle("group", "artifact", "2.0.0"), "Test Processor", new Position(0, 0));

            final ComponentBundleLookup lookup = mock(ComponentBundleLookup.class);
            when(lookup.getAvailableBundles(PROCESSOR_TYPE)).thenReturn(List.of(
                new Bundle("group", "artifact", "2.0.0"),
                new Bundle("group", "artifact", "2.0.0-SNAPSHOT"),
                new Bundle("group", "artifact", "2.0.0-M1"),
                new Bundle("group", "artifact", "2.0.0-RC1")
            ));

            VersionedFlowUtils.updateToLatestBundles(group, lookup);

            assertEquals("2.0.0", processor.getBundle().getVersion());
        }

        private VersionedProcessGroup createProcessGroup() {
            final VersionedProcessGroup group = new VersionedProcessGroup();
            group.setIdentifier("test-group-id");
            group.setName("Test Process Group");
            group.setProcessors(new HashSet<>());
            group.setProcessGroups(new HashSet<>());
            group.setControllerServices(new HashSet<>());
            group.setConnections(new HashSet<>());
            group.setInputPorts(new HashSet<>());
            group.setOutputPorts(new HashSet<>());
            group.setFunnels(new HashSet<>());
            group.setLabels(new HashSet<>());
            group.setComponentType(ComponentType.PROCESS_GROUP);
            return group;
        }

        private VersionedProcessGroup createChildProcessGroup(final VersionedProcessGroup parent, final String identifier) {
            final VersionedProcessGroup childGroup = new VersionedProcessGroup();
            childGroup.setIdentifier(identifier);
            childGroup.setName("Child Process Group");
            childGroup.setGroupIdentifier(parent.getIdentifier());
            childGroup.setProcessors(new HashSet<>());
            childGroup.setProcessGroups(new HashSet<>());
            childGroup.setControllerServices(new HashSet<>());
            childGroup.setConnections(new HashSet<>());
            childGroup.setInputPorts(new HashSet<>());
            childGroup.setOutputPorts(new HashSet<>());
            childGroup.setFunnels(new HashSet<>());
            childGroup.setLabels(new HashSet<>());
            childGroup.setComponentType(ComponentType.PROCESS_GROUP);
            parent.getProcessGroups().add(childGroup);
            return childGroup;
        }
    }

}
