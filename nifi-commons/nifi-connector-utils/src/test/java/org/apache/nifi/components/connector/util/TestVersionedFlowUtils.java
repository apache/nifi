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
import java.util.Optional;

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
