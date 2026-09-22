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
package org.apache.nifi.controller;

import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.registry.flow.AbstractFlowRegistryClient;
import org.apache.nifi.registry.flow.FlowRegistryClientNode;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RegistryFlowSynchronizationTaskTest {

    private static final long DEFAULT_INTERVAL_SECONDS = 1800L;

    @Test
    void testParseIntervalSecondsFallsBackToDefaultWhenNotConfigured() {
        assertEquals(DEFAULT_INTERVAL_SECONDS, RegistryFlowSynchronizationTask.parseIntervalSeconds(null, DEFAULT_INTERVAL_SECONDS));
        assertEquals(DEFAULT_INTERVAL_SECONDS, RegistryFlowSynchronizationTask.parseIntervalSeconds("", DEFAULT_INTERVAL_SECONDS));
        assertEquals(DEFAULT_INTERVAL_SECONDS, RegistryFlowSynchronizationTask.parseIntervalSeconds("   ", DEFAULT_INTERVAL_SECONDS));
    }

    @Test
    void testParseIntervalSecondsParsesConfiguredDuration() {
        assertEquals(300L, RegistryFlowSynchronizationTask.parseIntervalSeconds("5 min", DEFAULT_INTERVAL_SECONDS));
        assertEquals(45L, RegistryFlowSynchronizationTask.parseIntervalSeconds("45 secs", DEFAULT_INTERVAL_SECONDS));
        assertEquals(45L, RegistryFlowSynchronizationTask.parseIntervalSeconds("  45 secs  ", DEFAULT_INTERVAL_SECONDS));
    }

    @Test
    void testParseIntervalSecondsFallsBackToDefaultWhenInvalid() {
        assertEquals(DEFAULT_INTERVAL_SECONDS, RegistryFlowSynchronizationTask.parseIntervalSeconds("not-a-duration", DEFAULT_INTERVAL_SECONDS));
    }

    @Test
    void testGetEffectiveIntervalSeconds() {
        final FlowManager flowManager = mock(FlowManager.class);
        final RegistryFlowSynchronizationTask task = new RegistryFlowSynchronizationTask(flowManager, DEFAULT_INTERVAL_SECONDS);

        final FlowRegistryClientNode configuredClient = mock(FlowRegistryClientNode.class);
        when(configuredClient.getEffectivePropertyValue(AbstractFlowRegistryClient.SYNCHRONIZATION_INTERVAL)).thenReturn("10 min");
        when(flowManager.getFlowRegistryClient("configured")).thenReturn(configuredClient);
        assertEquals(600L, task.getEffectiveIntervalSeconds("configured"));

        final FlowRegistryClientNode unconfiguredClient = mock(FlowRegistryClientNode.class);
        when(unconfiguredClient.getEffectivePropertyValue(AbstractFlowRegistryClient.SYNCHRONIZATION_INTERVAL)).thenReturn(null);
        when(flowManager.getFlowRegistryClient("unconfigured")).thenReturn(unconfiguredClient);
        assertEquals(DEFAULT_INTERVAL_SECONDS, task.getEffectiveIntervalSeconds("unconfigured"));

        when(flowManager.getFlowRegistryClient("missing")).thenReturn(null);
        assertEquals(DEFAULT_INTERVAL_SECONDS, task.getEffectiveIntervalSeconds("missing"));
    }

    @Test
    void testPostInitializationSynchronizationDoesNotDelayFirstPeriodicSynchronization() {
        final FlowManager flowManager = mock(FlowManager.class);
        final ProcessGroup rootGroup = mock(ProcessGroup.class);
        final ProcessGroup childGroup = mock(ProcessGroup.class);
        final VersionControlInformation rootVersionControlInformation = versionControlInformation("root-registry");
        final VersionControlInformation childVersionControlInformation = versionControlInformation("child-registry");

        when(flowManager.getRootGroup()).thenReturn(rootGroup);
        when(rootGroup.findAllProcessGroups()).thenAnswer(invocation -> new ArrayList<>(List.of(childGroup)));
        when(rootGroup.getVersionControlInformation()).thenReturn(rootVersionControlInformation);
        when(childGroup.getVersionControlInformation()).thenReturn(childVersionControlInformation);

        final RegistryFlowSynchronizationTask task = new RegistryFlowSynchronizationTask(flowManager, DEFAULT_INTERVAL_SECONDS);

        task.synchronizeAllProcessGroups();
        task.run();

        verify(rootGroup, times(2)).synchronizeWithFlowRegistry(flowManager);
        verify(childGroup, times(2)).synchronizeWithFlowRegistry(flowManager);
    }

    @Test
    void testGroupsSharingRegistryClientAreProcessedInSingleClientBatch() {
        final FlowManager flowManager = mock(FlowManager.class);
        final ProcessGroup rootGroup = mock(ProcessGroup.class);
        final ProcessGroup firstChild = mock(ProcessGroup.class);
        final ProcessGroup secondChild = mock(ProcessGroup.class);
        final FlowRegistryClientNode clientNode = mock(FlowRegistryClientNode.class);
        final VersionControlInformation firstVersionControlInformation = versionControlInformation("shared-registry");
        final VersionControlInformation secondVersionControlInformation = versionControlInformation("shared-registry");

        when(flowManager.getRootGroup()).thenReturn(rootGroup);
        when(rootGroup.findAllProcessGroups()).thenReturn(new ArrayList<>(List.of(firstChild, secondChild)));
        when(rootGroup.getVersionControlInformation()).thenReturn(null);
        when(firstChild.getVersionControlInformation()).thenReturn(firstVersionControlInformation);
        when(secondChild.getVersionControlInformation()).thenReturn(secondVersionControlInformation);
        when(flowManager.getFlowRegistryClient("shared-registry")).thenReturn(clientNode);
        when(clientNode.getEffectivePropertyValue(AbstractFlowRegistryClient.SYNCHRONIZATION_INTERVAL)).thenReturn("10 min");

        final RegistryFlowSynchronizationTask task = new RegistryFlowSynchronizationTask(flowManager, DEFAULT_INTERVAL_SECONDS);

        task.run();

        verify(flowManager, times(1)).getFlowRegistryClient("shared-registry");
        verify(firstChild).synchronizeWithFlowRegistry(flowManager);
        verify(secondChild).synchronizeWithFlowRegistry(flowManager);
    }

    @Test
    void testSecondRunBeforeIntervalDoesNotRepeatSynchronization() {
        final FlowManager flowManager = mock(FlowManager.class);
        final ProcessGroup rootGroup = mock(ProcessGroup.class);
        final ProcessGroup childGroup = mock(ProcessGroup.class);
        final FlowRegistryClientNode clientNode = mock(FlowRegistryClientNode.class);
        final VersionControlInformation childVersionControlInformation = versionControlInformation("shared-registry");

        when(flowManager.getRootGroup()).thenReturn(rootGroup);
        when(rootGroup.findAllProcessGroups()).thenReturn(new ArrayList<>(List.of(childGroup)));
        when(rootGroup.getVersionControlInformation()).thenReturn(null);
        when(childGroup.getVersionControlInformation()).thenReturn(childVersionControlInformation);
        when(flowManager.getFlowRegistryClient("shared-registry")).thenReturn(clientNode);
        when(clientNode.getEffectivePropertyValue(AbstractFlowRegistryClient.SYNCHRONIZATION_INTERVAL)).thenReturn("10 min");

        final RegistryFlowSynchronizationTask task = new RegistryFlowSynchronizationTask(flowManager, DEFAULT_INTERVAL_SECONDS);

        task.run();
        task.run();

        verify(childGroup, times(1)).synchronizeWithFlowRegistry(flowManager);
    }

    @Test
    void testFailureInOneGroupDoesNotPreventSiblingSynchronization() {
        final FlowManager flowManager = mock(FlowManager.class);
        final ProcessGroup rootGroup = mock(ProcessGroup.class);
        final ProcessGroup failingGroup = mock(ProcessGroup.class);
        final ProcessGroup siblingGroup = mock(ProcessGroup.class);
        final FlowRegistryClientNode clientNode = mock(FlowRegistryClientNode.class);
        final VersionControlInformation failingVersionControlInformation = versionControlInformation("shared-registry");
        final VersionControlInformation siblingVersionControlInformation = versionControlInformation("shared-registry");

        when(flowManager.getRootGroup()).thenReturn(rootGroup);
        when(rootGroup.findAllProcessGroups()).thenReturn(new ArrayList<>(List.of(failingGroup, siblingGroup)));
        when(rootGroup.getVersionControlInformation()).thenReturn(null);
        when(failingGroup.getVersionControlInformation()).thenReturn(failingVersionControlInformation);
        when(siblingGroup.getVersionControlInformation()).thenReturn(siblingVersionControlInformation);
        when(flowManager.getFlowRegistryClient("shared-registry")).thenReturn(clientNode);
        when(clientNode.getEffectivePropertyValue(AbstractFlowRegistryClient.SYNCHRONIZATION_INTERVAL)).thenReturn("10 min");
        doThrow(new RuntimeException("boom")).when(failingGroup).synchronizeWithFlowRegistry(flowManager);

        final RegistryFlowSynchronizationTask task = new RegistryFlowSynchronizationTask(flowManager, DEFAULT_INTERVAL_SECONDS);

        task.run();

        verify(failingGroup).synchronizeWithFlowRegistry(flowManager);
        verify(siblingGroup).synchronizeWithFlowRegistry(flowManager);
    }

    @Test
    void testRemovedClientIsForgottenAndSynchronizesImmediatelyWhenReintroduced() {
        final FlowManager flowManager = mock(FlowManager.class);
        final ProcessGroup rootGroup = mock(ProcessGroup.class);
        final ProcessGroup childGroup = mock(ProcessGroup.class);
        final FlowRegistryClientNode clientNode = mock(FlowRegistryClientNode.class);
        final VersionControlInformation childVersionControlInformation = versionControlInformation("shared-registry");

        when(flowManager.getRootGroup()).thenReturn(rootGroup);
        when(rootGroup.getVersionControlInformation()).thenReturn(null);
        when(childGroup.getVersionControlInformation()).thenReturn(childVersionControlInformation);
        when(flowManager.getFlowRegistryClient("shared-registry")).thenReturn(clientNode);
        when(clientNode.getEffectivePropertyValue(AbstractFlowRegistryClient.SYNCHRONIZATION_INTERVAL)).thenReturn("10 min");
        when(rootGroup.findAllProcessGroups())
                .thenReturn(new ArrayList<>(List.of(childGroup)))
                .thenReturn(new ArrayList<>())
                .thenReturn(new ArrayList<>(List.of(childGroup)));

        final RegistryFlowSynchronizationTask task = new RegistryFlowSynchronizationTask(flowManager, DEFAULT_INTERVAL_SECONDS);

        task.run();
        task.run();
        task.run();

        verify(childGroup, times(2)).synchronizeWithFlowRegistry(flowManager);
    }

    private VersionControlInformation versionControlInformation(final String registryIdentifier) {
        final VersionControlInformation versionControlInformation = mock(VersionControlInformation.class);
        when(versionControlInformation.getRegistryIdentifier()).thenReturn(registryIdentifier);
        return versionControlInformation;
    }
}
