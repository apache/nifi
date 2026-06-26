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
import org.apache.nifi.registry.flow.AbstractFlowRegistryClient;
import org.apache.nifi.registry.flow.FlowRegistryClientNode;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
        final FlowManager flowManager = Mockito.mock(FlowManager.class);
        final RegistryFlowSynchronizationTask task = new RegistryFlowSynchronizationTask(flowManager, DEFAULT_INTERVAL_SECONDS);

        final FlowRegistryClientNode configuredClient = Mockito.mock(FlowRegistryClientNode.class);
        Mockito.when(configuredClient.getEffectivePropertyValue(AbstractFlowRegistryClient.SYNCHRONIZATION_INTERVAL)).thenReturn("10 min");
        Mockito.when(flowManager.getFlowRegistryClient("configured")).thenReturn(configuredClient);
        assertEquals(600L, task.getEffectiveIntervalSeconds("configured"));

        final FlowRegistryClientNode unconfiguredClient = Mockito.mock(FlowRegistryClientNode.class);
        Mockito.when(unconfiguredClient.getEffectivePropertyValue(AbstractFlowRegistryClient.SYNCHRONIZATION_INTERVAL)).thenReturn(null);
        Mockito.when(flowManager.getFlowRegistryClient("unconfigured")).thenReturn(unconfiguredClient);
        assertEquals(DEFAULT_INTERVAL_SECONDS, task.getEffectiveIntervalSeconds("unconfigured"));

        Mockito.when(flowManager.getFlowRegistryClient("missing")).thenReturn(null);
        assertEquals(DEFAULT_INTERVAL_SECONDS, task.getEffectiveIntervalSeconds("missing"));
    }
}
