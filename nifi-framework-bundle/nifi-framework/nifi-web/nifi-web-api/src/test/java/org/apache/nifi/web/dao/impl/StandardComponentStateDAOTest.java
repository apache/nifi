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
package org.apache.nifi.web.dao.impl;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.web.api.dto.ComponentStateDTO;
import org.apache.nifi.web.api.dto.StateEntryDTO;
import org.apache.nifi.web.api.dto.StateMapDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardComponentStateDAOTest {

    private static final String COMPONENT_ID = "component-id";

    @Mock
    private StateManagerProvider stateManagerProvider;

    @Mock
    private StateManager stateManager;

    @Mock
    private ProcessorNode processorNode;

    private final AtomicReference<Map<String, String>> clusterState = new AtomicReference<>();

    private StandardComponentStateDAO dao;

    @BeforeEach
    void setUp() throws IOException {
        dao = new StandardComponentStateDAO();
        dao.setStateManagerProvider(stateManagerProvider);

        when(processorNode.getIdentifier()).thenReturn(COMPONENT_ID);
        when(stateManagerProvider.getStateManager(COMPONENT_ID)).thenReturn(stateManager);
        when(stateManager.getState(Scope.LOCAL)).thenReturn(new TestStateMap(Map.of()));
    }

    @Test
    void testClearStateRemovesSingleClusterStateKey() throws IOException {
        setUpSharedClusterState(Map.of("a", "1", "b", "1", "c", "1"));
        setUpClusterStateUpdates();

        dao.clearState(processorNode, createComponentState(Map.of("b", "1", "c", "1")));

        assertEquals(Map.of("b", "1", "c", "1"), clusterState.get());
        verify(stateManager).setState(eq(Map.of("b", "1", "c", "1")), eq(Scope.CLUSTER));
        verify(stateManager).clear(Scope.LOCAL);
    }

    @Test
    void testClearStateReplicatedRequestAlreadyApplied() throws IOException {
        setUpSharedClusterState(Map.of("a", "1", "b", "1", "c", "1"));
        when(stateManagerProvider.isClusterProviderEnabled()).thenReturn(true);
        setUpClusterStateUpdates();
        final ComponentStateDTO componentState = createComponentState(Map.of("b", "1", "c", "1"));

        // The same replicated request is processed by each node against the shared cluster state
        dao.clearState(processorNode, componentState);
        dao.clearState(processorNode, componentState);

        assertEquals(Map.of("b", "1", "c", "1"), clusterState.get());
        verify(stateManager, times(1)).setState(any(), eq(Scope.CLUSTER));
        verify(stateManager, times(2)).clear(Scope.LOCAL);
    }

    @Test
    void testClearStateMultipleClusterStateKeysRemoved() throws IOException {
        setUpSharedClusterState(Map.of("a", "1", "b", "1", "c", "1"));
        when(stateManagerProvider.isClusterProviderEnabled()).thenReturn(true);

        assertThrows(IllegalStateException.class, () -> dao.clearState(processorNode, createComponentState(Map.of("c", "1"))));

        verify(stateManager, never()).setState(any(), eq(Scope.CLUSTER));
    }

    @Test
    void testClearStateClusterStateValueChanged() throws IOException {
        setUpSharedClusterState(Map.of("a", "1", "b", "1", "c", "1"));
        when(stateManagerProvider.isClusterProviderEnabled()).thenReturn(true);

        assertThrows(IllegalStateException.class, () -> dao.clearState(processorNode, createComponentState(Map.of("b", "2", "c", "1"))));

        verify(stateManager, never()).setState(any(), eq(Scope.CLUSTER));
    }

    @Test
    void testClearStateStandaloneUnchangedState() throws IOException {
        setUpSharedClusterState(Map.of("a", "1", "b", "1", "c", "1"));
        when(stateManagerProvider.isClusterProviderEnabled()).thenReturn(false);

        assertThrows(IllegalStateException.class, () -> dao.clearState(processorNode, createComponentState(Map.of("a", "1", "b", "1", "c", "1"))));

        verify(stateManager, never()).setState(any(), eq(Scope.CLUSTER));
    }

    private void setUpSharedClusterState(final Map<String, String> initialState) throws IOException {
        clusterState.set(initialState);
        when(stateManager.isStateKeyDropSupported()).thenReturn(true);
        when(stateManager.getState(Scope.CLUSTER)).thenAnswer(invocation -> new TestStateMap(clusterState.get()));
    }

    private void setUpClusterStateUpdates() throws IOException {
        doAnswer(invocation -> {
            clusterState.set(Map.copyOf(invocation.getArgument(0)));
            return null;
        }).when(stateManager).setState(any(), eq(Scope.CLUSTER));
    }

    private static ComponentStateDTO createComponentState(final Map<String, String> state) {
        final List<StateEntryDTO> entries = new ArrayList<>();
        state.forEach((key, value) -> {
            final StateEntryDTO entry = new StateEntryDTO();
            entry.setKey(key);
            entry.setValue(value);
            entries.add(entry);
        });

        final StateMapDTO stateMap = new StateMapDTO();
        stateMap.setState(entries);

        final ComponentStateDTO componentState = new ComponentStateDTO();
        componentState.setClusterState(stateMap);
        return componentState;
    }

    private record TestStateMap(Map<String, String> state) implements StateMap {

        @Override
        public Optional<String> getStateVersion() {
            return Optional.of("1");
        }

        @Override
        public String get(final String key) {
            return state.get(key);
        }

        @Override
        public Map<String, String> toMap() {
            return state;
        }
    }
}
