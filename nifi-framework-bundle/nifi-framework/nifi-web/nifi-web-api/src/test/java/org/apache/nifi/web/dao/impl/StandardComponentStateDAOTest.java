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
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardComponentStateDAOTest {

    private static final String COMPONENT_ID = "component-id";

    private static final Map<String, String> INITIAL_STATE = Map.of("a", "1", "b", "1", "c", "1");

    private static final Map<String, String> KEY_REMOVED_STATE = Map.of("b", "1", "c", "1");

    @Mock
    private StateManagerProvider stateManagerProvider;

    @Mock
    private StateManager stateManager;

    @Mock
    private ProcessorNode processorNode;

    private final AtomicReference<TestStateMap> clusterState = new AtomicReference<>();

    private final AtomicReference<TestStateMap> localState = new AtomicReference<>(new TestStateMap(Map.of(), 0));

    private StandardComponentStateDAO dao;

    @BeforeEach
    void setUp() throws IOException {
        dao = new StandardComponentStateDAO();
        dao.setStateManagerProvider(stateManagerProvider);

        when(processorNode.getIdentifier()).thenReturn(COMPONENT_ID);
        when(stateManagerProvider.getStateManager(COMPONENT_ID)).thenReturn(stateManager);
        when(stateManager.getState(Scope.LOCAL)).thenAnswer(invocation -> localState.get());
    }

    @Test
    void testClearStateRemovesSingleClusterStateKey() throws IOException {
        setUpSharedClusterState();
        setUpReplace(Scope.CLUSTER, clusterState, UnaryOperator.identity());

        dao.clearState(processorNode, createClusterComponentState(KEY_REMOVED_STATE));

        assertEquals(KEY_REMOVED_STATE, clusterState.get().toMap());
        verify(stateManager).replace(any(), eq(KEY_REMOVED_STATE), eq(Scope.CLUSTER));
        verify(stateManager, never()).setState(any(), any());
        verify(stateManager).clear(Scope.LOCAL);
    }

    @Test
    void testClearStateReplicatedRequestAlreadyApplied() throws IOException {
        setUpSharedClusterState();
        when(stateManagerProvider.isClusterProviderEnabled()).thenReturn(true);
        setUpReplace(Scope.CLUSTER, clusterState, UnaryOperator.identity());
        final ComponentStateDTO componentState = createClusterComponentState(KEY_REMOVED_STATE);

        // The same replicated request is processed by each node against the shared cluster state
        dao.clearState(processorNode, componentState);
        dao.clearState(processorNode, componentState);

        assertEquals(KEY_REMOVED_STATE, clusterState.get().toMap());
        verify(stateManager, times(1)).replace(any(), any(), eq(Scope.CLUSTER));
        verify(stateManager, times(2)).clear(Scope.LOCAL);
    }

    @Test
    void testClearStateReplicatedRequestAppliedAfterRead() throws IOException {
        setUpSharedClusterState();
        when(stateManagerProvider.isClusterProviderEnabled()).thenReturn(true);
        // Another node applies the same replicated request between the read and the replace on this node
        setUpReplace(Scope.CLUSTER, clusterState, current -> current.update(KEY_REMOVED_STATE));

        dao.clearState(processorNode, createClusterComponentState(KEY_REMOVED_STATE));

        assertEquals(KEY_REMOVED_STATE, clusterState.get().toMap());
        verify(stateManager).clear(Scope.LOCAL);
    }

    @Test
    void testClearStateClusterStateModifiedConcurrently() throws IOException {
        setUpSharedClusterState();
        when(stateManagerProvider.isClusterProviderEnabled()).thenReturn(true);
        final Map<String, String> processorUpdatedState = Map.of("a", "1", "b", "2", "c", "1");
        // The processor updates the state between the read and the replace
        setUpReplace(Scope.CLUSTER, clusterState, current -> current.update(processorUpdatedState));

        final IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> dao.clearState(processorNode, createClusterComponentState(KEY_REMOVED_STATE)));

        assertTrue(exception.getMessage().contains("modified concurrently"));
        assertEquals(processorUpdatedState, clusterState.get().toMap());
        verify(stateManager, never()).setState(any(), any());
        verify(stateManager, never()).clear(Scope.LOCAL);
    }

    @Test
    void testClearStateMultipleClusterStateKeysRemoved() throws IOException {
        setUpSharedClusterState();
        when(stateManagerProvider.isClusterProviderEnabled()).thenReturn(true);

        assertThrows(IllegalStateException.class, () -> dao.clearState(processorNode, createClusterComponentState(Map.of("c", "1"))));

        verify(stateManager, never()).replace(any(), any(), any());
        verify(stateManager, never()).setState(any(), any());
    }

    @Test
    void testClearStateClusterStateValueChanged() throws IOException {
        setUpSharedClusterState();
        when(stateManagerProvider.isClusterProviderEnabled()).thenReturn(true);

        assertThrows(IllegalStateException.class, () -> dao.clearState(processorNode, createClusterComponentState(Map.of("b", "2", "c", "1"))));

        verify(stateManager, never()).replace(any(), any(), any());
        verify(stateManager, never()).setState(any(), any());
    }

    @Test
    void testClearStateStandaloneUnchangedState() throws IOException {
        setUpSharedClusterState();
        when(stateManagerProvider.isClusterProviderEnabled()).thenReturn(false);

        assertThrows(IllegalStateException.class, () -> dao.clearState(processorNode, createClusterComponentState(INITIAL_STATE)));

        verify(stateManager, never()).replace(any(), any(), any());
        verify(stateManager, never()).setState(any(), any());
    }

    @Test
    void testClearStateRemovesSingleLocalStateKey() throws IOException {
        localState.set(new TestStateMap(INITIAL_STATE, 1));
        when(stateManagerProvider.isClusterProviderEnabled()).thenReturn(false);
        when(stateManager.isStateKeyDropSupported()).thenReturn(true);
        setUpReplace(Scope.LOCAL, localState, UnaryOperator.identity());

        dao.clearState(processorNode, createLocalComponentState(KEY_REMOVED_STATE));

        assertEquals(KEY_REMOVED_STATE, localState.get().toMap());
        verify(stateManager, never()).setState(any(), any());
    }

    @Test
    void testClearStateLocalStateModifiedConcurrently() throws IOException {
        localState.set(new TestStateMap(INITIAL_STATE, 1));
        when(stateManagerProvider.isClusterProviderEnabled()).thenReturn(false);
        when(stateManager.isStateKeyDropSupported()).thenReturn(true);
        final Map<String, String> processorUpdatedState = Map.of("a", "2", "b", "1", "c", "1");
        setUpReplace(Scope.LOCAL, localState, current -> current.update(processorUpdatedState));

        assertThrows(IllegalStateException.class, () -> dao.clearState(processorNode, createLocalComponentState(KEY_REMOVED_STATE)));

        assertEquals(processorUpdatedState, localState.get().toMap());
        verify(stateManager, never()).setState(any(), any());
    }

    private void setUpSharedClusterState() throws IOException {
        clusterState.set(new TestStateMap(INITIAL_STATE, 1));
        when(stateManager.isStateKeyDropSupported()).thenReturn(true);
        when(stateManager.getState(Scope.CLUSTER)).thenAnswer(invocation -> clusterState.get());
    }

    /**
     * Configure replace as a compare-and-set on the state version, applying the concurrent update before the comparison
     */
    private void setUpReplace(final Scope scope, final AtomicReference<TestStateMap> state, final UnaryOperator<TestStateMap> concurrentUpdate) throws IOException {
        when(stateManager.replace(any(), any(), eq(scope))).thenAnswer(invocation -> {
            state.set(concurrentUpdate.apply(state.get()));

            final StateMap expected = invocation.getArgument(0);
            final Map<String, String> newValue = invocation.getArgument(1);
            final TestStateMap current = state.get();
            if (!current.getStateVersion().equals(expected.getStateVersion())) {
                return false;
            }
            state.set(current.update(newValue));
            return true;
        });
    }

    private static ComponentStateDTO createClusterComponentState(final Map<String, String> state) {
        final ComponentStateDTO componentState = new ComponentStateDTO();
        componentState.setClusterState(createStateMap(state));
        return componentState;
    }

    private static ComponentStateDTO createLocalComponentState(final Map<String, String> state) {
        final ComponentStateDTO componentState = new ComponentStateDTO();
        componentState.setLocalState(createStateMap(state));
        return componentState;
    }

    private static StateMapDTO createStateMap(final Map<String, String> state) {
        final List<StateEntryDTO> entries = new ArrayList<>();
        state.forEach((key, value) -> {
            final StateEntryDTO entry = new StateEntryDTO();
            entry.setKey(key);
            entry.setValue(value);
            entries.add(entry);
        });

        final StateMapDTO stateMap = new StateMapDTO();
        stateMap.setState(entries);
        return stateMap;
    }

    private record TestStateMap(Map<String, String> state, long version) implements StateMap {

        TestStateMap update(final Map<String, String> newState) {
            return new TestStateMap(Map.copyOf(newState), version + 1);
        }

        @Override
        public Optional<String> getStateVersion() {
            return Optional.of(String.valueOf(version));
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
