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
package org.apache.nifi.processors.azure.eventhub.checkpoint;

import com.azure.messaging.eventhubs.models.Checkpoint;
import com.azure.messaging.eventhubs.models.PartitionOwnership;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.state.MockStateMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.azure.eventhub.checkpoint.ComponentStateCheckpointStoreUtils.createCheckpointKey;
import static org.apache.nifi.processors.azure.eventhub.checkpoint.ComponentStateCheckpointStoreUtils.createCheckpointValue;
import static org.apache.nifi.processors.azure.eventhub.checkpoint.ComponentStateCheckpointStoreUtils.createOwnershipKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ComponentStateCheckpointStoreConcurrencyTest extends AbstractComponentStateCheckpointStoreTest {

    @Mock(strictness = Mock.Strictness.WARN)
    private StateManager stateManager;

    @Captor
    private ArgumentCaptor<Map<String, String>> updatedMapCaptor;

    @Override
    StateManager getStateManager() {
        return stateManager;
    }

    @Test
    void testConcurrentClaimDifferentOwnerships() throws IOException {
        StateMap state1 = new MockStateMap(initMap(), 1);
        StateMap state2 = new MockStateMap(initMap(partitionOwnership1), 2);

        when(stateManager.getState(Scope.CLUSTER))
                .thenReturn(state1)
                .thenReturn(state2);

        when(stateManager.replace(eq(state1), anyMap(), eq(Scope.CLUSTER))).thenReturn(false);
        when(stateManager.replace(eq(state2), anyMap(), eq(Scope.CLUSTER))).thenReturn(true);

        List<PartitionOwnership> requestedOwnerships = Collections.singletonList(partitionOwnership2);

        List<PartitionOwnership> claimedOwnerships = new ArrayList<>();
        checkpointStore.claimOwnership(requestedOwnerships).subscribe(claimedOwnerships::add);

        assertEquals(1, claimedOwnerships.size());
        PartitionOwnership claimedOwnership = claimedOwnerships.getFirst();
        assertClaimedOwnership(partitionOwnership2, claimedOwnership);

        verify(stateManager, times(2)).getState(eq(Scope.CLUSTER));
        verify(stateManager, times(2)).replace(any(StateMap.class), updatedMapCaptor.capture(), eq(Scope.CLUSTER));
        verifyNoMoreInteractions(stateManager);

        Map<String, String> updatedMap1 = updatedMapCaptor.getAllValues().getFirst();
        assertEquals(1, updatedMap1.size());
        assertOwnershipFound(updatedMap1, partitionOwnership2);

        Map<String, String> updatedMap2 = updatedMapCaptor.getAllValues().get(1);
        assertEquals(2, updatedMap2.size());
        assertOwnershipFound(updatedMap2, partitionOwnership1);
        assertOwnershipFound(updatedMap2, partitionOwnership2);
    }

    @Test
    void testConcurrentClaimSameOwnership() throws IOException {
        StateMap state1 = new MockStateMap(initMap(), 1);
        StateMap state2 = new MockStateMap(initMap(partitionOwnership1), 2);

        when(stateManager.getState(Scope.CLUSTER))
                .thenReturn(state1)
                .thenReturn(state2);

        when(stateManager.replace(eq(state1), anyMap(), eq(Scope.CLUSTER))).thenReturn(false);

        List<PartitionOwnership> requestedOwnerships = Collections.singletonList(partitionOwnership1);

        List<PartitionOwnership> claimedOwnerships = new ArrayList<>();
        checkpointStore.claimOwnership(requestedOwnerships).subscribe(claimedOwnerships::add);

        assertTrue(claimedOwnerships.isEmpty());

        verify(stateManager, times(2)).getState(eq(Scope.CLUSTER));
        verify(stateManager, times(1)).replace(any(StateMap.class), updatedMapCaptor.capture(), eq(Scope.CLUSTER));
        verifyNoMoreInteractions(stateManager);

        Map<String, String> updatedMap1 = updatedMapCaptor.getAllValues().getFirst();
        assertEquals(1, updatedMap1.size());
        assertOwnershipFound(updatedMap1, partitionOwnership1);
    }

    @Test
    void testConcurrentUpdateCheckpoint() throws IOException {
        StateMap state1 = new MockStateMap(initMap(partitionOwnership1), 1);
        StateMap state2 = new MockStateMap(initMap(partitionOwnership1, partitionOwnership2), 2);

        when(stateManager.getState(Scope.CLUSTER))
                .thenReturn(state1)
                .thenReturn(state2);

        when(stateManager.replace(eq(state1), anyMap(), eq(Scope.CLUSTER))).thenReturn(false);
        when(stateManager.replace(eq(state2), anyMap(), eq(Scope.CLUSTER))).thenReturn(true);

        checkpointStore.updateCheckpoint(checkpoint1).subscribe();

        verify(stateManager, times(2)).getState(eq(Scope.CLUSTER));
        verify(stateManager, times(2)).replace(any(StateMap.class), updatedMapCaptor.capture(), eq(Scope.CLUSTER));
        verifyNoMoreInteractions(stateManager);

        Map<String, String> updatedMap1 = updatedMapCaptor.getAllValues().getFirst();
        assertEquals(2, updatedMap1.size());
        assertOwnershipFound(updatedMap1, partitionOwnership1);
        assertCheckpointFound(updatedMap1, checkpoint1);

        Map<String, String> updatedMap2 = updatedMapCaptor.getAllValues().get(1);
        assertEquals(3, updatedMap2.size());
        assertOwnershipFound(updatedMap2, partitionOwnership1);
        assertOwnershipFound(updatedMap2, partitionOwnership2);
        assertCheckpointFound(updatedMap1, checkpoint1);
    }

    @Test
    void testConcurrentCleanUp() throws IOException {
        StateMap state1 = new MockStateMap(initMap(partitionOwnership1), 1);
        StateMap state2 = new MockStateMap(initMap(), 2);

        when(stateManager.getState(Scope.CLUSTER))
                .thenReturn(state1)
                .thenReturn(state2);

        when(stateManager.replace(eq(state1), anyMap(), eq(Scope.CLUSTER))).thenReturn(false);

        checkpointStore.cleanUp(EVENT_HUB_NAMESPACE, EVENT_HUB_NAME, CONSUMER_GROUP + "-2");

        verify(stateManager, times(2)).getState(eq(Scope.CLUSTER));
        verify(stateManager, times(1)).replace(any(StateMap.class), updatedMapCaptor.capture(), eq(Scope.CLUSTER));
        verifyNoMoreInteractions(stateManager);

        Map<String, String> updatedMap1 = updatedMapCaptor.getAllValues().getFirst();
        assertTrue(updatedMap1.isEmpty());
    }

    private void assertOwnershipFound(final Map<String, String> map, final PartitionOwnership ownership) {
        final String key = createOwnershipKey(ownership);
        assertTrue(map.containsKey(key));

        final String value = map.get(key);
        assertTrue(value.startsWith(ownership.getOwnerId()));
    }

    private void assertCheckpointFound(final Map<String, String> map, final Checkpoint checkpoint) {
        final String key = createCheckpointKey(checkpoint);
        assertTrue(map.containsKey(key));

        final String value = map.get(key);
        assertEquals(createCheckpointValue(checkpoint), value);
    }
}
