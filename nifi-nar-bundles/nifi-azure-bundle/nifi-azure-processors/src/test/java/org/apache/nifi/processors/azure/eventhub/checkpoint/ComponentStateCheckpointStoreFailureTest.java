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

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.processors.azure.eventhub.checkpoint.exception.ConcurrentStateModificationException;
import org.apache.nifi.processors.azure.eventhub.checkpoint.exception.StateNotAvailableException;
import org.apache.nifi.state.MockStateMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ComponentStateCheckpointStoreFailureTest extends AbstractComponentStateCheckpointStoreTest {

    @Mock(strictness = Mock.Strictness.WARN)
    private StateManager stateManager;

    @Override
    StateManager getStateManager() {
        return stateManager;
    }

    @Test
    void testListOwnership_GetState_IOException() throws IOException {
        when(stateManager.getState(Scope.CLUSTER)).thenThrow(IOException.class);

        StepVerifier.create(checkpointStore.listOwnership(EVENT_HUB_NAMESPACE, EVENT_HUB_NAME, CONSUMER_GROUP))
                .expectError(StateNotAvailableException.class)
                .verify();

        verify(stateManager).getState(Scope.CLUSTER);
        verifyNoMoreInteractions(stateManager);
    }

    @Test
    void testListCheckpoints_GetState_IOException() throws IOException {
        when(stateManager.getState(Scope.CLUSTER)).thenThrow(IOException.class);

        StepVerifier.create(checkpointStore.listCheckpoints(EVENT_HUB_NAMESPACE, EVENT_HUB_NAME, CONSUMER_GROUP))
                .expectError(StateNotAvailableException.class)
                .verify();

        verify(stateManager).getState(Scope.CLUSTER);
        verifyNoMoreInteractions(stateManager);
    }

    @Test
    void testClaimOwnership_GetState_IOException() throws IOException {
        when(stateManager.getState(Scope.CLUSTER)).thenThrow(IOException.class);

        StepVerifier.create(checkpointStore.claimOwnership(Collections.singletonList(partitionOwnership1)))
                .expectError(StateNotAvailableException.class)
                .verify();

        verify(stateManager, times(1)).getState(Scope.CLUSTER);
        verifyNoMoreInteractions(stateManager);
    }

    @Test
    void testClaimOwnership_ReplaceState_IOException() throws IOException {
        StateMap state = new MockStateMap(new HashMap<>(), 1);
        when(stateManager.getState(Scope.CLUSTER)).thenReturn(state);
        when(stateManager.replace(eq(state), anyMap(), eq(Scope.CLUSTER))).thenThrow(IOException.class);

        StepVerifier.create(checkpointStore.claimOwnership(Collections.singletonList(partitionOwnership1)))
                .expectError(StateNotAvailableException.class)
                .verify();

        verify(stateManager).getState(Scope.CLUSTER);
        verify(stateManager).replace(eq(state), anyMap(), eq(Scope.CLUSTER));
        verifyNoMoreInteractions(stateManager);
    }

    @Test
    void testUpdateCheckpoint_GetState_IOException() throws IOException {
        when(stateManager.getState(Scope.CLUSTER)).thenThrow(IOException.class);

        StepVerifier.create(checkpointStore.updateCheckpoint(checkpoint1))
                .expectError(StateNotAvailableException.class)
                .verify();

        verify(stateManager, times(1)).getState(Scope.CLUSTER);
        verifyNoMoreInteractions(stateManager);
    }

    @Test
    void testUpdateCheckpoint_ReplaceState_IOException() throws IOException {
        StateMap state = new MockStateMap(new HashMap<>(), 1);
        when(stateManager.getState(Scope.CLUSTER)).thenReturn(state);
        when(stateManager.replace(eq(state), anyMap(), eq(Scope.CLUSTER))).thenThrow(IOException.class);

        StepVerifier.create(checkpointStore.updateCheckpoint(checkpoint1))
                .expectError(StateNotAvailableException.class)
                .verify();

        verify(stateManager, times(1)).getState(Scope.CLUSTER);
        verify(stateManager).replace(eq(state), anyMap(), eq(Scope.CLUSTER));
        verifyNoMoreInteractions(stateManager);
    }

    @Test
    void testCleanUp_GetState_IOException() throws IOException {
        when(stateManager.getState(Scope.CLUSTER)).thenThrow(IOException.class);

        StepVerifier.create(checkpointStore.cleanUpMono(EVENT_HUB_NAMESPACE, EVENT_HUB_NAME, CONSUMER_GROUP + "-2"))
                .expectError(StateNotAvailableException.class)
                .verify();

        verify(stateManager).getState(Scope.CLUSTER);
        verifyNoMoreInteractions(stateManager);
    }

    @Test
    void testCleanUp_ReplaceState_IOException() throws IOException {
        StateMap state = new MockStateMap(new HashMap<>(initMap(partitionOwnership1)), 1);
        when(stateManager.getState(Scope.CLUSTER)).thenReturn(state);
        when(stateManager.replace(eq(state), anyMap(), eq(Scope.CLUSTER))).thenThrow(IOException.class);

        StepVerifier.create(checkpointStore.cleanUpMono(EVENT_HUB_NAMESPACE, EVENT_HUB_NAME, CONSUMER_GROUP + "-2"))
                .expectError(StateNotAvailableException.class)
                .verify();

        verify(stateManager, times(1)).getState(Scope.CLUSTER);
        verify(stateManager).replace(eq(state), anyMap(), eq(Scope.CLUSTER));
        verifyNoMoreInteractions(stateManager);
    }

    @Test
    void testClaimOwnership_ReplaceState_ConcurrentStateModificationException_Success() throws IOException {
        StateMap state = new MockStateMap(new HashMap<>(), 1);
        when(stateManager.getState(Scope.CLUSTER)).thenReturn(state);
        when(stateManager.replace(eq(state), anyMap(), eq(Scope.CLUSTER))).thenReturn(false, false, true);

        StepVerifier.withVirtualTime(() -> checkpointStore.claimOwnership(Collections.singletonList(partitionOwnership1)))
                .thenAwait(Duration.ofSeconds(1))
                .expectNextCount(1)
                .verifyComplete();

        verify(stateManager, times(3)).getState(Scope.CLUSTER);
        verify(stateManager, times(3)).replace(eq(state), anyMap(), eq(Scope.CLUSTER));
        verifyNoMoreInteractions(stateManager);
    }

    @Test
    void testClaimOwnership_ReplaceState_ConcurrentStateModificationException_Failure() throws IOException {
        StateMap state = new MockStateMap(new HashMap<>(), 1);
        when(stateManager.getState(Scope.CLUSTER)).thenReturn(state);
        when(stateManager.replace(eq(state), anyMap(), eq(Scope.CLUSTER))).thenReturn(false);

        StepVerifier.withVirtualTime(() -> checkpointStore.claimOwnership(Collections.singletonList(partitionOwnership1)))
                .thenAwait(Duration.ofSeconds(10))
                .expectError(ConcurrentStateModificationException.class)
                .verify();

        verify(stateManager, times(11)).getState(Scope.CLUSTER);
        verify(stateManager, times(11)).replace(eq(state), anyMap(), eq(Scope.CLUSTER));
        verifyNoMoreInteractions(stateManager);
    }

    @Test
    void testUpdateCheckpoint_ReplaceState_ConcurrentStateModificationException_Success() throws IOException {
        StateMap state = new MockStateMap(new HashMap<>(), 1);
        when(stateManager.getState(Scope.CLUSTER)).thenReturn(state);
        when(stateManager.replace(eq(state), anyMap(), eq(Scope.CLUSTER))).thenReturn(false, false, true);

        StepVerifier.withVirtualTime(() -> checkpointStore.updateCheckpoint(checkpoint1))
                .thenAwait(Duration.ofSeconds(1))
                .expectNext()
                .verifyComplete();

        verify(stateManager, times(3)).getState(Scope.CLUSTER);
        verify(stateManager, times(3)).replace(eq(state), anyMap(), eq(Scope.CLUSTER));
        verifyNoMoreInteractions(stateManager);
    }

    @Test
    void testUpdateCheckpoint_ReplaceState_ConcurrentStateModificationException_Failure() throws IOException {
        StateMap state = new MockStateMap(new HashMap<>(), 1);
        when(stateManager.getState(Scope.CLUSTER)).thenReturn(state);
        when(stateManager.replace(eq(state), anyMap(), eq(Scope.CLUSTER))).thenReturn(false);

        StepVerifier.withVirtualTime(() -> checkpointStore.updateCheckpoint(checkpoint1))
                .thenAwait(Duration.ofSeconds(10))
                .expectError(ConcurrentStateModificationException.class)
                .verify();

        verify(stateManager, times(11)).getState(Scope.CLUSTER);
        verify(stateManager, times(11)).replace(eq(state), anyMap(), eq(Scope.CLUSTER));
        verifyNoMoreInteractions(stateManager);
    }

    @Test
    void testCleanUp_ReplaceState_ConcurrentStateModificationException_Success() throws IOException {
        StateMap state = new MockStateMap(new HashMap<>(initMap(partitionOwnership1)), 1);
        when(stateManager.getState(Scope.CLUSTER)).thenReturn(state);
        when(stateManager.replace(eq(state), anyMap(), eq(Scope.CLUSTER))).thenReturn(false, false, true);

        StepVerifier.withVirtualTime(() -> checkpointStore.cleanUpMono(EVENT_HUB_NAMESPACE, EVENT_HUB_NAME, CONSUMER_GROUP + "-2"))
                .thenAwait(Duration.ofSeconds(1))
                .expectNext()
                .verifyComplete();

        verify(stateManager, times(3)).getState(Scope.CLUSTER);
        verify(stateManager, times(3)).replace(eq(state), anyMap(), eq(Scope.CLUSTER));
        verifyNoMoreInteractions(stateManager);
    }

    @Test
    void testCleanUp_ReplaceState_ConcurrentStateModificationException_Failure() throws IOException {
        StateMap state = new MockStateMap(new HashMap<>(initMap(partitionOwnership1)), 1);
        when(stateManager.getState(Scope.CLUSTER)).thenReturn(state);
        when(stateManager.replace(eq(state), anyMap(), eq(Scope.CLUSTER))).thenReturn(false);

        StepVerifier.withVirtualTime(() -> checkpointStore.cleanUpMono(EVENT_HUB_NAMESPACE, EVENT_HUB_NAME, CONSUMER_GROUP + "-2"))
                .thenAwait(Duration.ofSeconds(10))
                .expectError(ConcurrentStateModificationException.class)
                .verify();

        verify(stateManager, times(11)).getState(Scope.CLUSTER);
        verify(stateManager, times(11)).replace(eq(state), anyMap(), eq(Scope.CLUSTER));
        verifyNoMoreInteractions(stateManager);
    }

}
