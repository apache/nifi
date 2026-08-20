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

package org.apache.nifi.web.util;

import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.status.ConnectionStatusDTO;
import org.apache.nifi.web.api.dto.status.ConnectionStatusSnapshotDTO;
import org.apache.nifi.web.api.entity.ConnectionStatusEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.URI;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LocalComponentLifecycleTest {
    @Mock
    private NiFiServiceFacade serviceFacade;

    @Test
    void testWaitForConnectionQueuesEmptyReturnsTrueImmediatelyWhenEveryQueueIsEmpty() throws LifecycleManagementException {
        final LocalComponentLifecycle lifecycle = new LocalComponentLifecycle();
        lifecycle.setServiceFacade(serviceFacade);

        when(serviceFacade.getConnectionStatus("connection-a")).thenReturn(connectionStatusEntity(0));
        when(serviceFacade.getConnectionStatus("connection-b")).thenReturn(connectionStatusEntity(0));

        final boolean result = lifecycle.waitForConnectionQueuesEmpty(URI.create("http://localhost:8080/nifi-api"), Set.of("connection-a", "connection-b"), new TestPause(true));

        assertTrue(result);
        verify(serviceFacade).getConnectionStatus("connection-a");
        verify(serviceFacade).getConnectionStatus("connection-b");
    }

    @Test
    void testWaitForConnectionQueuesEmptyPollsUntilEveryQueueIsEmpty() throws LifecycleManagementException {
        final LocalComponentLifecycle lifecycle = new LocalComponentLifecycle();
        lifecycle.setServiceFacade(serviceFacade);
        final TestPause pause = new TestPause(true, true);

        when(serviceFacade.getConnectionStatus("connection-a")).thenReturn(connectionStatusEntity(1), connectionStatusEntity(0));
        when(serviceFacade.getConnectionStatus("connection-b")).thenReturn(connectionStatusEntity(0), connectionStatusEntity(0));

        final boolean result = lifecycle.waitForConnectionQueuesEmpty(URI.create("http://localhost:8080/nifi-api"), Set.of("connection-a", "connection-b"), pause);

        assertTrue(result);
        assertTrue(pause.wasInvoked());
        verify(serviceFacade, times(2)).getConnectionStatus("connection-a");
        verify(serviceFacade).getConnectionStatus("connection-b");
    }

    @Test
    void testWaitForConnectionQueuesEmptyReturnsFalseWhenPauseStopsBeforeDrainCompletes() throws LifecycleManagementException {
        final LocalComponentLifecycle lifecycle = new LocalComponentLifecycle();
        lifecycle.setServiceFacade(serviceFacade);
        final TestPause pause = new TestPause(false);

        when(serviceFacade.getConnectionStatus("connection-a")).thenReturn(connectionStatusEntity(2));
        final boolean result = lifecycle.waitForConnectionQueuesEmpty(URI.create("http://localhost:8080/nifi-api"), Set.of("connection-a", "connection-b"), pause);

        assertFalse(result);
        assertTrue(pause.wasInvoked());
    }

    @Test
    void testWaitForConnectionQueuesEmptyTreatsUnacknowledgedFlowFilesAsNotEmpty() throws LifecycleManagementException {
        final LocalComponentLifecycle lifecycle = new LocalComponentLifecycle();
        lifecycle.setServiceFacade(serviceFacade);
        final TestPause pause = new TestPause(false);

        when(serviceFacade.getConnectionStatus("connection-a")).thenReturn(connectionStatusEntity(1));

        final boolean result = lifecycle.waitForConnectionQueuesEmpty(URI.create("http://localhost:8080/nifi-api"), Set.of("connection-a"), pause);

        assertFalse(result);
    }

    private ConnectionStatusEntity connectionStatusEntity(final int flowFilesQueued) {
        final ConnectionStatusSnapshotDTO aggregateSnapshot = new ConnectionStatusSnapshotDTO();
        aggregateSnapshot.setFlowFilesQueued(flowFilesQueued);

        final ConnectionStatusDTO connectionStatus = new ConnectionStatusDTO();
        connectionStatus.setAggregateSnapshot(aggregateSnapshot);
        connectionStatus.setNodeSnapshots(List.of());

        final ConnectionStatusEntity entity = new ConnectionStatusEntity();
        entity.setConnectionStatus(connectionStatus);
        return entity;
    }

    private static final class TestPause implements Pause {
        private final List<Boolean> decisions;
        private int index = 0;
        private boolean invoked;

        private TestPause(final Boolean... decisions) {
            this.decisions = List.of(decisions);
        }

        @Override
        public boolean pause() {
            invoked = true;
            if (index >= decisions.size()) {
                return false;
            }

            return decisions.get(index++);
        }

        private boolean wasInvoked() {
            return invoked;
        }
    }
}
