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
import org.apache.nifi.web.api.dto.ListingRequestDTO;
import org.apache.nifi.web.api.dto.QueueSizeDTO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.URI;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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

        when(serviceFacade.createFlowFileListingRequest(eq("connection-a"), anyString())).thenReturn(listingRequest(0));
        when(serviceFacade.createFlowFileListingRequest(eq("connection-b"), anyString())).thenReturn(listingRequest(0));

        final boolean result = lifecycle.waitForConnectionQueuesEmpty(URI.create("http://localhost:8080/nifi-api"), Set.of("connection-a", "connection-b"), new TestPause(true));

        assertTrue(result);
        verify(serviceFacade).createFlowFileListingRequest(eq("connection-a"), anyString());
        verify(serviceFacade).createFlowFileListingRequest(eq("connection-b"), anyString());
        verify(serviceFacade).deleteFlowFileListingRequest(eq("connection-a"), anyString());
        verify(serviceFacade).deleteFlowFileListingRequest(eq("connection-b"), anyString());
    }

    @Test
    void testWaitForConnectionQueuesEmptyPollsUntilEveryQueueIsEmpty() throws LifecycleManagementException {
        final LocalComponentLifecycle lifecycle = new LocalComponentLifecycle();
        lifecycle.setServiceFacade(serviceFacade);
        final TestPause pause = new TestPause(true, true);

        when(serviceFacade.createFlowFileListingRequest(eq("connection-a"), anyString())).thenReturn(listingRequest(1), listingRequest(0));
        when(serviceFacade.createFlowFileListingRequest(eq("connection-b"), anyString())).thenReturn(listingRequest(0));

        final boolean result = lifecycle.waitForConnectionQueuesEmpty(URI.create("http://localhost:8080/nifi-api"), Set.of("connection-a", "connection-b"), pause);

        assertTrue(result);
        assertTrue(pause.wasInvoked());
        verify(serviceFacade, times(2)).createFlowFileListingRequest(eq("connection-a"), anyString());
        verify(serviceFacade).createFlowFileListingRequest(eq("connection-b"), anyString());
        verify(serviceFacade, times(2)).deleteFlowFileListingRequest(eq("connection-a"), anyString());
        verify(serviceFacade).deleteFlowFileListingRequest(eq("connection-b"), anyString());
    }

    @Test
    void testWaitForConnectionQueuesEmptyReturnsFalseWhenPauseStopsBeforeDrainCompletes() throws LifecycleManagementException {
        final LocalComponentLifecycle lifecycle = new LocalComponentLifecycle();
        lifecycle.setServiceFacade(serviceFacade);
        final TestPause pause = new TestPause(false);

        when(serviceFacade.createFlowFileListingRequest(eq("connection-a"), anyString())).thenReturn(listingRequest(2));
        final boolean result = lifecycle.waitForConnectionQueuesEmpty(URI.create("http://localhost:8080/nifi-api"), Set.of("connection-a", "connection-b"), pause);

        assertFalse(result);
        assertTrue(pause.wasInvoked());
        verify(serviceFacade).deleteFlowFileListingRequest(eq("connection-a"), anyString());
    }

    @Test
    void testWaitForConnectionQueuesEmptyTreatsUnacknowledgedFlowFilesAsNotEmpty() throws LifecycleManagementException {
        final LocalComponentLifecycle lifecycle = new LocalComponentLifecycle();
        lifecycle.setServiceFacade(serviceFacade);
        final TestPause pause = new TestPause(false);

        when(serviceFacade.createFlowFileListingRequest(eq("connection-a"), anyString())).thenReturn(listingRequest(1));

        final boolean result = lifecycle.waitForConnectionQueuesEmpty(URI.create("http://localhost:8080/nifi-api"), Set.of("connection-a"), pause);

        assertFalse(result);
        verify(serviceFacade).deleteFlowFileListingRequest(eq("connection-a"), anyString());
    }

    private ListingRequestDTO listingRequest(final int flowFilesQueued) {
        final QueueSizeDTO queueSize = new QueueSizeDTO();
        queueSize.setObjectCount(flowFilesQueued);

        final ListingRequestDTO listingRequest = new ListingRequestDTO();
        listingRequest.setQueueSize(queueSize);
        return listingRequest;
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
