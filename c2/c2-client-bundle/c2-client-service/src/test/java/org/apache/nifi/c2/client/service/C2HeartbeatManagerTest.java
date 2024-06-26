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

package org.apache.nifi.c2.client.service;

import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.apache.nifi.c2.client.api.C2Client;
import org.apache.nifi.c2.client.service.model.RuntimeInfoWrapper;
import org.apache.nifi.c2.protocol.api.C2Heartbeat;
import org.apache.nifi.c2.protocol.api.C2HeartbeatResponse;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class C2HeartbeatManagerTest {

    @Mock
    private C2Client mockC2Client;

    @Mock
    private C2HeartbeatFactory mockC2HeartbeatFactory;

    @Mock
    private ReentrantLock mockHeartbeatLock;

    @Mock
    private Supplier<RuntimeInfoWrapper> mockRuntimeInfoWrapperSupplier;

    @Mock
    private RuntimeInfoWrapper mockRuntimeInfoWrapper;

    @Mock
    private C2OperationManager mockC2OperationManager;

    @InjectMocks
    private C2HeartbeatManager testHeartbeatManager;

    @Test
    void shouldSkipSendingHeartbeatIfHeartbeatLockIsAcquired() {
        when(mockHeartbeatLock.tryLock()).thenReturn(false);

        testHeartbeatManager.run();

        verify(mockC2HeartbeatFactory, never()).create(any());
        verify(mockC2Client, never()).publishHeartbeat(any());
        verify(mockC2OperationManager, never()).add(any());
        verify(mockHeartbeatLock, never()).unlock();
    }

    @Test
    void shouldSendHeartbeatAndProcessEmptyResponse() {
        when(mockRuntimeInfoWrapperSupplier.get()).thenReturn(mockRuntimeInfoWrapper);
        when(mockHeartbeatLock.tryLock()).thenReturn(true);
        C2Heartbeat mockC2Heartbeat = mock(C2Heartbeat.class);
        when(mockC2HeartbeatFactory.create(mockRuntimeInfoWrapper)).thenReturn(mockC2Heartbeat);
        when(mockC2Client.publishHeartbeat(mockC2Heartbeat)).thenReturn(empty());

        testHeartbeatManager.run();

        verify(mockC2HeartbeatFactory, times(1)).create(mockRuntimeInfoWrapper);
        verify(mockC2Client, times(1)).publishHeartbeat(mockC2Heartbeat);
        verify(mockC2OperationManager, never()).add(any());
        verify(mockHeartbeatLock, times(1)).unlock();

    }

    @Test
    void shouldSendHeartbeatAndProcessResponseWithNoOperation() {
        when(mockRuntimeInfoWrapperSupplier.get()).thenReturn(mockRuntimeInfoWrapper);
        when(mockHeartbeatLock.tryLock()).thenReturn(true);
        C2Heartbeat mockC2Heartbeat = mock(C2Heartbeat.class);
        when(mockC2HeartbeatFactory.create(mockRuntimeInfoWrapper)).thenReturn(mockC2Heartbeat);
        C2HeartbeatResponse mockC2HeartbeatResponse = mock(C2HeartbeatResponse.class);
        when(mockC2HeartbeatResponse.getRequestedOperations()).thenReturn(List.of());
        when(mockC2Client.publishHeartbeat(mockC2Heartbeat)).thenReturn(Optional.of(mockC2HeartbeatResponse));

        testHeartbeatManager.run();

        verify(mockC2HeartbeatFactory, times(1)).create(mockRuntimeInfoWrapper);
        verify(mockC2Client, times(1)).publishHeartbeat(mockC2Heartbeat);
        verify(mockC2OperationManager, never()).add(any());
        verify(mockHeartbeatLock, times(1)).unlock();
    }

    @Test
    void shouldSendHeartbeatAndProcessResponseWithMultipleOperation() {
        when(mockRuntimeInfoWrapperSupplier.get()).thenReturn(mockRuntimeInfoWrapper);
        when(mockHeartbeatLock.tryLock()).thenReturn(true);
        C2Heartbeat mockC2Heartbeat = mock(C2Heartbeat.class);
        when(mockC2HeartbeatFactory.create(mockRuntimeInfoWrapper)).thenReturn(mockC2Heartbeat);
        C2HeartbeatResponse mockC2HeartbeatResponse = mock(C2HeartbeatResponse.class);
        C2Operation mockOperation1 = mock(C2Operation.class);
        C2Operation mockOperation2 = mock(C2Operation.class);
        when(mockC2HeartbeatResponse.getRequestedOperations()).thenReturn(List.of(mockOperation1, mockOperation2));
        when(mockC2Client.publishHeartbeat(mockC2Heartbeat)).thenReturn(ofNullable(mockC2HeartbeatResponse));

        testHeartbeatManager.run();

        verify(mockC2HeartbeatFactory, times(1)).create(mockRuntimeInfoWrapper);
        verify(mockC2Client, times(1)).publishHeartbeat(mockC2Heartbeat);
        verify(mockC2OperationManager, times(1)).add(mockOperation1);
        verify(mockC2OperationManager, times(1)).add(mockOperation2);
        verify(mockHeartbeatLock, times(1)).unlock();
    }

    @Test
    void shouldReleaseHeartbeatLockWhenExceptionOccurs() {
        when(mockRuntimeInfoWrapperSupplier.get()).thenReturn(mockRuntimeInfoWrapper);
        when(mockHeartbeatLock.tryLock()).thenReturn(true);
        when(mockC2HeartbeatFactory.create(mockRuntimeInfoWrapper)).thenThrow(new RuntimeException());

        testHeartbeatManager.run();

        verify(mockC2HeartbeatFactory, times(1)).create(mockRuntimeInfoWrapper);
        verify(mockC2Client, never()).publishHeartbeat(any());
        verify(mockC2OperationManager, never()).add(any());
        verify(mockHeartbeatLock, times(1)).unlock();
    }
}
