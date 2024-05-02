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
import static java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.FULLY_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NOT_APPLIED;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.nifi.c2.client.api.C2Client;
import org.apache.nifi.c2.client.service.operation.C2OperationHandler;
import org.apache.nifi.c2.client.service.operation.C2OperationHandlerProvider;
import org.apache.nifi.c2.client.service.operation.C2OperationRestartHandler;
import org.apache.nifi.c2.client.service.operation.OperationQueue;
import org.apache.nifi.c2.client.service.operation.OperationQueueDAO;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.apache.nifi.c2.protocol.api.C2OperationState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class C2OperationManagerTest {

    private static final long MAX_WAIT_TIME_MS = 1000;

    @Mock
    private C2Client mockC2Client;

    @Mock
    private C2OperationHandlerProvider mockC2OperationHandlerProvider;

    @Mock
    private ReentrantLock mockHeartbeatLock;

    @Mock
    private OperationQueueDAO mockOperationQueueDAO;

    @Mock
    private C2OperationRestartHandler mockC2OperationRestartHandler;

    @InjectMocks
    private C2OperationManager testC2OperationManager;

    @Captor
    ArgumentCaptor<C2OperationAck> c2OperationAckArgumentCaptor;

    private ExecutorService executorService;

    @BeforeEach
    void setup() {
        executorService = newVirtualThreadPerTaskExecutor();
    }

    @AfterEach
    void teardown() {
        executorService.shutdownNow();
    }

    @Test
    void shouldWaitForIncomingOperationThenTimeout() {
        Future<?> future = executorService.submit(testC2OperationManager);

        assertThrows(TimeoutException.class, () -> future.get(MAX_WAIT_TIME_MS, MILLISECONDS));
        verify(mockC2OperationHandlerProvider, never()).getHandlerForOperation(any());
    }

    @Test
    void shouldContinueWithoutProcessingWhenNoHandlerIsDefined() {
        C2Operation testOperation = mock(C2Operation.class);
        when(mockC2OperationHandlerProvider.getHandlerForOperation(testOperation)).thenReturn(empty());

        Future<?> future = executorService.submit(testC2OperationManager);
        testC2OperationManager.add(testOperation);

        assertThrows(TimeoutException.class, () -> future.get(MAX_WAIT_TIME_MS, MILLISECONDS));
        verify(mockC2Client, never()).acknowledgeOperation(any());
        verify(mockHeartbeatLock, never()).lock();
    }

    @Test
    void shouldProcessOperationWithoutRestartAndAcknowledge() {
        C2Operation mockOperation = mock(C2Operation.class);
        C2OperationHandler mockOperationHandler = mock(C2OperationHandler.class);
        C2OperationAck mockC2OperationAck = mock(C2OperationAck.class);
        when(mockC2OperationHandlerProvider.getHandlerForOperation(mockOperation)).thenReturn(ofNullable(mockOperationHandler));
        when(mockOperationHandler.handle(mockOperation)).thenReturn(mockC2OperationAck);
        when(mockOperationHandler.requiresRestart()).thenReturn(false);

        Future<?> future = executorService.submit(testC2OperationManager);
        testC2OperationManager.add(mockOperation);

        assertThrows(TimeoutException.class, () -> future.get(MAX_WAIT_TIME_MS, MILLISECONDS));
        verify(mockC2Client, times(1)).acknowledgeOperation(mockC2OperationAck);
        verify(mockHeartbeatLock, never()).lock();
    }

    @Test
    void shouldProcessOperationWithSuccessfulRestart() {
        C2Operation mockOperation = mock(C2Operation.class);
        C2OperationHandler mockOperationHandler = mock(C2OperationHandler.class);
        C2OperationAck mockC2OperationAck = mock(C2OperationAck.class);
        C2OperationState mockC2OperationState = mock(C2OperationState.class);
        when(mockC2OperationHandlerProvider.getHandlerForOperation(mockOperation)).thenReturn(ofNullable(mockOperationHandler));
        when(mockOperationHandler.handle(mockOperation)).thenReturn(mockC2OperationAck);
        when(mockOperationHandler.requiresRestart()).thenReturn(true);
        when(mockC2OperationAck.getOperationState()).thenReturn(mockC2OperationState);
        when(mockC2OperationState.getState()).thenReturn(FULLY_APPLIED);
        when(mockC2OperationRestartHandler.handleRestart(mockOperation)).thenReturn(empty());

        Future<?> future = executorService.submit(testC2OperationManager);
        testC2OperationManager.add(mockOperation);

        assertDoesNotThrow(() -> future.get());
        verify(mockC2Client, never()).acknowledgeOperation(mockC2OperationAck);
        verify(mockHeartbeatLock, times(1)).lock();
        verify(mockHeartbeatLock, never()).unlock();
        verify(mockOperationQueueDAO, times(1)).save(any());
        verify(mockOperationQueueDAO, never()).cleanup();
    }

    @Test
    void shouldProcessOperationWithFailedRestartDueToFailedResponse() {
        C2Operation mockOperation = mock(C2Operation.class);
        C2OperationHandler mockOperationHandler = mock(C2OperationHandler.class);
        C2OperationAck mockC2OperationAck = mock(C2OperationAck.class);
        C2OperationState mockC2OperationState = mock(C2OperationState.class);
        when(mockC2OperationHandlerProvider.getHandlerForOperation(mockOperation)).thenReturn(ofNullable(mockOperationHandler));
        when(mockOperationHandler.handle(mockOperation)).thenReturn(mockC2OperationAck);
        when(mockOperationHandler.requiresRestart()).thenReturn(true);
        when(mockC2OperationAck.getOperationState()).thenReturn(mockC2OperationState);
        when(mockC2OperationState.getState()).thenReturn(FULLY_APPLIED);
        when(mockC2OperationRestartHandler.handleRestart(mockOperation)).thenReturn(ofNullable(NOT_APPLIED));

        Future<?> future = executorService.submit(testC2OperationManager);
        testC2OperationManager.add(mockOperation);

        assertThrows(TimeoutException.class, () -> future.get(MAX_WAIT_TIME_MS, MILLISECONDS));
        verify(mockHeartbeatLock, times(1)).lock();
        verify(mockHeartbeatLock, times(1)).unlock();
        verify(mockC2Client, times(1)).acknowledgeOperation(mockC2OperationAck);
        verify(mockOperationQueueDAO, times(1)).save(any());
        verify(mockOperationQueueDAO, times(1)).cleanup();
    }

    @Test
    void shouldProcessOperationWithFailedRestartDueToException() {
        C2Operation mockOperation = mock(C2Operation.class);
        C2OperationHandler mockOperationHandler = mock(C2OperationHandler.class);
        C2OperationAck mockC2OperationAck = mock(C2OperationAck.class);
        C2OperationState mockC2OperationState = mock(C2OperationState.class);
        when(mockC2OperationHandlerProvider.getHandlerForOperation(mockOperation)).thenReturn(ofNullable(mockOperationHandler));
        when(mockOperationHandler.handle(mockOperation)).thenReturn(mockC2OperationAck);
        when(mockOperationHandler.requiresRestart()).thenReturn(true);
        when(mockC2OperationAck.getOperationState()).thenReturn(mockC2OperationState);
        when(mockC2OperationState.getState()).thenReturn(FULLY_APPLIED);
        when(mockC2OperationRestartHandler.handleRestart(mockOperation)).thenThrow(new RuntimeException());

        Future<?> future = executorService.submit(testC2OperationManager);
        testC2OperationManager.add(mockOperation);

        assertThrows(TimeoutException.class, () -> future.get(MAX_WAIT_TIME_MS, MILLISECONDS));
        verify(mockHeartbeatLock, times(1)).lock();
        verify(mockHeartbeatLock, times(1)).unlock();
        verify(mockC2Client, times(1)).acknowledgeOperation(mockC2OperationAck);
        verify(mockOperationQueueDAO, times(1)).save(any());
        verify(mockOperationQueueDAO, times(1)).cleanup();
    }

    @Test
    void shouldProcessStateWithOneCurrentAndNoRemainingOperations() {
        OperationQueue mockOperationQueue = mock(OperationQueue.class);
        C2Operation mockCurrentOperation = mock(C2Operation.class);
        when(mockOperationQueue.getCurrentOperation()).thenReturn(mockCurrentOperation);
        when(mockOperationQueue.getRemainingOperations()).thenReturn(List.of());
        when(mockOperationQueueDAO.load()).thenReturn(ofNullable(mockOperationQueue));
        when(mockC2OperationRestartHandler.waitForResponse()).thenReturn(ofNullable(FULLY_APPLIED));

        Future<?> future = executorService.submit(testC2OperationManager);

        assertThrows(TimeoutException.class, () -> future.get(MAX_WAIT_TIME_MS, MILLISECONDS));
        verify(mockHeartbeatLock, never()).lock();
        verify(mockHeartbeatLock, never()).unlock();
        verify(mockC2Client, times(1)).acknowledgeOperation(c2OperationAckArgumentCaptor.capture());
        assertEquals(FULLY_APPLIED, c2OperationAckArgumentCaptor.getValue().getOperationState().getState());
    }

    @Test
    void shouldProcessStateWithOneCurrentAndOneRemainingOperation() {
        OperationQueue mockOperationQueue = mock(OperationQueue.class);
        C2Operation mockCurrentOperation = mock(C2Operation.class);
        C2Operation mockRemainingOperation = mock(C2Operation.class);
        when(mockOperationQueue.getCurrentOperation()).thenReturn(mockCurrentOperation);
        when(mockOperationQueue.getRemainingOperations()).thenReturn(List.of(mockRemainingOperation));
        when(mockOperationQueueDAO.load()).thenReturn(ofNullable(mockOperationQueue));
        when(mockC2OperationRestartHandler.waitForResponse()).thenReturn(ofNullable(FULLY_APPLIED));
        C2OperationHandler mockOperationHandler = mock(C2OperationHandler.class);
        C2OperationAck mockC2OperationAck = mock(C2OperationAck.class);
        when(mockC2OperationHandlerProvider.getHandlerForOperation(mockRemainingOperation)).thenReturn(ofNullable(mockOperationHandler));
        when(mockOperationHandler.handle(mockRemainingOperation)).thenReturn(mockC2OperationAck);
        when(mockOperationHandler.requiresRestart()).thenReturn(false);

        Future<?> future = executorService.submit(testC2OperationManager);

        assertThrows(TimeoutException.class, () -> future.get(MAX_WAIT_TIME_MS, MILLISECONDS));
        verify(mockHeartbeatLock, times(1)).lock();
        verify(mockHeartbeatLock, times(1)).unlock();
        verify(mockC2Client, times(2)).acknowledgeOperation(any());
    }
}
