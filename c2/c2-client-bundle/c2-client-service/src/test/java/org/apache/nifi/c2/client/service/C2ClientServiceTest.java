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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.nifi.c2.client.api.C2Client;
import org.apache.nifi.c2.client.service.model.RuntimeInfoWrapper;
import org.apache.nifi.c2.client.service.operation.C2OperationHandler;
import org.apache.nifi.c2.client.service.operation.C2OperationHandlerProvider;
import org.apache.nifi.c2.client.service.operation.OperationQueue;
import org.apache.nifi.c2.client.service.operation.RequestedOperationDAO;
import org.apache.nifi.c2.protocol.api.C2Heartbeat;
import org.apache.nifi.c2.protocol.api.C2HeartbeatResponse;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.apache.nifi.c2.protocol.api.C2OperationState;
import org.apache.nifi.c2.protocol.api.C2OperationState.OperationState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class C2ClientServiceTest {

    @Mock
    private C2Client client;

    @Mock
    private C2HeartbeatFactory c2HeartbeatFactory;

    @Mock
    private C2OperationHandlerProvider operationService;

    @Mock
    private RuntimeInfoWrapper runtimeInfoWrapper;

    @Mock
    private RequestedOperationDAO requestedOperationDAO;

    @Mock
    private Consumer<C2Operation> c2OperationRegister;

    @InjectMocks
    private C2ClientService c2ClientService;

    @Test
    void testSendHeartbeatAndAckWhenOperationPresent() {
        C2Heartbeat heartbeat = mock(C2Heartbeat.class);
        when(c2HeartbeatFactory.create(any())).thenReturn(heartbeat);
        C2HeartbeatResponse hbResponse = new C2HeartbeatResponse();
        final List<C2Operation> c2Operations = generateOperation(1);
        hbResponse.setRequestedOperations(c2Operations);
        when(client.publishHeartbeat(heartbeat)).thenReturn(Optional.of(hbResponse));
        C2OperationHandler c2OperationHandler = mock(C2OperationHandler.class);
        when(operationService.getHandlerForOperation(any())).thenReturn(Optional.of(c2OperationHandler));
        when(c2OperationHandler.handle(c2Operations.get(0))).thenReturn(new C2OperationAck());

        c2ClientService.sendHeartbeat(runtimeInfoWrapper);

        verify(c2HeartbeatFactory).create(any());
        verify(client).publishHeartbeat(heartbeat);
        verify(c2OperationHandler).handle(any());
        verify(client).acknowledgeOperation(any());
    }

    @Test
    void testSendHeartbeatAndAckForMultipleOperationPresent() {
        int operationNum = 5;
        C2Heartbeat heartbeat = mock(C2Heartbeat.class);
        when(c2HeartbeatFactory.create(any())).thenReturn(heartbeat);
        C2HeartbeatResponse hbResponse = new C2HeartbeatResponse();
        hbResponse.setRequestedOperations(generateOperation(operationNum));
        C2OperationHandler c2OperationHandler = mock(C2OperationHandler.class);
        when(client.publishHeartbeat(heartbeat)).thenReturn(Optional.of(hbResponse));
        when(operationService.getHandlerForOperation(any())).thenReturn(Optional.of(c2OperationHandler));
        when(c2OperationHandler.handle(any())).thenReturn(new C2OperationAck());

        c2ClientService.sendHeartbeat(runtimeInfoWrapper);

        verify(c2HeartbeatFactory).create(any());
        verify(client).publishHeartbeat(heartbeat);
        verify(c2OperationHandler, times(operationNum)).handle(any());
        verify(client, times(operationNum)).acknowledgeOperation(any());
    }

    @Test
    void testSendHeartbeatHandlesNoHeartbeatResponse() {
        C2Heartbeat heartbeat = mock(C2Heartbeat.class);
        when(c2HeartbeatFactory.create(any())).thenReturn(heartbeat);
        when(client.publishHeartbeat(heartbeat)).thenReturn(Optional.empty());

        c2ClientService.sendHeartbeat(runtimeInfoWrapper);

        verify(c2HeartbeatFactory).create(any());
        verify(client).publishHeartbeat(heartbeat);
        verify(client, times(0)).acknowledgeOperation(any());
    }

    @Test
    void testSendHeartbeatNotHandledWhenThereAreNoOperationsSent() {
        C2Heartbeat heartbeat = mock(C2Heartbeat.class);
        when(c2HeartbeatFactory.create(any())).thenReturn(heartbeat);
        C2HeartbeatResponse hbResponse = new C2HeartbeatResponse();
        when(client.publishHeartbeat(heartbeat)).thenReturn(Optional.of(hbResponse));

        c2ClientService.sendHeartbeat(runtimeInfoWrapper);

        verify(c2HeartbeatFactory).create(any());
        verify(client).publishHeartbeat(heartbeat);
        verify(client, times(0)).acknowledgeOperation(any());
    }

    @Test
    void testSendHeartbeatNotAckWhenOperationAckMissing() {
        C2Heartbeat heartbeat = mock(C2Heartbeat.class);
        when(c2HeartbeatFactory.create(any())).thenReturn(heartbeat);
        C2HeartbeatResponse hbResponse = new C2HeartbeatResponse();
        hbResponse.setRequestedOperations(generateOperation(1));
        when(client.publishHeartbeat(heartbeat)).thenReturn(Optional.of(hbResponse));
        when(operationService.getHandlerForOperation(any())).thenReturn(Optional.empty());

        c2ClientService.sendHeartbeat(runtimeInfoWrapper);

        verify(c2HeartbeatFactory).create(any());
        verify(client).publishHeartbeat(heartbeat);
        verify(client, times(0)).acknowledgeOperation(any());
    }

    @Test
    void shouldHeartbeatSendingNotPropagateExceptions() {
        when(c2HeartbeatFactory.create(runtimeInfoWrapper)).thenThrow(new RuntimeException());

        c2ClientService.sendHeartbeat(runtimeInfoWrapper);
    }

    @Test
    void shouldAckSendingNotPropagateExceptions() {
        C2OperationAck operationAck = mock(C2OperationAck.class);
        doThrow(new RuntimeException()).when(client).acknowledgeOperation(operationAck);

        c2ClientService.sendAcknowledge(operationAck);
    }

    @Test
    void shouldSendAcknowledgeWithoutPersistingOperationsWhenOperationRequiresRestartButHandlerReturnsNonFullyAppliedState() {
        C2OperationHandler c2OperationHandler = mock(C2OperationHandler.class);
        C2OperationAck operationAck = new C2OperationAck();
        C2OperationState c2OperationState = new C2OperationState();
        c2OperationState.setState(OperationState.NOT_APPLIED);
        operationAck.setOperationState(c2OperationState);
        when(c2OperationHandler.requiresRestart()).thenReturn(true);
        when(operationService.getHandlerForOperation(any(C2Operation.class))).thenReturn(Optional.of(c2OperationHandler));
        when(c2OperationHandler.handle(any(C2Operation.class))).thenReturn(operationAck);

        c2ClientService.handleRequestedOperations(generateOperation(1));

        verify(operationService).getHandlerForOperation(any(C2Operation.class));
        verify(c2OperationHandler).handle(any(C2Operation.class));
        verify(requestedOperationDAO).cleanup();
        verify(client).acknowledgeOperation(operationAck);
        verifyNoMoreInteractions(operationService, client, requestedOperationDAO);
        verifyNoInteractions(c2HeartbeatFactory, c2OperationRegister);
    }

    @Test
    void shouldSaveOperationQueueIfRestartIsNeededAndThereAreMultipleRequestedOperations() {
        C2Operation c2Operation1 = new C2Operation();
        c2Operation1.setIdentifier("1");
        C2Operation c2Operation2 = new C2Operation();
        c2Operation2.setIdentifier("2");
        C2OperationHandler c2OperationHandler = mock(C2OperationHandler.class);
        when(c2OperationHandler.requiresRestart()).thenReturn(true);
        when(operationService.getHandlerForOperation(any(C2Operation.class))).thenReturn(Optional.of(c2OperationHandler));
        C2OperationAck c2OperationAck = new C2OperationAck();
        C2OperationState c2OperationState = new C2OperationState();
        c2OperationState.setState(OperationState.FULLY_APPLIED);
        c2OperationAck.setOperationState(c2OperationState);
        when(c2OperationHandler.handle(any(C2Operation.class))).thenReturn(c2OperationAck);

        c2ClientService.handleRequestedOperations(Arrays.asList(c2Operation1, c2Operation2));

        verify(requestedOperationDAO).save(new OperationQueue(c2Operation1, Collections.singletonList(c2Operation2)));
        verify(c2OperationRegister).accept(c2Operation1);
        verifyNoInteractions(client, c2HeartbeatFactory);
        verifyNoMoreInteractions(requestedOperationDAO, c2OperationRegister, operationService);
    }

    @Test
    void shouldReEnableHeartbeatsIfExceptionHappensDuringRegisteringOperationAndThereIsNoMoreOperationInQueue() {
        C2OperationHandler c2OperationHandler = mock(C2OperationHandler.class);
        C2Operation operation = new C2Operation();
        when(c2OperationHandler.requiresRestart()).thenReturn(true);
        when(operationService.getHandlerForOperation(any(C2Operation.class))).thenReturn(Optional.of(c2OperationHandler));
        C2OperationAck c2OperationAck = new C2OperationAck();
        C2OperationState c2OperationState = new C2OperationState();
        c2OperationState.setState(OperationState.FULLY_APPLIED);
        c2OperationAck.setOperationState(c2OperationState);
        when(c2OperationHandler.handle(any(C2Operation.class))).thenReturn(c2OperationAck);
        doThrow(new RuntimeException()).when(c2OperationRegister).accept(any(C2Operation.class));
        c2ClientService.handleRequestedOperations(Collections.singletonList(operation));
        when(c2HeartbeatFactory.create(runtimeInfoWrapper)).thenReturn(new C2Heartbeat());

        c2ClientService.sendHeartbeat(runtimeInfoWrapper);

        verify(c2HeartbeatFactory).create(runtimeInfoWrapper);
        verify(client).publishHeartbeat(any(C2Heartbeat.class));
    }

    @Test
    void shouldContinueWithRemainingOperationsIfExceptionHappensDuringRegisteringOperationAndThereAreMoreOperationsInQueue() {
        C2OperationHandler c2OperationHandlerForRestart = mock(C2OperationHandler.class);
        C2OperationHandler c2OperationHandler = mock(C2OperationHandler.class);
        C2Operation operation1 = new C2Operation();
        operation1.setIdentifier("1");
        C2Operation operation2 = new C2Operation();
        operation2.setIdentifier("2");
        C2OperationAck c2OperationAck = new C2OperationAck();
        C2OperationState c2OperationState = new C2OperationState();
        c2OperationState.setState(OperationState.FULLY_APPLIED);
        c2OperationAck.setOperationState(c2OperationState);
        when(c2OperationHandler.requiresRestart()).thenReturn(false);
        when(c2OperationHandlerForRestart.requiresRestart()).thenReturn(true);
        when(operationService.getHandlerForOperation(operation1)).thenReturn(Optional.of(c2OperationHandlerForRestart));
        when(operationService.getHandlerForOperation(operation2)).thenReturn(Optional.of(c2OperationHandler));
        when(c2OperationHandlerForRestart.handle(operation1)).thenReturn(c2OperationAck);
        when(c2OperationHandler.handle(operation2)).thenReturn(c2OperationAck);

        doThrow(new RuntimeException()).when(c2OperationRegister).accept(operation1);

        c2ClientService.handleRequestedOperations(Arrays.asList(operation1, operation2));

        verify(client, times(2)).acknowledgeOperation(c2OperationAck);
    }

    private List<C2Operation> generateOperation(int num) {
        return IntStream.range(0, num)
            .mapToObj(x -> new C2Operation())
            .collect(Collectors.toList());
    }
}
