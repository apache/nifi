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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.nifi.c2.client.api.C2Client;
import org.apache.nifi.c2.client.service.model.RuntimeInfoWrapper;
import org.apache.nifi.c2.client.service.operation.C2OperationService;
import org.apache.nifi.c2.protocol.api.C2Heartbeat;
import org.apache.nifi.c2.protocol.api.C2HeartbeatResponse;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
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
    private C2OperationService operationService;

    @Mock
    private RuntimeInfoWrapper runtimeInfoWrapper;

    @InjectMocks
    private C2ClientService c2ClientService;

    @Test
    void testSendHeartbeatAndAckWhenOperationPresent() {
        C2Heartbeat heartbeat = mock(C2Heartbeat.class);
        when(c2HeartbeatFactory.create(any())).thenReturn(heartbeat);
        C2HeartbeatResponse hbResponse = new C2HeartbeatResponse();
        hbResponse.setRequestedOperations(generateOperation(1));
        when(client.publishHeartbeat(heartbeat)).thenReturn(Optional.of(hbResponse));
        when(operationService.handleOperation(any())).thenReturn(Optional.of(new C2OperationAck()));

        c2ClientService.sendHeartbeat(runtimeInfoWrapper);

        verify(c2HeartbeatFactory).create(any());
        verify(client).publishHeartbeat(heartbeat);
        verify(operationService).handleOperation(any());
        verify(client).acknowledgeOperation(any());
    }

    @Test
    void testSendHeartbeatAndAckForMultipleOperationPresent() {
        int operationNum = 5;
        C2Heartbeat heartbeat = mock(C2Heartbeat.class);
        when(c2HeartbeatFactory.create(any())).thenReturn(heartbeat);
        C2HeartbeatResponse hbResponse = new C2HeartbeatResponse();
        hbResponse.setRequestedOperations(generateOperation(operationNum));
        when(client.publishHeartbeat(heartbeat)).thenReturn(Optional.of(hbResponse));
        when(operationService.handleOperation(any())).thenReturn(Optional.of(new C2OperationAck()));

        c2ClientService.sendHeartbeat(runtimeInfoWrapper);

        verify(c2HeartbeatFactory).create(any());
        verify(client).publishHeartbeat(heartbeat);
        verify(operationService, times(operationNum)).handleOperation(any());
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
        verify(operationService, times(0)).handleOperation(any());
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
        verify(operationService, times(0)).handleOperation(any());
        verify(client, times(0)).acknowledgeOperation(any());
    }

    @Test
    void testSendHeartbeatNotAckWhenOperationAckMissing() {
        C2Heartbeat heartbeat = mock(C2Heartbeat.class);
        when(c2HeartbeatFactory.create(any())).thenReturn(heartbeat);
        C2HeartbeatResponse hbResponse = new C2HeartbeatResponse();
        hbResponse.setRequestedOperations(generateOperation(1));
        when(client.publishHeartbeat(heartbeat)).thenReturn(Optional.of(hbResponse));
        when(operationService.handleOperation(any())).thenReturn(Optional.empty());

        c2ClientService.sendHeartbeat(runtimeInfoWrapper);

        verify(c2HeartbeatFactory).create(any());
        verify(client).publishHeartbeat(heartbeat);
        verify(operationService).handleOperation(any());
        verify(client, times(0)).acknowledgeOperation(any());
    }

    private List<C2Operation> generateOperation(int num) {
        return IntStream.range(0, num)
            .mapToObj(x -> new C2Operation())
            .collect(Collectors.toList());
    }
}
