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
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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

    @InjectMocks
    private C2ClientService c2ClientService;

    @Test
    public void shouldSendHeartbeatAndAckWhenOperationPresent() {
        // given
        C2Heartbeat heartbeat = mock(C2Heartbeat.class);
        given(c2HeartbeatFactory.create(any())).willReturn(heartbeat);

        C2HeartbeatResponse hbResponse = new C2HeartbeatResponse();
        hbResponse.setRequestedOperations(generateOperation(1));
        given(client.publishHeartbeat(heartbeat)).willReturn(Optional.of(hbResponse));

        given(operationService.handleOperation(any())).willReturn(Optional.of(new C2OperationAck()));

        // when
        c2ClientService.sendHeartbeat(mock(RuntimeInfoWrapper.class));

        // then
        verify(c2HeartbeatFactory, times(1)).create(any());
        verify(client, times(1)).publishHeartbeat(heartbeat);
        verify(operationService, times(1)).handleOperation(any());
        verify(client, times(1)).acknowledgeOperation(any());
    }

    @Test
    public void shouldSendHeartbeatAndAckForMultipleOperationPresent() {
        // given
        C2Heartbeat heartbeat = mock(C2Heartbeat.class);
        given(c2HeartbeatFactory.create(any())).willReturn(heartbeat);

        C2HeartbeatResponse hbResponse = new C2HeartbeatResponse();
        int operationNum = 5;
        hbResponse.setRequestedOperations(generateOperation(operationNum));
        given(client.publishHeartbeat(heartbeat)).willReturn(Optional.of(hbResponse));

        given(operationService.handleOperation(any())).willReturn(Optional.of(new C2OperationAck()));

        // when
        c2ClientService.sendHeartbeat(mock(RuntimeInfoWrapper.class));

        // then
        verify(c2HeartbeatFactory, times(1)).create(any());
        verify(client, times(1)).publishHeartbeat(heartbeat);
        verify(operationService, times(operationNum)).handleOperation(any());
        verify(client, times(operationNum)).acknowledgeOperation(any());
    }

    @Test
    public void shouldHandleNoHeartbeatResponse() {
        // given
        C2Heartbeat heartbeat = mock(C2Heartbeat.class);
        given(c2HeartbeatFactory.create(any())).willReturn(heartbeat);
        given(client.publishHeartbeat(heartbeat)).willReturn(Optional.empty());

        // when
        c2ClientService.sendHeartbeat(mock(RuntimeInfoWrapper.class));

        // then
        verify(c2HeartbeatFactory, times(1)).create(any());
        verify(client, times(1)).publishHeartbeat(heartbeat);
        verify(operationService, times(0)).handleOperation(any());
        verify(client, times(0)).acknowledgeOperation(any());
    }

    @Test
    public void shouldNotHandleWhenThereAreNoOperationsSent() {
        // given
        C2Heartbeat heartbeat = mock(C2Heartbeat.class);
        given(c2HeartbeatFactory.create(any())).willReturn(heartbeat);

        C2HeartbeatResponse hbResponse = new C2HeartbeatResponse();
        given(client.publishHeartbeat(heartbeat)).willReturn(Optional.of(hbResponse));

        // when
        c2ClientService.sendHeartbeat(mock(RuntimeInfoWrapper.class));

        // then
        verify(c2HeartbeatFactory, times(1)).create(any());
        verify(client, times(1)).publishHeartbeat(heartbeat);
        verify(operationService, times(0)).handleOperation(any());
        verify(client, times(0)).acknowledgeOperation(any());
    }

    @Test
    public void shouldNotAckWhenOperationAckMissing() {
        // given
        C2Heartbeat heartbeat = mock(C2Heartbeat.class);
        given(c2HeartbeatFactory.create(any())).willReturn(heartbeat);

        C2HeartbeatResponse hbResponse = new C2HeartbeatResponse();
        hbResponse.setRequestedOperations(generateOperation(1));
        given(client.publishHeartbeat(heartbeat)).willReturn(Optional.of(hbResponse));

        given(operationService.handleOperation(any())).willReturn(Optional.empty());

        // when
        c2ClientService.sendHeartbeat(mock(RuntimeInfoWrapper.class));

        // then
        verify(c2HeartbeatFactory, times(1)).create(any());
        verify(client, times(1)).publishHeartbeat(heartbeat);
        verify(operationService, times(1)).handleOperation(any());
        verify(client, times(0)).acknowledgeOperation(any());
    }

    private List<C2Operation> generateOperation(int num) {
        return IntStream.range(0, num)
            .mapToObj(x -> new C2Operation())
            .collect(Collectors.toList());
    }
}
