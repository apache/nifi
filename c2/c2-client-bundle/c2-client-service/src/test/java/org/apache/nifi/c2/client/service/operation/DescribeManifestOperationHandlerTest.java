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
package org.apache.nifi.c2.client.service.operation;

import static org.apache.nifi.c2.protocol.api.RunStatus.RUNNING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import org.apache.nifi.c2.client.service.C2HeartbeatFactory;
import org.apache.nifi.c2.client.service.model.RuntimeInfoWrapper;
import org.apache.nifi.c2.protocol.api.AgentInfo;
import org.apache.nifi.c2.protocol.api.C2Heartbeat;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.apache.nifi.c2.protocol.api.C2OperationState;
import org.apache.nifi.c2.protocol.api.DeviceInfo;
import org.apache.nifi.c2.protocol.api.FlowInfo;
import org.apache.nifi.c2.protocol.api.OperandType;
import org.apache.nifi.c2.protocol.api.OperationType;
import org.apache.nifi.c2.protocol.component.api.RuntimeManifest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DescribeManifestOperationHandlerTest {

    private static final String OPERATION_ID = "operationId";

    @Mock
    private C2HeartbeatFactory heartbeatFactory;
    @Mock
    private OperandPropertiesProvider operandPropertiesProvider;

    @Test
    void testDescribeManifestOperationHandlerCreateSuccess() {
        DescribeManifestOperationHandler handler = new DescribeManifestOperationHandler(null, null, operandPropertiesProvider);

        assertEquals(OperationType.DESCRIBE, handler.getOperationType());
        assertEquals(OperandType.MANIFEST, handler.getOperandType());
    }

    @Test
    void testDescribeManifestOperationHandlerPopulatesAckSuccessfully() {
        RuntimeManifest manifest = new RuntimeManifest();
        manifest.setIdentifier("manifestId");
        RuntimeInfoWrapper runtimeInfoWrapper = new RuntimeInfoWrapper(null, manifest, null, null, null, RUNNING);

        C2Heartbeat heartbeat = new C2Heartbeat();
        AgentInfo agentInfo = new AgentInfo();
        DeviceInfo deviceInfo = new DeviceInfo();
        FlowInfo flowInfo = new FlowInfo();
        heartbeat.setAgentInfo(agentInfo);
        heartbeat.setDeviceInfo(deviceInfo);
        heartbeat.setFlowInfo(flowInfo);

        when(heartbeatFactory.create(runtimeInfoWrapper)).thenReturn(heartbeat);
        DescribeManifestOperationHandler handler = new DescribeManifestOperationHandler(heartbeatFactory, () -> runtimeInfoWrapper, operandPropertiesProvider);

        C2Operation operation = new C2Operation();
        operation.setIdentifier(OPERATION_ID);

        C2OperationAck response = handler.handle(operation);

        assertEquals(OPERATION_ID, response.getOperationId());
        assertEquals(C2OperationState.OperationState.FULLY_APPLIED, response.getOperationState().getState());
        assertEquals(agentInfo, response.getAgentInfo());
        assertEquals(manifest, response.getAgentInfo().getAgentManifest());
        assertEquals(deviceInfo, response.getDeviceInfo());
        assertEquals(flowInfo, response.getFlowInfo());
    }
}
