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

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.nifi.c2.client.service.operation.UpdateConfigurationOperationHandler.FLOW_ID;
import static org.apache.nifi.c2.client.service.operation.UpdateConfigurationOperationHandler.LOCATION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.nifi.c2.client.api.C2Client;
import org.apache.nifi.c2.client.service.FlowIdHolder;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.apache.nifi.c2.protocol.api.C2OperationState;
import org.apache.nifi.c2.protocol.api.OperandType;
import org.apache.nifi.c2.protocol.api.OperationType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class UpdateConfigurationOperationHandlerTest {
    private static final String OPERATION_ID = "operationId";
    private static final String CORRECT_LOCATION = "/path/for/the/" + FLOW_ID;
    private static final String INCORRECT_LOCATION = "incorrect/location";

    private static final Map<String, Object> CORRECT_LOCATION_MAP = Collections.singletonMap(LOCATION, CORRECT_LOCATION);
    private static final Map<String, Object> INCORRECT_LOCATION_MAP = Collections.singletonMap(LOCATION, INCORRECT_LOCATION);

    @Mock
    private FlowIdHolder flowIdHolder;
    @Mock
    private C2Client client;
    @Mock
    private OperandPropertiesProvider operandPropertiesProvider;

    @Test
    void testUpdateConfigurationOperationHandlerCreateSuccess() {
        UpdateConfigurationOperationHandler handler = new UpdateConfigurationOperationHandler(null, null, null, operandPropertiesProvider);

        assertEquals(OperationType.UPDATE, handler.getOperationType());
        assertEquals(OperandType.CONFIGURATION, handler.getOperandType());
    }

    @Test
    void testHandleIncorrectArg() {
        UpdateConfigurationOperationHandler handler = new UpdateConfigurationOperationHandler(client, null, null, operandPropertiesProvider);
        C2Operation operation = new C2Operation();
        operation.setArgs(INCORRECT_LOCATION_MAP);

        when(client.getCallbackUrl(any(), any())).thenReturn(INCORRECT_LOCATION);

        C2OperationAck response = handler.handle(operation);

        assertEquals(C2OperationState.OperationState.NOT_APPLIED, response.getOperationState().getState());
    }

    @Test
    void testHandleFlowIdInArg() {
        UpdateConfigurationStrategy successUpdate = mock(UpdateConfigurationStrategy.class);
        when(flowIdHolder.getFlowId()).thenReturn(FLOW_ID);
        when(client.retrieveUpdateConfigurationContent(any())).thenReturn(Optional.of("content".getBytes()));
        when(client.getCallbackUrl(any(), any())).thenReturn(INCORRECT_LOCATION);
        UpdateConfigurationOperationHandler handler = new UpdateConfigurationOperationHandler(client, flowIdHolder, successUpdate, operandPropertiesProvider);
        C2Operation operation = new C2Operation();
        operation.setIdentifier(OPERATION_ID);

        Map<String, Object> args = new HashMap<>();
        args.putAll(INCORRECT_LOCATION_MAP);
        args.put(FLOW_ID, "argsFlowId");
        operation.setArgs(args);

        C2OperationAck response = handler.handle(operation);

        assertEquals(OPERATION_ID, response.getOperationId());
        assertEquals(C2OperationState.OperationState.FULLY_APPLIED, response.getOperationState().getState());
    }

    @Test
    void testHandleReturnsNoOperationWithNoContent() {
        when(flowIdHolder.getFlowId()).thenReturn(FLOW_ID);
        when(client.getCallbackUrl(any(), any())).thenReturn(CORRECT_LOCATION);
        UpdateConfigurationOperationHandler handler = new UpdateConfigurationOperationHandler(client, flowIdHolder, null, operandPropertiesProvider);
        C2Operation operation = new C2Operation();
        operation.setArgs(CORRECT_LOCATION_MAP);

        C2OperationAck response = handler.handle(operation);

        assertEquals(EMPTY, response.getOperationId());
        assertEquals(C2OperationState.OperationState.NO_OPERATION, response.getOperationState().getState());
    }

    @Test
    void testHandleReturnsNotAppliedWithContentApplyIssues() {
        UpdateConfigurationStrategy failedToUpdate = mock(UpdateConfigurationStrategy.class);
        doThrow(new IllegalStateException()).when(failedToUpdate).update(any());
        when(flowIdHolder.getFlowId()).thenReturn("previous_flow_id");
        when(client.retrieveUpdateConfigurationContent(any())).thenReturn(Optional.of("content".getBytes()));
        when(client.getCallbackUrl(any(), any())).thenReturn(CORRECT_LOCATION);
        UpdateConfigurationOperationHandler handler = new UpdateConfigurationOperationHandler(client, flowIdHolder, failedToUpdate, operandPropertiesProvider);
        C2Operation operation = new C2Operation();
        operation.setIdentifier(OPERATION_ID);
        operation.setArgs(CORRECT_LOCATION_MAP);

        C2OperationAck response = handler.handle(operation);

        assertEquals(OPERATION_ID, response.getOperationId());
        assertEquals(C2OperationState.OperationState.NOT_APPLIED, response.getOperationState().getState());
    }

    @Test
    void testHandleReturnsFullyApplied() {
        UpdateConfigurationStrategy successUpdate = mock(UpdateConfigurationStrategy.class);
        when(flowIdHolder.getFlowId()).thenReturn("previous_flow_id");
        when(client.getCallbackUrl(any(), any())).thenReturn(CORRECT_LOCATION);
        when(client.retrieveUpdateConfigurationContent(any())).thenReturn(Optional.of("content".getBytes()));
        UpdateConfigurationOperationHandler handler = new UpdateConfigurationOperationHandler(client, flowIdHolder, successUpdate, operandPropertiesProvider);
        C2Operation operation = new C2Operation();
        operation.setIdentifier(OPERATION_ID);
        operation.setArgs(CORRECT_LOCATION_MAP);

        C2OperationAck response = handler.handle(operation);

        verify(flowIdHolder, times(1)).setFlowId(FLOW_ID);
        assertEquals(OPERATION_ID, response.getOperationId());
        assertEquals(C2OperationState.OperationState.FULLY_APPLIED, response.getOperationState().getState());
    }

}
