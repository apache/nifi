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

import java.util.stream.Stream;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.apache.nifi.c2.protocol.api.C2OperationState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.nifi.c2.client.service.operation.StopFlowOperationHandler.FULLY_APPLIED_DETAILS;
import static org.apache.nifi.c2.client.service.operation.StopFlowOperationHandler.NOT_APPLIED_DETAILS;
import static org.apache.nifi.c2.client.service.operation.StopFlowOperationHandler.PARTIALLY_APPLIED_DETAILS;
import static org.apache.nifi.c2.client.service.operation.StopFlowOperationHandler.UNEXPECTED_DETAILS;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.FULLY_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NOT_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NO_OPERATION;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.PARTIALLY_APPLIED;
import static org.apache.nifi.c2.protocol.api.OperandType.FLOW;
import static org.apache.nifi.c2.protocol.api.OperationType.STOP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StopFlowOperationHandlerTest {

    private static final String OPERATION_ID = "operation id";

    @Mock
    private FlowStateStrategy flowStateStrategy;

    @InjectMocks
    private StopFlowOperationHandler victim;

    @Test
    public void testOperationAndOperandTypes() {
        assertEquals(STOP, victim.getOperationType());
        assertEquals(FLOW, victim.getOperandType());
    }

    @ParameterizedTest(name = "operationId={0} ackOperationId={1} ackState={2} ackDetails={3}")
    @MethodSource("testHandleArguments")
    public void testHandle(String operationId, String ackOperationId, C2OperationState.OperationState ackState, String ackDetails) {
        when(flowStateStrategy.stop()).thenReturn(ackState);

        C2OperationAck result = victim.handle(anOperation(operationId));

        assertEquals(ackOperationId, result.getOperationId());
        assertEquals(ackState, result.getOperationState().getState());
        assertEquals(ackDetails, result.getOperationState().getDetails());
    }

    private C2Operation anOperation(String identifier) {
        C2Operation operation = new C2Operation();
        operation.setIdentifier(identifier);

        return operation;
    }

    private static Stream<Arguments> testHandleArguments() {
        return Stream.of(
                Arguments.of(null, EMPTY, NOT_APPLIED, NOT_APPLIED_DETAILS),
                Arguments.of(null, EMPTY, FULLY_APPLIED, FULLY_APPLIED_DETAILS),
                Arguments.of(null, EMPTY, PARTIALLY_APPLIED, PARTIALLY_APPLIED_DETAILS),
                Arguments.of(null, EMPTY, NO_OPERATION, UNEXPECTED_DETAILS),
                Arguments.of(OPERATION_ID, OPERATION_ID, NOT_APPLIED, NOT_APPLIED_DETAILS),
                Arguments.of(OPERATION_ID, OPERATION_ID, FULLY_APPLIED, FULLY_APPLIED_DETAILS),
                Arguments.of(OPERATION_ID, OPERATION_ID, PARTIALLY_APPLIED, PARTIALLY_APPLIED_DETAILS),
                Arguments.of(OPERATION_ID, OPERATION_ID, NO_OPERATION, UNEXPECTED_DETAILS)
        );
    }
}