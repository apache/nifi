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

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.nifi.c2.client.service.operation.StartProcessorOperationHandler.FULLY_APPLIED_DETAILS;
import static org.apache.nifi.c2.client.service.operation.StartProcessorOperationHandler.NOT_APPLIED_DETAILS;
import static org.apache.nifi.c2.client.service.operation.StartProcessorOperationHandler.PARTIALLY_APPLIED_DETAILS;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.FULLY_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NOT_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NO_OPERATION;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.PARTIALLY_APPLIED;
import static org.apache.nifi.c2.protocol.api.OperandType.PROCESSOR;
import static org.apache.nifi.c2.protocol.api.OperationType.START;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StartProcessorOperationHandlerTest {

    private static final String OPERATION_ID = "operation id";
    private static final String PROCESSOR_ID = "processor id";

    @Mock
    private ProcessorStateStrategy processorStateStrategy;

    @InjectMocks
    private StartProcessorOperationHandler victim;

    @Test
    public void testOperationAndOperandTypes() {
        assertEquals(START, victim.getOperationType());
        assertEquals(PROCESSOR, victim.getOperandType());
    }

    @ParameterizedTest(name = "operationId={0} ackOperationId={1} state={2} details={3}")
    @MethodSource("handleArguments")
    public void testHandle(String operationId, String expectedAckOperationId, C2OperationState.OperationState state, String expectedDetails) {
        when(processorStateStrategy.startProcessor(PROCESSOR_ID)).thenReturn(state);

        C2Operation operation = anOperation(operationId, PROCESSOR_ID);
        C2OperationAck ack = victim.handle(operation);

        assertEquals(expectedAckOperationId, ack.getOperationId());
        assertEquals(state, ack.getOperationState().getState());
        assertEquals(expectedDetails, ack.getOperationState().getDetails());
    }

    private static Stream<Arguments> handleArguments() {
        return Stream.of(
            Arguments.of(null, EMPTY, NOT_APPLIED, NOT_APPLIED_DETAILS),
            Arguments.of(null, EMPTY, FULLY_APPLIED, FULLY_APPLIED_DETAILS),
            Arguments.of(null, EMPTY, PARTIALLY_APPLIED, PARTIALLY_APPLIED_DETAILS),
            Arguments.of(null, EMPTY, NO_OPERATION, PARTIALLY_APPLIED_DETAILS),
            Arguments.of(OPERATION_ID, OPERATION_ID, NOT_APPLIED, NOT_APPLIED_DETAILS),
            Arguments.of(OPERATION_ID, OPERATION_ID, FULLY_APPLIED, FULLY_APPLIED_DETAILS),
            Arguments.of(OPERATION_ID, OPERATION_ID, PARTIALLY_APPLIED, PARTIALLY_APPLIED_DETAILS),
            Arguments.of(OPERATION_ID, OPERATION_ID, NO_OPERATION, PARTIALLY_APPLIED_DETAILS)
        );
    }

    @Test
    public void testHandleMissingProcessorId() {
        C2Operation operation = new C2Operation();
        operation.setIdentifier(OPERATION_ID);
        C2OperationAck ack = victim.handle(operation);
        assertEquals(OPERATION_ID, ack.getOperationId());
        assertEquals(NOT_APPLIED, ack.getOperationState().getState());
        assertEquals(NOT_APPLIED_DETAILS, ack.getOperationState().getDetails());
    }

    private C2Operation anOperation(String operationId, String processorId) {
        C2Operation operation = new C2Operation();
        operation.setIdentifier(operationId);
        Map<String, Object> args = new HashMap<>();
        args.put(StartProcessorOperationHandler.PROCESSOR_ID_ARG, processorId);
        operation.setArgs(args);
        return operation;
    }
}
