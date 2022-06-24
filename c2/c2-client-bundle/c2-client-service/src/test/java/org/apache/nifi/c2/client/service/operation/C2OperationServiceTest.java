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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.Optional;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.apache.nifi.c2.protocol.api.OperandType;
import org.apache.nifi.c2.protocol.api.OperationType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class C2OperationServiceTest {

    private static C2OperationAck operationAck;

    @BeforeAll
    public static void setup(){
        operationAck = new C2OperationAck();
        operationAck.setOperationId("12345");
    }

    @Test
    void testHandleOperationReturnsEmptyForUnrecognisedOperationType() {
        C2OperationService service = new C2OperationService(Collections.emptyList());

        C2Operation operation = new C2Operation();
        operation.setOperation(OperationType.UPDATE);
        operation.setOperand(OperandType.CONFIGURATION);
        Optional<C2OperationAck> ack = service.handleOperation(operation);

        assertFalse(ack.isPresent());
    }

    @Test
    void testHandleOperation() {
        C2OperationService service = new C2OperationService(Collections.singletonList(new TestDescribeOperationHandler()));

        C2Operation operation = new C2Operation();
        operation.setOperation(OperationType.DESCRIBE);
        operation.setOperand(OperandType.MANIFEST);
        Optional<C2OperationAck> ack = service.handleOperation(operation);

        assertTrue(ack.isPresent());
        assertEquals(operationAck, ack.get());
    }

    @Test
    void testHandleOperationReturnsEmptyForOperandMismatch() {
        C2OperationService service = new C2OperationService(Collections.singletonList(new TestInvalidOperationHandler()));

        C2Operation operation = new C2Operation();
        operation.setOperation(OperationType.DESCRIBE);
        operation.setOperand(OperandType.MANIFEST);
        Optional<C2OperationAck> ack = service.handleOperation(operation);

        assertFalse(ack.isPresent());
    }

    private static class TestDescribeOperationHandler implements C2OperationHandler {

        @Override
        public OperationType getOperationType() {
            return OperationType.DESCRIBE;
        }

        @Override
        public OperandType getOperandType() {
            return OperandType.MANIFEST;
        }

        @Override
        public C2OperationAck handle(C2Operation operation) {
            return operationAck;
        }
    }

    private static class TestInvalidOperationHandler implements C2OperationHandler {

        @Override
        public OperationType getOperationType() {
            return OperationType.DESCRIBE;
        }

        @Override
        public OperandType getOperandType() {
            return OperandType.CONFIGURATION;
        }

        @Override
        public C2OperationAck handle(C2Operation operation) {
            return null;
        }
    }
}
