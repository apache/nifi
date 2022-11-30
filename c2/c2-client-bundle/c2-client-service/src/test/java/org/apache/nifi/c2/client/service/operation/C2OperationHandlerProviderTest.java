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

import static org.apache.nifi.c2.protocol.api.OperandType.CONFIGURATION;
import static org.apache.nifi.c2.protocol.api.OperandType.MANIFEST;
import static org.apache.nifi.c2.protocol.api.OperationType.DESCRIBE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.apache.nifi.c2.protocol.api.OperandType;
import org.apache.nifi.c2.protocol.api.OperationType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class C2OperationHandlerProviderTest {

    private static C2OperationAck operationAck;

    @BeforeAll
    public static void setup(){
        operationAck = new C2OperationAck();
        operationAck.setOperationId("12345");
    }

    @Test
    void testGetHandlerForReturnsEmptyForUnrecognisedOperationType() {
        C2OperationHandlerProvider service = new C2OperationHandlerProvider(Collections.emptyList());

        C2Operation operation = new C2Operation();
        operation.setOperation(DESCRIBE);
        operation.setOperand(MANIFEST);
        Optional<C2OperationHandler> handler = service.getHandlerForOperation(operation);

        assertFalse(handler.isPresent());
    }

    @Test
    void testGetHandlerForOperationReturnsEmptyForOperandMismatch() {
        C2OperationHandlerProvider service = new C2OperationHandlerProvider(Collections.singletonList(new TestInvalidOperationHandler()));

        C2Operation operation = new C2Operation();
        operation.setOperation(DESCRIBE);
        operation.setOperand(MANIFEST);
        Optional<C2OperationHandler> handler = service.getHandlerForOperation(operation);

        assertFalse(handler.isPresent());
    }

    @Test
    void testHandleOperation() {
        TestDescribeOperationHandler describeOperationHandler = new TestDescribeOperationHandler();
        C2OperationHandlerProvider service = new C2OperationHandlerProvider(Collections.singletonList(describeOperationHandler));

        C2Operation operation = new C2Operation();
        operation.setOperation(DESCRIBE);
        operation.setOperand(MANIFEST);
        Optional<C2OperationHandler> handler = service.getHandlerForOperation(operation);

        assertTrue(handler.isPresent());
        assertEquals(describeOperationHandler, handler.get());
    }

    @Test
    void testHandlersAreReturned() {
        C2OperationHandlerProvider service = new C2OperationHandlerProvider(Arrays.asList(new TestDescribeOperationHandler(), new TestInvalidOperationHandler()));

        Map<OperationType, Map<OperandType, C2OperationHandler>> handlers = service.getHandlers();

        assertEquals(1, handlers.keySet().size());
        assertTrue(handlers.keySet().contains(DESCRIBE));
        Map<OperandType, C2OperationHandler> operands = handlers.values().stream().findFirst().get();
        assertEquals(2, operands.size());
        assertTrue(operands.keySet().containsAll(Arrays.asList(MANIFEST, CONFIGURATION)));
    }

    private static class TestDescribeOperationHandler implements C2OperationHandler {

        @Override
        public OperationType getOperationType() {
            return DESCRIBE;
        }

        @Override
        public OperandType getOperandType() {
            return MANIFEST;
        }

        @Override
        public Map<String, Object> getProperties() {
            return Collections.emptyMap();
        }

        @Override
        public C2OperationAck handle(C2Operation operation) {
            return operationAck;
        }
    }

    private static class TestInvalidOperationHandler implements C2OperationHandler {

        @Override
        public OperationType getOperationType() {
            return DESCRIBE;
        }

        @Override
        public OperandType getOperandType() {
            return CONFIGURATION;
        }

        @Override
        public Map<String, Object> getProperties() {
            return Collections.emptyMap();
        }

        @Override
        public C2OperationAck handle(C2Operation operation) {
            return null;
        }
    }
}
