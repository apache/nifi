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

import static org.apache.nifi.c2.protocol.api.OperandType.PROPERTIES;
import static org.apache.nifi.c2.protocol.api.OperationType.UPDATE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
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
public class UpdatePropertiesOperationHandlerTest {

    private static final String ID = "id";
    private static final Map<String, String> ARGS = Collections.singletonMap("key", "value");

    @Mock
    private OperandPropertiesProvider operandPropertiesProvider;

    @Mock
    private Function<Map<String, String>, Boolean> persistProperties;

    @InjectMocks
    private UpdatePropertiesOperationHandler updatePropertiesOperationHandler;

    @Test
    void shouldReturnStaticSettings() {
        assertEquals(UPDATE, updatePropertiesOperationHandler.getOperationType());
        assertEquals(PROPERTIES, updatePropertiesOperationHandler.getOperandType());
        assertTrue(updatePropertiesOperationHandler.requiresRestart());
    }

    @Test
    void shouldReturnProperties() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("test", new Object());
        when(operandPropertiesProvider.getProperties()).thenReturn(properties);

        Map<String, Object> result = updatePropertiesOperationHandler.getProperties();

        assertEquals(properties, result);
    }

    @Test
    void shouldReturnAckWithFullyAppliedWhenPersistIsSuccessful() {
        C2Operation c2Operation = getC2Operation();
        when(persistProperties.apply(ARGS)).thenReturn(true);

        C2OperationAck result = updatePropertiesOperationHandler.handle(c2Operation);

        assertEquals(getExpected(OperationState.FULLY_APPLIED), result);
    }

    @Test
    void shouldReturnAckWithNoOperationWhenPersistReturnFalse() {
        C2Operation c2Operation = getC2Operation();
        when(persistProperties.apply(ARGS)).thenReturn(false);

        C2OperationAck result = updatePropertiesOperationHandler.handle(c2Operation);

        assertEquals(getExpected(OperationState.NO_OPERATION), result);
    }

    @Test
    void shouldReturnNotAppliedInCaseOfIllegalArgumentException() {
        C2Operation c2Operation = getC2Operation();
        when(persistProperties.apply(ARGS)).thenThrow(new IllegalArgumentException());

        C2OperationAck result = updatePropertiesOperationHandler.handle(c2Operation);

        assertEquals(getExpected(OperationState.NOT_APPLIED), result);
    }

    @Test
    void shouldReturnNotAppliedInCaseOfException() {
        C2Operation c2Operation = getC2Operation();
        when(persistProperties.apply(ARGS)).thenThrow(new RuntimeException());

        C2OperationAck result = updatePropertiesOperationHandler.handle(c2Operation);

        C2OperationAck expected = getExpected(OperationState.NOT_APPLIED);
        expected.getOperationState().setDetails("Failed to persist properties");
        assertEquals(expected, result);
    }

    private C2OperationAck getExpected(OperationState operationState) {
        final C2OperationAck c2OperationAck = new C2OperationAck();
        c2OperationAck.setOperationId(ID);
        C2OperationState c2OperationState = new C2OperationState();
        c2OperationState.setState(operationState);
        c2OperationAck.setOperationState(c2OperationState);
        return c2OperationAck;
    }

    private C2Operation getC2Operation() {
        C2Operation c2Operation = new C2Operation();
        c2Operation.setArgs(ARGS);
        c2Operation.setIdentifier(ID);
        return c2Operation;
    }
}