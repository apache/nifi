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

import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;
import static org.apache.nifi.c2.client.service.operation.SyncResourceOperationHandler.GLOBAL_HASH_FIELD;
import static org.apache.nifi.c2.client.service.operation.SyncResourceOperationHandler.RESOURCE_LIST_FIELD;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NOT_APPLIED;
import static org.apache.nifi.c2.protocol.api.OperandType.RESOURCE;
import static org.apache.nifi.c2.protocol.api.OperationType.SYNC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.nifi.c2.client.api.C2Client;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.apache.nifi.c2.protocol.api.C2OperationState.OperationState;
import org.apache.nifi.c2.protocol.api.ResourceItem;
import org.apache.nifi.c2.protocol.api.ResourcesGlobalHash;
import org.apache.nifi.c2.serializer.C2Serializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SyncResourceOperationHandlerTest {

    private static final String OPERATION_ID = "operationId";

    @Mock
    private C2Client mockC2Client;

    @Mock
    private OperandPropertiesProvider mockOperandPropertiesProvider;

    @Mock
    private SyncResourceStrategy mockSyncResourceStrategy;

    @Mock
    private C2Serializer mockC2Serializer;

    @InjectMocks
    private SyncResourceOperationHandler testHandler;

    @ParameterizedTest(name = "c2Client={0} operandPropertiesProvider={1} syncResourceStrategy={2} c2Serializer={3}")
    @MethodSource("invalidConstructorArguments")
    public void testAttemptingCreateWithInvalidParametersWillThrowException(C2Client c2Client, OperandPropertiesProvider operandPropertiesProvider,
                                                                            SyncResourceStrategy syncResourceStrategy, C2Serializer c2Serializer) {
        assertThrows(IllegalArgumentException.class, () -> SyncResourceOperationHandler.create(c2Client, operandPropertiesProvider, syncResourceStrategy, c2Serializer));
    }

    @Test
    public void testOperationAndOperandTypesAreMatching() {
        assertEquals(SYNC, testHandler.getOperationType());
        assertEquals(RESOURCE, testHandler.getOperandType());
    }

    @Test
    public void testResourcesGlobalHashArgumentIsNull() {
        C2Operation inputOperation = operation(null, List.of(new ResourceItem()));

        C2OperationAck c2OperationAck = testHandler.handle(inputOperation);

        assertEquals(OPERATION_ID, c2OperationAck.getOperationId());
        assertEquals(NOT_APPLIED, c2OperationAck.getOperationState().getState());
        verify(mockSyncResourceStrategy, never()).synchronizeResourceRepository(any(), anyList(), any(), any());
    }

    @Test
    public void testResourceListArgumentIsNull() {
        ResourcesGlobalHash resourcesGlobalHash = new ResourcesGlobalHash();
        C2Operation inputOperation = operation(resourcesGlobalHash, null);
        when(mockC2Serializer.convert(eq(resourcesGlobalHash), any())).thenReturn(ofNullable(resourcesGlobalHash));

        C2OperationAck c2OperationAck = testHandler.handle(inputOperation);

        assertEquals(OPERATION_ID, c2OperationAck.getOperationId());
        assertEquals(NOT_APPLIED, c2OperationAck.getOperationState().getState());
        verify(mockSyncResourceStrategy, never()).synchronizeResourceRepository(any(), anyList(), any(), any());
    }

    @Test
    public void testArgumentConversionFailure() {
        ResourcesGlobalHash resourcesGlobalHash = new ResourcesGlobalHash();
        List<ResourceItem> resourceItems = List.of(new ResourceItem());
        C2Operation inputOperation = operation(resourcesGlobalHash, resourceItems);
        when(mockC2Serializer.convert(eq(resourcesGlobalHash), any())).thenReturn(empty());

        C2OperationAck c2OperationAck = testHandler.handle(inputOperation);

        assertEquals(OPERATION_ID, c2OperationAck.getOperationId());
        assertEquals(NOT_APPLIED, c2OperationAck.getOperationState().getState());
        verify(mockSyncResourceStrategy, never()).synchronizeResourceRepository(any(), anyList(), any(), any());
    }

    @ParameterizedTest(name = "operationState={0}")
    @MethodSource("synchronizeStrategyArguments")
    public void testSynchronizeResourceStrategyExecutions(OperationState operationState) {
        ResourcesGlobalHash resourcesGlobalHash = new ResourcesGlobalHash();
        List<ResourceItem> resourceItems = List.of(new ResourceItem());
        C2Operation inputOperation = operation(resourcesGlobalHash, resourceItems);
        when(mockC2Serializer.convert(eq(resourcesGlobalHash), any())).thenReturn(Optional.of(resourcesGlobalHash));
        when(mockC2Serializer.convert(eq(resourceItems), any())).thenReturn(Optional.of(resourceItems));
        when(mockSyncResourceStrategy.synchronizeResourceRepository(eq(resourcesGlobalHash), eq(resourceItems), any(), any())).thenReturn(operationState);

        C2OperationAck c2OperationAck = testHandler.handle(inputOperation);

        assertEquals(OPERATION_ID, c2OperationAck.getOperationId());
        assertEquals(operationState, c2OperationAck.getOperationState().getState());
    }

    private static Stream<Arguments> invalidConstructorArguments() {
        return Stream.of(
            Arguments.of(null, null, null, null),
            Arguments.of(mock(C2Client.class), null, null, null),
            Arguments.of(mock(C2Client.class), mock(OperandPropertiesProvider.class), null, null),
            Arguments.of(mock(C2Client.class), mock(OperandPropertiesProvider.class), mock(SyncResourceStrategy.class), null),
            Arguments.of(mock(C2Client.class), mock(OperandPropertiesProvider.class), null, mock(C2Serializer.class)));
    }

    private static Stream<Arguments> synchronizeStrategyArguments() {
        return Stream.of(OperationState.values()).map(Arguments::of);
    }

    private C2Operation operation(ResourcesGlobalHash resourcesGlobalHash, List<ResourceItem> resourceItems) {
        C2Operation c2Operation = new C2Operation();
        c2Operation.setIdentifier(OPERATION_ID);
        Map<String, Object> arguments = new HashMap<>();
        arguments.put(GLOBAL_HASH_FIELD, resourcesGlobalHash);
        arguments.put(RESOURCE_LIST_FIELD, resourceItems);
        c2Operation.setArgs(arguments);
        return c2Operation;
    }
}
