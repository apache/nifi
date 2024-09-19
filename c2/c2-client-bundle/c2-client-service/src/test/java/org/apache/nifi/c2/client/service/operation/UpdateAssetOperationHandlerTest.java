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

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.lang.Boolean.parseBoolean;
import static java.util.Optional.empty;
import static org.apache.nifi.c2.client.service.operation.UpdateAssetOperationHandler.ASSET_FILE_KEY;
import static org.apache.nifi.c2.client.service.operation.UpdateAssetOperationHandler.ASSET_FILE_NAME_NOT_FOUND;
import static org.apache.nifi.c2.client.service.operation.UpdateAssetOperationHandler.ASSET_FORCE_DOWNLOAD_KEY;
import static org.apache.nifi.c2.client.service.operation.UpdateAssetOperationHandler.ASSET_URL_KEY;
import static org.apache.nifi.c2.client.service.operation.UpdateAssetOperationHandler.C2_CALLBACK_URL_NOT_FOUND;
import static org.apache.nifi.c2.client.service.operation.UpdateAssetOperationHandler.FAILED_TO_PERSIST_ASSET_TO_DISK;
import static org.apache.nifi.c2.client.service.operation.UpdateAssetOperationHandler.SUCCESSFULLY_UPDATE_ASSET;
import static org.apache.nifi.c2.client.service.operation.UpdateAssetOperationHandler.UPDATE_ASSET_PRECONDITIONS_WERE_NOT_MET;
import static org.apache.nifi.c2.client.service.operation.UpdateAssetOperationHandler.UPDATE_ASSET_RETRIEVAL_RESULTED_IN_EMPTY_CONTENT;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.FULLY_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NOT_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NO_OPERATION;
import static org.apache.nifi.c2.protocol.api.OperandType.ASSET;
import static org.apache.nifi.c2.protocol.api.OperationType.UPDATE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.stream.Stream;
import org.apache.nifi.c2.client.api.C2Client;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class UpdateAssetOperationHandlerTest {

    private static final String OPERATION_ID = "operationId";
    private static final String ASSET_URL = "http://c2.server/asset/1";
    private static final String ASSET_FILE_NAME = "asset.file";
    private static final String FORCE_DOWNLOAD = "true";

    @Mock
    private C2Client c2Client;

    @Mock
    private OperandPropertiesProvider operandPropertiesProvider;

    @Mock
    private BiPredicate<String, Boolean> assetUpdatePrecondition;

    @Mock
    private BiFunction<String, byte[], Boolean> assetPersistFunction;

    @InjectMocks
    private UpdateAssetOperationHandler testHandler;

    private static Stream<Arguments> invalidConstructorArguments() {
        return Stream.of(
            Arguments.of(null, null, null, null),
            Arguments.of(mock(C2Client.class), null, null, null),
            Arguments.of(mock(C2Client.class), mock(OperandPropertiesProvider.class), null, null),
            Arguments.of(mock(C2Client.class), mock(OperandPropertiesProvider.class), mock(BiPredicate.class), null));
    }

    @BeforeEach
    public void setup() {
        lenient().when(c2Client.getCallbackUrl(any(), any())).thenReturn(ASSET_URL);
    }

    @ParameterizedTest(name = "c2Client={0} operandPropertiesProvider={1} bundleFileList={2} contentFilter={3}")
    @MethodSource("invalidConstructorArguments")
    public void testAttemptingCreateWithInvalidParametersWillThrowException(C2Client c2Client, OperandPropertiesProvider operandPropertiesProvider,
                                                                            BiPredicate<String, Boolean> assetUpdatePrecondition,
                                                                            BiFunction<String, byte[], Boolean> assetPersistFunction) {
        assertThrows(IllegalArgumentException.class, () -> UpdateAssetOperationHandler.create(c2Client, operandPropertiesProvider, assetUpdatePrecondition, assetPersistFunction));
    }

    @Test
    public void testOperationAndOperandTypesAreMatching() {
        assertEquals(UPDATE, testHandler.getOperationType());
        assertEquals(ASSET, testHandler.getOperandType());
    }

    @Test
    public void testAssetUrlCanNotBeNull() {
        // given
        C2Operation operation = operation(null, ASSET_FILE_NAME, FORCE_DOWNLOAD);
        when(c2Client.getCallbackUrl(any(), any())).thenThrow(new IllegalArgumentException());

        // when
        C2OperationAck result = testHandler.handle(operation);

        // then
        assertEquals(OPERATION_ID, result.getOperationId());
        assertEquals(NOT_APPLIED, result.getOperationState().getState());
        assertEquals(C2_CALLBACK_URL_NOT_FOUND, result.getOperationState().getDetails());
    }

    @Test
    public void testAssetFileNameCanNotBeNull() {
        // given
        C2Operation operation = operation(ASSET_URL, null, FORCE_DOWNLOAD);

        // when
        C2OperationAck result = testHandler.handle(operation);

        // then
        assertEquals(OPERATION_ID, result.getOperationId());
        assertEquals(NOT_APPLIED, result.getOperationState().getState());
        assertEquals(ASSET_FILE_NAME_NOT_FOUND, result.getOperationState().getDetails());
    }

    @Test
    public void testAssetPreconditionIsNotMet() {
        // given
        C2Operation operation = operation(ASSET_URL, ASSET_FILE_NAME, FORCE_DOWNLOAD);
        when(assetUpdatePrecondition.test(ASSET_FILE_NAME, parseBoolean(FORCE_DOWNLOAD))).thenReturn(FALSE);

        // when
        C2OperationAck result = testHandler.handle(operation);

        // then
        assertEquals(OPERATION_ID, result.getOperationId());
        assertEquals(NO_OPERATION, result.getOperationState().getState());
        assertEquals(UPDATE_ASSET_PRECONDITIONS_WERE_NOT_MET, result.getOperationState().getDetails());
    }

    @Test
    public void testAssetRetrieveReturnsEmptyResult() {
        // given
        C2Operation operation = operation(ASSET_URL, ASSET_FILE_NAME, FORCE_DOWNLOAD);
        when(assetUpdatePrecondition.test(ASSET_FILE_NAME, parseBoolean(FORCE_DOWNLOAD))).thenReturn(TRUE);
        when(c2Client.retrieveUpdateAssetContent(ASSET_URL)).thenReturn(empty());

        // when
        C2OperationAck result = testHandler.handle(operation);

        // then
        assertEquals(OPERATION_ID, result.getOperationId());
        assertEquals(NOT_APPLIED, result.getOperationState().getState());
        assertEquals(UPDATE_ASSET_RETRIEVAL_RESULTED_IN_EMPTY_CONTENT, result.getOperationState().getDetails());
    }

    @Test
    public void testAssetPersistFunctionFails() {
        // given
        C2Operation operation = operation(ASSET_URL, ASSET_FILE_NAME, FORCE_DOWNLOAD);
        when(assetUpdatePrecondition.test(ASSET_FILE_NAME, parseBoolean(FORCE_DOWNLOAD))).thenReturn(TRUE);
        byte[] mockUpdateContent = new byte[0];
        when(c2Client.retrieveUpdateAssetContent(ASSET_URL)).thenReturn(Optional.of(mockUpdateContent));
        when(assetPersistFunction.apply(ASSET_FILE_NAME, mockUpdateContent)).thenReturn(FALSE);

        // when
        C2OperationAck result = testHandler.handle(operation);

        // then
        assertEquals(OPERATION_ID, result.getOperationId());
        assertEquals(NOT_APPLIED, result.getOperationState().getState());
        assertEquals(FAILED_TO_PERSIST_ASSET_TO_DISK, result.getOperationState().getDetails());
    }

    @Test
    public void testAssetIsDownloadedAndPersistedSuccessfully() {
        // given
        C2Operation operation = operation(ASSET_URL, ASSET_FILE_NAME, FORCE_DOWNLOAD);
        when(assetUpdatePrecondition.test(ASSET_FILE_NAME, parseBoolean(FORCE_DOWNLOAD))).thenReturn(TRUE);
        byte[] mockUpdateContent = new byte[0];
        when(c2Client.retrieveUpdateAssetContent(ASSET_URL)).thenReturn(Optional.of(mockUpdateContent));
        when(assetPersistFunction.apply(ASSET_FILE_NAME, mockUpdateContent)).thenReturn(TRUE);

        // when
        C2OperationAck result = testHandler.handle(operation);

        // then
        assertEquals(OPERATION_ID, result.getOperationId());
        assertEquals(FULLY_APPLIED, result.getOperationState().getState());
        assertEquals(SUCCESSFULLY_UPDATE_ASSET, result.getOperationState().getDetails());
    }

    private C2Operation operation(String assetUrl, String assetFile, String forceDownload) {
        C2Operation c2Operation = new C2Operation();
        c2Operation.setIdentifier(OPERATION_ID);

        Map<String, Object> arguments = new HashMap<>();
        arguments.put(ASSET_URL_KEY, assetUrl);
        arguments.put(ASSET_FILE_KEY, assetFile);
        arguments.put(ASSET_FORCE_DOWNLOAD_KEY, forceDownload);
        c2Operation.setArgs(arguments);
        return c2Operation;
    }
}
