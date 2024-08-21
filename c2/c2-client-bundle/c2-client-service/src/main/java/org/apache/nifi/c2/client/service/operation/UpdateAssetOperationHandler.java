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
import static java.util.Optional.ofNullable;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.FULLY_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NOT_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NO_OPERATION;
import static org.apache.nifi.c2.protocol.api.OperandType.ASSET;
import static org.apache.nifi.c2.protocol.api.OperationType.UPDATE;
import static org.apache.nifi.c2.util.Preconditions.requires;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import org.apache.nifi.c2.client.api.C2Client;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.apache.nifi.c2.protocol.api.C2OperationState;
import org.apache.nifi.c2.protocol.api.OperandType;
import org.apache.nifi.c2.protocol.api.OperationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateAssetOperationHandler implements C2OperationHandler {

    static final String ASSET_URL_KEY = "url";
    static final String ASSET_RELATIVE_URL_KEY = "relativeUrl";
    static final String ASSET_FILE_KEY = "file";
    static final String ASSET_FORCE_DOWNLOAD_KEY = "forceDownload";

    static final String C2_CALLBACK_URL_NOT_FOUND = "C2 Server callback URL was not found in request";
    static final String ASSET_FILE_NAME_NOT_FOUND = "Asset file name was not found in request";
    static final String SUCCESSFULLY_UPDATE_ASSET = "Successfully update asset";
    static final String FAILED_TO_PERSIST_ASSET_TO_DISK = "Failed to persist asset to disk";
    static final String UPDATE_ASSET_RETRIEVAL_RESULTED_IN_EMPTY_CONTENT = "Update asset retrieval resulted in empty content";
    static final String UPDATE_ASSET_PRECONDITIONS_WERE_NOT_MET = "Update asset preconditions were not met. No update will be performed";

    private static final Logger LOG = LoggerFactory.getLogger(UpdateAssetOperationHandler.class);

    private final C2Client c2Client;
    private final OperandPropertiesProvider operandPropertiesProvider;
    private final BiPredicate<String, Boolean> assetUpdatePrecondition;
    private final BiFunction<String, byte[], Boolean> assetPersistFunction;

    public UpdateAssetOperationHandler(C2Client c2Client, OperandPropertiesProvider operandPropertiesProvider,
                                       BiPredicate<String, Boolean> assetUpdatePrecondition, BiFunction<String, byte[], Boolean> assetPersistFunction) {
        this.c2Client = c2Client;
        this.operandPropertiesProvider = operandPropertiesProvider;
        this.assetUpdatePrecondition = assetUpdatePrecondition;
        this.assetPersistFunction = assetPersistFunction;
    }

    public static UpdateAssetOperationHandler create(C2Client c2Client, OperandPropertiesProvider operandPropertiesProvider,
                                                     BiPredicate<String, Boolean> assetUpdatePrecondition, BiFunction<String, byte[], Boolean> assetPersistFunction) {
        requires(c2Client != null, "C2Client should not be null");
        requires(operandPropertiesProvider != null, "OperandPropertiesProvider should not be not null");
        requires(assetUpdatePrecondition != null, "Asset update precondition should not be null");
        requires(assetPersistFunction != null, "Asset persist function should not be null");
        return new UpdateAssetOperationHandler(c2Client, operandPropertiesProvider, assetUpdatePrecondition, assetPersistFunction);
    }

    @Override
    public OperationType getOperationType() {
        return UPDATE;
    }

    @Override
    public OperandType getOperandType() {
        return ASSET;
    }

    @Override
    public Map<String, Object> getProperties() {
        return operandPropertiesProvider.getProperties();
    }

    @Override
    public C2OperationAck handle(C2Operation operation) {
        String operationId = ofNullable(operation.getIdentifier()).orElse(EMPTY);

        String callbackUrl;

        try {
            callbackUrl = c2Client.getCallbackUrl(getOperationArg(operation, ASSET_URL_KEY).orElse(EMPTY), getOperationArg(operation, ASSET_RELATIVE_URL_KEY).orElse(EMPTY));
        } catch (Exception e) {
            LOG.error("Callback URL could not be constructed from C2 request and current configuration");
            return operationAck(operationId, operationState(NOT_APPLIED, C2_CALLBACK_URL_NOT_FOUND));
        }

        Optional<String> assetFileName = getOperationArg(operation, ASSET_FILE_KEY);
        if (assetFileName.isEmpty()) {
            LOG.error("Asset file name with key={} was not found in C2 request. C2 request arguments={}", ASSET_FILE_KEY, operation.getArgs());
            return operationAck(operationId, operationState(NOT_APPLIED, ASSET_FILE_NAME_NOT_FOUND));
        }
        boolean forceDownload = getOperationArg(operation, ASSET_FORCE_DOWNLOAD_KEY).map(Boolean::parseBoolean).orElse(FALSE);

        LOG.info("Initiating asset update from url {} with name {}, force update is {}", callbackUrl, assetFileName, forceDownload);

        C2OperationState operationState = assetUpdatePrecondition.test(assetFileName.get(), forceDownload)
            ? c2Client.retrieveUpdateAssetContent(callbackUrl)
            .map(content -> assetPersistFunction.apply(assetFileName.get(), content)
                ? operationState(FULLY_APPLIED, SUCCESSFULLY_UPDATE_ASSET)
                : operationState(NOT_APPLIED, FAILED_TO_PERSIST_ASSET_TO_DISK))
            .orElseGet(() -> operationState(NOT_APPLIED, UPDATE_ASSET_RETRIEVAL_RESULTED_IN_EMPTY_CONTENT))
            : operationState(NO_OPERATION, UPDATE_ASSET_PRECONDITIONS_WERE_NOT_MET);

        return operationAck(operationId, operationState);
    }
}
