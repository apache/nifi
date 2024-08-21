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

import static java.util.Optional.ofNullable;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NOT_APPLIED;
import static org.apache.nifi.c2.protocol.api.OperandType.RESOURCE;
import static org.apache.nifi.c2.protocol.api.OperationType.SYNC;
import static org.apache.nifi.c2.util.Preconditions.requires;

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.List;
import java.util.Map;
import org.apache.nifi.c2.client.api.C2Client;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.apache.nifi.c2.protocol.api.C2OperationState;
import org.apache.nifi.c2.protocol.api.C2OperationState.OperationState;
import org.apache.nifi.c2.protocol.api.OperandType;
import org.apache.nifi.c2.protocol.api.OperationType;
import org.apache.nifi.c2.protocol.api.ResourceItem;
import org.apache.nifi.c2.protocol.api.ResourcesGlobalHash;
import org.apache.nifi.c2.serializer.C2Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncResourceOperationHandler implements C2OperationHandler {

    static final String GLOBAL_HASH_FIELD = "globalHash";
    static final String RESOURCE_LIST_FIELD = "resourceList";

    private static final Logger LOG = LoggerFactory.getLogger(SyncResourceOperationHandler.class);

    private final C2Client c2Client;
    private final OperandPropertiesProvider operandPropertiesProvider;
    private final SyncResourceStrategy syncResourceStrategy;
    private final C2Serializer c2Serializer;

    public SyncResourceOperationHandler(C2Client c2Client, OperandPropertiesProvider operandPropertiesProvider, SyncResourceStrategy syncResourceStrategy,
                                        C2Serializer c2Serializer) {
        this.c2Client = c2Client;
        this.operandPropertiesProvider = operandPropertiesProvider;
        this.syncResourceStrategy = syncResourceStrategy;
        this.c2Serializer = c2Serializer;
    }

    public static SyncResourceOperationHandler create(C2Client c2Client, OperandPropertiesProvider operandPropertiesProvider, SyncResourceStrategy syncResourceStrategy,
                                                      C2Serializer c2Serializer) {
        requires(c2Client != null, "C2Client should not be null");
        requires(operandPropertiesProvider != null, "OperandPropertiesProvider should not be not null");
        requires(syncResourceStrategy != null, "Sync resource strategy should not be null");
        requires(c2Serializer != null, "C2 serializer should not be null");
        return new SyncResourceOperationHandler(c2Client, operandPropertiesProvider, syncResourceStrategy, c2Serializer);
    }

    @Override
    public OperationType getOperationType() {
        return SYNC;
    }

    @Override
    public OperandType getOperandType() {
        return RESOURCE;
    }

    @Override
    public Map<String, Object> getProperties() {
        return operandPropertiesProvider.getProperties();
    }

    @Override
    public C2OperationAck handle(C2Operation operation) {
        String operationId = ofNullable(operation.getIdentifier()).orElse(EMPTY);

        ResourcesGlobalHash resourcesGlobalHash;
        try {
            resourcesGlobalHash = getOperationArg(operation, GLOBAL_HASH_FIELD, new TypeReference<>() { }, c2Serializer);
        } catch (Exception e) {
            LOG.error("Resources global hash could not be constructed from C2 request");
            return operationAck(operationId, operationState(NOT_APPLIED, "Resources global hash element was not found", e));
        }

        List<ResourceItem> resourceItems;
        try {
           resourceItems = getOperationArg(operation, RESOURCE_LIST_FIELD, new TypeReference<>() { }, c2Serializer);

        } catch (Exception e) {
            LOG.error("Resource item list could not be constructed from C2 request");
            return operationAck(operationId, operationState(NOT_APPLIED, "Resource item list element was not found", e));
        }

        OperationState operationState = syncResourceStrategy.synchronizeResourceRepository(resourcesGlobalHash, resourceItems, c2Client::retrieveResourceItem,
            relativeUrl -> c2Client.getCallbackUrl(null, relativeUrl));
        C2OperationState resultState = operationState(
            operationState,
            switch (operationState) {
                case NOT_APPLIED -> "No resource items were retrieved, please check the log for errors";
                case PARTIALLY_APPLIED -> "Resource repository is partially synced, retrieving some items failed. Pleas check log for errors";
                case FULLY_APPLIED -> "Agent Resource repository is in sync with the C2 server";
                default -> "Unexpected status, please check the log for errors";
            }
        );

        return operationAck(operationId, resultState);
    }
}
