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
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.FULLY_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NOT_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NO_OPERATION;
import static org.apache.nifi.c2.protocol.api.OperandType.PROPERTIES;
import static org.apache.nifi.c2.protocol.api.OperationType.UPDATE;

import java.util.Map;
import java.util.function.Function;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.apache.nifi.c2.protocol.api.C2OperationState;
import org.apache.nifi.c2.protocol.api.OperandType;
import org.apache.nifi.c2.protocol.api.OperationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdatePropertiesOperationHandler implements C2OperationHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdatePropertiesOperationHandler.class);

    private final OperandPropertiesProvider operandPropertiesProvider;
    private final Function<Map<String, Object>, Boolean> persistProperties;

    public UpdatePropertiesOperationHandler(OperandPropertiesProvider operandPropertiesProvider, Function<Map<String, Object>, Boolean> persistProperties) {
        this.operandPropertiesProvider = operandPropertiesProvider;
        this.persistProperties = persistProperties;
    }

    @Override
    public OperationType getOperationType() {
        return UPDATE;
    }

    @Override
    public OperandType getOperandType() {
        return PROPERTIES;
    }

    @Override
    public Map<String, Object> getProperties() {
        return operandPropertiesProvider.getProperties();
    }

    @Override
    public C2OperationAck handle(C2Operation operation) {
        String operationId = ofNullable(operation.getIdentifier()).orElse(EMPTY);

        C2OperationState c2OperationState;
        try {
            if (persistProperties.apply(operation.getArgs())) {
                c2OperationState = operationState(FULLY_APPLIED, null);
            } else {
                LOGGER.info("Properties are already in desired state");
                c2OperationState = operationState(NO_OPERATION, null);
            }
        } catch (IllegalArgumentException e) {
            LOGGER.error("Operation not applied due to issues with the arguments: {}", e.getMessage());
            c2OperationState = operationState(NOT_APPLIED, e.getMessage());
        } catch (Exception e) {
            LOGGER.error("Unexpected error happened during persisting properties", e);
            c2OperationState = operationState(NOT_APPLIED, "Failed to persist properties");
        }
        return operationAck(operationId, c2OperationState);
    }

    @Override
    public boolean requiresRestart() {
        return true;
    }
}
