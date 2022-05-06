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

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.nifi.c2.protocol.api.OperandType.CONFIGURATION;
import static org.apache.nifi.c2.protocol.api.OperationType.UPDATE;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.function.Function;
import org.apache.nifi.c2.client.api.C2Client;
import org.apache.nifi.c2.client.api.FlowUpdateInfo;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.apache.nifi.c2.protocol.api.C2OperationState;
import org.apache.nifi.c2.protocol.api.OperandType;
import org.apache.nifi.c2.protocol.api.OperationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateConfigurationOperationHandler implements C2OperationHandler {

    private static final Logger logger = LoggerFactory.getLogger(UpdateConfigurationOperationHandler.class);

    private static final String LOCATION = "location";

    private final C2Client client;
    private final Function<ByteBuffer, Boolean> updateFlow;

    private FlowUpdateInfo currentFlowUpdateInfo;

    public UpdateConfigurationOperationHandler(C2Client client, Function<ByteBuffer, Boolean> updateFlow) {
        this.client = client;
        this.updateFlow = updateFlow;
    }

    @Override
    public OperationType getOperationType() {
        return UPDATE;
    }

    @Override
    public OperandType getOperandType() {
        return CONFIGURATION;
    }

    @Override
    public C2OperationAck handle(C2Operation operation) {
        String opIdentifier = Optional.ofNullable(operation.getIdentifier())
            .orElse(EMPTY);
        C2OperationAck operationAck = new C2OperationAck();
        C2OperationState state = new C2OperationState();
        operationAck.setOperationState(state);
        operationAck.setOperationId(opIdentifier);

        String updateLocation = Optional.ofNullable(operation.getArgs())
            .map(map -> map.get(LOCATION))
            .orElse(EMPTY);

        FlowUpdateInfo flowUpdateInfo = new FlowUpdateInfo(updateLocation, opIdentifier);
        if (currentFlowUpdateInfo == null || !currentFlowUpdateInfo.getFlowId().equals(flowUpdateInfo.getFlowId())) {
            logger.info("Will perform flow update from {} for operation #{}. Previous flow id was {}, replacing with new id {}", updateLocation, opIdentifier,
                currentFlowUpdateInfo == null ? "not set" : currentFlowUpdateInfo.getFlowId(), flowUpdateInfo.getFlowId());
        } else {
            logger.info("Flow is current, no update is necessary...");
        }

        currentFlowUpdateInfo = flowUpdateInfo;
        ByteBuffer updateContent = client.retrieveUpdateContent(flowUpdateInfo);
        if (updateContent != null) {
            if (updateFlow.apply(updateContent)) {
                state.setState(C2OperationState.OperationState.FULLY_APPLIED);
                logger.debug("Update configuration applied for operation #{}.", opIdentifier);
            } else {
                state.setState(C2OperationState.OperationState.NOT_APPLIED);
                logger.error("Update resulted in error for operation #{}.", opIdentifier);
            }
        } else {
            state.setState(C2OperationState.OperationState.NOT_APPLIED);
            logger.error("Update content retrieval resulted in empty content so flow update was omitted for operation #{}.", opIdentifier);
        }

        return operationAck;
    }

    public FlowUpdateInfo getCurrentFlowUpdateInfo() {
        return currentFlowUpdateInfo;
    }
}
