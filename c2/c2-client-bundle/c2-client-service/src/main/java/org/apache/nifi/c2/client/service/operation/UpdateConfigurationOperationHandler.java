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

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.nifi.c2.client.api.C2Client;
import org.apache.nifi.c2.client.service.FlowIdHolder;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.apache.nifi.c2.protocol.api.C2OperationState;
import org.apache.nifi.c2.protocol.api.OperandType;
import org.apache.nifi.c2.protocol.api.OperationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateConfigurationOperationHandler implements C2OperationHandler {
    private static final Logger logger = LoggerFactory.getLogger(UpdateConfigurationOperationHandler.class);
    private static final Pattern FLOW_ID_PATTERN = Pattern.compile("/[^/]+?/[^/]+?/[^/]+?/([^/]+)?/?.*");
    static final String FLOW_ID = "flowId";
    static final String LOCATION = "location";

    private final C2Client client;
    private final Function<byte[], Boolean> updateFlow;
    private final FlowIdHolder flowIdHolder;
    private final OperandPropertiesProvider operandPropertiesProvider;

    public UpdateConfigurationOperationHandler(C2Client client, FlowIdHolder flowIdHolder, Function<byte[], Boolean> updateFlow,
        OperandPropertiesProvider operandPropertiesProvider) {
        this.client = client;
        this.updateFlow = updateFlow;
        this.flowIdHolder = flowIdHolder;
        this.operandPropertiesProvider = operandPropertiesProvider;
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

        String flowId = getFlowId(operation.getArgs(), updateLocation);
        if (flowId == null) {
            state.setState(C2OperationState.OperationState.NOT_APPLIED);
            state.setDetails("Could not get flowId from the operation.");
            logger.info("FlowId is missing, no update will be performed.");
        } else {
            if (flowIdHolder.getFlowId() == null || !flowIdHolder.getFlowId().equals(flowId)) {
                logger.info("Will perform flow update from {} for operation #{}. Previous flow id was {}, replacing with new id {}", updateLocation, opIdentifier,
                        flowIdHolder.getFlowId() == null ? "not set" : flowIdHolder.getFlowId(), flowId);
            } else {
                logger.info("Flow is current, no update is necessary...");
            }
            flowIdHolder.setFlowId(flowId);
            state.setState(updateFlow(opIdentifier, updateLocation));
        }
        return operationAck;
    }

    private C2OperationState.OperationState updateFlow(String opIdentifier, String updateLocation) {
        Optional<byte[]> updateContent = client.retrieveUpdateContent(updateLocation);
        if (updateContent.isPresent()) {
            if (updateFlow.apply(updateContent.get())) {
                logger.debug("Update configuration applied for operation #{}.", opIdentifier);
                return C2OperationState.OperationState.FULLY_APPLIED;
            } else {
                logger.error("Update resulted in error for operation #{}.", opIdentifier);
                return C2OperationState.OperationState.NOT_APPLIED;
            }
        } else {
            logger.error("Update content retrieval resulted in empty content so flow update was omitted for operation #{}.", opIdentifier);
            return C2OperationState.OperationState.NOT_APPLIED;
        }
    }

    private String getFlowId(Map<String, String> args, String updateLocation) {
        return Optional.ofNullable(args)
        .map(map -> map.get(FLOW_ID))
        .orElseGet(() -> parseFlowId(updateLocation));
    }

    private String parseFlowId(String flowUpdateUrl) {
        try {
            URI flowUri = new URI(flowUpdateUrl);
            Matcher matcher = FLOW_ID_PATTERN.matcher(flowUri.getPath());

            if (matcher.matches()) {
                return matcher.group(1);
            }
        } catch (Exception e) {
            logger.error("Could not get flow id from the provided URL, flow update URL format unexpected [{}]", flowUpdateUrl);
        }
        return null;
    }

    @Override
    public Map<String, Object> getProperties() {
        return operandPropertiesProvider.getProperties();
    }

    @Override
    public boolean requiresRestart() {
        return true;
    }
}
