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

import static java.util.Collections.emptyMap;
import static java.util.Optional.ofNullable;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.FULLY_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NOT_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NO_OPERATION;
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
    public static final String FLOW_URL_KEY = "flowUrl";
    public static final String FLOW_RELATIVE_URL_KEY = "relativeFlowUrl";

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
    public Map<String, Object> getProperties() {
        return operandPropertiesProvider.getProperties();
    }

    @Override
    public boolean requiresRestart() {
        return true;
    }

    @Override
    public C2OperationAck handle(C2Operation operation) {
        String operationId = Optional.ofNullable(operation.getIdentifier()).orElse(EMPTY);

        Map<String, String> arguments = ofNullable(operation.getArgs()).orElse(emptyMap());
        String absoluteFlowUrl = ofNullable(arguments.get(FLOW_URL_KEY)).orElse(arguments.get(LOCATION));
        Optional<String> callbackUrl = client.getCallbackUrl(absoluteFlowUrl, arguments.get(FLOW_RELATIVE_URL_KEY));
        if (!callbackUrl.isPresent()) {
            logger.error("Callback URL could not be constructed from C2 request and current configuration");
            return operationAck(operationId, operationState(NOT_APPLIED, "Could not get callback url from operation and current configuration"));
        }

        String flowId = getFlowId(operation.getArgs(), callbackUrl.get());
        if (flowId == null) {
            logger.error("FlowId is missing, no update will be performed");
            return operationAck(operationId, operationState(NOT_APPLIED, "Could not get flowId from the operation"));
        }

        if (flowIdHolder.getFlowId() != null && flowIdHolder.getFlowId().equals(flowId)) {
            logger.info("Flow is current, no update is necessary");
            return operationAck(operationId, operationState(NO_OPERATION, "Flow is current, no update is necessary"));
        }

        logger.info("Will perform flow update from {} for operation #{}. Previous flow id was {}, replacing with new id {}",
            callbackUrl, operationId, ofNullable(flowIdHolder.getFlowId()).orElse("not set"), flowId);
        flowIdHolder.setFlowId(flowId);
        return operationAck(operationId, updateFlow(operationId, callbackUrl.get()));
    }

    private C2OperationState updateFlow(String opIdentifier, String callbackUrl) {
        Optional<byte[]> updateContent = client.retrieveUpdateContent(callbackUrl);

        if (!updateContent.isPresent()) {
            logger.error("Update content retrieval resulted in empty content so flow update was omitted for operation #{}.", opIdentifier);
            return operationState(NOT_APPLIED, "Update content retrieval resulted in empty content");
        }

        if (!updateFlow.apply(updateContent.get())) {
            logger.error("Update resulted in error for operation #{}.", opIdentifier);
            return operationState(NOT_APPLIED, "Update resulted in error");
        }

        logger.debug("Update configuration applied for operation #{}.", opIdentifier);
        return operationState(FULLY_APPLIED, "Update configuration applied successfully");
    }

    private String getFlowId(Map<String, String> args, String callbackUrl) {
        return Optional.ofNullable(args)
            .map(map -> map.get(FLOW_ID))
            .orElseGet(() -> parseFlowId(callbackUrl));
    }

    private String parseFlowId(String callbackUrl) {
        try {
            URI flowUri = new URI(callbackUrl);
            Matcher matcher = FLOW_ID_PATTERN.matcher(flowUri.getPath());

            if (matcher.matches()) {
                return matcher.group(1);
            }
        } catch (Exception e) {
            logger.error("Could not get flow id from the provided URL, flow update URL format unexpected [{}]", callbackUrl);
        }
        return null;
    }

    private C2OperationState operationState(C2OperationState.OperationState operationState, String details) {
        C2OperationState state = new C2OperationState();
        state.setState(operationState);
        state.setDetails(details);
        return state;
    }

    private C2OperationAck operationAck(String operationId, C2OperationState operationState) {
        C2OperationAck operationAck = new C2OperationAck();
        operationAck.setOperationState(operationState);
        operationAck.setOperationId(operationId);
        return operationAck;
    }
}
