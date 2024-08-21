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
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.FULLY_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NOT_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NO_OPERATION;
import static org.apache.nifi.c2.protocol.api.OperandType.CONFIGURATION;
import static org.apache.nifi.c2.protocol.api.OperationType.UPDATE;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
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

    public static final String FLOW_URL_KEY = "flowUrl";
    public static final String FLOW_RELATIVE_URL_KEY = "relativeFlowUrl";

    static final String FLOW_ID = "flowId";
    static final String LOCATION = "location";

    private static final Logger logger = LoggerFactory.getLogger(UpdateConfigurationOperationHandler.class);

    private static final Pattern FLOW_ID_PATTERN = Pattern.compile("/[^/]+?/[^/]+?/[^/]+?/([^/]+)?/?.*");

    private final C2Client client;
    private final UpdateConfigurationStrategy updateConfigurationStrategy;
    private final FlowIdHolder flowIdHolder;
    private final OperandPropertiesProvider operandPropertiesProvider;

    public UpdateConfigurationOperationHandler(C2Client client, FlowIdHolder flowIdHolder, UpdateConfigurationStrategy updateConfigurationStrategy,
                                               OperandPropertiesProvider operandPropertiesProvider) {
        this.client = client;
        this.updateConfigurationStrategy = updateConfigurationStrategy;
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
        return false;
    }

    @Override
    public C2OperationAck handle(C2Operation operation) {
        String operationId = ofNullable(operation.getIdentifier()).orElse(EMPTY);

        String absoluteFlowUrl = getOperationArg(operation, FLOW_URL_KEY).orElse(getOperationArg(operation, LOCATION).orElse(EMPTY));
        String callbackUrl;
        try {
            callbackUrl = client.getCallbackUrl(absoluteFlowUrl, getOperationArg(operation, FLOW_RELATIVE_URL_KEY).orElse(EMPTY));
        } catch (Exception e) {
            logger.error("Callback URL could not be constructed from C2 request and current configuration");
            return operationAck(operationId, operationState(NOT_APPLIED, "Could not get callback url from operation and current configuration"));
        }

        Optional<String> flowId = getFlowId(operation, callbackUrl);
        if (flowId.isEmpty()) {
            logger.error("FlowId is missing, no update will be performed");
            return operationAck(operationId, operationState(NOT_APPLIED, "Could not get flowId from the operation"));
        }

        if (flowIdHolder.getFlowId() != null && flowIdHolder.getFlowId().equals(flowId.get())) {
            logger.info("Flow is current, no update is necessary");
            return operationAck(operationId, operationState(NO_OPERATION, "Flow is current, no update is necessary"));
        }

        logger.info("Will perform flow update from {} for operation #{}. Previous flow id was {}, replacing with new id {}",
            callbackUrl, operationId, ofNullable(flowIdHolder.getFlowId()).orElse("not set"), flowId.get());
        C2OperationState state = updateFlow(operationId, callbackUrl);
        if (state.getState() == FULLY_APPLIED) {
            flowIdHolder.setFlowId(flowId.get());
        }
        return operationAck(operationId, state);
    }

    private C2OperationState updateFlow(String opIdentifier, String callbackUrl) {
        Optional<byte[]> updateContent = client.retrieveUpdateConfigurationContent(callbackUrl);

        if (updateContent.isEmpty()) {
            logger.error("Update content retrieval resulted in empty content so flow update was omitted for operation #{}.", opIdentifier);
            return operationState(NOT_APPLIED, "Update content retrieval resulted in empty content");
        }

        try {
            updateConfigurationStrategy.update(updateContent.get());
        } catch (Exception e) {
            logger.error("Update resulted in error for operation #{}.", opIdentifier);
            return operationState(NOT_APPLIED, "Update resulted in error:", e);
        }

        logger.debug("Update configuration applied for operation #{}.", opIdentifier);
        return operationState(FULLY_APPLIED, "Update configuration applied successfully");
    }

    private Optional<String> getFlowId(C2Operation operation, String callbackUrl) {
        return getOperationArg(operation, FLOW_ID).or(() -> parseFlowId(callbackUrl));
    }

    private Optional<String> parseFlowId(String callbackUrl) {
        try {
            URI flowUri = new URI(callbackUrl);
            Matcher matcher = FLOW_ID_PATTERN.matcher(flowUri.getPath());

            if (matcher.matches()) {
                return ofNullable(matcher.group(1));
            }
        } catch (Exception e) {
            logger.error("Could not get flow id from the provided URL, flow update URL format unexpected [{}]", callbackUrl);
        }
        return empty();
    }
}
