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

import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.apache.nifi.c2.protocol.api.C2OperationState;
import org.apache.nifi.c2.protocol.api.OperandType;
import org.apache.nifi.c2.protocol.api.OperationType;

import java.util.Collections;
import java.util.Map;

import static java.util.Optional.ofNullable;
import static org.apache.commons.lang3.StringUtils.EMPTY;

public class StopFlowOperationHandler implements C2OperationHandler {

    public static final String NOT_APPLIED_DETAILS = "Failed to stop flow, please check the log for errors";
    public static final String FULLY_APPLIED_DETAILS = "Flow stopped";
    public static final String PARTIALLY_APPLIED_DETAILS = "Some components failed to stop, please check the log for errors";
    public static final String UNEXPECTED_DETAILS = "Unexpected status, please check the log for errors";

    private final FlowStateStrategy flowStateStrategy;

    public StopFlowOperationHandler(FlowStateStrategy flowStateStrategy) {
        this.flowStateStrategy = flowStateStrategy;
    }

    @Override
    public OperationType getOperationType() {
        return OperationType.STOP;
    }

    @Override
    public OperandType getOperandType() {
        return OperandType.FLOW;
    }

    @Override
    public Map<String, Object> getProperties() {
        return Collections.emptyMap();
    }

    @Override
    public C2OperationAck handle(C2Operation operation) {
        String operationId = ofNullable(operation.getIdentifier()).orElse(EMPTY);
        C2OperationState.OperationState operationState = flowStateStrategy.stop();

        C2OperationState resultState = operationState(
                operationState,
                switch (operationState) {
                    case NOT_APPLIED -> NOT_APPLIED_DETAILS;
                    case FULLY_APPLIED -> FULLY_APPLIED_DETAILS;
                    case PARTIALLY_APPLIED -> PARTIALLY_APPLIED_DETAILS;
                    default -> UNEXPECTED_DETAILS;
                }
        );

        return operationAck(operationId, resultState);
    }
}
