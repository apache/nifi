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
import static org.apache.nifi.c2.protocol.api.OperandType.MANIFEST;
import static org.apache.nifi.c2.protocol.api.OperationType.DESCRIBE;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.nifi.c2.client.service.C2HeartbeatFactory;
import org.apache.nifi.c2.client.service.model.RuntimeInfoWrapper;
import org.apache.nifi.c2.protocol.api.AgentInfo;
import org.apache.nifi.c2.protocol.api.C2Heartbeat;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.apache.nifi.c2.protocol.api.C2OperationState;
import org.apache.nifi.c2.protocol.api.OperandType;
import org.apache.nifi.c2.protocol.api.OperationType;

public class DescribeManifestOperationHandler implements C2OperationHandler {

    private final C2HeartbeatFactory heartbeatFactory;
    private final Supplier<RuntimeInfoWrapper> runtimeInfoSupplier;
    private final OperandPropertiesProvider operandPropertiesProvider;

    public DescribeManifestOperationHandler(C2HeartbeatFactory heartbeatFactory, Supplier<RuntimeInfoWrapper> runtimeInfoSupplier,
        OperandPropertiesProvider operandPropertiesProvider) {
        this.heartbeatFactory = heartbeatFactory;
        this.runtimeInfoSupplier = runtimeInfoSupplier;
        this.operandPropertiesProvider = operandPropertiesProvider;
    }

    @Override
    public OperationType getOperationType() {
        return DESCRIBE;
    }

    @Override
    public OperandType getOperandType() {
        return MANIFEST;
    }

    @Override
    public C2OperationAck handle(C2Operation operation) {
        String opIdentifier = Optional.ofNullable(operation.getIdentifier())
            .orElse(EMPTY);
        C2OperationAck operationAck = new C2OperationAck();
        C2OperationState state = new C2OperationState();
        operationAck.setOperationState(state);
        operationAck.setOperationId(opIdentifier);

        RuntimeInfoWrapper runtimeInfoWrapper = runtimeInfoSupplier.get();
        C2Heartbeat heartbeat = heartbeatFactory.create(runtimeInfoWrapper);

        AgentInfo agentInfo = heartbeat.getAgentInfo();
        agentInfo.setAgentManifest(runtimeInfoWrapper.getManifest());
        operationAck.setAgentInfo(agentInfo);
        operationAck.setDeviceInfo(heartbeat.getDeviceInfo());
        operationAck.setFlowInfo(heartbeat.getFlowInfo());

        state.setState(C2OperationState.OperationState.FULLY_APPLIED);

        return operationAck;
    }

    @Override
    public Map<String, Object> getProperties() {
        return operandPropertiesProvider.getProperties();
    }
}
