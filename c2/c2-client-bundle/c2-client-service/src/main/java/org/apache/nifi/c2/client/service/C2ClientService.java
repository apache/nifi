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
package org.apache.nifi.c2.client.service;

import static org.apache.commons.lang3.StringUtils.EMPTY;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import org.apache.nifi.c2.client.api.C2Client;
import org.apache.nifi.c2.client.api.FlowUpdateInfo;
import org.apache.nifi.c2.client.service.model.RuntimeInfoWrapper;
import org.apache.nifi.c2.protocol.api.C2Heartbeat;
import org.apache.nifi.c2.protocol.api.C2HeartbeatResponse;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.apache.nifi.c2.protocol.api.OperandType;
import org.apache.nifi.c2.protocol.api.OperationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class C2ClientService {

    private static final Logger logger = LoggerFactory.getLogger(C2ClientService.class);
    private static final String LOCATION = "location";

    private final C2Client client;
    private final C2HeartbeatFactory c2HeartbeatFactory;
    private FlowUpdateInfo currentFlowUpdateInfo;

    public C2ClientService(C2Client client, C2HeartbeatFactory c2HeartbeatFactory) {
        this.client = client;
        this.c2HeartbeatFactory = c2HeartbeatFactory;
    }

    public void sendHeartbeat(RuntimeInfoWrapper runtimeInfoWrapper) {
        try {
            // TODO exception handling for all the C2 Client interactions (IOExceptions, logger.error vs logger.warn, etc.)
            C2Heartbeat c2Heartbeat = c2HeartbeatFactory.create(runtimeInfoWrapper);
            Optional.ofNullable(currentFlowUpdateInfo)
                .map(FlowUpdateInfo::getFlowId)
                .ifPresent(flowId -> {
                    logger.trace("Determined that current flow id is {}.", flowId);
                    c2Heartbeat.getFlowInfo().setFlowId(flowId);
                });
            client.publishHeartbeat(c2Heartbeat)
                .ifPresent(this::processResponse);
        } catch (IOException ioe) {
            // TODO
            logger.error("C2 Error", ioe);
        }
    }

    private void processResponse(C2HeartbeatResponse response) {
        List<C2Operation> requestedOperations = response.getRequestedOperations();
        if (requestedOperations != null && !requestedOperations.isEmpty()) {
            logger.info("Received {} operations from the C2 server", requestedOperations.size());
            handleRequestedOperations(requestedOperations);
        } else {
            logger.trace("No operations received from the C2 server in the server. Nothing to do.");
        }
    }

    private void handleRequestedOperations(List<C2Operation> requestedOperations) {
        for (C2Operation requestedOperation : requestedOperations) {
            C2OperationAck operationAck = new C2OperationAck();
            if (requestedOperation.getOperation().equals(OperationType.UPDATE) && requestedOperation.getOperand().equals(OperandType.CONFIGURATION)) {
                String opIdentifier = Optional.ofNullable(requestedOperation.getIdentifier())
                    .orElse(EMPTY);
                String updateLocation = Optional.ofNullable(requestedOperation.getArgs())
                    .map(map -> map.get(LOCATION))
                    .orElse(EMPTY);

                FlowUpdateInfo flowUpdateInfo = new FlowUpdateInfo(updateLocation, opIdentifier);
                if (currentFlowUpdateInfo == null || !currentFlowUpdateInfo.getFlowId().equals(flowUpdateInfo.getFlowId())) {
                    logger.info("Will perform flow update from {} for command #{}.  Previous flow id was {} with new id {}", updateLocation, opIdentifier,
                        currentFlowUpdateInfo == null ? "not set" : currentFlowUpdateInfo.getFlowId(), flowUpdateInfo.getFlowId());
                    currentFlowUpdateInfo = flowUpdateInfo;
                } else {
                    logger.info("Flow is current...");
                }

                currentFlowUpdateInfo = flowUpdateInfo;
                ByteBuffer updateContent = client.retrieveUpdateContent(flowUpdateInfo);
                if (updateContent != null) {
                    // TODO processUpdateContent(ByteBuffer updateContentByteBuffer);
                }

                operationAck.setOperationId(flowUpdateInfo.getRequestId());
            } // else other operations

            client.acknowledgeOperation(operationAck);
        }
    }
}

