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
package org.apache.nifi.minifi.c2.service;

import org.apache.nifi.c2.protocol.api.C2Heartbeat;
import org.apache.nifi.c2.protocol.api.C2HeartbeatResponse;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.apache.nifi.c2.protocol.api.C2OperationState;
import org.apache.nifi.c2.protocol.api.OperandType;
import org.apache.nifi.c2.protocol.api.OperationState;
import org.apache.nifi.c2.protocol.api.OperationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Service
public class SimpleC2ProtocolService implements C2ProtocolService {

    private static final Logger logger = LoggerFactory.getLogger(SimpleC2ProtocolService.class);

    private static final Set<String> issuedOperationIds = new HashSet<>();
    private static final String LOCATION = "location";
    private static final String FLOW_ID = "flowId";

    private final Map<String, String> currentFlowIds;

    public SimpleC2ProtocolService() {
        currentFlowIds = new HashMap<>(1000);
    }

    @Override
    public void processOperationAck(final C2OperationAck operationAck, final C2ProtocolContext context) {
        // This service assumes there is a single Operation UPDATE to pass over the updated flow
        logger.debug("Received operation acknowledgement: {}; {}", operationAck, context);
        // Remove the operator ID from the list of issued operations and log the state
        final String operationId = operationAck.getOperationId();
        try {
            OperationState opState = OperationState.DONE;
            String details = null;

            /* Partial applications are rare and only happen when an operation consists of updating multiple config
             * items and some succeed ( we don't yet have the concept of rollback in agents ).
             * Fully Applied yields an operation success.
             * Operation Not Understood and Not Applied give little details but also will result in Operation Failure.
             * We should explore if providing textual details. */
            final C2OperationState c2OperationState = operationAck.getOperationState();
            if (null != c2OperationState) {
                details = c2OperationState.getDetails();
                if (c2OperationState.getState() != C2OperationState.OperationState.FULLY_APPLIED) {
                    opState = OperationState.FAILED;
                }
            }

            if (!issuedOperationIds.remove(operationId)) {
                logger.warn("Operation with ID " + operationId + " has either already been acknowledged or is unknown to this server");
            } else if (null != c2OperationState) {
                final C2OperationState.OperationState operationState = c2OperationState.getState();
                logger.debug("Operation with ID " + operationId + " acknowledged with a state of " + operationState.name() + "(" + opState.name() + "), details = "
                        + (details == null ? "" : details));
            }

            // Optionally, an acknowledgement can include some of the info normally passed in a heartbeat.
            // If this info is present, process it as a heartbeat, so we update our latest known state of the agent.
            if (operationAck.getAgentInfo() != null
                    || operationAck.getDeviceInfo() != null
                    || operationAck.getFlowInfo() != null) {
                final C2Heartbeat heartbeatInfo = toHeartbeat(operationAck);
                logger.trace("Operation acknowledgement contains additional info. Processing as heartbeat: {}", heartbeatInfo);
                processHeartbeat(heartbeatInfo, context);
            }

        } catch (final Exception e) {
            logger.warn("Encountered exception while processing operation ack", e);
        }
    }

    @Override
    public C2HeartbeatResponse processHeartbeat(final C2Heartbeat heartbeat, final C2ProtocolContext context) {

        C2HeartbeatResponse c2HeartbeatResponse = new C2HeartbeatResponse();
        String currentFlowId = currentFlowIds.get(heartbeat.getAgentId());
        if (currentFlowId == null || !currentFlowId.equals(context.getSha256())) {
            // Create a single UPDATE operation to fetch the flow from the specified URL
            C2Operation c2Operation = new C2Operation();
            final String operationID = UUID.randomUUID().toString();
            issuedOperationIds.add(operationID);
            c2Operation.setIdentifier(operationID);
            c2Operation.setOperation(OperationType.UPDATE);
            c2Operation.setOperand(OperandType.CONFIGURATION);
            Map<String, String> args = new HashMap<>();
            args.put(LOCATION, context.getBaseUri().toString());
            args.put(FLOW_ID, context.getSha256());
            c2Operation.setArgs(args);
            List<C2Operation> requestedOperations = Collections.singletonList(c2Operation);
            c2HeartbeatResponse.setRequestedOperations(requestedOperations);
            currentFlowIds.put(heartbeat.getAgentId(), context.getSha256());
        }

        return c2HeartbeatResponse;
    }

    private static C2Heartbeat toHeartbeat(final C2OperationAck ack) {
        final C2Heartbeat heartbeat = new C2Heartbeat();
        heartbeat.setDeviceInfo(ack.getDeviceInfo());
        heartbeat.setAgentInfo(ack.getAgentInfo());
        heartbeat.setFlowInfo(ack.getFlowInfo());
        return heartbeat;
    }
}
