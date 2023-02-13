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

import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.FULLY_APPLIED;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.nifi.c2.client.api.C2Client;
import org.apache.nifi.c2.client.service.model.RuntimeInfoWrapper;
import org.apache.nifi.c2.client.service.operation.C2OperationHandler;
import org.apache.nifi.c2.client.service.operation.C2OperationHandlerProvider;
import org.apache.nifi.c2.client.service.operation.OperationQueue;
import org.apache.nifi.c2.client.service.operation.RequestedOperationDAO;
import org.apache.nifi.c2.protocol.api.C2Heartbeat;
import org.apache.nifi.c2.protocol.api.C2HeartbeatResponse;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.apache.nifi.c2.protocol.api.C2OperationState;
import org.apache.nifi.c2.protocol.api.C2OperationState.OperationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class C2ClientService {

    private static final Logger logger = LoggerFactory.getLogger(C2ClientService.class);

    private final C2Client client;
    private final C2HeartbeatFactory c2HeartbeatFactory;
    private final C2OperationHandlerProvider c2OperationHandlerProvider;
    private final RequestedOperationDAO requestedOperationDAO;
    private final Consumer<C2Operation> c2OperationRegister;
    private volatile boolean heartbeatLocked = false;

    public C2ClientService(C2Client client, C2HeartbeatFactory c2HeartbeatFactory, C2OperationHandlerProvider c2OperationHandlerProvider,
        RequestedOperationDAO requestedOperationDAO, Consumer<C2Operation> c2OperationRegister) {
        this.client = client;
        this.c2HeartbeatFactory = c2HeartbeatFactory;
        this.c2OperationHandlerProvider = c2OperationHandlerProvider;
        this.requestedOperationDAO = requestedOperationDAO;
        this.c2OperationRegister = c2OperationRegister;
    }

    public void sendHeartbeat(RuntimeInfoWrapper runtimeInfoWrapper) {
            if (heartbeatLocked) {
                logger.debug("Heartbeats are locked, skipping sending for now");
            } else {
                try {
                    C2Heartbeat c2Heartbeat = c2HeartbeatFactory.create(runtimeInfoWrapper);
                    client.publishHeartbeat(c2Heartbeat).ifPresent(this::processResponse);
                } catch (Exception e) {
                    logger.error("Failed to send/process heartbeat:", e);
                }
            }
    }

    public void sendAcknowledge(C2OperationAck operationAck) {
        try {
            client.acknowledgeOperation(operationAck);
        } catch (Exception e) {
            logger.error("Failed to send acknowledge:", e);
        }
    }

    public void enableHeartbeat() {
        heartbeatLocked = false;
    }

    private void disableHeartbeat() {
        heartbeatLocked = true;
    }

    public void handleRequestedOperations(List<C2Operation> requestedOperations) {
        LinkedList<C2Operation> c2Operations = new LinkedList<>(requestedOperations);
        C2Operation requestedOperation;
        while ((requestedOperation = c2Operations.poll()) != null) {
            Optional<C2OperationHandler> c2OperationHandler = c2OperationHandlerProvider.getHandlerForOperation(requestedOperation);
            if (!c2OperationHandler.isPresent()) {
                continue;
            }
            C2OperationHandler operationHandler = c2OperationHandler.get();
            C2OperationAck c2OperationAck = operationHandler.handle(requestedOperation);
            if (requiresRestart(operationHandler, c2OperationAck)) {
                if (initiateRestart(c2Operations, requestedOperation)) {
                    return;
                }
                C2OperationState c2OperationState = new C2OperationState();
                c2OperationState.setState(OperationState.NOT_APPLIED);
                c2OperationAck.setOperationState(c2OperationState);
            }
            sendAcknowledge(c2OperationAck);
        }
        enableHeartbeat();
        requestedOperationDAO.cleanup();
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

    private boolean requiresRestart(C2OperationHandler c2OperationHandler, C2OperationAck c2OperationAck) {
        return c2OperationHandler.requiresRestart() && isOperationFullyApplied(c2OperationAck);
    }

    private boolean isOperationFullyApplied(C2OperationAck c2OperationAck) {
        return Optional.ofNullable(c2OperationAck)
            .map(C2OperationAck::getOperationState)
            .map(C2OperationState::getState)
            .filter(FULLY_APPLIED::equals)
            .isPresent();
    }

    private boolean initiateRestart(LinkedList<C2Operation> requestedOperations, C2Operation requestedOperation) {
        try {
            disableHeartbeat();
            requestedOperationDAO.save(new OperationQueue(requestedOperation, requestedOperations));
            c2OperationRegister.accept(requestedOperation);
            return true;
        } catch (Exception e) {
            logger.error("Failed to initiate restart. Dropping operation and continue with remaining operations", e);
            requestedOperationDAO.cleanup();
        }
        return false;
    }

}

