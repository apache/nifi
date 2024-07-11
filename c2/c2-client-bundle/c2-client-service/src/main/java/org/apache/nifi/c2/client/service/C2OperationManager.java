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

import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static java.util.function.Predicate.not;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.FULLY_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NOT_APPLIED;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.nifi.c2.client.api.C2Client;
import org.apache.nifi.c2.client.service.operation.C2OperationHandler;
import org.apache.nifi.c2.client.service.operation.C2OperationHandlerProvider;
import org.apache.nifi.c2.client.service.operation.C2OperationRestartHandler;
import org.apache.nifi.c2.client.service.operation.OperationQueue;
import org.apache.nifi.c2.client.service.operation.OperationQueueDAO;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.apache.nifi.c2.protocol.api.C2OperationState;
import org.apache.nifi.c2.protocol.api.C2OperationState.OperationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class C2OperationManager implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(C2OperationManager.class);

    private final C2Client client;
    private final C2OperationHandlerProvider c2OperationHandlerProvider;
    private final ReentrantLock heartbeatLock;
    private final OperationQueueDAO operationQueueDAO;
    private final C2OperationRestartHandler c2OperationRestartHandler;
    private final BlockingQueue<C2Operation> c2Operations;

    public C2OperationManager(C2Client client, C2OperationHandlerProvider c2OperationHandlerProvider, ReentrantLock heartbeatLock,
                              OperationQueueDAO operationQueueDAO, C2OperationRestartHandler c2OperationRestartHandler) {
        this.client = client;
        this.c2OperationHandlerProvider = c2OperationHandlerProvider;
        this.heartbeatLock = heartbeatLock;
        this.operationQueueDAO = operationQueueDAO;
        this.c2OperationRestartHandler = c2OperationRestartHandler;
        this.c2Operations = new LinkedBlockingQueue<>();
    }

    public void add(C2Operation c2Operation) {
        try {
            c2Operations.put(c2Operation);
        } catch (InterruptedException e) {
            LOGGER.warn("Thread was interrupted", e);
        }
    }

    @Override
    public void run() {
        processRestartState();
        processOperationsInLoop();
    }

    private void processOperationsInLoop() {
        while (true) {
            C2Operation operation;
            try {
                operation = c2Operations.take();
            } catch (InterruptedException e) {
                LOGGER.warn("Thread was interrupted", e);
                return;
            }

            LOGGER.debug("Processing operation {}", operation);
            C2OperationHandler operationHandler = c2OperationHandlerProvider.getHandlerForOperation(operation).orElse(null);
            if (operationHandler == null) {
                LOGGER.debug("No handler is present for C2 Operation {}, available handlers {}", operation, c2OperationHandlerProvider.getHandlers());
                continue;
            }

            C2OperationAck operationAck;
            try {
                operationAck = operationHandler.handle(operation);
            } catch (Exception e) {
                LOGGER.error("Failed to process operation " + operation, e);
                continue;
            }

            if (!requiresRestart(operationHandler, operationAck)) {
                LOGGER.debug("No restart is required. Sending ACK to C2 server {}", operationAck);
                sendAcknowledge(operationAck);
                continue;
            }

            heartbeatLock.lock();
            LOGGER.debug("Restart is required. Heartbeats are stopped until restart is completed");
            Optional<C2OperationState> restartState = initRestart(operation);
            if (!restartState.isPresent()) {
                LOGGER.debug("Restart in progress, stopping C2OperationManager");
                break;
            }

            try {
                C2OperationState failedState = restartState.get();
                LOGGER.debug("Restart handler returned with a failed state {}", failedState);
                operationAck.setOperationState(failedState);
                sendAcknowledge(operationAck);
            } finally {
                operationQueueDAO.cleanup();
                LOGGER.debug("Heartbeats are enabled again");
                heartbeatLock.unlock();
            }
        }
    }

    private void processRestartState() {
        Optional<OperationQueue> operationQueue = operationQueueDAO.load();

        operationQueue.map(OperationQueue::getRemainingOperations)
            .filter(not(List::isEmpty))
            .ifPresent(this::processRemainingOperations);

        operationQueue.map(OperationQueue::getCurrentOperation)
            .ifPresentOrElse(this::processCurrentOperation,
                () -> LOGGER.debug("No operation to acknowledge to C2 server"));

        operationQueue.ifPresent(__ -> operationQueueDAO.cleanup());
    }

    private void processRemainingOperations(List<C2Operation> remainingOperations) {
        LOGGER.debug("Found remaining operations operations after restart. Heartbeats are stopped until processing is completed");
        heartbeatLock.lock();
        try {
            List<C2Operation> mergedOperations = new LinkedList<>();
            mergedOperations.addAll(remainingOperations);
            mergedOperations.addAll(c2Operations);
            c2Operations.clear();
            mergedOperations.forEach(c2Operations::add);
        } catch (Exception e) {
            LOGGER.warn("Unable to recover operations from operation queue", e);
        } finally {
            heartbeatLock.unlock();
            LOGGER.debug("Heartbeat lock released");
        }
    }

    private void processCurrentOperation(C2Operation operation) {
        LOGGER.debug("Found operation {} to acknowledge to C2 server", operation);

        C2OperationState c2OperationState = c2OperationRestartHandler.waitForResponse()
            .map(this::c2OperationState)
            .orElse(c2OperationState(NOT_APPLIED));

        C2OperationAck c2OperationAck = new C2OperationAck();
        c2OperationAck.setOperationId(operation.getIdentifier());
        c2OperationAck.setOperationState(c2OperationState);

        sendAcknowledge(c2OperationAck);
    }

    private Optional<C2OperationState> initRestart(C2Operation operation) {
        try {
            LOGGER.debug("Restart initiated");
            OperationQueue operationQueue = OperationQueue.create(operation, c2Operations);
            operationQueueDAO.save(operationQueue);
            return c2OperationRestartHandler.handleRestart(operation).map(this::c2OperationState);
        } catch (Exception e) {
            LOGGER.error("Failed to initiate restart. Dropping operation and continue with remaining operations", e);
            return of(c2OperationState(NOT_APPLIED));
        }
    }

    private C2OperationState c2OperationState(OperationState operationState) {
        C2OperationState c2OperationState = new C2OperationState();
        c2OperationState.setState(operationState);
        return c2OperationState;
    }

    private void sendAcknowledge(C2OperationAck operationAck) {
        try {
            client.acknowledgeOperation(operationAck);
        } catch (Exception e) {
            LOGGER.error("Failed to send acknowledge", e);
        }
    }

    private boolean requiresRestart(C2OperationHandler c2OperationHandler, C2OperationAck c2OperationAck) {
        return c2OperationHandler.requiresRestart() && isOperationFullyApplied(c2OperationAck);
    }

    private boolean isOperationFullyApplied(C2OperationAck c2OperationAck) {
        return ofNullable(c2OperationAck)
            .map(C2OperationAck::getOperationState)
            .map(C2OperationState::getState)
            .filter(FULLY_APPLIED::equals)
            .isPresent();
    }
}
