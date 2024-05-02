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

package org.apache.nifi.minifi.c2;

import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.nifi.bootstrap.CommandResult.FAILURE;
import static org.apache.nifi.bootstrap.CommandResult.SUCCESS;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NOT_APPLIED;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.apache.nifi.bootstrap.BootstrapCommunicator;
import org.apache.nifi.bootstrap.CommandResult;
import org.apache.nifi.c2.client.service.operation.C2OperationRestartHandler;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationState.OperationState;
import org.apache.nifi.minifi.commons.api.MiNiFiCommandState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BootstrapC2OperationRestartHandler implements C2OperationRestartHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(BootstrapC2OperationRestartHandler.class);

    private static final String ACKNOWLEDGE_OPERATION = "ACKNOWLEDGE_OPERATION";
    private static final String TIMEOUT = "timeout";
    private static final Map<MiNiFiCommandState, OperationState> OPERATION_STATE_MAP = Map.of(
        MiNiFiCommandState.FULLY_APPLIED, OperationState.FULLY_APPLIED,
        MiNiFiCommandState.NO_OPERATION, OperationState.NO_OPERATION,
        MiNiFiCommandState.NOT_APPLIED_WITH_RESTART, NOT_APPLIED,
        MiNiFiCommandState.NOT_APPLIED_WITHOUT_RESTART, NOT_APPLIED);

    private final BootstrapCommunicator bootstrapCommunicator;
    private final BlockingQueue<OperationState> operationStateHolder;
    private final long bootstrapAcknowledgeTimeoutMs;

    public BootstrapC2OperationRestartHandler(BootstrapCommunicator bootstrapCommunicator, long bootstrapAcknowledgeTimeoutMs) {
        this.bootstrapCommunicator = bootstrapCommunicator;
        this.operationStateHolder = new ArrayBlockingQueue<>(1);
        this.bootstrapAcknowledgeTimeoutMs = bootstrapAcknowledgeTimeoutMs;
        bootstrapCommunicator.registerMessageHandler(ACKNOWLEDGE_OPERATION, this::bootstrapCallback);
    }

    @Override
    public Optional<OperationState> handleRestart(C2Operation c2Operation) {
        CommandResult sendCommandResult = sendBootstrapCommand(c2Operation);
        if (sendCommandResult == SUCCESS) {
            LOGGER.debug("Bootstrap successfully received command. Waiting for response");
            return waitForResponse();
        } else {
            LOGGER.debug("Bootstrap failed to receive command");
            return Optional.of(NOT_APPLIED);
        }
    }

    @Override
    public Optional<OperationState> waitForResponse() {
        try {
            OperationState operationState = operationStateHolder.poll(bootstrapAcknowledgeTimeoutMs, MILLISECONDS);
            LOGGER.debug("Bootstrap returned response: {}", ofNullable(operationState).map(Objects::toString).orElse(TIMEOUT));
            return Optional.of(ofNullable(operationState).orElse(NOT_APPLIED));
        } catch (InterruptedException e) {
            LOGGER.debug("Bootstrap response waiting interrupted, possible due to Bootstrap is restarting MiNiFi process");
            return empty();
        }
    }

    private CommandResult sendBootstrapCommand(C2Operation c2Operation) {
        String command = createBootstrapCommand(c2Operation);
        try {
            return bootstrapCommunicator.sendCommand(command);
        } catch (IOException e) {
            LOGGER.error("Failed to send operation to bootstrap", e);
            return FAILURE;
        }
    }

    private String createBootstrapCommand(C2Operation c2Operation) {
        return ofNullable(c2Operation.getOperand())
            .map(operand -> c2Operation.getOperation().name() + "_" + operand.name())
            .orElse(c2Operation.getOperation().name());
    }

    private void bootstrapCallback(String[] params, OutputStream outputStream) {
        LOGGER.info("Received acknowledge message from bootstrap process");
        if (params.length < 1) {
            LOGGER.error("Invalid arguments coming from bootstrap");
            return;
        }
        MiNiFiCommandState miNiFiCommandState = MiNiFiCommandState.valueOf(params[0]);
        OperationState operationState = OPERATION_STATE_MAP.get(miNiFiCommandState);
        try {
            operationStateHolder.put(operationState);
        } catch (InterruptedException e) {
            LOGGER.warn("Bootstrap hook thread was interrupted");
        }
    }
}
