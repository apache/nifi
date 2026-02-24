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

import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationState.OperationState;
import org.apache.nifi.minifi.bootstrap.BootstrapCommunicator;
import org.apache.nifi.minifi.bootstrap.CommandResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;

import static java.lang.Thread.sleep;
import static java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.FULLY_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NOT_APPLIED;
import static org.apache.nifi.c2.protocol.api.OperationType.START;
import static org.apache.nifi.minifi.bootstrap.CommandResult.FAILURE;
import static org.apache.nifi.minifi.bootstrap.CommandResult.SUCCESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class BootstrapC2OperationRestartHandlerTest {

    @Test
    void shouldReturnNotAppliedWhenBootstrapCommunicatorReturnsFalse() throws IOException {
        final C2Operation inputOperation = new C2Operation();
        inputOperation.setOperation(START);
        final BootstrapCommunicator bootstrapCommunicator = mock(BootstrapCommunicator.class);
        when(bootstrapCommunicator.sendCommand(START.name())).thenReturn(FAILURE);
        final long bootstrapAcknowledgeTimeoutMs = 0;

        final BootstrapC2OperationRestartHandler testHandler = new BootstrapC2OperationRestartHandler(bootstrapCommunicator, bootstrapAcknowledgeTimeoutMs);
        final Optional<OperationState> result = testHandler.handleRestart(inputOperation);

        assertTrue(result.isPresent());
        assertEquals(NOT_APPLIED, result.get());
    }

    @Test
    void shouldReturnNotAppliedWhenBootstrapCommunicatorThrowsException() throws IOException {
        final C2Operation inputOperation = new C2Operation();
        inputOperation.setOperation(START);
        final BootstrapCommunicator bootstrapCommunicator = mock(BootstrapCommunicator.class);
        when(bootstrapCommunicator.sendCommand(START.name())).thenThrow(new IOException());
        final long bootstrapAcknowledgeTimeoutMs = 0;

        final BootstrapC2OperationRestartHandler testHandler = new BootstrapC2OperationRestartHandler(bootstrapCommunicator, bootstrapAcknowledgeTimeoutMs);
        final Optional<OperationState> result = testHandler.handleRestart(inputOperation);

        assertTrue(result.isPresent());
        assertEquals(NOT_APPLIED, result.get());
    }

    @Test
    void shouldReturnStateAcknowledgedByBootstrapCommunicator() {
        final C2Operation inputOperation = new C2Operation();
        inputOperation.setOperation(START);
        final long bootstrapAcknowledgeTimeoutMs = 1000;
        final long waitBeforeAcknowledgeMs = 100;
        final String[] callbackResult = new String[] {FULLY_APPLIED.name()};
        final BootstrapCommunicatorStub bootstrapCommunicator = new BootstrapCommunicatorStub(SUCCESS, callbackResult, waitBeforeAcknowledgeMs);

        final BootstrapC2OperationRestartHandler testHandler = new BootstrapC2OperationRestartHandler(bootstrapCommunicator, bootstrapAcknowledgeTimeoutMs);
        try (ExecutorService executorService = newVirtualThreadPerTaskExecutor()) {
            executorService.execute(bootstrapCommunicator);
            final Optional<OperationState> result = testHandler.handleRestart(inputOperation);

            assertTrue(result.isPresent());
            assertEquals(FULLY_APPLIED, result.get());
        }
    }

    @Test
    void shouldReturnNotAppliedWhenBootstrapAcknowledgeTimesOut() {
        final C2Operation inputOperation = new C2Operation();
        inputOperation.setOperation(START);
        final String[] callbackResult = new String[] {FULLY_APPLIED.name()};
        final long bootstrapAcknowledgeTimeoutMs = 1000;
        final long waitBeforeAcknowledgeMs = 2000;
        final BootstrapCommunicatorStub bootstrapCommunicator = new BootstrapCommunicatorStub(SUCCESS, callbackResult, waitBeforeAcknowledgeMs);

        final BootstrapC2OperationRestartHandler testHandler = new BootstrapC2OperationRestartHandler(bootstrapCommunicator, bootstrapAcknowledgeTimeoutMs);
        try (ExecutorService executorService = newVirtualThreadPerTaskExecutor()) {
            executorService.execute(bootstrapCommunicator);
            final Optional<OperationState> result = testHandler.handleRestart(inputOperation);

            assertTrue(result.isPresent());
            assertEquals(NOT_APPLIED, result.get());
        }
    }

    @Test
    void shouldReturnNotAppliedWhenBootstrapSendInvalidResponse() {
        final C2Operation inputOperation = new C2Operation();
        inputOperation.setOperation(START);
        final String[] callbackResult = new String[] {};
        final long bootstrapAcknowledgeTimeoutMs = 1000;
        final long waitBeforeAcknowledgeMs = 100;
        final BootstrapCommunicatorStub bootstrapCommunicator = new BootstrapCommunicatorStub(SUCCESS, callbackResult, waitBeforeAcknowledgeMs);

        final BootstrapC2OperationRestartHandler testHandler = new BootstrapC2OperationRestartHandler(bootstrapCommunicator, bootstrapAcknowledgeTimeoutMs);
        try (ExecutorService executorService = newVirtualThreadPerTaskExecutor()) {
            executorService.execute(bootstrapCommunicator);
            final Optional<OperationState> result = testHandler.handleRestart(inputOperation);

            assertTrue(result.isPresent());
            assertEquals(NOT_APPLIED, result.get());
        }
    }

    static class BootstrapCommunicatorStub implements BootstrapCommunicator, Runnable {

        private final CommandResult sendCommandResult;
        private final String[] callbackResult;
        private final long waitBeforeAcknowledgeMs;
        private BiConsumer<String[], OutputStream> handler;

        BootstrapCommunicatorStub(final CommandResult sendCommandResult, final String[] callbackResult, final long waitBeforeAcknowledgeMs) {
            this.sendCommandResult = sendCommandResult;
            this.callbackResult = callbackResult;
            this.waitBeforeAcknowledgeMs = waitBeforeAcknowledgeMs;
        }

        @Override
        public void run() {
            try {
                sleep(waitBeforeAcknowledgeMs);
            } catch (final InterruptedException ignored) {
            }
            handler.accept(callbackResult, null);
        }

        @Override
        public CommandResult sendCommand(final String command, final String... args) {
            return sendCommandResult;
        }

        @Override
        public void registerMessageHandler(final String command, final BiConsumer<String[], OutputStream> handler) {
            this.handler = handler;
        }
    }

}

