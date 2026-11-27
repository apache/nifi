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

package org.apache.nifi.minifi.c2.command;

import org.apache.nifi.c2.protocol.api.C2OperationState.OperationState;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.queue.DropFlowFileState;
import org.apache.nifi.controller.queue.DropFlowFileStatus;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.FULLY_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NOT_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.PARTIALLY_APPLIED;
import static org.apache.nifi.minifi.c2.command.DefaultConnectionQueueStrategy.REQUESTOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DefaultConnectionQueueStrategyTest {

    private static final String CONNECTION_ID = "5c1d0f3e-8a4b-4a7e-9f2c-1b6d8e3a7c42";

    @Mock
    private FlowController flowController;

    @Mock
    private FlowManager flowManager;

    @Mock
    private Connection connection;

    @Mock
    private FlowFileQueue flowFileQueue;

    @Mock
    private DropFlowFileStatus dropFlowFileStatus;

    private DefaultConnectionQueueStrategy victim;

    @BeforeEach
    public void init() {
        victim = new DefaultConnectionQueueStrategy(flowController, Duration.ofMillis(100));
        when(flowController.getFlowManager()).thenReturn(flowManager);
    }

    @Test
    public void testClearQueueNotAppliedWhenConnectionMissing() {
        when(flowManager.getConnection(CONNECTION_ID)).thenReturn(null);

        OperationState result = victim.clearQueue(CONNECTION_ID);

        assertEquals(NOT_APPLIED, result);
    }

    @Test
    public void testClearQueueFullyApplied() {
        givenDropRequest(CompletableFuture.completedFuture(null));
        when(dropFlowFileStatus.getState()).thenReturn(DropFlowFileState.COMPLETE);

        OperationState result = victim.clearQueue(CONNECTION_ID);

        assertEquals(FULLY_APPLIED, result);
    }

    @Test
    public void testClearQueueNotAppliedWhenDropFails() {
        givenDropRequest(CompletableFuture.completedFuture(null));
        when(dropFlowFileStatus.getState()).thenReturn(DropFlowFileState.FAILURE);

        OperationState result = victim.clearQueue(CONNECTION_ID);

        assertEquals(NOT_APPLIED, result);
    }

    @Test
    public void testClearQueuePartiallyAppliedWhenDropDoesNotCompleteInTime() {
        givenDropRequest(new CompletableFuture<>());
        when(dropFlowFileStatus.getState()).thenReturn(DropFlowFileState.DROPPING_FLOWFILES);

        OperationState result = victim.clearQueue(CONNECTION_ID);

        assertEquals(PARTIALLY_APPLIED, result);
    }

    @Test
    public void testClearQueueNotAppliedOnError() {
        when(flowManager.getConnection(CONNECTION_ID)).thenReturn(connection);
        when(connection.getFlowFileQueue()).thenReturn(flowFileQueue);
        when(flowFileQueue.dropFlowFiles(anyString(), eq(REQUESTOR))).thenThrow(new IllegalStateException("boom"));

        OperationState result = victim.clearQueue(CONNECTION_ID);

        assertEquals(NOT_APPLIED, result);
    }

    private void givenDropRequest(CompletableFuture<Void> completionFuture) {
        when(flowManager.getConnection(CONNECTION_ID)).thenReturn(connection);
        when(connection.getFlowFileQueue()).thenReturn(flowFileQueue);
        when(flowFileQueue.dropFlowFiles(anyString(), eq(REQUESTOR))).thenReturn(dropFlowFileStatus);
        when(dropFlowFileStatus.getCompletionFuture()).thenReturn(completionFuture);
    }
}
