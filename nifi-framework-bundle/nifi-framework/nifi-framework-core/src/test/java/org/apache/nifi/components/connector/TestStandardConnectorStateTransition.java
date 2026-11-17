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

package org.apache.nifi.components.connector;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestStandardConnectorStateTransition {

    @Test
    void testAddPendingStartFutureWhenRunningCompletesFuture() {
        final StandardConnectorStateTransition stateTransition = new StandardConnectorStateTransition("Test Connector");
        stateTransition.setCurrentState(ConnectorState.RUNNING);
        final CompletableFuture<Void> startFuture = new CompletableFuture<>();

        stateTransition.addPendingStartFuture(startFuture);

        assertTrue(startFuture.isDone());
    }

    @Test
    void testPendingStopFutureCompletesOnlyWhenStoppedAndFailsOnOtherStates() {
        final StandardConnectorStateTransition stateTransition = new StandardConnectorStateTransition("Test Connector");
        stateTransition.setCurrentState(ConnectorState.STOPPING);
        final CompletableFuture<Void> stoppedFuture = new CompletableFuture<>();
        stateTransition.addPendingStopFuture(stoppedFuture);

        stateTransition.setCurrentState(ConnectorState.STOPPING);
        assertFalse(stoppedFuture.isDone());
        stateTransition.setCurrentState(ConnectorState.STOPPED);
        assertTrue(stoppedFuture.isDone());
        assertFalse(stoppedFuture.isCompletedExceptionally());

        stateTransition.setCurrentState(ConnectorState.PREPARING_FOR_UPDATE);
        final CompletableFuture<Void> abandonedFuture = new CompletableFuture<>();
        stateTransition.addPendingStopFuture(abandonedFuture);
        assertTrue(stateTransition.trySetCurrentState(ConnectorState.PREPARING_FOR_UPDATE, ConnectorState.UPDATE_FAILED));

        final ExecutionException failure = assertThrows(ExecutionException.class, abandonedFuture::get);
        assertInstanceOf(IllegalStateException.class, failure.getCause());
        assertTrue(failure.getCause().getMessage().contains("UPDATE_FAILED"), failure.getCause().getMessage());
    }
}
