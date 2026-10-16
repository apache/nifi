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

import org.apache.nifi.c2.client.service.operation.ConnectionQueueStrategy;
import org.apache.nifi.c2.protocol.api.C2OperationState.OperationState;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.queue.DropFlowFileState;
import org.apache.nifi.controller.queue.DropFlowFileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.FULLY_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NOT_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.PARTIALLY_APPLIED;

public class DefaultConnectionQueueStrategy implements ConnectionQueueStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultConnectionQueueStrategy.class);

    static final String REQUESTOR = "C2";
    static final Duration DEFAULT_DROP_TIMEOUT = Duration.ofSeconds(30);

    private final FlowController flowController;
    private final Duration dropTimeout;

    public DefaultConnectionQueueStrategy(FlowController flowController) {
        this(flowController, DEFAULT_DROP_TIMEOUT);
    }

    DefaultConnectionQueueStrategy(FlowController flowController, Duration dropTimeout) {
        this.flowController = flowController;
        this.dropTimeout = dropTimeout;
    }

    @Override
    public OperationState clearQueue(String connectionId) {
        try {
            Connection connection = flowController.getFlowManager().getConnection(connectionId);
            if (connection == null) {
                LOGGER.warn("Connection with id {} not found", connectionId);
                return NOT_APPLIED;
            }

            DropFlowFileStatus status = connection.getFlowFileQueue().dropFlowFiles(randomUUID().toString(), REQUESTOR);
            try {
                status.getCompletionFuture().get(dropTimeout.toMillis(), MILLISECONDS);
            } catch (TimeoutException e) {
                LOGGER.warn("Clearing queue of connection {} did not complete within {}, current state: {}", connectionId, dropTimeout, status.getState());
                return PARTIALLY_APPLIED;
            }

            if (status.getState() == DropFlowFileState.COMPLETE) {
                LOGGER.info("Cleared queue of connection {}, dropped {}", connectionId, status.getDroppedSize());
                return FULLY_APPLIED;
            }
            LOGGER.warn("Clearing queue of connection {} ended in state {}: {}", connectionId, status.getState(), status.getFailureReason());
            return NOT_APPLIED;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error("Interrupted while clearing queue of connection {}", connectionId, e);
            return NOT_APPLIED;
        } catch (Exception e) {
            LOGGER.error("Failed to clear queue of connection {}", connectionId, e);
            return NOT_APPLIED;
        }
    }
}
