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

import org.apache.nifi.c2.client.service.operation.FlowStateStrategy;
import org.apache.nifi.c2.protocol.api.C2OperationState.OperationState;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.FULLY_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NOT_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.PARTIALLY_APPLIED;

public class DefaultFlowStateStrategy implements FlowStateStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultFlowStateStrategy.class);

    private final FlowController flowController;

    public DefaultFlowStateStrategy(FlowController flowController) {
        this.flowController = flowController;
    }

    @Override
    public OperationState start() {
        try {
            ProcessGroup rootProcessGroup = flowController.getFlowManager().getRootGroup();
            if (rootProcessGroup != null) {
                startProcessGroup(rootProcessGroup);
                LOGGER.info("Flow started");
                return FULLY_APPLIED;
            } else {
                LOGGER.error("Failed to start flow as the root process group is null.");
                return NOT_APPLIED;
            }
        } catch (Exception e) {
            LOGGER.error("Failed to start flow as one or more components failed to stop.", e);
            return PARTIALLY_APPLIED;
        }
    }

    @Override
    public OperationState stop() {
        try {
            ProcessGroup rootProcessGroup = flowController.getFlowManager().getRootGroup();
            if (rootProcessGroup != null) {
                stopProcessGroup(rootProcessGroup);
                LOGGER.info("Flow stopped");
                return FULLY_APPLIED;
            } else {
                LOGGER.error("Failed to stop flow as the root process group is null.");
                return NOT_APPLIED;
            }
        } catch (Exception e) {
            LOGGER.error("Failed to stop flow as one or more components failed to stop.", e);
            return PARTIALLY_APPLIED;
        }
    }

    private void startProcessGroup(ProcessGroup processGroup) {
        processGroup.startProcessing();
        processGroup.getRemoteProcessGroups().forEach(RemoteProcessGroup::startTransmitting);

        processGroup.getProcessGroups().forEach(this::startProcessGroup);
    }

    private void stopProcessGroup(ProcessGroup processGroup) {
        waitForStopOrLogTimeOut(processGroup.stopProcessing());
        processGroup.getRemoteProcessGroups().stream()
                .map(RemoteProcessGroup::stopTransmitting)
                .forEach(this::waitForStopOrLogTimeOut);

        processGroup.getProcessGroups().forEach(this::stopProcessGroup);
    }

    private void waitForStopOrLogTimeOut(Future<?> future) {
        try {
            future.get(10, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            LOGGER.warn("Unable to stop component within defined interval", e);
        }
    }
}
