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

import org.apache.nifi.c2.client.service.operation.ProcessorStateStrategy;
import org.apache.nifi.c2.protocol.api.C2OperationState.OperationState;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiConsumer;

import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.FULLY_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NOT_APPLIED;

public class DefaultProcessorStateStrategy implements ProcessorStateStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultProcessorStateStrategy.class);

    private final FlowController flowController;

    public DefaultProcessorStateStrategy(final FlowController flowController) {
        this.flowController = flowController;
    }

    @Override
    public OperationState startProcessor(final String processorId) {
        return changeState(processorId, this::start);
    }

    @Override
    public OperationState stopProcessor(final String processorId) {
        return changeState(processorId, this::stop);
    }

    private OperationState changeState(final String processorId, final BiConsumer<String, String> action) {
        try {
            final ProcessorNode node = flowController.getFlowManager().getProcessorNode(processorId);
            if (node == null) {
                LOGGER.warn("Processor with id {} not found", processorId);
                return NOT_APPLIED;
            }
            final String parentGroupId = node.getProcessGroupIdentifier();
            action.accept(processorId, parentGroupId);
            return FULLY_APPLIED;
        } catch (final Exception e) {
            LOGGER.error("Failed to change state for processor {}", processorId, e);
            return NOT_APPLIED;
        }
    }

    private void start(final String processorId, final String parentGroupId) {
        flowController.startProcessor(parentGroupId, processorId, true);
        LOGGER.info("Started processor {} (group={})", processorId, parentGroupId);
    }

    private void stop(final String processorId, final String parentGroupId) {
        flowController.stopProcessor(parentGroupId, processorId);
        LOGGER.info("Stopped processor {} (group={})", processorId, parentGroupId);
    }
}
