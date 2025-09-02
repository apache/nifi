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

package org.apache.nifi.components.connector.facades.standalone;

import org.apache.nifi.components.connector.components.ProcessorLifecycle;
import org.apache.nifi.components.connector.components.ProcessorState;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.lifecycle.ProcessorStopLifecycleMethods;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class StandaloneProcessorLifecycle implements ProcessorLifecycle {
    private static final Logger logger = LoggerFactory.getLogger(StandaloneProcessorLifecycle.class);
    private final ProcessorNode processorNode;
    private final ProcessScheduler processScheduler;

    public StandaloneProcessorLifecycle(final ProcessorNode processorNode, final ProcessScheduler scheduler) {
        this.processorNode = processorNode;
        this.processScheduler = scheduler;
    }

    @Override
    public ProcessorState getState() {
        final ScheduledState scheduledState = processorNode.getScheduledState();
        return switch (scheduledState) {
            case DISABLED -> ProcessorState.DISABLED;
            case RUNNING, RUN_ONCE -> ProcessorState.RUNNING;
            case STOPPED -> processorNode.getActiveThreadCount() == 0 ? ProcessorState.STOPPED : ProcessorState.STOPPING;
            case STOPPING -> ProcessorState.STOPPING;
            case STARTING -> ProcessorState.STARTING;
        };
    }

    @Override
    public int getActiveThreadCount() {
        return processorNode.getActiveThreadCount();
    }

    @Override
    public void terminate() {
        try {
            processScheduler.terminateProcessor(processorNode);
        } catch (final Exception e) {
            logger.warn("Failed to terminate processor {}", processorNode, e);
        }
    }

    @Override
    public CompletableFuture<Void> stop() {
        return processScheduler.stopProcessor(processorNode, ProcessorStopLifecycleMethods.TRIGGER_ALL);
    }

    @Override
    public CompletableFuture<Void> start() {
        // Perform validation if necessary before starting.
        final ValidationStatus validationStatus = processorNode.getValidationStatus();
        if (validationStatus != ValidationStatus.VALID) {
            processorNode.performValidation();
        }

        return processScheduler.startProcessor(processorNode, false);
    }

    @Override
    public void disable() {
        processScheduler.disableProcessor(processorNode);
    }

    @Override
    public void enable() {
        processScheduler.enableProcessor(processorNode);
    }
}
