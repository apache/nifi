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

package org.apache.nifi.components.validation;

import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

/**
 * Triggers validation of every component and Connector by submitting each one to an {@link ExecutorService} and
 * waiting for all of them to complete, instead of validating one at a time on the calling thread. Each
 * {@link ComponentNode} tracks its own validation state independently, so validating distinct components
 * concurrently is safe.
 * <p>
 * This is intended for the one-time initial validation sweep performed when a flow is loaded during
 * {@code FlowController} initialization, where the number of components can be large and validating them one at a
 * time on a single thread can noticeably slow down startup. The periodic re-validation performed thereafter
 * continues to use the serial {@link TriggerValidationTask}.
 */
public class ParallelTriggerValidationTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ParallelTriggerValidationTask.class);

    private final FlowManager flowManager;
    private final ValidationTrigger validationTrigger;
    private final ExecutorService executorService;
    private volatile boolean completed = false;

    public ParallelTriggerValidationTask(final FlowManager flowManager, final ValidationTrigger validationTrigger, final ExecutorService executorService) {
        this.flowManager = Objects.requireNonNull(flowManager, "FlowManager is required");
        this.validationTrigger = Objects.requireNonNull(validationTrigger, "ValidationTrigger is required");
        this.executorService = Objects.requireNonNull(executorService, "ExecutorService is required");
    }

    /**
     * @return <code>true</code> if the most recent call to {@link #run()} triggered validation of every component
     *         and waited for it to complete; <code>false</code> if that call was interrupted, or the
     *         {@link ExecutorService} rejected work, before validation of every component could be triggered and
     *         completed.
     */
    public boolean isValidationComplete() {
        return completed;
    }

    @Override
    public void run() {
        completed = false;

        try {
            logger.debug("Triggering validation of all components in parallel");

            final List<ComponentNode> nodes = ValidatableComponents.getComponentNodes(flowManager);
            final List<ConnectorNode> connectors = ValidatableComponents.getConnectors(flowManager);

            completed = triggerInParallel(nodes, connectors);
        } catch (final Throwable t) {
            logger.error("Encountered unexpected error when attempting to validate components", t);
        }
    }

    /**
     * Submits validation of every given component and Connector to the executor and waits for all of it to
     * complete before returning.
     *
     * @return <code>true</code> if every component and Connector was successfully submitted for validation and
     *         validation of all of them completed; <code>false</code> otherwise.
     */
    private boolean triggerInParallel(final List<ComponentNode> nodes, final List<ConnectorNode> connectors) {
        final List<Future<?>> futures = new ArrayList<>(nodes.size() + connectors.size());
        boolean allSubmitted = true;

        for (final ComponentNode node : nodes) {
            if (!submit(futures, () -> validationTrigger.trigger(node))) {
                allSubmitted = false;
                break;
            }
        }

        if (allSubmitted) {
            for (final ConnectorNode connector : connectors) {
                if (!submit(futures, () -> connector.validateComponents(validationTrigger))) {
                    allSubmitted = false;
                    break;
                }
            }
        }

        final boolean allAwaited = awaitAll(futures);
        return allSubmitted && allAwaited;
    }

    private boolean submit(final List<Future<?>> futures, final Runnable task) {
        try {
            futures.add(executorService.submit(task));
            return true;
        } catch (final RejectedExecutionException e) {
            logger.warn("Validation thread pool rejected further work while triggering initial validation; not all components were submitted for validation");
            return false;
        }
    }

    /**
     * Waits for every given Future to complete. If interrupted while waiting, does not block any further:
     * the remaining, not-yet-started Futures are cancelled and this method returns immediately, so that shutdown
     * is not delayed.
     *
     * @return <code>true</code> if every Future completed (whether successfully or with an exception);
     *         <code>false</code> if interrupted before all of them completed.
     */
    private boolean awaitAll(final List<Future<?>> futures) {
        for (int i = 0; i < futures.size(); i++) {
            try {
                futures.get(i).get();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Interrupted while waiting for initial component validation to complete; {} of {} validation tasks had not yet finished",
                        futures.size() - i, futures.size());

                for (int j = i; j < futures.size(); j++) {
                    futures.get(j).cancel(false);
                }

                return false;
            } catch (final ExecutionException e) {
                logger.error("Failed to validate a component during initial validation", e.getCause());
            }
        }

        return true;
    }
}
