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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

public class TriggerValidationTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(TriggerValidationTask.class);

    private final FlowManager flowManager;
    private final ValidationTrigger validationTrigger;
    private final ExecutorService executorService;
    private volatile boolean completed = false;

    public TriggerValidationTask(final FlowManager flowManager, final ValidationTrigger validationTrigger) {
        this(flowManager, validationTrigger, null);
    }

    /**
     * @param executorService when non-null, each component's (and Connector's) validation is submitted to this
     *                        executor and this task blocks in {@link #run()} until all of them complete, allowing
     *                        components to be validated concurrently instead of one at a time. When null,
     *                        components are validated serially on the calling thread, preserving the original
     *                        behavior. This is intended for the initial validation performed when a flow is loaded,
     *                        where the number of components can be large and validating them one at a time on a
     *                        single thread can noticeably slow down startup.
     */
    public TriggerValidationTask(final FlowManager flowManager, final ValidationTrigger validationTrigger, final ExecutorService executorService) {
        this.flowManager = flowManager;
        this.validationTrigger = validationTrigger;
        this.executorService = executorService;
    }

    /**
     * @return <code>true</code> if the most recent call to {@link #run()} triggered validation of every component
     *         and waited for it to complete; <code>false</code> if that call was interrupted, or the supplied
     *         {@link ExecutorService} rejected work, before validation of every component could be triggered and
     *         completed. Only meaningful when an <code>ExecutorService</code> was supplied to the constructor -
     *         serial validation always completes synchronously within {@link #run()}.
     */
    public boolean isValidationComplete() {
        return completed;
    }

    @Override
    public void run() {
        completed = false;

        try {
            logger.debug("Triggering validation of all components");

            final List<ComponentNode> nodes = new ArrayList<>();
            nodes.addAll(flowManager.getAllControllerServices());
            nodes.addAll(flowManager.getAllReportingTasks());
            nodes.addAll(flowManager.getAllFlowAnalysisRules());
            nodes.addAll(flowManager.getAllParameterProviders());
            nodes.addAll(flowManager.getRootGroup().findAllProcessors());
            nodes.addAll(flowManager.getAllFlowRegistryClients());

            final List<ConnectorNode> connectors = new ArrayList<>(flowManager.getAllConnectors());

            if (executorService == null) {
                for (final ComponentNode node : nodes) {
                    validationTrigger.trigger(node);
                }

                for (final ConnectorNode connector : connectors) {
                    connector.validateComponents(validationTrigger);
                }

                completed = true;
            } else {
                completed = triggerInParallel(nodes, connectors);
            }
        } catch (final Throwable t) {
            logger.error("Encountered unexpected error when attempting to validate components", t);
        }
    }

    /**
     * Submits validation of every given component and Connector to the executor and waits for all of it to
     * complete before returning. Each component maintains its own validation state independently, so validating
     * distinct components concurrently on the executor is safe.
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
