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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * Standard implementation of ConnectorValidationTrigger that submits validation
 * tasks to an ScheduledExecutorService for asynchronous execution.
 */
public class StandardConnectorValidationTrigger implements ConnectorValidationTrigger {
    private static final Logger logger = LoggerFactory.getLogger(StandardConnectorValidationTrigger.class);

    private final ScheduledExecutorService threadPool;
    private final BooleanSupplier flowInitialized;
    private final Set<ConnectorNode> activelyValidating = Collections.synchronizedSet(new HashSet<>());

    public StandardConnectorValidationTrigger(final ScheduledExecutorService threadPool, final BooleanSupplier flowInitialized) {
        this.threadPool = threadPool;
        this.flowInitialized = flowInitialized;
    }

    @Override
    public void triggerAsync(final ConnectorNode connector) {
        // Avoid adding multiple validation tasks for the same connector concurrently. This is not 100% thread safe because when a task
        // is rescheduled, there's a small window where a second thread could be scheduled after the Connector is removed from 'activelyValidating' and
        // before the task is rescheduled. However, this is acceptable because having multiple threads validating concurrently is safe, it's just inefficient.
        final boolean added = activelyValidating.add(connector);
        if (!added) {
            logger.debug("Connector {} is already undergoing validation; will not trigger another validation concurrently", connector);
            return;
        }

        if (!flowInitialized.getAsBoolean()) {
            logger.debug("Triggered to perform validation on {} asynchronously but flow is not yet initialized so will ignore validation", connector);
            reschedule(connector, Duration.ofSeconds(1));
            return;
        }

        threadPool.submit(() -> {
            try {
                if (connector.isValidationPaused()) {
                    logger.debug("Connector {} is currently marked as having validation paused; will retry in 1 second", connector);
                    reschedule(connector, Duration.ofSeconds(1));
                    return;
                }

                trigger(connector);

                activelyValidating.remove(connector);
            } catch (final Exception e) {
                logger.error("Validation for connector {} failed; will retry in 5 seconds", connector, e);
                reschedule(connector, Duration.ofSeconds(5));
            }
        });
    }

    @Override
    public void trigger(final ConnectorNode connector) {
        connector.performValidation();
    }

    private void reschedule(final ConnectorNode connector, final Duration delay) {
        activelyValidating.remove(connector);
        threadPool.schedule(() -> triggerAsync(connector), delay.toMillis(), TimeUnit.MILLISECONDS);
    }
}

