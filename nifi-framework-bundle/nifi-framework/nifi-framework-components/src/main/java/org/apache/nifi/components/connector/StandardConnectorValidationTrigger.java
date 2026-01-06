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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * Standard implementation of ConnectorValidationTrigger that submits validation
 * tasks to an ExecutorService for asynchronous execution.
 */
public class StandardConnectorValidationTrigger implements ConnectorValidationTrigger {
    private static final Logger logger = LoggerFactory.getLogger(StandardConnectorValidationTrigger.class);

    private final ScheduledExecutorService threadPool;
    private final BooleanSupplier flowInitialized;

    public StandardConnectorValidationTrigger(final ScheduledExecutorService threadPool, final BooleanSupplier flowInitialized) {
        this.threadPool = threadPool;
        this.flowInitialized = flowInitialized;
    }

    @Override
    public void triggerAsync(final ConnectorNode connector) {
        if (!flowInitialized.getAsBoolean()) {
            logger.debug("Triggered to perform validation on {} asynchronously but flow is not yet initialized so will ignore validation", connector);
            threadPool.schedule(() -> triggerAsync(connector), 1, TimeUnit.SECONDS);
            return;
        }

        threadPool.submit(() -> {
            try {
                if (connector.isValidationPaused()) {
                    logger.debug("Connector {} is currently marked as having validation paused; will retry in 1 second", connector);
                    threadPool.schedule(() -> triggerAsync(connector), 1, TimeUnit.SECONDS);
                }

                trigger(connector);
            } catch (final Exception e) {
                logger.error("Validation for connector {} failed; will retry in 5 seconds", connector, e);
                threadPool.schedule(() -> triggerAsync(connector), 5, TimeUnit.SECONDS);
            }
        });
    }

    @Override
    public void trigger(final ConnectorNode connector) {
        connector.performValidation();
    }
}

