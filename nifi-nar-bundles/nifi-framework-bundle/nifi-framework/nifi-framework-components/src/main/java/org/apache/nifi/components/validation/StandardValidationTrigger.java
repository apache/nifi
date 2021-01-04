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

import java.util.concurrent.ExecutorService;
import java.util.function.BooleanSupplier;

import org.apache.nifi.controller.ComponentNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandardValidationTrigger implements ValidationTrigger {
    private static final Logger logger = LoggerFactory.getLogger(StandardValidationTrigger.class);

    private final ExecutorService threadPool;
    private final BooleanSupplier flowInitialized;

    public StandardValidationTrigger(final ExecutorService threadPool, final BooleanSupplier flowInitialized) {
        this.threadPool = threadPool;
        this.flowInitialized = flowInitialized;
    }

    @Override
    public void triggerAsync(final ComponentNode component) {
        if (!flowInitialized.getAsBoolean()) {
            logger.debug("Triggered to perform validation on {} asynchronously but flow is not yet initialized so will ignore validation", component);
            return;
        }

        threadPool.submit(() -> trigger(component));
    }

    @Override
    public void trigger(final ComponentNode component) {
        try {
            if (component.isValidationNecessary()) {
                component.performValidation();
            }
        } catch (final Throwable t) {
            component.getLogger().error("Failed to perform validation due to " + t, t);
        }
    }

}
