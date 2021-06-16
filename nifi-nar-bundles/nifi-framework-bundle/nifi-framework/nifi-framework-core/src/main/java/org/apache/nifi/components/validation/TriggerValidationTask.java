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

import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TriggerValidationTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(TriggerValidationTask.class);

    private final FlowManager flowManager;
    private final ValidationTrigger validationTrigger;

    public TriggerValidationTask(final FlowManager flowManager, final ValidationTrigger validationTrigger) {
        this.flowManager = flowManager;
        this.validationTrigger = validationTrigger;
    }

    @Override
    public void run() {
        try {
            logger.debug("Triggering validation of all components");

            for (final ComponentNode node : flowManager.getAllControllerServices()) {
                validationTrigger.trigger(node);
            }

            for (final ComponentNode node : flowManager.getAllReportingTasks()) {
                validationTrigger.trigger(node);
            }

            for (final ComponentNode node : flowManager.getRootGroup().findAllProcessors()) {
                validationTrigger.trigger(node);
            }
        } catch (final Throwable t) {
            logger.error("Encountered unexpected error when attempting to validate components", t);
        }
    }
}
