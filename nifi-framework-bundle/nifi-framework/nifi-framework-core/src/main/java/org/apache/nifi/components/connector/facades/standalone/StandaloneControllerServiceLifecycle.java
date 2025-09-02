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

import org.apache.nifi.components.connector.components.ControllerServiceLifecycle;
import org.apache.nifi.components.connector.components.ControllerServiceState;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.service.ControllerServiceNode;

import java.util.concurrent.CompletableFuture;

public class StandaloneControllerServiceLifecycle implements ControllerServiceLifecycle {
    private final ControllerServiceNode controllerServiceNode;
    private final ProcessScheduler processScheduler;

    public StandaloneControllerServiceLifecycle(final ControllerServiceNode controllerServiceNode, final ProcessScheduler scheduler) {
        this.controllerServiceNode = controllerServiceNode;
        this.processScheduler = scheduler;
    }

    @Override
    public ControllerServiceState getState() {
        return switch (controllerServiceNode.getState()) {
            case DISABLED -> ControllerServiceState.DISABLED;
            case ENABLING -> ControllerServiceState.ENABLING;
            case ENABLED -> ControllerServiceState.ENABLED;
            case DISABLING -> ControllerServiceState.DISABLING;
        };
    }

    @Override
    public CompletableFuture<Void> enable() {
        // If validating, perform validation to ensure it's complete before enabling
        final ValidationStatus currentStatus = controllerServiceNode.getValidationStatus();
        if (currentStatus != ValidationStatus.VALID) {
            controllerServiceNode.performValidation();
        }

        return processScheduler.enableControllerService(controllerServiceNode);
    }

    @Override
    public CompletableFuture<Void> disable() {
        return processScheduler.disableControllerService(controllerServiceNode);
    }
}
