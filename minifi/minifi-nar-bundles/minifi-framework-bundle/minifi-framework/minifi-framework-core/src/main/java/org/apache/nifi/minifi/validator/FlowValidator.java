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

package org.apache.nifi.minifi.validator;

import static org.apache.nifi.components.AsyncLoadedProcessor.LoadState.DOWNLOADING_DEPENDENCIES;
import static org.apache.nifi.components.AsyncLoadedProcessor.LoadState.INITIALIZING_ENVIRONMENT;
import static org.apache.nifi.components.AsyncLoadedProcessor.LoadState.LOADING_PROCESSOR_CODE;
import static org.apache.nifi.components.validation.ValidationStatus.INVALID;
import static org.apache.nifi.components.validation.ValidationStatus.VALIDATING;
import static org.apache.nifi.controller.service.ControllerServiceState.ENABLING;
import static org.apache.nifi.minifi.commons.utils.RetryUtil.retry;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.nifi.components.AsyncLoadedProcessor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.service.StandardControllerServiceNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FlowValidator {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlowValidator.class);

    private static final Set<AsyncLoadedProcessor.LoadState> INITIALIZING_ASYNC_PROCESSOR_STATES =
        Set.of(INITIALIZING_ENVIRONMENT, DOWNLOADING_DEPENDENCIES, LOADING_PROCESSOR_CODE);

    private static final int ASYNC_LOADING_COMPONENT_INIT_RETRY_PAUSE_DURATION_MS = 5000;
    private static final int ASYNC_LOADING_COMPONENT_INIT_MAX_RETRIES = 60;
    private static final int VALIDATION_RETRY_PAUSE_DURATION_MS = 1000;
    private static final int VALIDATION_MAX_RETRIES = 5;
    private static final int CONTROLLER_SERVICE_ENABLEMENT_RETRY_PAUSE_DURATION_MS = 500;
    private static final int CONTROLLER_SERVICE_ENABLEMENT_MAX_RETRIES = 20;

    private FlowValidator() {
        throw new UnsupportedOperationException();
    }

    public static List<ValidationResult> validate(FlowManager flowManager) {
        List<? extends ComponentNode> componentNodes = extractComponentNodes(flowManager);

        waitForAsyncLoadingComponentsToInitialize(componentNodes);
        waitForControllerServicesToEnable(componentNodes);
        waitForComponentsToValidate(componentNodes);

        return collectValidationErrors(componentNodes);
    }

    private static List<? extends ComponentNode> extractComponentNodes(FlowManager flowManager) {
        return Stream.of(
                flowManager.getAllControllerServices(),
                flowManager.getAllReportingTasks(),
                Set.copyOf(flowManager.getRootGroup().findAllProcessors()))
            .flatMap(Set::stream)
            .toList();
    }

    private static void waitForAsyncLoadingComponentsToInitialize(List<? extends ComponentNode> componentNodes) {
        retry(
            () -> initializingAsyncLoadingComponents(componentNodes),
            List::isEmpty,
            ASYNC_LOADING_COMPONENT_INIT_MAX_RETRIES,
            ASYNC_LOADING_COMPONENT_INIT_RETRY_PAUSE_DURATION_MS
        )
        .ifPresent(components -> {
            LOGGER.error("The following components are async loading components and are still initializing: {}", components);
            throw new IllegalStateException("Maximum retry number exceeded while waiting for async loading components to be initialized");
        });
    }

    private static List<? extends ComponentNode> initializingAsyncLoadingComponents(List<? extends ComponentNode> componentNodes) {
        return componentNodes.stream()
            .filter(componentNode -> componentNode.performValidation() == INVALID)
            .filter(componentNode -> componentNode.getComponent() instanceof AsyncLoadedProcessor asyncLoadedProcessor
                && INITIALIZING_ASYNC_PROCESSOR_STATES.contains(asyncLoadedProcessor.getState()))
            .toList();
    }

    private static void waitForControllerServicesToEnable(List<? extends ComponentNode> componentNodes) {
        retry(
            () -> controllerServicesInEnablingState(componentNodes),
            List::isEmpty,
            CONTROLLER_SERVICE_ENABLEMENT_MAX_RETRIES,
            CONTROLLER_SERVICE_ENABLEMENT_RETRY_PAUSE_DURATION_MS
        )
        .ifPresent(controllerServices -> {
            LOGGER.error("The following controller services are still in ENABLING state: {}", controllerServices);
        });
    }

    private static List<? extends ComponentNode> controllerServicesInEnablingState(List<? extends ComponentNode> componentNodes) {
        return componentNodes.stream()
            .filter(componentNode -> componentNode instanceof StandardControllerServiceNode controllerServiceNode && controllerServiceNode.getState() == ENABLING)
            .toList();
    }

    private static void waitForComponentsToValidate(List<? extends ComponentNode> componentNodes) {
        retry(
            () -> componentsInValidatingState(componentNodes),
            List::isEmpty,
            VALIDATION_MAX_RETRIES,
            VALIDATION_RETRY_PAUSE_DURATION_MS
        )
        .ifPresent(components -> {
            LOGGER.error("The following components are still in VALIDATING state: {}", components);
            throw new IllegalStateException("Maximum retry number exceeded while waiting for components to be validated");
        });
    }

    private static List<? extends ComponentNode> componentsInValidatingState(List<? extends ComponentNode> componentNodes) {
        return componentNodes.stream()
            .filter(componentNode -> componentNode.performValidation() == VALIDATING)
            .toList();
    }

    private static List<ValidationResult> collectValidationErrors(List<? extends ComponentNode> componentNodes) {
        return componentNodes.stream()
            .map(ComponentNode::getValidationErrors)
            .flatMap(Collection::stream)
            .toList();
    }
}
