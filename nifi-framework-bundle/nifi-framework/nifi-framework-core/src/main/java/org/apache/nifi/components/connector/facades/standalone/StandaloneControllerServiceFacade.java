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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.ConnectorParameterLookup;
import org.apache.nifi.components.connector.InvocationFailedException;
import org.apache.nifi.components.connector.components.ControllerServiceFacade;
import org.apache.nifi.components.connector.components.ControllerServiceLifecycle;
import org.apache.nifi.components.validation.DisabledServiceValidationResult;
import org.apache.nifi.components.validation.ValidationState;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterLookup;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StandaloneControllerServiceFacade implements ControllerServiceFacade {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL);

    private final ControllerServiceNode controllerServiceNode;
    private final VersionedControllerService versionedControllerService;
    private final ParameterContext parameterContext;
    private final ControllerServiceLifecycle lifecycle;
    private final ComponentContextProvider componentContextProvider;
    private final ComponentLog connectorLogger;
    private final ExtensionManager extensionManager;
    private final AssetManager assetManager;

    public StandaloneControllerServiceFacade(final ControllerServiceNode controllerServiceNode, final VersionedControllerService versionedControllerService,
            final ParameterContext parameterContext, final ProcessScheduler processScheduler, final ComponentContextProvider componentContextProvider, final ComponentLog connectorLogger,
            final ExtensionManager extensionManager, final AssetManager assetManager) {

        this.controllerServiceNode = controllerServiceNode;
        this.versionedControllerService = versionedControllerService;
        this.parameterContext = parameterContext;
        this.componentContextProvider = componentContextProvider;
        this.connectorLogger = connectorLogger;
        this.extensionManager = extensionManager;
        this.assetManager = assetManager;

        this.lifecycle = new StandaloneControllerServiceLifecycle(controllerServiceNode, processScheduler);
    }

    @Override
    public VersionedControllerService getDefinition() {
        return versionedControllerService;
    }

    @Override
    public ControllerServiceLifecycle getLifecycle() {
        return lifecycle;
    }

    @Override
    public List<ValidationResult> validate() {
        return validate(getDefinition().getProperties());
    }

    // TODO: Refactor to avoid duplicate code with StandaloneProcessorFacade
    @Override
    public List<ValidationResult> validate(final Map<String, String> propertyValues) {
        final ValidationContext validationContext = componentContextProvider.createValidationContext(controllerServiceNode, propertyValues, parameterContext);
        final ValidationState validationState = controllerServiceNode.performValidation(validationContext);
        final List<ValidationResult> validationErrors = validationState.getValidationErrors().stream()
            .filter(result -> !result.isValid())
            .filter(result -> !DisabledServiceValidationResult.isMatch(result))
            .toList();

        return validationErrors;
    }

    @Override
    public List<ConfigVerificationResult> verify(final Map<String, String> propertyValues, final Map<String, String> variables) {
        final ConfigurationContext configurationContext = componentContextProvider.createConfigurationContext(controllerServiceNode, propertyValues, parameterContext);
        return controllerServiceNode.verifyConfiguration(configurationContext, connectorLogger, variables, extensionManager, parameterContext);
    }

    @Override
    public List<ConfigVerificationResult> verify(final Map<String, String> propertyValues, final VersionedParameterContext parameterContext, final Map<String, String> variables) {
        final ParameterLookup parameterLookup = new ConnectorParameterLookup(List.of(parameterContext), assetManager);
        final ConfigurationContext configurationContext = componentContextProvider.createConfigurationContext(controllerServiceNode, propertyValues, parameterLookup);
        return controllerServiceNode.verifyConfiguration(configurationContext, connectorLogger, variables, extensionManager, parameterLookup);
    }

    @Override
    public List<ConfigVerificationResult> verify(final VersionedExternalFlow versionedExternalFlow, final Map<String, String> variables) {
        final VersionedProcessGroup parentGroup = findVersionedServiceGroup(versionedExternalFlow.getFlowContents());
        final VersionedControllerService versionedService = findVersionedService(parentGroup);
        if (versionedService == null) {
            throw new IllegalArgumentException("Cannot verify Controller Service with ID " + versionedControllerService.getIdentifier() + " because it does not exist in the provided flow");
        }

        final ConnectorParameterLookup connectorParameterLookup = new ConnectorParameterLookup(versionedExternalFlow.getParameterContexts().values(), assetManager);

        final Map<String, String> propertyValues = versionedService.getProperties();
        final ConfigurationContext configurationContext = componentContextProvider.createConfigurationContext(controllerServiceNode, propertyValues, connectorParameterLookup);
        return controllerServiceNode.verifyConfiguration(configurationContext, connectorLogger, variables, extensionManager, connectorParameterLookup);
    }

    private VersionedProcessGroup findVersionedServiceGroup(final VersionedProcessGroup group) {
        for (final VersionedControllerService service : group.getControllerServices()) {
            if (service.getIdentifier().equals(versionedControllerService.getIdentifier())) {
                return group;
            }
        }

        for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
            final VersionedProcessGroup serviceGroup = findVersionedServiceGroup(childGroup);
            if (serviceGroup != null) {
                return serviceGroup;
            }
        }

        return null;
    }

    private VersionedControllerService findVersionedService(final VersionedProcessGroup group) {
        if (group == null) {
            return null;
        }

        final VersionedProcessGroup parentGroup = findVersionedServiceGroup(group);
        if (parentGroup == null) {
            return null;
        }

        for (final VersionedControllerService service : parentGroup.getControllerServices()) {
            if (service.getIdentifier().equals(controllerServiceNode.getVersionedComponentId().orElseThrow())) {
                return service;
            }
        }

        return null;
    }

    @Override
    public Object invokeConnectorMethod(final String methodName, final Map<String, Object> arguments) throws InvocationFailedException {
        final Map<String, String> jsonArguments = serializeArgumentsToJson(arguments);
        final ConfigurationContext configurationContext = componentContextProvider.createConfigurationContext(controllerServiceNode, parameterContext);
        final String jsonResult = controllerServiceNode.invokeConnectorMethod(methodName, jsonArguments, configurationContext);
        if (jsonResult == null) {
            return null;
        }

        try {
            return OBJECT_MAPPER.readValue(jsonResult, Object.class);
        } catch (final JsonProcessingException e) {
            throw new InvocationFailedException("Failed to deserialize return value from Connector Method '" + methodName + "'", e);
        }
    }

    @Override
    public <T> T invokeConnectorMethod(final String methodName, final Map<String, Object> arguments, final Class<T> returnType) throws InvocationFailedException {
        final Map<String, String> jsonArguments = serializeArgumentsToJson(arguments);
        final ConfigurationContext configurationContext = componentContextProvider.createConfigurationContext(controllerServiceNode, parameterContext);
        final String jsonResult = controllerServiceNode.invokeConnectorMethod(methodName, jsonArguments, configurationContext);
        if (jsonResult == null) {
            return null;
        }

        try {
            return OBJECT_MAPPER.readValue(jsonResult, returnType);
        } catch (final JsonProcessingException e) {
            throw new InvocationFailedException("Failed to deserialize return value from Connector Method '" + methodName + "'", e);
        }
    }

    private Map<String, String> serializeArgumentsToJson(final Map<String, Object> arguments) throws InvocationFailedException {
        final Map<String, String> jsonArguments = new HashMap<>();
        for (final Map.Entry<String, Object> entry : arguments.entrySet()) {
            if (entry.getValue() == null) {
                jsonArguments.put(entry.getKey(), null);
            } else {
                try {
                    jsonArguments.put(entry.getKey(), OBJECT_MAPPER.writeValueAsString(entry.getValue()));
                } catch (final JsonProcessingException e) {
                    throw new InvocationFailedException("Failed to serialize argument '" + entry.getKey() + "' to JSON", e);
                }
            }
        }
        return jsonArguments;
    }
}
