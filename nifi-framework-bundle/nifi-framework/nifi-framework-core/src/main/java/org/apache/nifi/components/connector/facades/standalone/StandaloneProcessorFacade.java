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
import org.apache.nifi.components.connector.components.ProcessorFacade;
import org.apache.nifi.components.connector.components.ProcessorLifecycle;
import org.apache.nifi.components.validation.DisabledServiceValidationResult;
import org.apache.nifi.components.validation.ValidationState;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.processor.ProcessContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class StandaloneProcessorFacade implements ProcessorFacade {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL);

    private final ProcessorNode processorNode;
    private final VersionedProcessor versionedProcessor;
    private final ParameterContext parameterContext;
    private final ProcessorLifecycle lifecycle;
    private final ComponentContextProvider componentContextProvider;
    private final ComponentLog connectorLogger;
    private final ExtensionManager extensionManager;
    private final AssetManager assetManager;

    public StandaloneProcessorFacade(final ProcessorNode processorNode, final VersionedProcessor versionedProcessor, final ProcessScheduler scheduler,
            final ParameterContext parameterContext, final ComponentContextProvider componentContextProvider, final ComponentLog connectorLogger,
            final ExtensionManager extensionManager, final AssetManager assetManager) {

        this.processorNode = processorNode;
        this.versionedProcessor = versionedProcessor;
        this.parameterContext = parameterContext;
        this.componentContextProvider = componentContextProvider;
        this.connectorLogger = connectorLogger;
        this.extensionManager = extensionManager;
        this.assetManager = assetManager;

        this.lifecycle = new StandaloneProcessorLifecycle(processorNode, scheduler);
    }

    @Override
    public VersionedProcessor getDefinition() {
        return versionedProcessor;
    }

    @Override
    public ProcessorLifecycle getLifecycle() {
        return lifecycle;
    }

    @Override
    public List<ValidationResult> validate() {
        final ValidationStatus status = processorNode.performValidation();
        if (status == ValidationStatus.VALID) {
            return Collections.emptyList();
        }

        return processorNode.getValidationErrors().stream()
            .filter(result -> !result.isValid())
            .filter(result -> !DisabledServiceValidationResult.isMatch(result))
            .toList();
    }

    @Override
    public List<ValidationResult> validate(final Map<String, String> propertyValues) {
        final ValidationContext validationContext = componentContextProvider.createValidationContext(processorNode, propertyValues, parameterContext);
        final ValidationState validationState = processorNode.performValidation(validationContext);
        final List<ValidationResult> validationErrors = validationState.getValidationErrors().stream()
            .filter(result -> !result.isValid())
            .filter(result -> !DisabledServiceValidationResult.isMatch(result))
            .toList();

        return validationErrors;
    }

    @Override
    public List<ConfigVerificationResult> verify(final Map<String, String> propertyValues, final Map<String, String> attributes) {
        final ProcessContext processContext = componentContextProvider.createProcessContext(processorNode, propertyValues, parameterContext);
        return processorNode.verifyConfiguration(processContext, connectorLogger, attributes, extensionManager, parameterContext);
    }

    @Override
    public List<ConfigVerificationResult> verify(final Map<String, String> propertyValues, final VersionedParameterContext versionedParameterContext, final Map<String, String> attributes) {
        final ParameterLookup parameterLookup = new ConnectorParameterLookup(List.of(versionedParameterContext), assetManager);
        final ProcessContext processContext = componentContextProvider.createProcessContext(processorNode, propertyValues, parameterLookup);
        return processorNode.verifyConfiguration(processContext, connectorLogger, attributes, extensionManager, parameterContext);
    }

    @Override
    public List<ConfigVerificationResult> verify(final VersionedExternalFlow versionedExternalFlow, final Map<String, String> attributes) {
        final VersionedProcessGroup parentGroup = findParentGroup(versionedExternalFlow.getFlowContents());
        final VersionedProcessor providedProcessor = findVersionedProcessor(parentGroup);
        if (providedProcessor == null) {
            throw new IllegalArgumentException("Cannot verify Processor with ID " + this.versionedProcessor.getIdentifier() + " because it does not exist in the provided flow");
        }
        final ParameterLookup parameterLookup = new ConnectorParameterLookup(versionedExternalFlow.getParameterContexts().values(), assetManager);

        final ProcessContext processContext = componentContextProvider.createProcessContext(processorNode, providedProcessor.getProperties(), parameterLookup);
        return processorNode.verifyConfiguration(processContext, connectorLogger, attributes, extensionManager, parameterLookup);
    }

    private VersionedProcessGroup findParentGroup(final VersionedProcessGroup group) {
        for (final VersionedProcessor processor : group.getProcessors()) {
            if (processor.getIdentifier().equals(versionedProcessor.getIdentifier())) {
                return group;
            }
        }

        for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
            final VersionedProcessGroup serviceGroup = findParentGroup(childGroup);
            if (serviceGroup != null) {
                return serviceGroup;
            }
        }

        return null;
    }

    private VersionedProcessor findVersionedProcessor(final VersionedProcessGroup group) {
        if (group == null) {
            return null;
        }

        final VersionedProcessGroup parentGroup = findParentGroup(group);
        if (parentGroup == null) {
            return null;
        }

        for (final VersionedProcessor processor : group.getProcessors()) {
            if (processor.getIdentifier().equals(processorNode.getVersionedComponentId().orElseThrow())) {
                return processor;
            }
        }

        return null;
    }


    @Override
    public Object invokeConnectorMethod(final String methodName, final Map<String, Object> arguments) throws InvocationFailedException {
        final Map<String, String> jsonArguments = serializeArgumentsToJson(arguments);
        final ProcessContext processContext = componentContextProvider.createProcessContext(processorNode, parameterContext);
        final String jsonResult = processorNode.invokeConnectorMethod(methodName, jsonArguments, processContext);
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
        final ProcessContext processContext = componentContextProvider.createProcessContext(processorNode, parameterContext);
        final String jsonResult = processorNode.invokeConnectorMethod(methodName, jsonArguments, processContext);
        if (jsonResult == null) {
            return null;
        }

        try {
            return OBJECT_MAPPER.readValue(jsonResult, returnType);
        } catch (final JsonProcessingException e) {
            throw new InvocationFailedException("Failed to deserialize return value from Connector Method '" + methodName + "'", e);
        }
    }

    @Override
    public String toString() {
        final String type = versionedProcessor.getType();
        final String simpleType = type == null ? null : type.substring(type.lastIndexOf('.') + 1);
        return "StandaloneProcessorFacade[id=" + versionedProcessor.getIdentifier() + ", name=" + versionedProcessor.getName() + ", type=" + simpleType + "]";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final StandaloneProcessorFacade that = (StandaloneProcessorFacade) o;
        return Objects.equals(versionedProcessor.getIdentifier(), that.versionedProcessor.getIdentifier());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(versionedProcessor.getIdentifier());
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
