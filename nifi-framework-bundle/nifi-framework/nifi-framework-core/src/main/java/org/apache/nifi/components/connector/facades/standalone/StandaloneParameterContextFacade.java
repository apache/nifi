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

import org.apache.nifi.asset.Asset;
import org.apache.nifi.components.connector.components.ParameterContextFacade;
import org.apache.nifi.components.connector.components.ParameterValue;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.serialization.AffectedComponentSet;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.parameter.ParameterReferenceManager;
import org.apache.nifi.parameter.StandardParameterReferenceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class StandaloneParameterContextFacade implements ParameterContextFacade {
    private static final Logger logger = LoggerFactory.getLogger(StandaloneParameterContextFacade.class);

    private final FlowController flowController;
    private final ProcessGroup managedProcessGroup;
    private final ParameterContext parameterContext;

    public StandaloneParameterContextFacade(final FlowController flowController, final ProcessGroup managedProcessGroup) {
        this.flowController = flowController;
        this.managedProcessGroup = managedProcessGroup;
        this.parameterContext = managedProcessGroup.getParameterContext();
    }

    @Override
    public void updateParameters(final Collection<ParameterValue> updatedValues) {
        // Check if the parameter already exists and if the sensitivity matches
        validateParameterSensitivities(updatedValues);

        final AffectedComponentSet affectedComponentSet = new AffectedComponentSet(flowController);
        final ParameterReferenceManager parameterReferenceManager = new StandardParameterReferenceManager(() -> managedProcessGroup);

        final Set<ProcessorNode> allReferencingProcessors = new HashSet<>();
        final Set<ControllerServiceNode> allReferencingServices = new HashSet<>();
        for (final ParameterValue parameterValue : updatedValues) {
            if (!isParameterChanged(parameterValue)) {
                continue;
            }

            final String parameterName = parameterValue.getName();
            final Set<ProcessorNode> referencingProcessors = parameterReferenceManager.getProcessorsReferencing(managedProcessGroup.getParameterContext(), parameterName);
            final Set<ControllerServiceNode> referencingServices = parameterReferenceManager.getControllerServicesReferencing(managedProcessGroup.getParameterContext(), parameterName);
            referencingProcessors.forEach(affectedComponentSet::addProcessor);
            referencingServices.forEach(affectedComponentSet::addControllerService);

            allReferencingProcessors.addAll(referencingProcessors);
            allReferencingServices.addAll(referencingServices);
        }

        if (affectedComponentSet.isEmpty()) {
            logger.info("No components are affected by the parameter update; no need to restart anything.");
            return;
        }

        final AffectedComponentSet activeSet = affectedComponentSet.toActiveSet();
        logger.info("Stopping {} affected components due to parameter update", activeSet.getComponentCount());
        activeSet.stop();

        // Update the parameter value in our map
        final Map<String, Parameter> updatedParameters = createParameterMap(updatedValues);
        managedProcessGroup.getParameterContext().setParameters(updatedParameters);

        allReferencingProcessors.forEach(ProcessorNode::resetValidationState);
        allReferencingServices.forEach(ControllerServiceNode::resetValidationState);

        logger.info("Parameter Context updated {} parameter. Restarting {} affected components.", updatedValues.size(), activeSet.getComponentCount());
        activeSet.start();
    }

    private Map<String, Parameter> createParameterMap(final Collection<ParameterValue> parameterValues) {
        final Map<String, Parameter> paramMap = new HashMap<>();
        for (final ParameterValue parameterValue : parameterValues) {
            final Parameter parameter = new Parameter.Builder()
                .name(parameterValue.getName())
                .value(parameterValue.getValue())
                .sensitive(parameterValue.isSensitive())
                .build();

            paramMap.put(parameterValue.getName(), parameter);
        }

        return paramMap;
    }

    private boolean isParameterChanged(final ParameterValue parameterValue) {
        final String parameterName = parameterValue.getName();
        final Optional<Parameter> optionalParameter = parameterContext.getParameter(parameterName);
        if (optionalParameter.isEmpty()) {
            return true; // New parameter
        }

        final Parameter existingParameter = optionalParameter.get();
        if (!Objects.equals(existingParameter.getValue(), parameterValue.getValue())) {
            return true; // Value changed
        }

        if (!Objects.equals(existingParameter.getReferencedAssets(), parameterValue.getAssets())) {
            return true; // Assets changed
        }

        return false;
    }

    private void validateParameterSensitivities(final Collection<ParameterValue> updatedValues) {
        for (final ParameterValue parameterValue : updatedValues) {
            final String parameterName = parameterValue.getName();
            final boolean sensitive = parameterValue.isSensitive();

            final Optional<Parameter> optionalParameter = parameterContext.getParameter(parameterName);
            if (optionalParameter.isEmpty()) {
                continue; // New parameter, no existing sensitivity to check
            }

            final boolean existingSensitive = optionalParameter.get().getDescriptor().isSensitive();
            if (existingSensitive != sensitive) {
                throw new IllegalStateException("Cannot update parameter " + parameterName + " because the Parameter is already defined as being "
                                                + (existingSensitive ? "sensitive" : "non-sensitive") + " but the update is defined as being "
                                                + (sensitive ? "sensitive" : "non-sensitive"));
            }
        }
    }

    @Override
    public synchronized String getValue(final String parameterName) {
        final Optional<Parameter> optionalParameter = parameterContext.getParameter(parameterName);
        if (optionalParameter.isEmpty()) {
            return null;
        }

        final Parameter existing = optionalParameter.get();
        final String explicitValue = existing.getValue();
        if (explicitValue != null) {
            return explicitValue;
        }

        final List<Asset> assets = existing.getReferencedAssets();
        if (assets.isEmpty()) {
            return null;
        }

        final StringBuilder sb = new StringBuilder();
        for (final Asset asset : assets) {
            sb.append(asset.getFile().getAbsolutePath());
            sb.append(",");
        }
        sb.setLength(sb.length() - 1); // Remove trailing comma
        return sb.toString();
    }

    @Override
    public synchronized Set<String> getDefinedParameterNames() {
        final Set<String> parameterNames = new HashSet<>();
        for (final ParameterDescriptor descriptor : parameterContext.getEffectiveParameters().keySet()) {
            parameterNames.add(descriptor.getName());
        }
        return parameterNames;
    }

    @Override
    public synchronized boolean isSensitive(final String parameterName) {
        return parameterContext.getParameter(parameterName)
            .map(Parameter::getDescriptor)
            .map(ParameterDescriptor::isSensitive)
            .orElse(false);
    }

    @Override
    public Asset createAsset(final InputStream inputStream) throws IOException {
        // TODO: Implement
        return null;
    }

}
