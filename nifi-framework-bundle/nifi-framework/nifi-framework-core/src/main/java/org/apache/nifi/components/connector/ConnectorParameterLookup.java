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

import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.components.connector.components.ParameterValue;
import org.apache.nifi.flow.VersionedAsset;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterLookup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ConnectorParameterLookup implements ParameterLookup {
    private final Map<String, Parameter> parameters;
    private final List<ParameterValue> parameterValues;
    private final AssetManager assetManager;

    // TODO: Refactor how parameters work
    //       It is possible that a VersionedProcessGroup has a child where different parameter contexts are used or are used in a different inheritance order.
    public ConnectorParameterLookup(final Collection<VersionedParameterContext> parameterContexts, final AssetManager assetManager) {
        this.assetManager = assetManager;

        parameterValues = Collections.unmodifiableList(createParameterValues(parameterContexts));
        parameters = new HashMap<>();
        for (final ParameterValue parameterValue : parameterValues) {
            parameters.put(parameterValue.getName(), createParameter(parameterValue));
        }
    }

    private Parameter createParameter(final ParameterValue parameterValue) {
        return new Parameter.Builder()
            .name(parameterValue.getName())
            .value(parameterValue.getValue())
            .sensitive(parameterValue.isSensitive())
            .referencedAssets(parameterValue.getAssets())
            .build();
    }

    @Override
    public Optional<Parameter> getParameter(final String parameterName) {
        return Optional.ofNullable(parameters.get(parameterName));
    }

    @Override
    public boolean isEmpty() {
        return parameters.isEmpty();
    }

    @Override
    public long getVersion() {
        return 0;
    }

    public List<ParameterValue> getParameterValues() {
        return parameterValues;
    }

    /**
     * Converts a {@code List<VersionedParameterContext>} found in a VersionedExternalFlow to a
     * {@code List<ParameterValue>} that can be used to update a ParameterContext from a Connector,
     * respecting parameter context inheritance and precedence.
     *
     * @param parameterContexts the list of parameter contexts from a VersionedExternalFlow
     * @return the list of ParameterValues
     */
    public List<ParameterValue> createParameterValues(final Collection<VersionedParameterContext> parameterContexts) {
        final List<ParameterValue> parameterValues = new ArrayList<>();

        if (parameterContexts == null || parameterContexts.isEmpty()) {
            return parameterValues;
        }

        // Create a map for easy lookup of parameter contexts by name
        final Map<String, VersionedParameterContext> contextMap = new HashMap<>();
        for (final VersionedParameterContext context : parameterContexts) {
            contextMap.put(context.getName(), context);
        }

        // Process each parameter context, including inherited contexts
        final Set<String> processedContexts = new HashSet<>();
        for (final VersionedParameterContext context : parameterContexts) {
            collectParameterValues(context, contextMap, processedContexts, parameterValues);
        }

        return parameterValues;
    }

    private void collectParameterValues(final VersionedParameterContext context, final Map<String, VersionedParameterContext> contextMap,
                final Set<String> processedContexts, final List<ParameterValue> parameterValues) {

        if (context == null || processedContexts.contains(context.getName())) {
            return;
        }

        processedContexts.add(context.getName());

        // Create a map to track existing parameters for efficient lookup
        final Map<String, ParameterValue> existingParametersByName = new HashMap<>();
        for (final ParameterValue existing : parameterValues) {
            existingParametersByName.put(existing.getName(), existing);
        }

        // First, process inherited parameter contexts in reverse order (lowest precedence first)
        // This ensures that the first inherited context (highest precedence) will override later ones
        if (context.getInheritedParameterContexts() != null && !context.getInheritedParameterContexts().isEmpty()) {
            final List<String> inheritedContextNames = context.getInheritedParameterContexts();
            // Process in reverse order so that the first (highest precedence) inherited context processes last
            for (int i = inheritedContextNames.size() - 1; i >= 0; i--) {
                final String inheritedContextName = inheritedContextNames.get(i);
                final VersionedParameterContext inheritedContext = contextMap.get(inheritedContextName);
                if (inheritedContext != null) {
                    collectParameterValues(inheritedContext, contextMap, processedContexts, parameterValues);
                }
            }
        }

        // Then, process this context's own parameters (they have the highest precedence and override all inherited ones)
        if (context.getParameters() != null) {
            // Rebuild the existing parameters map since inherited contexts may have added parameters
            existingParametersByName.clear();
            for (final ParameterValue existing : parameterValues) {
                existingParametersByName.put(existing.getName(), existing);
            }

            for (final VersionedParameter versionedParameter : context.getParameters()) {
                final String parameterName = versionedParameter.getName();

                // Remove existing parameter if present, then add the new one (current context overrides)
                if (existingParametersByName.containsKey(parameterName)) {
                    parameterValues.removeIf(param -> param.getName().equals(parameterName));
                }

                final ParameterValue.Builder builder = new ParameterValue.Builder()
                    .name(parameterName)
                    .value(versionedParameter.getValue())
                    .sensitive(versionedParameter.isSensitive());

                if (assetManager != null && versionedParameter.getReferencedAssets() != null) {
                    for (final VersionedAsset versionedAsset : versionedParameter.getReferencedAssets()) {
                        assetManager.getAsset(versionedAsset.getIdentifier()).ifPresent(builder::addReferencedAsset);
                    }
                }

                final ParameterValue parameterValue = builder.build();

                parameterValues.add(parameterValue);
            }
        }
    }

}
