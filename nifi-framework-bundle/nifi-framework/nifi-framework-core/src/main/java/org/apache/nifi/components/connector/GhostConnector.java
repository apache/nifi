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

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.flow.VersionedExternalFlow;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class GhostConnector implements Connector {
    private final String identifier;
    private final String canonicalClassName;
    private final List<ValidationResult> validationResults;
    private final List<ConfigVerificationResult> configVerificationResults;

    public GhostConnector(final String identifier, final String canonicalClassName) {
        this.identifier = identifier;
        this.canonicalClassName = canonicalClassName;

        validationResults = List.of(new ValidationResult.Builder()
            .subject("Missing Connector")
            .valid(false)
            .explanation("Could not create Connector of type " + canonicalClassName)
            .build());

        configVerificationResults = List.of(new ConfigVerificationResult.Builder()
            .verificationStepName("Create Connector")
            .outcome(Outcome.FAILED)
            .explanation("Could not create Connector of type " + canonicalClassName)
            .build());
    }

    @Override
    public void initialize(final ConnectorInitializationContext initContext) {
    }

    @Override
    public VersionedExternalFlow getInitialFlow() {
        return null;
    }

    @Override
    public void start(final FlowContext activeContext) throws FlowUpdateException {
    }

    @Override
    public void stop(final FlowContext activeContext) throws FlowUpdateException {
    }

    @Override
    public List<ValidationResult> validate(final FlowContext flowContext, final ConnectorValidationContext connectorValidationContext) {
        return validationResults;
    }

    @Override
    public List<ValidationResult> validateConfigurationStep(final ConfigurationStep configurationStep, final ConnectorConfigurationContext connectorConfigurationContext,
            final ConnectorValidationContext connectorValidationContext) {
        return validationResults;
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of();
    }

    @Override
    public void onConfigurationStepConfigured(final String stepName, final FlowContext workingContext) {
    }

    @Override
    public void prepareForUpdate(final FlowContext workingContext, final FlowContext activeContext) throws FlowUpdateException {
    }

    @Override
    public void abortUpdate(final FlowContext workingContext, final Throwable throwable) {
    }

    @Override
    public void applyUpdate(final FlowContext workingContext, final FlowContext activeContext) throws FlowUpdateException {
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> overrides, final FlowContext workingContext) {
        return configVerificationResults;
    }

    @Override
    public List<ConfigVerificationResult> verify(final FlowContext flowContext) {
        return configVerificationResults;
    }

    @Override
    public List<AllowableValue> fetchAllowableValues(final String stepName, final String propertyName, final FlowContext workingContext, final String filter) {
        return List.of();
    }

    @Override
    public List<AllowableValue> fetchAllowableValues(final String stepName, final String propertyName, final FlowContext workingContext) {
        return List.of();
    }

    @Override
    public CompletableFuture<Void> drainFlowFiles(final FlowContext flowContext) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public String toString() {
        return "GhostConnector[id=" + identifier + ", type=" + canonicalClassName + "]";
    }
}
