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

package org.apache.nifi.controller.flow;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.Connector;
import org.apache.nifi.components.connector.ConnectorConfigurationContext;
import org.apache.nifi.components.connector.ConnectorInitializationContext;
import org.apache.nifi.components.connector.ConnectorValidationContext;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.flow.VersionedExternalFlow;

import java.util.List;
import java.util.Map;

/**
 * A simple no-op Connector implementation for testing purposes.
 * This connector does nothing but provides the basic interface implementation
 * required for testing StandardFlowManager's connector creation functionality.
 */
public class NopConnector implements Connector {

    private ConnectorInitializationContext context;
    private boolean initialized = false;
    private boolean started = false;
    private boolean configured = false;

    @Override
    public void initialize(final ConnectorInitializationContext context) {
        this.context = context;
        this.initialized = true;
    }

    @Override
    public VersionedExternalFlow getInitialFlow() {
        return null;
    }

    @Override
    public void start(final FlowContext activeContext) throws FlowUpdateException {
        if (!initialized) {
            throw new FlowUpdateException("Connector must be initialized before starting");
        }
        started = true;
    }

    @Override
    public void stop(final FlowContext activeContext) throws FlowUpdateException {
        started = false;
    }

    @Override
    public List<ValidationResult> validate(final FlowContext activeContext, final ConnectorValidationContext validationContext) {
        if (!initialized) {
            return List.of(new ValidationResult.Builder()
                .subject("Initialization")
                .valid(false)
                .explanation("Connector has not been initialized")
                .build());
        }

        return List.of(new ValidationResult.Builder()
            .subject("Test Connector")
            .valid(true)
            .explanation("Test connector is valid")
            .build());
    }

    @Override
    public List<ValidationResult> validateConfigurationStep(final ConfigurationStep configurationStep, final ConnectorConfigurationContext connectorConfigurationContext,
            final ConnectorValidationContext connectorValidationContext) {
        return List.of();
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps(final FlowContext flowContext) {
        return List.of(new ConfigurationStep.Builder()
            .name("Test Group")
            .description("A test configuration step")
            .build());
    }

    @Override
    public void onConfigurationStepConfigured(final String stepName, final FlowContext flowContext) {
    }

    @Override
    public void prepareForUpdate(final FlowContext workingContext, final FlowContext activeContext) {

    }

    @Override
    public void abortUpdate(final FlowContext flowContext, final Throwable throwable) {

    }

    @Override
    public void applyUpdate(final FlowContext workingContext, final FlowContext activeContext) throws FlowUpdateException {
        if (!initialized) {
            throw new FlowUpdateException("Connector must be initialized before configuration");
        }
        configured = true;
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> overrides, final FlowContext flowContext) {
        return List.of();
    }

    @Override
    public List<ConfigVerificationResult> verify(final FlowContext flowContext) {
        return List.of();
    }


    // Getters for test assertions
    public boolean isInitialized() {
        return initialized;
    }

    public boolean isStarted() {
        return started;
    }

    public boolean isConfigured() {
        return configured;
    }

    public ConnectorInitializationContext getContext() {
        return context;
    }

    @Override
    public List<AllowableValue> fetchAllowableValues(final String stepName, final String propertyName, final FlowContext workingContext, final String filter) {
        return List.of();
    }

    @Override
    public List<AllowableValue> fetchAllowableValues(final String stepName, final String propertyName, final FlowContext workingContext) {
        return List.of();
    }

}
