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

package org.apache.nifi.connectors.tests.system;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.connector.AbstractConnector;
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.ConnectorPropertyDescriptor;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.PropertyType;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.migration.ConnectorPropertyConfiguration;
import org.apache.nifi.migration.ConnectorStepPropertyConfiguration;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.Map;

/**
 * Current connector version used to load a provider document that still has the previous step and property names.
 * Migration renames and removes that document, and the declared defaults cover properties and steps the provider omits.
 */
public class ProviderMigrationConnector extends AbstractConnector {
    static final String LEGACY_STEP_NAME = "Legacy Connection";
    static final String CONNECTION_STEP_NAME = "Connection";
    static final String LEGACY_HOST_NAME = "legacy-host";
    static final String HOST_NAME = "Host";
    static final String HOST_VALUE = "db.example.com";
    static final String LEGACY_OBSOLETE_NAME = "legacy-obsolete";
    static final String OBSOLETE_VALUE = "gone";
    static final String CLIENT_ID_NAME = "Client Id";
    static final String CLIENT_ID_VALUE = "auto-migrated";
    static final String POOL_SIZE_NAME = "Pool Size";
    static final String POOL_SIZE_DEFAULT = "8";

    static final String REMOVED_STEP_NAME = "Removed Step";
    static final String REMOVED_PROPERTY_NAME = "Removed Property";
    static final String REMOVED_PROPERTY_VALUE = "drop-me";
    static final String REMOVED_PROPERTY_DEFAULT = "should-not-reappear";

    static final String ADDED_STEP_NAME = "Added Step";
    static final String ADDED_PROPERTY_NAME = "Added Property";
    static final String ADDED_PROPERTY_DEFAULT = "added-default";

    static final String OPTIONAL_STEP_NAME = "Optional Step";
    static final String OPTIONAL_PROPERTY_NAME = "Note";
    static final String OPTIONAL_PROPERTY_DEFAULT = "optional-note";

    private static final ConnectorPropertyDescriptor HOST = new ConnectorPropertyDescriptor.Builder()
        .name(HOST_NAME)
        .description("Host renamed from the legacy provider property.")
        .required(true)
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    private static final ConnectorPropertyDescriptor CLIENT_ID = new ConnectorPropertyDescriptor.Builder()
        .name(CLIENT_ID_NAME)
        .description("Property added by migration when the provider document does not contain it.")
        .required(false)
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    private static final ConnectorPropertyDescriptor POOL_SIZE = new ConnectorPropertyDescriptor.Builder()
        .name(POOL_SIZE_NAME)
        .description("Required property whose current default is applied when the provider document omits it.")
        .required(true)
        .defaultValue(POOL_SIZE_DEFAULT)
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    private static final ConnectorPropertyDescriptor REMOVED_PROPERTY = new ConnectorPropertyDescriptor.Builder()
        .name(REMOVED_PROPERTY_NAME)
        .description("Required property on a step that migration removes. Its default must not recreate the step.")
        .required(true)
        .defaultValue(REMOVED_PROPERTY_DEFAULT)
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    private static final ConnectorPropertyDescriptor ADDED_PROPERTY = new ConnectorPropertyDescriptor.Builder()
        .name(ADDED_PROPERTY_NAME)
        .description("Required property on a step the provider document does not contain.")
        .required(true)
        .defaultValue(ADDED_PROPERTY_DEFAULT)
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    private static final ConnectorPropertyDescriptor OPTIONAL_PROPERTY = new ConnectorPropertyDescriptor.Builder()
        .name(OPTIONAL_PROPERTY_NAME)
        .description("Optional property with a default on a step the provider document does not contain.")
        .required(false)
        .defaultValue(OPTIONAL_PROPERTY_DEFAULT)
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    private static final ConfigurationStep CONNECTION_STEP = new ConfigurationStep.Builder()
        .name(CONNECTION_STEP_NAME)
        .propertyGroups(List.of(new ConnectorPropertyGroup.Builder()
            .name("Connection")
            .description("Migrated connection properties.")
            .properties(List.of(HOST, CLIENT_ID, POOL_SIZE))
            .build()))
        .build();

    private static final ConfigurationStep REMOVED_STEP = new ConfigurationStep.Builder()
        .name(REMOVED_STEP_NAME)
        .propertyGroups(List.of(new ConnectorPropertyGroup.Builder()
            .name("Removed")
            .description("Step removed by migration.")
            .properties(List.of(REMOVED_PROPERTY))
            .build()))
        .build();

    private static final ConfigurationStep ADDED_STEP = new ConfigurationStep.Builder()
        .name(ADDED_STEP_NAME)
        .propertyGroups(List.of(new ConnectorPropertyGroup.Builder()
            .name("Added")
            .description("Step created because it has a required default and the provider omitted it.")
            .properties(List.of(ADDED_PROPERTY))
            .build()))
        .build();

    private static final ConfigurationStep OPTIONAL_STEP = new ConfigurationStep.Builder()
        .name(OPTIONAL_STEP_NAME)
        .propertyGroups(List.of(new ConnectorPropertyGroup.Builder()
            .name("Optional")
            .description("Step left absent because it has only an optional default.")
            .properties(List.of(OPTIONAL_PROPERTY))
            .build()))
        .build();

    @Override
    public void migrateProperties(final ConnectorPropertyConfiguration configuration) {
        configuration.renameStep(LEGACY_STEP_NAME, CONNECTION_STEP_NAME);
        final ConnectorStepPropertyConfiguration connection = configuration.forStep(CONNECTION_STEP_NAME);
        connection.renameProperty(LEGACY_HOST_NAME, HOST_NAME);
        connection.removeProperty(LEGACY_OBSOLETE_NAME);
        if (!connection.hasProperty(CLIENT_ID_NAME)) {
            connection.setProperty(CLIENT_ID_NAME, CLIENT_ID_VALUE);
        }
        configuration.removeStep(REMOVED_STEP_NAME);
    }

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) {
    }

    @Override
    public VersionedExternalFlow getInitialFlow() {
        final VersionedProcessGroup group = new VersionedProcessGroup();
        group.setName("Provider Migration Flow");

        final VersionedExternalFlow flow = new VersionedExternalFlow();
        flow.setFlowContents(group);
        return flow;
    }

    @Override
    public VersionedExternalFlow getActiveFlow(final FlowContext activeFlowContext) {
        return getInitialFlow();
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> propertyValueOverrides, final FlowContext flowContext) {
        return List.of(new ConfigVerificationResult.Builder()
            .outcome(Outcome.SUCCESSFUL)
            .subject(stepName)
            .verificationStepName("Provider Migration Verification")
            .explanation("Successful verification")
            .build());
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of(CONNECTION_STEP, REMOVED_STEP, ADDED_STEP, OPTIONAL_STEP);
    }

    @Override
    public void applyUpdate(final FlowContext workingFlowContext, final FlowContext activeFlowContext) {
    }
}
