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
 * Post-migration counterpart to the fixture Connector of the same simple type name in the
 * {@code nifi-system-test-extensions} bundle. The pre-migration fixture defines a single
 * {@code "Legacy Step"} with legacy property names; this class defines the migrated {@code "Kafka Connection"} step and
 * renames / removes / adds properties in {@link #migrateProperties(ConnectorPropertyConfiguration)} so that a NAR swap
 * followed by NiFi restart exercises the full {@code Connector.migrateProperties} code path end-to-end.
 */
public class MigratePropertiesConnector extends AbstractConnector {

    static final String LEGACY_STEP_NAME = "Legacy Step";
    static final String MIGRATED_STEP_NAME = "Kafka Connection";

    static final String LEGACY_BROKER_URL_NAME = "legacy-broker-url";
    static final String LEGACY_CREDENTIALS_NAME = "legacy-credentials";
    static final String LEGACY_OBSOLETE_NAME = "legacy-obsolete";

    static final String BROKER_URL_NAME = "Broker URL";
    static final String CREDENTIALS_NAME = "Credentials";
    static final String CLIENT_ID_NAME = "Client Id";
    static final String CLIENT_ID_MIGRATED_VALUE = "auto-migrated";

    private static final ConnectorPropertyDescriptor BROKER_URL = new ConnectorPropertyDescriptor.Builder()
        .name(BROKER_URL_NAME)
        .description("Migrated string property.")
        .required(false)
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    private static final ConnectorPropertyDescriptor CREDENTIALS = new ConnectorPropertyDescriptor.Builder()
        .name(CREDENTIALS_NAME)
        .description("Migrated secret property.")
        .required(false)
        .type(PropertyType.SECRET)
        .build();

    private static final ConnectorPropertyDescriptor CLIENT_ID = new ConnectorPropertyDescriptor.Builder()
        .name(CLIENT_ID_NAME)
        .description("Added by migration when not already present.")
        .required(false)
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    private static final ConnectorPropertyGroup MIGRATED_PROPERTY_GROUP = new ConnectorPropertyGroup.Builder()
        .name("Kafka Connection Properties")
        .description("Migrated property group.")
        .properties(List.of(BROKER_URL, CREDENTIALS, CLIENT_ID))
        .build();

    private static final ConfigurationStep MIGRATED_STEP = new ConfigurationStep.Builder()
        .name(MIGRATED_STEP_NAME)
        .propertyGroups(List.of(MIGRATED_PROPERTY_GROUP))
        .build();

    private final List<ConfigurationStep> configurationSteps = List.of(MIGRATED_STEP);

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) {
    }

    @Override
    public VersionedExternalFlow getInitialFlow() {
        final VersionedProcessGroup group = new VersionedProcessGroup();
        group.setName("Migrate Properties Flow");

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
            .verificationStepName("Migrate Properties Verification")
            .explanation("Successful verification with properties: " + propertyValueOverrides)
            .build());
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return configurationSteps;
    }

    @Override
    public void applyUpdate(final FlowContext workingFlowContext, final FlowContext activeFlowContext) {
    }

    @Override
    public void migrateProperties(final ConnectorPropertyConfiguration config) {
        config.renameStep(LEGACY_STEP_NAME, MIGRATED_STEP_NAME);
        final ConnectorStepPropertyConfiguration step = config.forStep(MIGRATED_STEP_NAME);
        step.renameProperty(LEGACY_BROKER_URL_NAME, BROKER_URL_NAME);
        step.renameProperty(LEGACY_CREDENTIALS_NAME, CREDENTIALS_NAME);
        step.removeProperty(LEGACY_OBSOLETE_NAME);
        if (!step.hasProperty(CLIENT_ID_NAME)) {
            step.setProperty(CLIENT_ID_NAME, CLIENT_ID_MIGRATED_VALUE);
        }
    }
}
