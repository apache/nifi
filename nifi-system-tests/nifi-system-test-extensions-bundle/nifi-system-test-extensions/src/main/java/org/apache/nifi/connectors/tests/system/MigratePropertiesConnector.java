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
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.Map;

/**
 * Pre-migration fixture for the Connector migrateProperties system test. Defines a single
 * {@link ConfigurationStep} named {@code "Legacy Step"} with legacy property names. Paired with the post-migration
 * counterpart of the same simple type name in the alternate-config extensions bundle, which renames the step and
 * properties, removes the obsolete property, and adds a new property via {@code migrateProperties}.
 */
public class MigratePropertiesConnector extends AbstractConnector {

    private static final ConnectorPropertyDescriptor LEGACY_BROKER_URL = new ConnectorPropertyDescriptor.Builder()
        .name("legacy-broker-url")
        .description("Legacy string property renamed by the post-migration connector.")
        .required(false)
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    private static final ConnectorPropertyDescriptor LEGACY_CREDENTIALS = new ConnectorPropertyDescriptor.Builder()
        .name("legacy-credentials")
        .description("Legacy secret property renamed by the post-migration connector.")
        .required(false)
        .type(PropertyType.SECRET)
        .build();

    private static final ConnectorPropertyDescriptor LEGACY_OBSOLETE = new ConnectorPropertyDescriptor.Builder()
        .name("legacy-obsolete")
        .description("Legacy property removed by the post-migration connector.")
        .required(false)
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    private static final ConnectorPropertyGroup LEGACY_PROPERTY_GROUP = new ConnectorPropertyGroup.Builder()
        .name("Legacy Properties")
        .description("Legacy properties migrated by the post-migration connector.")
        .properties(List.of(LEGACY_BROKER_URL, LEGACY_CREDENTIALS, LEGACY_OBSOLETE))
        .build();

    private static final ConfigurationStep LEGACY_STEP = new ConfigurationStep.Builder()
        .name("Legacy Step")
        .propertyGroups(List.of(LEGACY_PROPERTY_GROUP))
        .build();

    private final List<ConfigurationStep> configurationSteps = List.of(LEGACY_STEP);

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
}
