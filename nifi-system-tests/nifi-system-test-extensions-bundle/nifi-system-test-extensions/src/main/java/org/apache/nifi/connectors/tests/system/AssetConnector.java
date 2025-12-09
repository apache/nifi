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
import org.apache.nifi.components.connector.AbstractConnector;
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.ConnectorPropertyDescriptor;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.PropertyType;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.flow.VersionedExternalFlow;

import java.util.List;
import java.util.Map;

/**
 * Example Connector used by system tests for exercising Connector Asset APIs.
 * <p>
 * The connector defines a single configuration step with one property whose type is ASSET.
 * The connector itself does not manipulate the flow; it is intended only to drive API-level tests.
 */
public class AssetConnector extends AbstractConnector {

    static final ConnectorPropertyDescriptor ASSET_PROPERTY = new ConnectorPropertyDescriptor.Builder()
            .name("Test Asset")
            .description("Asset used for validating Connector Asset APIs in system tests")
            .type(PropertyType.ASSET)
            .required(true)
            .build();

    private static final ConnectorPropertyGroup ASSET_GROUP = new ConnectorPropertyGroup.Builder()
            .name("Asset Configuration")
            .description("Configuration properties related to Assets")
            .addProperty(ASSET_PROPERTY)
            .build();

    private static final ConfigurationStep ASSET_CONFIGURATION_STEP = new ConfigurationStep.Builder()
            .name("Asset Configuration")
            .description("Configuration Step that exposes an ASSET-type property")
            .propertyGroups(List.of(ASSET_GROUP))
            .build();

    @Override
    public VersionedExternalFlow getInitialFlow() {
        // This Connector is intended only for exercising the REST API and does not manage a flow.
        return null;
    }

    @Override
    public void prepareForUpdate(final FlowContext workingContext, final FlowContext activeContext) throws FlowUpdateException {
        // No-op: this connector does not manipulate the flow.
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps(final FlowContext flowContext) {
        return List.of(ASSET_CONFIGURATION_STEP);
    }

    @Override
    public void applyUpdate(final FlowContext workingContext, final FlowContext activeContext) throws FlowUpdateException {
        // No-op: this connector does not manipulate the flow.
    }

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) throws FlowUpdateException {
        // No-op: there is no additional behavior when the step is configured.
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> overrides, final FlowContext flowContext) {
        // No additional verification is required for this simple test connector.
        return List.of();
    }
}


