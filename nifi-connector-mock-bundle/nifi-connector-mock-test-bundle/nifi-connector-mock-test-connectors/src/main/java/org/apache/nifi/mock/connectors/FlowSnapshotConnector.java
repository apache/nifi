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

package org.apache.nifi.mock.connectors;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.connector.AbstractConnector;
import org.apache.nifi.components.connector.BundleCompatibility;
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.ConnectorPropertyDescriptor;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.util.VersionedFlowUtils;
import org.apache.nifi.flow.VersionedAsset;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessor;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Test Connector that installs sensitive and asset-backed parameters for exercising flow snapshots.
 */
public class FlowSnapshotConnector extends AbstractConnector {
    private static final String FLOW_RESOURCE = "flows/Generate_and_Update.json";
    private static final String STEP_NAME = "Snapshot Configuration";
    private static final String ASSET_IDENTIFIER_PROPERTY_NAME = "Asset Identifier";
    private static final String PARAMETER_CONTEXT_NAME = "Snapshot Parameter Context";
    private static final String SENSITIVE_PARAMETER_NAME = "sensitive_parameter";
    private static final String ASSET_PARAMETER_NAME = "asset_parameter";
    private static final String ASSET_NAME = "certificate.pem";

    private static final ConnectorPropertyDescriptor ASSET_IDENTIFIER = new ConnectorPropertyDescriptor.Builder()
        .name(ASSET_IDENTIFIER_PROPERTY_NAME)
        .description("Identifier of the asset referenced by the snapshot test flow")
        .required(true)
        .build();

    private static final ConnectorPropertyGroup PROPERTY_GROUP = new ConnectorPropertyGroup.Builder()
        .name("Snapshot Properties")
        .addProperty(ASSET_IDENTIFIER)
        .build();

    private static final ConfigurationStep CONFIGURATION_STEP = new ConfigurationStep.Builder()
        .name(STEP_NAME)
        .propertyGroups(List.of(PROPERTY_GROUP))
        .build();

    @Override
    public VersionedExternalFlow getInitialFlow() {
        return VersionedFlowUtils.loadFlowFromResource(FLOW_RESOURCE);
    }

    @Override
    public VersionedExternalFlow getActiveFlow(final FlowContext activeFlowContext) {
        final String assetIdentifier = getAssetIdentifier(activeFlowContext);
        return assetIdentifier == null ? getInitialFlow() : createConfiguredFlow(assetIdentifier);
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of(CONFIGURATION_STEP);
    }

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) throws FlowUpdateException {
        if (STEP_NAME.equals(stepName)) {
            final String assetIdentifier = getAssetIdentifier(workingContext);
            if (assetIdentifier != null) {
                getInitializationContext().updateFlow(workingContext, createConfiguredFlow(assetIdentifier), BundleCompatibility.RESOLVE_BUNDLE);
            }
        }
    }

    @Override
    public void applyUpdate(final FlowContext workingContext, final FlowContext activeContext) throws FlowUpdateException {
        final String assetIdentifier = getAssetIdentifier(workingContext);
        if (assetIdentifier != null) {
            getInitializationContext().updateFlow(activeContext, createConfiguredFlow(assetIdentifier), BundleCompatibility.RESOLVE_BUNDLE);
        }
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> overrides, final FlowContext workingContext) {
        return List.of();
    }

    private String getAssetIdentifier(final FlowContext flowContext) {
        return flowContext.getConfigurationContext().getProperty(CONFIGURATION_STEP, ASSET_IDENTIFIER).getValue();
    }

    private VersionedExternalFlow createConfiguredFlow(final String assetIdentifier) {
        final VersionedExternalFlow flow = getInitialFlow();

        final VersionedParameter sensitiveParameter = new VersionedParameter();
        sensitiveParameter.setName(SENSITIVE_PARAMETER_NAME);
        sensitiveParameter.setValue("sensitive-value");
        sensitiveParameter.setSensitive(true);
        sensitiveParameter.setProvided(false);

        final VersionedAsset asset = new VersionedAsset();
        asset.setIdentifier(assetIdentifier);
        asset.setName(ASSET_NAME);

        final VersionedParameter assetParameter = new VersionedParameter();
        assetParameter.setName(ASSET_PARAMETER_NAME);
        assetParameter.setSensitive(false);
        assetParameter.setProvided(false);
        assetParameter.setReferencedAssets(List.of(asset));

        final VersionedParameterContext parameterContext = new VersionedParameterContext();
        parameterContext.setName(PARAMETER_CONTEXT_NAME);
        parameterContext.setParameters(Set.of(sensitiveParameter, assetParameter));
        flow.setParameterContexts(Map.of(PARAMETER_CONTEXT_NAME, parameterContext));
        flow.getFlowContents().setParameterContextName(PARAMETER_CONTEXT_NAME);

        final VersionedProcessor generateFlowFile = VersionedFlowUtils.findProcessor(flow.getFlowContents(),
            processor -> processor.getType().equals("org.apache.nifi.processors.standard.GenerateFlowFile"))
            .orElseThrow();
        generateFlowFile.getProperties().put("Custom Text", "#{" + ASSET_PARAMETER_NAME + "}");

        return flow;
    }
}
