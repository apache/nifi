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
import org.apache.nifi.components.connector.BundleCompatibility;
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.ConnectorPropertyDescriptor;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.PropertyType;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.util.VersionedFlowUtils;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * A test connector that exercises bundle resolution capabilities.
 * It creates a flow with processors using different bundle specifications to test
 * how the framework handles unavailable bundles based on the configured BundleCompatability strategy.
 */
public class BundleResolutionConnector extends AbstractConnector {

    private static final String BUNDLE_COMPATABILITY_STEP = "Bundle Resolution";

    private static final ConnectorPropertyDescriptor BUNDLE_COMPATABILITY_PROPERTY = new ConnectorPropertyDescriptor.Builder()
        .name("Bundle Compatability")
        .description("Specifies how bundle resolution should be handled when the specified bundle is not available.")
        .required(true)
        .type(PropertyType.STRING)
        .allowableValues(
            BundleCompatibility.REQUIRE_EXACT_BUNDLE.name(),
            BundleCompatibility.RESOLVE_BUNDLE.name(),
            BundleCompatibility.RESOLVE_NEWEST_BUNDLE.name()
        )
        .defaultValue(BundleCompatibility.REQUIRE_EXACT_BUNDLE.name())
        .build();

    private static final ConnectorPropertyGroup BUNDLE_PROPERTY_GROUP = new ConnectorPropertyGroup.Builder()
        .name("Bundle Settings")
        .description("Settings for bundle resolution behavior.")
        .properties(List.of(BUNDLE_COMPATABILITY_PROPERTY))
        .build();

    private static final ConfigurationStep BUNDLE_STEP = new ConfigurationStep.Builder()
        .name(BUNDLE_COMPATABILITY_STEP)
        .propertyGroups(List.of(BUNDLE_PROPERTY_GROUP))
        .build();

    private final List<ConfigurationStep> configurationSteps = List.of(BUNDLE_STEP);

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) throws FlowUpdateException {
        if (BUNDLE_COMPATABILITY_STEP.equals(stepName)) {
            final String compatabilityValue = workingContext.getConfigurationContext()
                .getProperty(BUNDLE_STEP, BUNDLE_COMPATABILITY_PROPERTY)
                .getValue();
            final BundleCompatibility bundleCompatability = BundleCompatibility.valueOf(compatabilityValue);

            final VersionedExternalFlow flow = createFlowWithBundleScenarios();
            getInitializationContext().updateFlow(workingContext, flow, bundleCompatability);
        }
    }

    @Override
    public VersionedExternalFlow getInitialFlow() {
        final VersionedProcessGroup group = VersionedFlowUtils.createProcessGroup(UUID.randomUUID().toString(), "Bundle Resolution Flow");

        final VersionedParameter compatabilityParam = new VersionedParameter();
        compatabilityParam.setName("BUNDLE_COMPATABILITY");
        compatabilityParam.setValue(BundleCompatibility.REQUIRE_EXACT_BUNDLE.name());
        compatabilityParam.setSensitive(false);
        compatabilityParam.setProvided(false);
        compatabilityParam.setReferencedAssets(List.of());

        final VersionedParameterContext parameterContext = new VersionedParameterContext();
        parameterContext.setName("Bundle Resolution Parameter Context");
        parameterContext.setParameters(Set.of(compatabilityParam));

        final VersionedExternalFlow flow = new VersionedExternalFlow();
        flow.setParameterContexts(Map.of(parameterContext.getName(), parameterContext));
        flow.setFlowContents(group);
        return flow;
    }

    private VersionedExternalFlow createFlowWithBundleScenarios() {
        final VersionedProcessGroup group = VersionedFlowUtils.createProcessGroup(UUID.randomUUID().toString(), "Bundle Resolution Flow");

        // Add a processor with an unavailable bundle (fake version) that should be resolved based on BundleCompatability
        // Uses the system test GenerateFlowFile processor which is available in the system test extensions bundle
        final Bundle nonexistentBundle = new Bundle("org.apache.nifi", "nifi-system-test-extensions-nar", "0.0.0-NONEXISTENT");

        final VersionedProcessor testProcessor = VersionedFlowUtils.addProcessor(group,
            "org.apache.nifi.processors.tests.system.GenerateFlowFile", nonexistentBundle,
            "GenerateFlowFile for Bundle Resolution Test", new Position(100, 100));
        testProcessor.setSchedulingPeriod("1 sec");
        testProcessor.setAutoTerminatedRelationships(Set.of("success"));

        final VersionedParameterContext parameterContext = new VersionedParameterContext();
        parameterContext.setName("Bundle Resolution Parameter Context");
        parameterContext.setParameters(Set.of());

        final VersionedExternalFlow flow = new VersionedExternalFlow();
        flow.setParameterContexts(Map.of(parameterContext.getName(), parameterContext));
        flow.setFlowContents(group);
        return flow;
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName,
                                                                   final Map<String, String> propertyValueOverrides,
                                                                   final FlowContext flowContext) {
        return List.of(new ConfigVerificationResult.Builder()
            .outcome(Outcome.SUCCESSFUL)
            .subject(stepName)
            .verificationStepName("Bundle Resolution Verification")
            .explanation("Bundle resolution configuration verified successfully.")
            .build());
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return configurationSteps;
    }

    @Override
    public void applyUpdate(final FlowContext workingFlowContext, final FlowContext activeFlowContext) throws FlowUpdateException {
        final String compatabilityValue = workingFlowContext.getConfigurationContext()
            .getProperty(BUNDLE_STEP, BUNDLE_COMPATABILITY_PROPERTY)
            .getValue();
        final BundleCompatibility bundleCompatability = BundleCompatibility.valueOf(compatabilityValue);

        final VersionedExternalFlow flow = createFlowWithBundleScenarios();
        getInitializationContext().updateFlow(activeFlowContext, flow, bundleCompatability);
    }
}
