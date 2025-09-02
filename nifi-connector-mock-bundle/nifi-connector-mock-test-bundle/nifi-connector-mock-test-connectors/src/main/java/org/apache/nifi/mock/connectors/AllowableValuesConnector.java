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

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.connector.AbstractConnector;
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.ConnectorPropertyDescriptor;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.components.FlowContextType;
import org.apache.nifi.components.connector.util.VersionedFlowUtils;
import org.apache.nifi.flow.VersionedExternalFlow;

import java.util.List;
import java.util.Map;

/**
 * A test connector with a configuration step that has fetchable allowable values.
 * The {@link #fetchAllowableValues} method returns allowable values only when invoked
 * with a WORKING {@link FlowContextType}. If invoked with an ACTIVE context, it returns
 * an empty list. This design verifies that the framework passes the correct (working)
 * context during configuration verification, reproducing the scenario fixed in NIFI-15258.
 */
public class AllowableValuesConnector extends AbstractConnector {

    static final ConnectorPropertyDescriptor COLOR = new ConnectorPropertyDescriptor.Builder()
        .name("Color")
        .description("The color to select")
        .allowableValuesFetchable(true)
        .required(true)
        .build();

    static final ConnectorPropertyGroup SELECTION_GROUP = new ConnectorPropertyGroup.Builder()
        .name("Color Selection")
        .addProperty(COLOR)
        .build();

    static final ConfigurationStep SELECTION_STEP = new ConfigurationStep.Builder()
        .name("Selection")
        .propertyGroups(List.of(SELECTION_GROUP))
        .build();

    private static final List<ConfigurationStep> CONFIGURATION_STEPS = List.of(SELECTION_STEP);

    @Override
    public VersionedExternalFlow getInitialFlow() {
        return VersionedFlowUtils.loadFlowFromResource("flows/Generate_and_Update.json");
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return CONFIGURATION_STEPS;
    }

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext flowContext) {
    }

    @Override
    public void applyUpdate(final FlowContext workingContext, final FlowContext activeContext) throws FlowUpdateException {
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> overrides, final FlowContext workingContext) {
        return List.of();
    }

    @Override
    public List<DescribedValue> fetchAllowableValues(final String stepName, final String propertyName, final FlowContext flowContext) {
        if ("Selection".equals(stepName) && "Color".equals(propertyName)) {
            if (flowContext.getType() == FlowContextType.WORKING) {
                return List.of(
                    new AllowableValue("red", "Red"),
                    new AllowableValue("green", "Green"),
                    new AllowableValue("blue", "Blue")
                );
            }

            // Return empty when the context is ACTIVE, simulating the pre-fix behavior
            // where using the wrong context caused allowable values to be empty and
            // any value to be incorrectly accepted.
            return List.of();
        }

        return super.fetchAllowableValues(stepName, propertyName, flowContext);
    }
}
