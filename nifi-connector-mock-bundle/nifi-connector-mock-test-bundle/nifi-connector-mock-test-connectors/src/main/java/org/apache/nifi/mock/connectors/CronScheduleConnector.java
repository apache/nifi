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
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.ConnectorConfigurationContext;
import org.apache.nifi.components.connector.ConnectorPropertyDescriptor;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.components.ProcessorFacade;
import org.apache.nifi.components.connector.util.VersionedFlowUtils;
import org.apache.nifi.flow.VersionedExternalFlow;

import java.util.List;
import java.util.Map;

/**
 * A test connector that validates CRON scheduling expressions via the "Trigger Schedule" parameter.
 * The underlying flow ({@code Cron_Schedule_Connector.json}) contains a CRON-driven
 * GenerateFlowFile processor whose scheduling period references the {@code #{Trigger Schedule}} parameter.
 * Verification delegates to the processor's validate/verify cycle, which checks the CRON expression.
 */
public class CronScheduleConnector extends AbstractConnector {

    static final String SCHEDULE_STEP_NAME = "Schedule";
    static final String TRIGGER_SCHEDULE_PARAM = "Trigger Schedule";

    static final ConnectorPropertyDescriptor TRIGGER_SCHEDULE = new ConnectorPropertyDescriptor.Builder()
            .name(TRIGGER_SCHEDULE_PARAM)
            .description("CRON expression for the GenerateFlowFile trigger schedule")
            .required(true)
            .build();

    static final ConnectorPropertyGroup SCHEDULE_GROUP = new ConnectorPropertyGroup.Builder()
            .name("Schedule Settings")
            .addProperty(TRIGGER_SCHEDULE)
            .build();

    static final ConfigurationStep SCHEDULE_STEP = new ConfigurationStep.Builder()
            .name(SCHEDULE_STEP_NAME)
            .propertyGroups(List.of(SCHEDULE_GROUP))
            .build();

    @Override
    public VersionedExternalFlow getInitialFlow() {
        return VersionedFlowUtils.loadFlowFromResource("flows/Cron_Schedule_Connector.json");
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of(SCHEDULE_STEP);
    }

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext flowContext) throws FlowUpdateException {
        if (SCHEDULE_STEP_NAME.equals(stepName)) {
            final String triggerSchedule = flowContext.getConfigurationContext()
                    .getProperty(SCHEDULE_STEP_NAME, TRIGGER_SCHEDULE_PARAM).getValue();
            final VersionedExternalFlow flow = getInitialFlow();
            VersionedFlowUtils.setParameterValue(flow, TRIGGER_SCHEDULE_PARAM, triggerSchedule);
            getInitializationContext().updateFlow(flowContext, flow);
        }
    }

    @Override
    public void applyUpdate(final FlowContext workingContext, final FlowContext activeContext) throws FlowUpdateException {
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> overrides, final FlowContext flowContext) {
        if (SCHEDULE_STEP_NAME.equals(stepName)) {
            final ConnectorConfigurationContext configContext = flowContext.getConfigurationContext().createWithOverrides(stepName, overrides);
            final String triggerSchedule = configContext.getProperty(SCHEDULE_STEP_NAME, TRIGGER_SCHEDULE_PARAM).getValue();

            final VersionedExternalFlow flow = getInitialFlow();
            VersionedFlowUtils.setParameterValue(flow, TRIGGER_SCHEDULE_PARAM, triggerSchedule);

            final ProcessorFacade generateFlowFile = flowContext.getRootGroup().getProcessors().stream()
                    .filter(p -> p.getDefinition().getType().endsWith("GenerateFlowFile"))
                    .findFirst()
                    .orElseThrow();

            return generateFlowFile.verify(flow, Map.of());
        }

        return List.of();
    }
}
