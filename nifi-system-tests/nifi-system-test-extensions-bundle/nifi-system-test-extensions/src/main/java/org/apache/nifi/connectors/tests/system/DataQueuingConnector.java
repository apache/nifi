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
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.util.VersionedFlowUtils;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataQueuingConnector extends AbstractConnector {
    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) {

    }

    @Override
    public VersionedExternalFlow getInitialFlow() {
        final Bundle bundle = new Bundle("org.apache.nifi", "nifi-system-test-extensions-nar", "2.8.0-SNAPSHOT");
        final VersionedProcessGroup rootGroup = VersionedFlowUtils.createProcessGroup("1234", "Data Queuing Connector");

        final VersionedProcessor generate = VersionedFlowUtils.addProcessor(rootGroup,
            "org.apache.nifi.processors.tests.system.GenerateFlowFile", bundle, "GenerateFlowFile", new Position(0, 0));
        generate.getProperties().put("File Size", "1 KB");
        generate.setSchedulingPeriod("100 millis");

        final VersionedProcessor terminate = VersionedFlowUtils.addProcessor(rootGroup,
            "org.apache.nifi.processors.tests.system.TerminateFlowFile", bundle, "TerminateFlowFile", new Position(0, 0));
        terminate.setScheduledState(ScheduledState.DISABLED);

        VersionedFlowUtils.addConnection(rootGroup, VersionedFlowUtils.createConnectableComponent(generate),
            VersionedFlowUtils.createConnectableComponent(terminate), Set.of("success"));

        final VersionedExternalFlow flow = new VersionedExternalFlow();
        flow.setFlowContents(rootGroup);
        flow.setParameterContexts(Collections.emptyMap());
        return flow;
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> propertyValueOverrides, final FlowContext flowContext) {
        return List.of();
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of();
    }

    @Override
    public void applyUpdate(final FlowContext workingFlowContext, final FlowContext activeFlowContext) {
    }
}
