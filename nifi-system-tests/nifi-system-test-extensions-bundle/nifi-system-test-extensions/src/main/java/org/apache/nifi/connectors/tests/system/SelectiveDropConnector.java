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
import org.apache.nifi.components.connector.DropFlowFileSummary;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.components.ConnectionFacade;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.components.connector.util.VersionedFlowUtils;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flowfile.FlowFile;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Connector for testing selective dropping of FlowFiles.
 * When started, it generates FlowFiles with a 'flowFileIndex' attribute.
 * When stopped, it drops all FlowFiles where the flowFileIndex attribute has an even value.
 */
public class SelectiveDropConnector extends AbstractConnector {

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) {
    }

    @Override
    public VersionedExternalFlow getInitialFlow() {
        final Bundle bundle = new Bundle("org.apache.nifi", "nifi-system-test-extensions-nar", "2.7.0-SNAPSHOT");
        final VersionedProcessGroup rootGroup = VersionedFlowUtils.createProcessGroup("1234", "Selective Drop Connector");

        final VersionedProcessor generate = VersionedFlowUtils.addProcessor(rootGroup,
            "org.apache.nifi.processors.tests.system.GenerateFlowFile", bundle, "GenerateFlowFile", new Position(0, 0));
        generate.getProperties().putAll(Map.of(
            "File Size", "1 B",
            "Batch Size", "20000",
            "Max FlowFiles", "20000",
            "flowFileIndex", "${nextInt()}"
        ));
        generate.setSchedulingPeriod("10 sec");

        final VersionedProcessor terminate = VersionedFlowUtils.addProcessor(rootGroup,
            "org.apache.nifi.processors.tests.system.TerminateFlowFile", bundle, "TerminateFlowFile", new Position(0, 0));
        terminate.setScheduledState(ScheduledState.DISABLED);

        final VersionedConnection connection = VersionedFlowUtils.addConnection(rootGroup,
            VersionedFlowUtils.createConnectableComponent(generate), VersionedFlowUtils.createConnectableComponent(terminate), Set.of("success"));
        connection.setBackPressureDataSizeThreshold("100 GB");
        connection.setBackPressureObjectThreshold(100_000L);

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
    public void applyUpdate(final FlowContext workingFlowContext, final FlowContext activeFlowContext) throws FlowUpdateException {
        final VersionedExternalFlow flow = getInitialFlow();
        getInitializationContext().updateFlow(activeFlowContext, flow);
    }

    @Override
    public void stop(final FlowContext context) throws FlowUpdateException {
        super.stop(context);

        final ProcessGroupFacade rootGroup = context.getRootGroup();
        final ConnectionFacade connection = findFirstConnection(rootGroup);

        if (connection == null) {
            getLogger().warn("Could not find connection to perform selective drop");
            return;
        }

        try {
            final AtomicInteger dropCount = new AtomicInteger();
            final AtomicInteger seenCount = new AtomicInteger();
            final DropFlowFileSummary summary = connection.dropFlowFiles(ff -> {
                final boolean shouldDrop = hasEvenFlowFileIndex(ff);
                seenCount.incrementAndGet();
                if (shouldDrop) {
                    dropCount.incrementAndGet();
                }
                return shouldDrop;
            });
            getLogger().info("Selectively dropped {} out of {} FlowFiles ({} bytes) with even flowFileIndex values",
                dropCount.get(), seenCount.get(), summary.getDroppedBytes());
        } catch (final IOException e) {
            throw new FlowUpdateException("Failed to selectively drop FlowFiles", e);
        }
    }

    private boolean hasEvenFlowFileIndex(final FlowFile flowFile) {
        return Integer.parseInt(flowFile.getAttribute("flowFileIndex")) % 2 == 0;
    }

    private ConnectionFacade findFirstConnection(final ProcessGroupFacade group) {
        for (final ConnectionFacade connection : group.getConnections()) {
            return connection;
        }

        for (final ProcessGroupFacade childGroup : group.getProcessGroups()) {
            final ConnectionFacade found = findFirstConnection(childGroup);
            if (found != null) {
                return found;
            }
        }

        return null;
    }
}
