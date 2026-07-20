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

import org.apache.nifi.components.Backlog;
import org.apache.nifi.components.BacklogReportingException;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.connector.AbstractConnector;
import org.apache.nifi.components.connector.BacklogReportingConnector;
import org.apache.nifi.components.connector.BundleCompatibility;
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.ConnectorPropertyDescriptor;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.PropertyType;
import org.apache.nifi.components.connector.StepConfigurationContext;
import org.apache.nifi.components.connector.components.ConnectionFacade;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.components.connector.components.ProcessorFacade;
import org.apache.nifi.components.connector.components.QueueSnapshot;
import org.apache.nifi.components.connector.util.VersionedFlowUtils;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Test Connector that exercises Connector-level backlog reporting in three ways:
 * <ul>
 *     <li>{@code DELEGATE_TO_PROCESSOR}: forwards {@code getBacklog} to the embedded
 *         {@code BacklogReportingTestProcessor} via {@link ProcessorFacade#getBacklog()}.
 *         Combined with the Processor's own {@code Backlog Mode} this also covers the
 *         {@link BacklogReportingException} and {@link RuntimeException} propagation paths, which the
 *         asynchronous backlog request flow surfaces as the request's failure reason.</li>
 *     <li>{@code READ_QUEUE_ATTRIBUTES}: reads the atomic queue snapshot from the
 *         queued connection in the managed flow, inspects FlowFile attributes, and computes
 *         a Backlog from what it observes. Used to verify that a Connector can access
 *         queued FlowFile attributes via {@link ConnectionFacade#getQueueSnapshot()}.</li>
 *     <li>{@code RETURN_EMPTY}: returns {@link Optional#empty()}, the default opt-out.</li>
 * </ul>
 *
 * <p>The managed flow is {@code GenerateFlowFile -> BacklogReportingTestProcessor -> TerminateFlowFile},
 * with {@code TerminateFlowFile} {@link ScheduledState#DISABLED} so FlowFiles queue up on the
 * downstream connection. The downstream connection's load-balance strategy is controlled by
 * the {@code Load Balance Strategy} property so the clustered system tests can exercise both
 * {@code StandardFlowFileQueue} and {@code SocketLoadBalancedFlowFileQueue}.</p>
 */
public class BacklogReportingTestConnector extends AbstractConnector implements BacklogReportingConnector {

    static final String DELEGATE_TO_PROCESSOR = "DELEGATE_TO_PROCESSOR";
    static final String READ_QUEUE_ATTRIBUTES = "READ_QUEUE_ATTRIBUTES";
    static final String RETURN_EMPTY = "RETURN_EMPTY";

    static final String PROCESSOR_BACKLOG_MODE_NORMAL = "NORMAL";
    static final String PROCESSOR_BACKLOG_MODE_THROW_BACKLOG_REPORTING_EXCEPTION = "THROW_BACKLOG_REPORTING_EXCEPTION";
    static final String PROCESSOR_BACKLOG_MODE_THROW_RUNTIME_EXCEPTION = "THROW_RUNTIME_EXCEPTION";

    static final String LOAD_BALANCE_DO_NOT = "DO_NOT_LOAD_BALANCE";
    static final String LOAD_BALANCE_ROUND_ROBIN = "ROUND_ROBIN";

    static final ConnectorPropertyDescriptor DELEGATION_MODE = new ConnectorPropertyDescriptor.Builder()
            .name("Delegation Mode")
            .description("How this Connector should compute its Backlog")
            .type(PropertyType.STRING)
            .required(true)
            .defaultValue(DELEGATE_TO_PROCESSOR)
            .allowableValues(DELEGATE_TO_PROCESSOR, READ_QUEUE_ATTRIBUTES, RETURN_EMPTY)
            .build();

    static final ConnectorPropertyDescriptor PROCESSOR_BACKLOG_MODE = new ConnectorPropertyDescriptor.Builder()
            .name("Processor Backlog Mode")
            .description("Forwarded to the embedded BacklogReportingTestProcessor's Backlog Mode property")
            .type(PropertyType.STRING)
            .required(true)
            .defaultValue(PROCESSOR_BACKLOG_MODE_NORMAL)
            .allowableValues(PROCESSOR_BACKLOG_MODE_NORMAL, PROCESSOR_BACKLOG_MODE_THROW_BACKLOG_REPORTING_EXCEPTION, PROCESSOR_BACKLOG_MODE_THROW_RUNTIME_EXCEPTION)
            .build();

    static final ConnectorPropertyDescriptor FLOWFILE_BACKLOG = new ConnectorPropertyDescriptor.Builder()
            .name("FlowFile Backlog")
            .description("Number of FlowFiles to report on the source")
            .type(PropertyType.STRING)
            .required(true)
            .defaultValue("42")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    static final ConnectorPropertyDescriptor BYTE_BACKLOG = new ConnectorPropertyDescriptor.Builder()
            .name("Byte Backlog")
            .description("Number of bytes to report on the source")
            .type(PropertyType.STRING)
            .required(true)
            .defaultValue("1024")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    static final ConnectorPropertyDescriptor RECORD_BACKLOG = new ConnectorPropertyDescriptor.Builder()
            .name("Record Backlog")
            .description("Number of records to report on the source")
            .type(PropertyType.STRING)
            .required(true)
            .defaultValue("7")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    static final ConnectorPropertyDescriptor EXCEPTION_MESSAGE = new ConnectorPropertyDescriptor.Builder()
            .name("Exception Message")
            .description("Message included on the thrown exception when Processor Backlog Mode is one of the throw modes")
            .type(PropertyType.STRING)
            .required(true)
            .defaultValue("Simulated backlog reporting failure")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final ConnectorPropertyDescriptor LOAD_BALANCE_STRATEGY = new ConnectorPropertyDescriptor.Builder()
            .name("Load Balance Strategy")
            .description("Load balance strategy to apply to the connection between the BacklogReportingTestProcessor and the terminal Processor")
            .type(PropertyType.STRING)
            .required(true)
            .defaultValue(LOAD_BALANCE_DO_NOT)
            .allowableValues(LOAD_BALANCE_DO_NOT, LOAD_BALANCE_ROUND_ROBIN)
            .build();

    private static final ConnectorPropertyGroup PROPERTY_GROUP = new ConnectorPropertyGroup.Builder()
            .name("Backlog Configuration")
            .description("Controls how this Connector and its embedded Processor report backlog")
            .properties(List.of(DELEGATION_MODE, PROCESSOR_BACKLOG_MODE, FLOWFILE_BACKLOG, BYTE_BACKLOG, RECORD_BACKLOG, EXCEPTION_MESSAGE, LOAD_BALANCE_STRATEGY))
            .build();

    private static final ConfigurationStep CONFIG_STEP = new ConfigurationStep.Builder()
            .name("Backlog Configuration")
            .description("Configure the embedded Processor's backlog behavior and the managed flow's load balance strategy")
            .propertyGroups(List.of(PROPERTY_GROUP))
            .build();

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) {
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of(CONFIG_STEP);
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> propertyValueOverrides, final FlowContext flowContext) {
        return List.of();
    }

    @Override
    public VersionedExternalFlow getInitialFlow() {
        return buildFlow(
                PROCESSOR_BACKLOG_MODE_NORMAL,
                "0", "0", "0",
                "Simulated backlog reporting failure",
                LOAD_BALANCE_DO_NOT);
    }

    @Override
    public VersionedExternalFlow getActiveFlow(final FlowContext activeFlowContext) {
        // The authoritative Active flow is the flow produced by applying the current configuration. This
        // mirrors applyUpdate so that exiting Troubleshooting restores a flow equivalent to re-applying
        // the active configuration.
        return buildFlow(activeFlowContext.getConfigurationContext().scopedToStep(CONFIG_STEP));
    }

    @Override
    public void applyUpdate(final FlowContext workingFlowContext, final FlowContext activeFlowContext) throws FlowUpdateException {
        final VersionedExternalFlow flow = buildFlow(workingFlowContext.getConfigurationContext().scopedToStep(CONFIG_STEP));
        getInitializationContext().updateFlow(activeFlowContext, flow, BundleCompatibility.RESOLVE_BUNDLE);
    }

    private VersionedExternalFlow buildFlow(final StepConfigurationContext stepContext) {
        final String processorMode = stepContext.getProperty(PROCESSOR_BACKLOG_MODE).getValue();
        final String flowFileBacklog = stepContext.getProperty(FLOWFILE_BACKLOG).getValue();
        final String byteBacklog = stepContext.getProperty(BYTE_BACKLOG).getValue();
        final String recordBacklog = stepContext.getProperty(RECORD_BACKLOG).getValue();
        final String exceptionMessage = stepContext.getProperty(EXCEPTION_MESSAGE).getValue();
        final String loadBalanceStrategy = stepContext.getProperty(LOAD_BALANCE_STRATEGY).getValue();
        return buildFlow(processorMode, flowFileBacklog, byteBacklog, recordBacklog, exceptionMessage, loadBalanceStrategy);
    }

    private VersionedExternalFlow buildFlow(final String processorMode, final String flowFileBacklog, final String byteBacklog,
                                            final String recordBacklog, final String exceptionMessage, final String loadBalanceStrategy) {
        final Bundle bundle = new Bundle("org.apache.nifi", "nifi-system-test-extensions-nar", "2.8.0-SNAPSHOT");
        final VersionedProcessGroup rootGroup = VersionedFlowUtils.createProcessGroup("backlog-reporting-test-connector", "Backlog Reporting Test Connector");

        final VersionedProcessor generate = VersionedFlowUtils.addProcessor(rootGroup,
                "org.apache.nifi.processors.tests.system.GenerateFlowFile", bundle, "GenerateFlowFile", new Position(0, 0));
        generate.getProperties().putAll(Map.of(
                "File Size", "1 B",
                "Batch Size", "100",
                "Max FlowFiles", "1000",
                "flowFileIndex", "${nextInt()}"
        ));
        generate.setSchedulingPeriod("100 millis");

        final VersionedProcessor backlogProcessor = VersionedFlowUtils.addProcessor(rootGroup,
                "org.apache.nifi.processors.tests.system.BacklogReportingTestProcessor", bundle, "BacklogReportingTestProcessor", new Position(0, 100));
        backlogProcessor.getProperties().putAll(Map.of(
                "Backlog Mode", processorMode,
                "FlowFile Backlog", flowFileBacklog,
                "Byte Backlog", byteBacklog,
                "Record Backlog", recordBacklog,
                "Exception Message", exceptionMessage
        ));

        final VersionedProcessor terminate = VersionedFlowUtils.addProcessor(rootGroup,
                "org.apache.nifi.processors.tests.system.TerminateFlowFile", bundle, "TerminateFlowFile", new Position(0, 200));
        terminate.setScheduledState(ScheduledState.DISABLED);

        final VersionedConnection upstream = VersionedFlowUtils.addConnection(rootGroup,
                VersionedFlowUtils.createConnectableComponent(generate),
                VersionedFlowUtils.createConnectableComponent(backlogProcessor),
                Set.of("success"));
        upstream.setBackPressureDataSizeThreshold("100 GB");
        upstream.setBackPressureObjectThreshold(100_000L);

        final VersionedConnection downstream = VersionedFlowUtils.addConnection(rootGroup,
                VersionedFlowUtils.createConnectableComponent(backlogProcessor),
                VersionedFlowUtils.createConnectableComponent(terminate),
                Set.of("success"));
        downstream.setBackPressureDataSizeThreshold("100 GB");
        downstream.setBackPressureObjectThreshold(100_000L);
        downstream.setLoadBalanceStrategy(loadBalanceStrategy);

        final VersionedExternalFlow flow = new VersionedExternalFlow();
        flow.setFlowContents(rootGroup);
        flow.setParameterContexts(Collections.emptyMap());
        return flow;
    }

    @Override
    public Optional<Backlog> getBacklog(final FlowContext activeFlowContext) throws BacklogReportingException {
        final StepConfigurationContext stepContext = activeFlowContext.getConfigurationContext().scopedToStep(CONFIG_STEP);
        final String delegationMode = stepContext.getProperty(DELEGATION_MODE).getValue();

        if (RETURN_EMPTY.equals(delegationMode)) {
            return Optional.empty();
        }

        if (DELEGATE_TO_PROCESSOR.equals(delegationMode)) {
            final ProcessorFacade processor = findBacklogReportingTestProcessor(activeFlowContext)
                    .orElseThrow(() -> new BacklogReportingException("BacklogReportingTestProcessor not found in managed flow"));
            return processor.getBacklog();
        }

        if (READ_QUEUE_ATTRIBUTES.equals(delegationMode)) {
            return Optional.of(readBacklogFromQueue(activeFlowContext));
        }

        throw new BacklogReportingException("Unrecognized Delegation Mode: " + delegationMode);
    }

    private Optional<ProcessorFacade> findBacklogReportingTestProcessor(final FlowContext flowContext) {
        return findProcessors(flowContext.getRootGroup(),
                processor -> processor.getDefinition().getType().endsWith("BacklogReportingTestProcessor")).stream().findFirst();
    }

    private Backlog readBacklogFromQueue(final FlowContext activeFlowContext) {
        final ConnectionFacade downstreamConnection = findDownstreamConnection(activeFlowContext.getRootGroup())
                .orElseThrow(() -> new IllegalStateException("Could not find connection downstream of BacklogReportingTestProcessor"));

        final QueueSnapshot snapshot = downstreamConnection.getQueueSnapshot();
        long evenIndexCount = 0L;
        long evenIndexBytes = 0L;
        for (final FlowFile flowFile : snapshot.getActiveFlowFiles()) {
            final String indexAttribute = flowFile.getAttribute("flowFileIndex");
            if (indexAttribute == null) {
                continue;
            }
            final int index;
            try {
                index = Integer.parseInt(indexAttribute);
            } catch (final NumberFormatException ignored) {
                continue;
            }
            if (index % 2 == 0) {
                evenIndexCount++;
                evenIndexBytes += flowFile.getSize();
            }
        }

        final Backlog.Precision precision = snapshot.isActiveListExhaustive() ? Backlog.Precision.EXACT : Backlog.Precision.AT_LEAST;

        return Backlog.builder()
                .flowFiles(evenIndexCount)
                .bytes(evenIndexBytes)
                .records(evenIndexCount)
                .precision(precision)
                .build();
    }

    private Optional<ConnectionFacade> findDownstreamConnection(final ProcessGroupFacade group) {
        for (final ConnectionFacade connection : group.getConnections()) {
            final String sourceType = connection.getDefinition().getSource().getType().name();
            if (!"PROCESSOR".equals(sourceType)) {
                continue;
            }

            final String sourceId = connection.getDefinition().getSource().getId();
            for (final ProcessorFacade processor : group.getProcessors()) {
                if (processor.getDefinition().getIdentifier().equals(sourceId)
                        && processor.getDefinition().getType().endsWith("BacklogReportingTestProcessor")) {
                    return Optional.of(connection);
                }
            }
        }
        for (final ProcessGroupFacade child : group.getProcessGroups()) {
            final Optional<ConnectionFacade> found = findDownstreamConnection(child);
            if (found.isPresent()) {
                return found;
            }
        }
        return Optional.empty();
    }
}
