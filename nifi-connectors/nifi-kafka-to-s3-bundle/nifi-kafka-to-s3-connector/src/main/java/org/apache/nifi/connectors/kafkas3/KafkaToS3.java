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

package org.apache.nifi.connectors.kafkas3;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.connector.AbstractConnector;
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.ConnectorConfigurationContext;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.InvocationFailedException;
import org.apache.nifi.components.connector.components.ControllerServiceFacade;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.components.connector.components.ProcessorFacade;
import org.apache.nifi.components.connector.util.VersionedFlowUtils;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedExternalFlow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@CapabilityDescription("Provides the ability to ingest data from Apache Kafka topics, merge it together into an object of reasonable " +
                       "size, and write that data to Amazon S3.")
@Tags({"kafka", "s3"})
public class KafkaToS3 extends AbstractConnector {

    private static final List<ConfigurationStep> configurationSteps = List.of(
        KafkaConnectionStep.KAFKA_CONNECTION_STEP,
        KafkaTopicsStep.KAFKA_TOPICS_STEP,
        S3Step.S3_STEP
    );

    private volatile CompletableFuture<Void> drainFlowFileFuture = null;


    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return configurationSteps;
    }

    @Override
    public void prepareForUpdate(final FlowContext workingContext, final FlowContext activeContext) throws FlowUpdateException {
        final String activeS3DataFormat = activeContext.getConfigurationContext().getProperty(
            S3Step.S3_STEP_NAME, S3Step.S3_DATA_FORMAT.getName()).getValue();
        final String workingS3DataFormat = workingContext.getConfigurationContext().getProperty(
            S3Step.S3_STEP_NAME, S3Step.S3_DATA_FORMAT.getName()).getValue();

        if (!activeS3DataFormat.equals(workingS3DataFormat)) {
            getLogger().info("S3 Data Format changed from {} to {}; draining flow before updating it", activeS3DataFormat, workingS3DataFormat);

            drainFlowFileFuture = drainFlowFiles(activeContext);
            try {
                drainFlowFileFuture.get(5, TimeUnit.MINUTES);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FlowUpdateException("Interrupted while waiting for all FlowFiles to drain from the flow", e);
            } catch (final TimeoutException e) {
                throw new FlowUpdateException("Timed out waiting for all FlowFiles to drain from the flow", e);
            } catch (final ExecutionException e) {
                throw new FlowUpdateException("Failed to drain FlowFiles from flow", e.getCause());
            }

            getLogger().info("All FlowFiles drained from the flow; proceeding with flow update");
        }
    }

    @Override
    public void abortUpdate(final FlowContext workingContext, final Throwable throwable) {
        if (drainFlowFileFuture != null) {
            drainFlowFileFuture.cancel(true);
            drainFlowFileFuture = null;
        }
    }

    @Override
    public void applyUpdate(final FlowContext workingContext, final FlowContext activeContext) throws FlowUpdateException {
        final VersionedExternalFlow flow = buildFlow(workingContext.getConfigurationContext());
        getInitializationContext().updateFlow(activeContext, flow);
    }

    @Override
    public VersionedExternalFlow getInitialFlow() {
        return KafkaToS3FlowBuilder.loadInitialFlow();
    }

    @Override
    public void onStepConfigured(final String stepName, final FlowContext workingContext) throws FlowUpdateException {
        final VersionedExternalFlow flow = buildFlow(workingContext.getConfigurationContext());
        getInitializationContext().updateFlow(workingContext, flow);
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> propertyValueOverrides, final FlowContext workingFlowContext) {
        // Get the current ConfigurationContext and then create a new one that contains the provided property values
        final ConnectorConfigurationContext configurationContext = workingFlowContext.getConfigurationContext().createWithOverrides(stepName, propertyValueOverrides);
        final VersionedExternalFlow flow = buildFlow(configurationContext);

        // Validate Connectivity
        if (stepName.equals(KafkaConnectionStep.STEP_NAME)) {
            return verifyKafkaConnectivity(workingFlowContext, flow);
        }
        if (stepName.equals(KafkaTopicsStep.STEP_NAME)) {
            final List<ConfigVerificationResult> results = new ArrayList<>();
            results.addAll(verifyTopicsExists(workingFlowContext));
            results.addAll(verifyKafkaParsability(workingFlowContext, flow));
            return results;
        }

        return Collections.emptyList();
    }

    private List<ConfigVerificationResult> verifyKafkaParsability(final FlowContext workingFlowContext, final VersionedExternalFlow flow) {
        // Enable Controller Services necessary for parsing records.
        // We determine which Controller Services are referenced by the flow and enable them, but we do not use
        // getRootGroup().getLifecycle().enableReferencedControllerServices(ControllerServiceReferenceScope.INCLUDE_REFERENCED_SERVICES_ONLY)
        // because that would include the Controller Services that are referenced by the currently configured flow, and it's possible that the
        // what is being verified uses a different set of Controller Services (e.g., the verified flow may use a JSON Reader while the current
        // flow uses an Avro Reader).
        final ProcessGroupFacade rootGroup = workingFlowContext.getRootGroup();
        final Set<VersionedControllerService> referencedServices = VersionedFlowUtils.getReferencedControllerServices(flow.getFlowContents());
        final Set<String> serviceIds = referencedServices.stream()
            .map(VersionedControllerService::getIdentifier)
            .collect(Collectors.toSet());

        try {
            rootGroup.getLifecycle().enableControllerServices(serviceIds).get(10, TimeUnit.SECONDS);
        } catch (final Exception e) {
            return List.of(new ConfigVerificationResult.Builder()
                .verificationStepName("Record Parsing")
                .outcome(Outcome.FAILED)
                .explanation("Failed to enable Controller Services due to " + e)
                .build());
        }

        try {
            final ProcessorFacade consumeKafkaFacade = findProcessors(rootGroup,
                processor -> processor.getDefinition().getType().endsWith("ConsumeKafka")).getFirst();

            final List<ConfigVerificationResult> configVerificationResults = consumeKafkaFacade.verify(flow, Map.of());
            for (final ConfigVerificationResult result : configVerificationResults) {
                if (result.getOutcome() == Outcome.FAILED) {
                    return List.of(result);
                }
            }

            return Collections.emptyList();
        } finally {
            rootGroup.getLifecycle().disableControllerServices(serviceIds);
        }
    }


    private List<ConfigVerificationResult> verifyTopicsExists(final FlowContext workingFlowContext) {
        final List<String> topicsAvailable;
        try {
            topicsAvailable = getAvailableTopics(workingFlowContext);
        } catch (final Exception e) {
            return List.of(new ConfigVerificationResult.Builder()
                .verificationStepName("Verify Kafka topics exist")
                .outcome(Outcome.SKIPPED)
                .explanation("Unable to validate that topics exist due to " + e)
                .build());
        }

        final Set<String> topicNames = new HashSet<>(topicsAvailable);
        final List<String> specifiedTopics = workingFlowContext.getConfigurationContext().getProperty(KafkaTopicsStep.STEP_NAME,
            KafkaTopicsStep.TOPIC_NAMES.getName()).asList();
        final String missingTopics = specifiedTopics.stream()
            .filter(topic -> !topicNames.contains(topic))
            .collect(Collectors.joining(", "));

        if (!missingTopics.isEmpty()) {
            return List.of(new ConfigVerificationResult.Builder()
                .verificationStepName("Verify Kafka topics exist")
                .outcome(Outcome.FAILED)
                .explanation("The following topics do not exist in the Kafka cluster: " + missingTopics)
                .build());
        } else {
            return List.of(new ConfigVerificationResult.Builder()
                .verificationStepName("Verify Kafka topics exist")
                .outcome(Outcome.SUCCESSFUL)
                .explanation("All specified topics exist in the Kafka cluster")
                .build());
        }
    }

    private List<ConfigVerificationResult> verifyKafkaConnectivity(final FlowContext workingFlowContext, final VersionedExternalFlow flow) {
        // Build a new version of the flow so that we can get the relevant properties of the Kafka Connection Service
        final ControllerServiceFacade connectionService = getKafkaConnectionService(workingFlowContext);
        final List<ConfigVerificationResult> configVerificationResults = connectionService.verify(flow, Map.of());

        for (final ConfigVerificationResult result : configVerificationResults) {
            if (result.getOutcome() == Outcome.FAILED) {
                return List.of(new ConfigVerificationResult.Builder()
                    .verificationStepName("Verify Kafka connectivity")
                    .outcome(Outcome.FAILED)
                    .explanation(result.getExplanation())
                    .build());
            }
        }

        return Collections.emptyList();
    }

    private VersionedExternalFlow buildFlow(final ConnectorConfigurationContext configurationContext) {
        final KafkaToS3FlowBuilder flowBuilder = new KafkaToS3FlowBuilder(configurationContext);
        return flowBuilder.buildFlow();
    }

    @Override
    public List<AllowableValue> fetchAllowableValues(final String stepName, final String propertyName, final FlowContext flowContext) {
        if (stepName.equals(KafkaTopicsStep.STEP_NAME) && propertyName.equals(KafkaTopicsStep.TOPIC_NAMES.getName())) {
            return createAllowableValues(getAvailableTopics(flowContext));
        } else if (stepName.equals(S3Step.S3_STEP_NAME) && propertyName.equals(S3Step.S3_REGION.getName())) {
            return createAllowableValues(getPossibleS3Regions(flowContext));
        }

        return super.fetchAllowableValues(stepName, propertyName, flowContext);
    }

    private List<AllowableValue> createAllowableValues(final List<String> values) {
        return values.stream().map(this::createAllowableValue).collect(Collectors.toList());
    }

    private AllowableValue createAllowableValue(final String value) {
        return new AllowableValue(value, value, value);
    }

    @SuppressWarnings("unchecked")
    private List<String> getAvailableTopics(final FlowContext flowContext) {
        // If Kafka Brokers not yet set, return empty list
        final ConnectorConfigurationContext config = flowContext.getConfigurationContext();
        if (!config.getProperty(KafkaConnectionStep.KAFKA_CONNECTION_STEP, KafkaConnectionStep.KAFKA_BROKERS).isSet()) {
            return List.of();
        }

        final ControllerServiceFacade kafkaConnectionService = getKafkaConnectionService(flowContext);

        try {
            return (List<String>) kafkaConnectionService.invokeConnectorMethod("listTopicNames", Map.of());
        } catch (final Exception e) {
            getLogger().warn("Failed to retrieve available Kafka topics", e);
            return List.of();
        }
    }

    private ControllerServiceFacade getKafkaConnectionService(final FlowContext flowContext) {
        return flowContext.getRootGroup().getControllerServices().stream()
            .filter(service -> service.getDefinition().getType().endsWith("Kafka3ConnectionService"))
            .findFirst()
            .orElseThrow();
    }

    @SuppressWarnings("unchecked")
    private List<String> getPossibleS3Regions(final FlowContext flowContext) {
        final ProcessorFacade processorFacade = flowContext.getRootGroup().getProcessors().stream()
            .filter(proc -> proc.getDefinition().getType().endsWith("PutS3Object"))
            .findFirst()
            .orElseThrow();

        try {
            return (List<String>) processorFacade.invokeConnectorMethod("getAvailableRegions", Map.of());
        } catch (final InvocationFailedException e) {
            getLogger().error("Failed to obtain list of available S3 regions", e);
            return Collections.emptyList();
        }
    }
}
