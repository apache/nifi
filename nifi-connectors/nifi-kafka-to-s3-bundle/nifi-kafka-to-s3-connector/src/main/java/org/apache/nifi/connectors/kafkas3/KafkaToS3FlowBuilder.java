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

import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.ConnectorConfigurationContext;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.util.VersionedFlowUtils;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.processor.DataUnit;

import java.util.HashMap;
import java.util.Map;

public class KafkaToS3FlowBuilder {
    private static final String FLOW_JSON_PATH = "flows/Kafka_to_S3.json";

    private final ConnectorConfigurationContext configContext;

    public KafkaToS3FlowBuilder(final ConnectorConfigurationContext configurationContext) {
        this.configContext = configurationContext;
    }

    public static VersionedExternalFlow loadInitialFlow() {
        return VersionedFlowUtils.loadFlowFromResource(FLOW_JSON_PATH);
    }

    public VersionedExternalFlow buildFlow() {
        final VersionedExternalFlow externalFlow = VersionedFlowUtils.loadFlowFromResource(FLOW_JSON_PATH);
        configureSchemaRegistry(externalFlow);

        updateKafkaConnectionParameters(externalFlow);
        updateSchemaRegistryParameters(externalFlow);
        updateReaderWriter(externalFlow);
        updateKafkaTopicsParameters(externalFlow);
        updateS3Config(externalFlow);

        return externalFlow;
    }

    private void configureSchemaRegistry(final VersionedExternalFlow externalFlow) {
        final String schemaRegistryUrl = configContext.getProperty(KafkaConnectionStep.KAFKA_CONNECTION_STEP,
            KafkaConnectionStep.SCHEMA_REGISTRY_GROUP,
            KafkaConnectionStep.SCHEMA_REGISTRY_URL).getValue();

        if (schemaRegistryUrl == null) {
            final VersionedProcessGroup processGroup = externalFlow.getFlowContents();

            // Remove any references to the Schema Registry service.
            final VersionedControllerService schemaRegistryService = processGroup.getControllerServices().stream()
                .filter(service -> service.getType().endsWith("ConfluentSchemaRegistry"))
                .findFirst()
                .orElseThrow();
            VersionedFlowUtils.removeControllerServiceReferences(processGroup, schemaRegistryService.getIdentifier());

            final VersionedControllerService schemaReferenceReader = processGroup.getControllerServices().stream()
                .filter(service -> service.getType().endsWith("ConfluentEncodedSchemaReferenceReader"))
                .findFirst()
                .orElseThrow();
            VersionedFlowUtils.removeControllerServiceReferences(processGroup, schemaReferenceReader.getIdentifier());

            processGroup.getControllerServices().stream()
                .filter(service -> service.getType().endsWith("JsonTreeReader"))
                .forEach(service -> service.getProperties().put("Schema Access Strategy", "infer-schema"));

            processGroup.getControllerServices().stream()
                .filter(service -> service.getType().endsWith("JsonRecordSetWriter"))
                .forEach(service -> service.getProperties().put("Schema Write Strategy", "no-schema"));
        }
    }

    private void updateSchemaRegistryParameters(final VersionedExternalFlow externalFlow) {
        final ConfigurationStep connectionStep = KafkaConnectionStep.KAFKA_CONNECTION_STEP;
        final ConnectorPropertyGroup registryGroup = KafkaConnectionStep.SCHEMA_REGISTRY_GROUP;

        final String schemaRegistryUrl = configContext.getProperty(connectionStep, registryGroup, KafkaConnectionStep.SCHEMA_REGISTRY_URL).getValue();
        VersionedFlowUtils.setParameterValue(externalFlow, "Schema Registry URLs", schemaRegistryUrl);

        if (schemaRegistryUrl == null) {
            final Map<String, String> properties = new HashMap<>();
            properties.put("schema-access-strategy", "infer-schema");
            properties.put("schema-registry", null);
            properties.put("schema-reference-reader", null);

            externalFlow.getFlowContents().getControllerServices().stream()
                .filter(service -> service.getType().endsWith("JsonTreeReader"))
                .findFirst()
                .ifPresent(service -> service.setProperties(properties));
        } else {
            final String username = configContext.getProperty(connectionStep, registryGroup, KafkaConnectionStep.SCHEMA_REGISTRY_USERNAME).getValue();
            final String password = configContext.getProperty(connectionStep, registryGroup, KafkaConnectionStep.SCHEMA_REGISTRY_PASSWORD).getValue();

            VersionedFlowUtils.setParameterValue(externalFlow, "Schema Registry Username", username);
            VersionedFlowUtils.setParameterValue(externalFlow, "Schema Registry Password", password);

            final String authenticationType = (username == null || username.isEmpty()) ? "NONE" : "BASIC";
            VersionedFlowUtils.setParameterValue(externalFlow, "Schema Registry Authentication Type", authenticationType);
        }
    }

    private void updateKafkaConnectionParameters(final VersionedExternalFlow externalFlow) {
        final ConfigurationStep connectionStep = KafkaConnectionStep.KAFKA_CONNECTION_STEP;
        final ConnectorPropertyGroup serverGroup = KafkaConnectionStep.KAFKA_SERVER_GROUP;

        final String kafkaBrokers = configContext.getProperty(connectionStep, serverGroup, KafkaConnectionStep.KAFKA_BROKERS).getValue();
        VersionedFlowUtils.setParameterValue(externalFlow, "Kafka Bootstrap Servers", kafkaBrokers);

        final String securityProtocol = configContext.getProperty(connectionStep, serverGroup, KafkaConnectionStep.SECURITY_PROTOCOL).getValue();
        VersionedFlowUtils.setParameterValue(externalFlow, "Kafka Security Protocol", securityProtocol);

        if (securityProtocol.contains("SASL")) {
            final String saslMechanism = configContext.getProperty(connectionStep, serverGroup, KafkaConnectionStep.SASL_MECHANISM).getValue();
            VersionedFlowUtils.setParameterValue(externalFlow, "Kafka SASL Mechanism", saslMechanism);

            final String username = configContext.getProperty(connectionStep, serverGroup, KafkaConnectionStep.USERNAME).getValue();
            VersionedFlowUtils.setParameterValue(externalFlow, "Kafka SASL Username", username);

            final String password = configContext.getProperty(connectionStep, serverGroup, KafkaConnectionStep.PASSWORD).getValue();
            VersionedFlowUtils.setParameterValue(externalFlow, "Kafka SASL Password", password);
        }
    }

    private void updateReaderWriter(final VersionedExternalFlow externalFlow) {
        final VersionedProcessGroup rootGroup = externalFlow.getFlowContents();

        final VersionedControllerService avroService = rootGroup.getControllerServices().stream()
            .filter(service -> service.getType().endsWith("AvroReader"))
            .findFirst()
            .orElseThrow();

        final String kafkaDataFormat = configContext.getProperty(KafkaTopicsStep.STEP_NAME, KafkaTopicsStep.KAFKA_TOPICS_GROUP.getName(), KafkaTopicsStep.KAFKA_DATA_FORMAT.getName()).getValue();
        if (!kafkaDataFormat.equalsIgnoreCase("JSON")) {
            // Update ConsumeKafka processor to use Avro Reader
            rootGroup.getProcessors().stream()
                .filter(versionedProcessor ->  versionedProcessor.getType().endsWith("ConsumeKafka"))
                .findFirst()
                .ifPresent(processor -> processor.getProperties().put("Record Reader", avroService.getIdentifier()));
        }

        VersionedFlowUtils.setParameterValue(externalFlow, "Kafka Data Format", kafkaDataFormat);

        // TODO: Have this throw an Exception if GROUP does not contain the given property
        final String s3DataFormat = configContext.getProperty(S3Step.S3_STEP, S3Step.S3_DESTINATION_GROUP, S3Step.S3_DATA_FORMAT).getValue();
        VersionedFlowUtils.setParameterValue(externalFlow, "S3 Data Format", s3DataFormat);
    }

    private void updateKafkaTopicsParameters(final VersionedExternalFlow externalFlow) {
        final String groupName = KafkaTopicsStep.KAFKA_TOPICS_GROUP.getName();

        final String topics = configContext.getProperty(KafkaTopicsStep.STEP_NAME, groupName, KafkaTopicsStep.TOPIC_NAMES.getName()).getValue();
        VersionedFlowUtils.setParameterValue(externalFlow, "Topic Names", topics);

        final String groupId = configContext.getProperty(KafkaTopicsStep.STEP_NAME, groupName, KafkaTopicsStep.CONSUMER_GROUP_ID.getName()).getValue();
        VersionedFlowUtils.setParameterValue(externalFlow, "Consumer Group ID", groupId);

        final String offsetReset = configContext.getProperty(KafkaTopicsStep.STEP_NAME, groupName, KafkaTopicsStep.OFFSET_RESET.getName()).getValue();
        VersionedFlowUtils.setParameterValue(externalFlow, "Kafka Auto Offset Reset", offsetReset);
    }

    private void updateS3Config(final VersionedExternalFlow externalFlow) {
        final String region = configContext.getProperty(S3Step.S3_STEP, S3Step.S3_DESTINATION_GROUP, S3Step.S3_REGION).getValue();
        VersionedFlowUtils.setParameterValue(externalFlow, "S3 Region", region);

        final String bucket = configContext.getProperty(S3Step.S3_STEP, S3Step.S3_DESTINATION_GROUP, S3Step.S3_BUCKET).getValue();
        VersionedFlowUtils.setParameterValue(externalFlow, "S3 Bucket", bucket);

        final String prefix = configContext.getProperty(S3Step.S3_STEP, S3Step.S3_DESTINATION_GROUP, S3Step.S3_PREFIX).getValue();
        VersionedFlowUtils.setParameterValue(externalFlow, "S3 Prefix", prefix);

        final String endpointOverrideUrl = configContext.getProperty(S3Step.S3_STEP, S3Step.S3_DESTINATION_GROUP, S3Step.S3_ENDPOINT_OVERRIDE_URL).getValue();
        VersionedFlowUtils.setParameterValue(externalFlow, "S3 Endpoint Override URL", endpointOverrideUrl);

        final String authStrategy = configContext.getProperty(S3Step.S3_STEP, S3Step.S3_CREDENTIALS_GROUP, S3Step.S3_AUTHENTICATION_STRATEGY).getValue();
        if (authStrategy.equals(S3Step.DEFAULT_CREDENTIALS)) {
            final VersionedControllerService credentialsService = externalFlow.getFlowContents().getControllerServices().stream()
                .filter(service -> service.getType().endsWith("AWSCredentialsProviderControllerService"))
                .findFirst()
                .orElseThrow();

            credentialsService.setProperties(Map.of("default-credentials", "true"));
        } else {
            final String accessKey = configContext.getProperty(S3Step.S3_STEP, S3Step.S3_CREDENTIALS_GROUP, S3Step.S3_ACCESS_KEY_ID).getValue();
            VersionedFlowUtils.setParameterValue(externalFlow, "S3 Access Key ID", accessKey);

            final String secretKey = configContext.getProperty(S3Step.S3_STEP, S3Step.S3_CREDENTIALS_GROUP, S3Step.S3_SECRET_ACCESS_KEY).getValue();
            VersionedFlowUtils.setParameterValue(externalFlow, "S3 Secret Access Key", secretKey);
        }

        final long mergeBytes = configContext.getProperty(S3Step.S3_STEP, S3Step.MERGE_GROUP, S3Step.TARGET_OBJECT_SIZE).asDataSize(DataUnit.B).longValue();
        final String mergeSize = configContext.getProperty(S3Step.S3_STEP, S3Step.MERGE_GROUP, S3Step.TARGET_OBJECT_SIZE).getValue();
        VersionedFlowUtils.setParameterValue(externalFlow, "Target Object Size", mergeSize);

        // Max Bin size will be either 10% more than target size or target size + 100MB, whichever is smaller
        final long maxBinSize = (long) Math.min(mergeBytes + 100_000_000, mergeBytes * 1.1D);
        VersionedFlowUtils.setParameterValue(externalFlow, "Maximum Object Size", maxBinSize + " B");

        final String mergeLatency = configContext.getProperty(S3Step.S3_STEP, S3Step.MERGE_GROUP, S3Step.MERGE_LATENCY).getValue();
        VersionedFlowUtils.setParameterValue(externalFlow, "Merge Latency", mergeLatency);
    }
}
