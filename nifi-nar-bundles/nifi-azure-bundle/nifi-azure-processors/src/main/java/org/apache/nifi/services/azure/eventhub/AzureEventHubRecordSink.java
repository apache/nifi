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
package org.apache.nifi.services.azure.eventhub;

import com.azure.core.amqp.AmqpTransportType;
import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.messaging.eventhubs.models.CreateBatchOptions;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.sink.RecordSinkService;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.shared.azure.eventhubs.AzureEventHubComponent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Tags({"azure", "record", "sink"})
@CapabilityDescription("Format and send Records to Azure Event Hubs")
public class AzureEventHubRecordSink extends AbstractControllerService implements RecordSinkService, AzureEventHubComponent {

    static final AllowableValue AZURE_ENDPOINT = new AllowableValue(".servicebus.windows.net","Azure", "Default Service Bus Endpoint");

    static final AllowableValue AZURE_CHINA_ENDPOINT = new AllowableValue(".servicebus.chinacloudapi.cn", "Azure China", "China Service Bus Endpoint");

    static final AllowableValue AZURE_GERMANY_ENDPOINT = new AllowableValue(".servicebus.cloudapi.de", "Azure Germany", "Germany Service Bus Endpoint");

    static final AllowableValue AZURE_US_GOV_ENDPOINT = new AllowableValue(".servicebus.usgovcloudapi.net", "Azure US Government", "United States Government Endpoint");

    static final PropertyDescriptor SERVICE_BUS_ENDPOINT = new PropertyDescriptor.Builder()
            .name("Service Bus Endpoint")
            .description("Provides the domain for connecting to Azure Event Hubs")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(
                    AZURE_ENDPOINT,
                    AZURE_CHINA_ENDPOINT,
                    AZURE_GERMANY_ENDPOINT,
                    AZURE_US_GOV_ENDPOINT
            )
            .defaultValue(AZURE_ENDPOINT.getValue())
            .required(true)
            .build();

    static final PropertyDescriptor EVENT_HUB_NAMESPACE = new PropertyDescriptor.Builder()
            .name("Event Hub Namespace")
            .description("Provides provides the host for connecting to Azure Event Hubs")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor EVENT_HUB_NAME = new PropertyDescriptor.Builder()
            .name("Event Hub Name")
            .description("Provides the Event Hub Name for connections")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor AUTHENTICATION_STRATEGY = new PropertyDescriptor.Builder()
            .name("Authentication Strategy")
            .description("Strategy for authenticating to Azure Event Hubs")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .allowableValues(AzureAuthenticationStrategy.class)
            .required(true)
            .defaultValue(AzureAuthenticationStrategy.DEFAULT_AZURE_CREDENTIAL.getValue())
            .build();

    static final PropertyDescriptor SHARED_ACCESS_POLICY = new PropertyDescriptor.Builder()
            .name("Shared Access Policy")
            .description("The name of the shared access policy. This policy must have Send claims")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .dependsOn(AUTHENTICATION_STRATEGY, AzureAuthenticationStrategy.SHARED_ACCESS_KEY.getValue())
            .build();

    static final PropertyDescriptor SHARED_ACCESS_POLICY_KEY = new PropertyDescriptor.Builder()
            .name("Shared Access Policy Key")
            .description("The primary or secondary key of the shared access policy")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .required(false)
            .dependsOn(AUTHENTICATION_STRATEGY, AzureAuthenticationStrategy.SHARED_ACCESS_KEY.getValue())
            .build();

    static final PropertyDescriptor PARTITION_KEY = new PropertyDescriptor.Builder()
            .name("Partition Key")
            .description("A hint for Azure Event Hub message broker how to distribute messages across one or more partitions")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Collections.unmodifiableList(
            Arrays.asList(
                    SERVICE_BUS_ENDPOINT,
                    EVENT_HUB_NAMESPACE,
                    EVENT_HUB_NAME,
                    TRANSPORT_TYPE,
                    RECORD_WRITER_FACTORY,
                    AUTHENTICATION_STRATEGY,
                    SHARED_ACCESS_POLICY,
                    SHARED_ACCESS_POLICY_KEY,
                    PARTITION_KEY
            )
    );

    private volatile ConfigurationContext context;
    private RecordSetWriterFactory writerFactory;
    private EventHubProducerClient client;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    protected EventHubProducerClient createEventHubClient(final String namespace,
                                                          final String serviceBusEndpoint,
                                                          final String eventHubName,
                                                          final String policyName,
                                                          final String policyKey,
                                                          final AzureAuthenticationStrategy authenticationStrategy,
                                                          final AmqpTransportType transportType
    ) {
        final String fullyQualifiedNamespace = String.format("%s%s", namespace, serviceBusEndpoint);
        final EventHubClientBuilder eventHubClientBuilder = new EventHubClientBuilder();
        eventHubClientBuilder.transportType(transportType);
        if (AzureAuthenticationStrategy.SHARED_ACCESS_KEY == authenticationStrategy) {
            final AzureNamedKeyCredential azureNamedKeyCredential = new AzureNamedKeyCredential(policyName, policyKey);
            eventHubClientBuilder.credential(fullyQualifiedNamespace, eventHubName, azureNamedKeyCredential);
        } else {
            final DefaultAzureCredentialBuilder defaultAzureCredentialBuilder = new DefaultAzureCredentialBuilder();
            final DefaultAzureCredential defaultAzureCredential = defaultAzureCredentialBuilder.build();
            eventHubClientBuilder.credential(fullyQualifiedNamespace, eventHubName, defaultAzureCredential);
        }
        return eventHubClientBuilder.buildProducerClient();
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.context = context;
        writerFactory = context.getProperty(RECORD_WRITER_FACTORY).asControllerService(RecordSetWriterFactory.class);
        final String namespace = context.getProperty(EVENT_HUB_NAMESPACE).evaluateAttributeExpressions().getValue();
        final String serviceBusEndpoint = context.getProperty(SERVICE_BUS_ENDPOINT).evaluateAttributeExpressions().getValue();
        final String eventHubName = context.getProperty(EVENT_HUB_NAME).evaluateAttributeExpressions().getValue();
        final String policyName = context.getProperty(SHARED_ACCESS_POLICY).getValue();
        final String policyKey = context.getProperty(SHARED_ACCESS_POLICY_KEY).getValue();
        final String authenticationStrategy = context.getProperty(AUTHENTICATION_STRATEGY).getValue();
        final AzureAuthenticationStrategy azureAuthenticationStrategy = AzureAuthenticationStrategy.valueOf(authenticationStrategy);
        final AmqpTransportType transportType = AmqpTransportType.fromString(context.getProperty(TRANSPORT_TYPE).getValue());
        client = createEventHubClient(namespace, serviceBusEndpoint, eventHubName, policyName, policyKey, azureAuthenticationStrategy, transportType);
    }

    @OnDisabled
    public void onDisabled() {
        if (client == null) {
            getLogger().debug("Event Hub Client not configured");
        } else {
            client.close();
        }
    }

    @Override
    public WriteResult sendData(final RecordSet recordSet, Map<String, String> attributes, final boolean sendZeroResults) throws IOException {
        final Map<String, String> writeAttributes = new LinkedHashMap<>(attributes);
        final String partitionKey = context.getProperty(PARTITION_KEY).evaluateAttributeExpressions(attributes).getValue();
        final CreateBatchOptions createBatchOptions = new CreateBatchOptions();
        createBatchOptions.setPartitionKey(partitionKey);
        EventDataBatch eventDataBatch = client.createBatch(createBatchOptions);
        final String correlationId = writeAttributes.get(CoreAttributes.UUID.key());
        int recordCount = 0;
        try (
                final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                final RecordSetWriter writer = writerFactory.createWriter(getLogger(), recordSet.getSchema(), outputStream, attributes)
        ) {
            Record record;
            final String contentType = writer.getMimeType();
            while ((record = recordSet.next()) != null) {
                writer.write(record);
                writer.flush();
                recordCount++;

                final byte[] bytes = outputStream.toByteArray();
                outputStream.reset();
                final EventData eventData = new EventData(bytes);
                eventData.getProperties().putAll(writeAttributes);
                eventData.setContentType(contentType);
                eventData.setCorrelationId(correlationId);
                eventData.setMessageId(String.format("%s-%d", correlationId, recordCount));
                if (!eventDataBatch.tryAdd(eventData)) {
                    if (eventDataBatch.getCount() > 0) {
                        client.send(eventDataBatch);
                        eventDataBatch = client.createBatch(createBatchOptions);
                    }
                    if (!eventDataBatch.tryAdd(eventData)) {
                        throw new ProcessException("Record " + recordCount + " exceeds maximum event data size: " + eventDataBatch.getMaxSizeInBytes());
                    }
                }
            }
            if (eventDataBatch.getCount() > 0) {
                client.send(eventDataBatch);
            }
        } catch (final SchemaNotFoundException e) {
            throw new IOException("Record Schema not found", e);
        }
        return WriteResult.of(recordCount, writeAttributes);
    }
}

