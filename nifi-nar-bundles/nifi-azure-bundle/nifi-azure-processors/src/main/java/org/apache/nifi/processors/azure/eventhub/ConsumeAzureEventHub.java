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
package org.apache.nifi.processors.azure.eventhub;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventprocessorhost.CloseReason;
import com.microsoft.azure.eventprocessorhost.EventProcessorHost;
import com.microsoft.azure.eventprocessorhost.EventProcessorOptions;
import com.microsoft.azure.eventprocessorhost.IEventProcessor;
import com.microsoft.azure.eventprocessorhost.IEventProcessorFactory;
import com.microsoft.azure.eventprocessorhost.PartitionContext;
import com.microsoft.azure.servicebus.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.ReceiverDisconnectedException;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;

import static org.apache.nifi.util.StringUtils.isEmpty;

@Tags({"azure", "microsoft", "cloud", "eventhub", "events", "streaming", "streams"})
@CapabilityDescription("Receives messages from a Microsoft Azure Event Hub, writing the contents of the Azure message to the content of the FlowFile.")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@TriggerSerially
@WritesAttributes({
        @WritesAttribute(attribute = "eventhub.enqueued.timestamp", description = "The time (in milliseconds since epoch, UTC) at which the message was enqueued in the Azure Event Hub"),
        @WritesAttribute(attribute = "eventhub.offset", description = "The offset into the partition at which the message was stored"),
        @WritesAttribute(attribute = "eventhub.sequence", description = "The Azure Sequence number associated with the message"),
        @WritesAttribute(attribute = "eventhub.name", description = "The name of the Event Hub from which the message was pulled"),
        @WritesAttribute(attribute = "eventhub.partition", description = "The name of the Azure Partition from which the message was pulled")
})
public class ConsumeAzureEventHub extends AbstractSessionFactoryProcessor {

    static final PropertyDescriptor NAMESPACE = new PropertyDescriptor.Builder()
            .name("event-hub-namespace")
            .displayName("Event Hub Namespace")
            .description("The Azure Namespace that the Event Hub is assigned to. This is generally equal to <Event Hub Name>-ns.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();
    static final PropertyDescriptor EVENT_HUB_NAME = new PropertyDescriptor.Builder()
            .name("event-hub-name")
            .displayName("Event Hub Name")
            .description("The name of the Azure Event Hub to pull messages from.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();
    // TODO: Do we need to support custom service endpoints as GetAzureEventHub does? Is it possible?
    static final PropertyDescriptor ACCESS_POLICY_NAME = new PropertyDescriptor.Builder()
            .name("event-hub-shared-access-policy-name")
            .displayName("Shared Access Policy Name")
            .description("The name of the Event Hub Shared Access Policy. This Policy must have Listen permissions.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();
    static final PropertyDescriptor POLICY_PRIMARY_KEY = new PropertyDescriptor.Builder()
            .name("event-hub-shared-access-policy-primary-key")
            .displayName("Shared Access Policy Primary Key")
            .description("The primary key of the Event Hub Shared Access Policy.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .sensitive(true)
            .required(true)
            .build();
    static final PropertyDescriptor CONSUMER_GROUP = new PropertyDescriptor.Builder()
            .name("event-hub-consumer-group")
            .displayName("Event Hub Consumer Group")
            .description("The name of the Event Hub Consumer Group to use.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("$Default")
            .required(true)
            .build();
    static final PropertyDescriptor CONSUMER_HOSTNAME = new PropertyDescriptor.Builder()
            .name("event-hub-consumer-hostname")
            .displayName("Event Hub Consumer Hostname")
            .description("The hostname of this Event Hub Consumer instance." +
                    " If not specified, an unique identifier is generated in 'nifi-<UUID>' format.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false)
            .build();

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("The Record Reader to use for reading received messages." +
                    " The Event Hub name can be referred by Expression Language '${eventhub.name}' to access a schema.")
            .identifiesControllerService(RecordReaderFactory.class)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .build();
    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("The Record Writer to use for serializing Records to an output FlowFile." +
                    " The Event Hub name can be referred by Expression Language '${eventhub.name}' to access a schema." +
                    " If not specified, each message will create a FlowFile.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .build();

    static final AllowableValue INITIAL_OFFSET_START_OF_STREAM = new AllowableValue(
            "start-of-stream", "Start of stream", "Read from the oldest message retained in the stream.");
    static final AllowableValue INITIAL_OFFSET_END_OF_STREAM = new AllowableValue(
            "end-of-stream", "End of stream",
            "Ignore old retained messages even if exist, start reading new ones from now.");
    static final PropertyDescriptor INITIAL_OFFSET = new PropertyDescriptor.Builder()
            .name("event-hub-initial-offset")
            .displayName("Initial Offset")
            .description("Specify where to start receiving messages if offset is not stored in Azure Storage.")
            .required(true)
            .allowableValues(INITIAL_OFFSET_START_OF_STREAM, INITIAL_OFFSET_END_OF_STREAM)
            .defaultValue(INITIAL_OFFSET_END_OF_STREAM.getValue())
            .build();
    static final PropertyDescriptor PREFETCH_COUNT = new PropertyDescriptor.Builder()
            .name("event-hub-prefetch-count")
            .displayName("Prefetch Count")
            .defaultValue("The number of messages to fetch from Event Hub before processing." +
                    " This parameter affects throughput." +
                    " The more prefetch count, the better throughput in general, but consumes more resources (RAM)." +
                    " NOTE: Even though Event Hub client API provides this option," +
                    " actual number of messages can be pre-fetched is depend on the Event Hub server implementation." +
                    " It is reported that only one event is received at a time in certain situation." +
                    " https://github.com/Azure/azure-event-hubs-java/issues/125")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("300")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();
    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("event-hub-batch-size")
            .displayName("Batch Size")
            .description("The number of messages to process within a NiFi session." +
                    " This parameter affects throughput and consistency." +
                    " NiFi commits its session and Event Hub checkpoint after processing this number of messages." +
                    " If NiFi session is committed, but failed to create an Event Hub checkpoint," +
                    " then it is possible that the same messages to be retrieved again." +
                    " The higher number, the higher throughput, but possibly less consistent.")
            .defaultValue("10")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();
    static final PropertyDescriptor RECEIVE_TIMEOUT = new PropertyDescriptor.Builder()
            .name("event-hub-message-receive-timeout")
            .displayName("Message Receive Timeout")
            .description("The amount of time this consumer should wait to receive the Prefetch Count before returning.")
            .defaultValue("1 min")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();
    static final PropertyDescriptor STORAGE_ACCOUNT_NAME = new PropertyDescriptor.Builder()
            .name("storage-account-name")
            .displayName("Storage Account Name")
            .description("Name of the Azure Storage account to store Event Hub Consumer Group state.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();
    static final PropertyDescriptor STORAGE_ACCOUNT_KEY = new PropertyDescriptor.Builder()
            .name("storage-account-key")
            .displayName("Storage Account Key")
            .description("The Azure Storage account key to store Event Hub Consumer Group state.")
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();
    static final PropertyDescriptor STORAGE_CONTAINER_NAME = new PropertyDescriptor.Builder()
            .name("storage-container-name")
            .displayName("Storage Container Name")
            .description("Name of the Azure Storage Container to store Event Hub Consumer Group state." +
                    " If not specified, Event Hub name is used.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false)
            .build();


    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles received from Event Hub.")
            .build();

    static final Relationship REL_PARSE_FAILURE = new Relationship.Builder()
            .name("parse.failure")
            .description("If a message from Event Hub cannot be parsed using the configured Record Reader" +
                    " or failed to be written by the configured Record Writer," +
                    " the contents of the message will be routed to this Relationship as its own individual FlowFile.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS;
    private static final Set<Relationship> RECORD_RELATIONSHIPS;
    private static final List<PropertyDescriptor> PROPERTIES;

    static {
        PROPERTIES = Collections.unmodifiableList(Arrays.asList(
                NAMESPACE, EVENT_HUB_NAME, ACCESS_POLICY_NAME, POLICY_PRIMARY_KEY, CONSUMER_GROUP, CONSUMER_HOSTNAME,
                RECORD_READER, RECORD_WRITER,
                INITIAL_OFFSET, PREFETCH_COUNT, BATCH_SIZE, RECEIVE_TIMEOUT,
                STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY, STORAGE_CONTAINER_NAME
        ));

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        RELATIONSHIPS = Collections.unmodifiableSet(relationships);

        relationships.add(REL_PARSE_FAILURE);
        RECORD_RELATIONSHIPS = Collections.unmodifiableSet(relationships);
    }

    private volatile EventProcessorHost eventProcessorHost;
    private volatile ProcessSessionFactory processSessionFactory;
    private volatile RecordReaderFactory readerFactory;
    private volatile RecordSetWriterFactory writerFactory;
    // The namespace name can not be retrieved from a PartitionContext at EventProcessor.onEvents, so keep it here.
    private volatile String namespaceName;
    private volatile boolean isRecordReaderSet = false;
    private volatile boolean isRecordWriterSet = false;

    /**
     * For unit test.
     */
    void setProcessSessionFactory(ProcessSessionFactory processSessionFactory) {
        this.processSessionFactory = processSessionFactory;
    }

    /**
     * For unit test.
     */
    void setNamespaceName(String namespaceName) {
        this.namespaceName = namespaceName;
    }

    /**
     * For unit test.
     */
    public void setReaderFactory(RecordReaderFactory readerFactory) {
        this.readerFactory = readerFactory;
    }

    /**
     * For unit test.
     */
    public void setWriterFactory(RecordSetWriterFactory writerFactory) {
        this.writerFactory = writerFactory;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return isRecordReaderSet && isRecordWriterSet ? RECORD_RELATIONSHIPS : RELATIONSHIPS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();
        final ControllerService recordReader = validationContext.getProperty(RECORD_READER).asControllerService();
        final ControllerService recordWriter = validationContext.getProperty(RECORD_WRITER).asControllerService();
        if ((recordReader != null && recordWriter == null) || (recordReader == null && recordWriter != null)) {
            results.add(new ValidationResult.Builder()
                    .subject("Record Reader and Writer")
                    .explanation(String.format("Both %s and %s should be set in order to write FlowFiles as Records.",
                            RECORD_READER.getDisplayName(), RECORD_WRITER.getDisplayName()))
                    .valid(false)
                    .build());
        }
        return results;
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        if (RECORD_READER.equals(descriptor)) {
            isRecordReaderSet = !StringUtils.isEmpty(newValue);
        } else if (RECORD_WRITER.equals(descriptor)) {
            isRecordWriterSet = !StringUtils.isEmpty(newValue);
        }
    }

    public class EventProcessorFactory implements IEventProcessorFactory {
        @Override
        public IEventProcessor createEventProcessor(PartitionContext context) throws Exception {
            final EventProcessor eventProcessor = new EventProcessor();
            return eventProcessor;
        }
    }

    public class EventProcessor implements IEventProcessor {

        @Override
        public void onOpen(PartitionContext context) throws Exception {
            getLogger().info("Consumer group {} opened partition {} of {}",
                    new Object[]{context.getConsumerGroupName(), context.getPartitionId(), context.getEventHubPath()});
        }

        @Override
        public void onClose(PartitionContext context, CloseReason reason) throws Exception {
            getLogger().info("Consumer group {} closed partition {} of {}. reason={}",
                    new Object[]{context.getConsumerGroupName(), context.getPartitionId(), context.getEventHubPath(), reason});
        }

        @Override
        public void onEvents(PartitionContext context, Iterable<EventData> messages) throws Exception {
            final ProcessSession session = processSessionFactory.createSession();

            try {

                final StopWatch stopWatch = new StopWatch(true);

                if (readerFactory != null && writerFactory != null) {
                    writeRecords(context, messages, session, stopWatch);
                } else {
                    writeFlowFiles(context, messages, session, stopWatch);
                }

                // Commit NiFi first.
                session.commit();
                // If creating an Event Hub checkpoint failed, then the same message can be retrieved again.
                context.checkpoint();

            } catch (Exception e) {
                getLogger().error("Unable to fully process received message due to " + e, e);
                // FlowFiles those are already committed will not get rollback.
                session.rollback();
            }

        }

        private void putEventHubAttributes(Map<String, String> attributes, String eventHubName, String partitionId, EventData eventData) {
            final EventData.SystemProperties systemProperties = eventData.getSystemProperties();
            if (null != systemProperties) {
                attributes.put("eventhub.enqueued.timestamp", String.valueOf(systemProperties.getEnqueuedTime()));
                attributes.put("eventhub.offset", systemProperties.getOffset());
                attributes.put("eventhub.sequence", String.valueOf(systemProperties.getSequenceNumber()));
            }

            attributes.put("eventhub.name", eventHubName);
            attributes.put("eventhub.partition", partitionId);
        }

        private void writeFlowFiles(PartitionContext context, Iterable<EventData> messages, ProcessSession session, StopWatch stopWatch) {
            final String eventHubName = context.getEventHubPath();
            final String partitionId = context.getPartitionId();
            final String consumerGroup = context.getConsumerGroupName();
            messages.forEach(eventData -> {
                FlowFile flowFile = session.create();

                final Map<String, String> attributes = new HashMap<>();
                putEventHubAttributes(attributes, eventHubName, partitionId, eventData);

                flowFile = session.putAllAttributes(flowFile, attributes);
                flowFile = session.write(flowFile, out -> {
                    out.write(eventData.getBytes());
                });

                transferTo(REL_SUCCESS, session, stopWatch, eventHubName, partitionId, consumerGroup, flowFile);
            });
        }

        private void transferTo(Relationship relationship, ProcessSession session, StopWatch stopWatch,
                                String eventHubName, String partitionId, String consumerGroup, FlowFile flowFile) {
            session.transfer(flowFile, relationship);
            final String transitUri = "amqps://" + namespaceName + ".servicebus.windows.net/" + eventHubName + "/ConsumerGroups/" + consumerGroup + "/Partitions/" + partitionId;
            session.getProvenanceReporter().receive(flowFile, transitUri, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
        }

        private void writeRecords(PartitionContext context, Iterable<EventData> messages, ProcessSession session, StopWatch stopWatch)
                throws SchemaNotFoundException, IOException {

            final String eventHubName = context.getEventHubPath();
            final String partitionId = context.getPartitionId();
            final String consumerGroup = context.getConsumerGroupName();
            final Map<String, String> schemaRetrievalVariables = new HashMap<>();
            schemaRetrievalVariables.put("eventhub.name", eventHubName);

            final ComponentLog logger = getLogger();
            FlowFile flowFile = session.create();
            final Map<String, String> attributes = new HashMap<>();

            RecordSetWriter writer = null;
            EventData lastEventData = null;
            WriteResult lastWriteResult = null;
            int recordCount = 0;

            try (final OutputStream out = session.write(flowFile)) {
                for (final EventData eventData : messages) {

                    try (final InputStream in = new ByteArrayInputStream(eventData.getBytes())) {
                        final RecordReader reader = readerFactory.createRecordReader(schemaRetrievalVariables, in, logger);

                        Record record;
                        while ((record = reader.nextRecord()) != null) {

                            if (writer == null) {
                                // Initialize the writer when the first record is read.
                                final RecordSchema readerSchema = record.getSchema();
                                final RecordSchema writeSchema = writerFactory.getSchema(schemaRetrievalVariables, readerSchema);
                                writer = writerFactory.createWriter(logger, writeSchema, out);
                                writer.beginRecordSet();
                            }

                            lastWriteResult = writer.write(record);
                            recordCount += lastWriteResult.getRecordCount();
                        }

                        lastEventData = eventData;

                    } catch (Exception e) {
                        // Write it to the parse failure relationship.
                        logger.error("Failed to parse message from Azure Event Hub using configured Record Reader and Writer due to " + e, e);
                        FlowFile failed = session.create();
                        session.write(failed, o -> o.write(eventData.getBytes()));
                        putEventHubAttributes(attributes, eventHubName, partitionId, eventData);
                        failed = session.putAllAttributes(failed, attributes);
                        transferTo(REL_PARSE_FAILURE, session, stopWatch, eventHubName, partitionId, consumerGroup, failed);
                    }
                }

                if (lastEventData != null) {
                    putEventHubAttributes(attributes, eventHubName, partitionId, lastEventData);

                    attributes.put("record.count", String.valueOf(recordCount));
                    if (writer != null) {
                        writer.finishRecordSet();
                        attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                        if (lastWriteResult != null) {
                            attributes.putAll(lastWriteResult.getAttributes());
                        }

                        try {
                            writer.close();
                        } catch (IOException e) {
                            logger.warn("Failed to close Record Writer due to {}" + e, e);
                        }
                    }
                }
            }

            // This part has to be outside of 'session.write(flowFile)' code block.
            if (lastEventData != null) {
                flowFile = session.putAllAttributes(flowFile, attributes);
                transferTo(REL_SUCCESS, session, stopWatch, eventHubName, partitionId, consumerGroup, flowFile);
            } else {
                // If there's no successful event data, then remove the FlowFile.
                session.remove(flowFile);
            }
        }

        @Override
        public void onError(PartitionContext context, Throwable e) {
            if (e instanceof ReceiverDisconnectedException && e.getMessage().startsWith("New receiver with higher epoch of ")) {
                // This is a known behavior in a NiFi cluster where multiple nodes consumes from the same Event Hub.
                // Once another node connects, some partitions are given to that node to distribute consumer load.
                // When that happens, this exception is thrown.
                getLogger().info("New receiver took over partition {} of Azure Event Hub {}, consumerGroupName={}, message={}",
                        new Object[]{context.getPartitionId(), context.getEventHubPath(), context.getConsumerGroupName(), e.getMessage()});
                return;
            }
            getLogger().error("An error occurred while receiving messages from Azure Event Hub {} at partition {}," +
                            " consumerGroupName={}, exception={}",
                    new Object[]{context.getEventHubPath(), context.getPartitionId(), context.getConsumerGroupName(), e}, e);
        }

    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {

        if (eventProcessorHost == null) {
            try {
                registerEventProcessor(context);
            } catch (IllegalArgumentException e) {
                // In order to show simple error message without wrapping it by another ProcessException, just throw it as it is.
                throw e;
            } catch (Exception e) {
                throw new ProcessException("Failed to register the event processor due to " + e, e);
            }
            processSessionFactory = sessionFactory;

            readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
            writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        }

        // After a EventProcessor is registered successfully, nothing has to be done at onTrigger
        // because new sessions are created when new messages are arrived by the EventProcessor.
        context.yield();
    }

    @OnStopped
    public void unregisterEventProcessor(final ProcessContext context) {
        if (eventProcessorHost != null) {
            try {
                eventProcessorHost.unregisterEventProcessor();
                eventProcessorHost = null;
                processSessionFactory = null;
                readerFactory = null;
                writerFactory = null;
            } catch (Exception e) {
                throw new RuntimeException("Failed to unregister the event processor due to " + e, e);
            }
        }
    }

    private void registerEventProcessor(final ProcessContext context) throws Exception {
        // Validate required properties.
        final String consumerGroupName = context.getProperty(CONSUMER_GROUP).evaluateAttributeExpressions().getValue();
        validateRequiredProperty(CONSUMER_GROUP, consumerGroupName);

        namespaceName = context.getProperty(NAMESPACE).evaluateAttributeExpressions().getValue();
        validateRequiredProperty(NAMESPACE, namespaceName);

        final String eventHubName = context.getProperty(EVENT_HUB_NAME).evaluateAttributeExpressions().getValue();
        validateRequiredProperty(EVENT_HUB_NAME, eventHubName);

        final String sasName = context.getProperty(ACCESS_POLICY_NAME).evaluateAttributeExpressions().getValue();
        validateRequiredProperty(ACCESS_POLICY_NAME, sasName);

        final String sasKey = context.getProperty(POLICY_PRIMARY_KEY).evaluateAttributeExpressions().getValue();
        validateRequiredProperty(POLICY_PRIMARY_KEY, sasKey);

        final String storageAccountName = context.getProperty(STORAGE_ACCOUNT_NAME).evaluateAttributeExpressions().getValue();
        validateRequiredProperty(STORAGE_ACCOUNT_NAME, storageAccountName);

        final String storageAccountKey = context.getProperty(STORAGE_ACCOUNT_KEY).evaluateAttributeExpressions().getValue();
        validateRequiredProperty(STORAGE_ACCOUNT_KEY, storageAccountKey);


        final String consumerHostname = orDefault(context.getProperty(CONSUMER_HOSTNAME).evaluateAttributeExpressions().getValue(),
                EventProcessorHost.createHostName("nifi"));

        final String containerName = orDefault(context.getProperty(STORAGE_CONTAINER_NAME).evaluateAttributeExpressions().getValue(),
                eventHubName);


        final EventProcessorOptions options = new EventProcessorOptions();
        final String initialOffset = context.getProperty(INITIAL_OFFSET).getValue();
        if (INITIAL_OFFSET_START_OF_STREAM.getValue().equals(initialOffset)) {
            options.setInitialOffsetProvider(options.new StartOfStreamInitialOffsetProvider());
        } else if (INITIAL_OFFSET_END_OF_STREAM.getValue().equals(initialOffset)){
            options.setInitialOffsetProvider(options.new EndOfStreamInitialOffsetProvider());
        } else {
            throw new IllegalArgumentException("Initial offset " + initialOffset + " is not allowed.");
        }

        final Integer prefetchCount = context.getProperty(PREFETCH_COUNT).evaluateAttributeExpressions().asInteger();
        if (prefetchCount != null && prefetchCount > 0) {
            options.setPrefetchCount(prefetchCount);
        }

        final Integer batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        if (batchSize != null && batchSize > 0) {
            options.setMaxBatchSize(batchSize);
        }

        final Long receiveTimeoutMillis = context.getProperty(RECEIVE_TIMEOUT)
                .evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
        options.setReceiveTimeOut(Duration.ofMillis(receiveTimeoutMillis));

        final String storageConnectionString = String.format(AzureStorageUtils.FORMAT_BLOB_CONNECTION_STRING, storageAccountName, storageAccountKey);

        final ConnectionStringBuilder eventHubConnectionString = new ConnectionStringBuilder(namespaceName, eventHubName, sasName, sasKey);

        eventProcessorHost = new EventProcessorHost(consumerHostname, eventHubName, consumerGroupName, eventHubConnectionString.toString(), storageConnectionString, containerName);

        options.setExceptionNotification(e -> {
            getLogger().error("An error occurred while receiving messages from Azure Event Hub {}" +
                            " at consumer group {} and partition {}, action={}, hostname={}, exception={}",
                    new Object[]{eventHubName, consumerGroupName, e.getPartitionId(), e.getAction(), e.getHostname()}, e.getException());
        });


        eventProcessorHost.registerEventProcessorFactory(new EventProcessorFactory(), options).get();
    }

    private String orDefault(String value, String defaultValue) {
        return isEmpty(value) ? defaultValue : value;
    }

    private void validateRequiredProperty(PropertyDescriptor property, String value) {
        if (isEmpty(value)) {
            throw new IllegalArgumentException(String.format("'%s' is required, but not specified.", property.getDisplayName()));
        }
    }

}
