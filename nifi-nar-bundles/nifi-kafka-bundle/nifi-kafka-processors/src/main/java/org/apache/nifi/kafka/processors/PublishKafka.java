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
package org.apache.nifi.kafka.processors;

import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.kafka.processors.producer.common.PublishKafkaUtil;
import org.apache.nifi.kafka.processors.producer.config.DeliveryGuarantee;
import org.apache.nifi.kafka.processors.producer.convert.DelimitedStreamKafkaRecordConverter;
import org.apache.nifi.kafka.processors.producer.convert.FlowFileStreamKafkaRecordConverter;
import org.apache.nifi.kafka.processors.producer.convert.KafkaRecordConverter;
import org.apache.nifi.kafka.processors.producer.convert.RecordStreamKafkaRecordConverter;
import org.apache.nifi.kafka.processors.producer.convert.RecordWrapperStreamKafkaRecordConverter;
import org.apache.nifi.kafka.processors.producer.header.AttributesHeadersFactory;
import org.apache.nifi.kafka.processors.producer.header.HeadersFactory;
import org.apache.nifi.kafka.processors.producer.key.AttributeKeyFactory;
import org.apache.nifi.kafka.processors.producer.key.KeyFactory;
import org.apache.nifi.kafka.processors.producer.key.MessageKeyFactory;
import org.apache.nifi.kafka.processors.producer.wrapper.RecordMetadataStrategy;
import org.apache.nifi.kafka.service.api.KafkaConnectionService;
import org.apache.nifi.kafka.service.api.common.PartitionState;
import org.apache.nifi.kafka.service.api.producer.FlowFileResult;
import org.apache.nifi.kafka.service.api.producer.KafkaProducerService;
import org.apache.nifi.kafka.service.api.producer.ProducerConfiguration;
import org.apache.nifi.kafka.service.api.producer.PublishContext;
import org.apache.nifi.kafka.service.api.producer.RecordSummary;
import org.apache.nifi.kafka.service.api.record.KafkaRecord;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.component.KafkaPublishComponent;
import org.apache.nifi.kafka.shared.property.FailureStrategy;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.apache.nifi.kafka.shared.property.PublishStrategy;
import org.apache.nifi.kafka.shared.transaction.TransactionIdSupplier;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;

@Tags({"kafka", "producer", "record"})
public class PublishKafka extends AbstractProcessor implements KafkaPublishComponent, VerifiableProcessor {

    public static final PropertyDescriptor CONNECTION_SERVICE = new PropertyDescriptor.Builder()
            .name("Kafka Connection Service")
            .displayName("Kafka Connection Service")
            .description("Provides connections to Kafka Broker for publishing Kafka Records")
            .identifiesControllerService(KafkaConnectionService.class)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .build();

    public static final PropertyDescriptor TOPIC_NAME = new PropertyDescriptor.Builder()
            .name("Topic Name")
            .displayName("Topic Name")
            .description("Name of the Kafka Topic to which the Processor publishes Kafka Records")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("The Record Reader to use for incoming FlowFiles")
            .identifiesControllerService(RecordReaderFactory.class)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("The Record Writer to use in order to serialize the data before sending to Kafka")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    static final PropertyDescriptor PUBLISH_STRATEGY = new PropertyDescriptor.Builder()
            .name("publish-strategy")
            .displayName("Publish Strategy")
            .description("The format used to publish the incoming FlowFile record to Kafka.")
            .required(true)
            .defaultValue(PublishStrategy.USE_VALUE.getValue())
            .allowableValues(PublishStrategy.class)
            .build();

    static final PropertyDescriptor MESSAGE_KEY_FIELD = new PropertyDescriptor.Builder()
            .name("message-key-field")
            .displayName("Message Key Field")
            .description("The name of a field in the Input Records that should be used as the Key for the Kafka message.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dependsOn(PUBLISH_STRATEGY, PublishStrategy.USE_VALUE.getValue())
            .required(false)
            .build();

    static final PropertyDescriptor MESSAGE_DEMARCATOR = new PropertyDescriptor.Builder()
            .name("message-demarcator")
            .displayName("Message Demarcator")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .description("Specifies the string (interpreted as UTF-8) to use for demarcating multiple messages within "
                    + "a single FlowFile. If not specified, the entire content of the FlowFile will be used as a single message. If specified, the "
                    + "contents of the FlowFile will be split on this delimiter and each section sent as a separate Kafka message. "
                    + "To enter special character such as 'new line' use CTRL+Enter or Shift+Enter, depending on your OS.")
            .build();

    public static final PropertyDescriptor MAX_REQUEST_SIZE = new PropertyDescriptor.Builder()
            .name("max.request.size")
            .displayName("Max Request Size")
            .description("The maximum size of a request in bytes. Corresponds to Kafka's 'max.request.size' property and defaults to 1 MB (1048576).")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 MB")
            .build();

    static final PropertyDescriptor KEY = new PropertyDescriptor.Builder()
            .name("kafka-key")
            .displayName("Kafka Key")
            .description("The Key to use for the Message. "
                    + "If not specified, the flow file attribute 'kafka.key' is used as the message key, if it is present."
                    + "Beware that setting Kafka key and demarcating at the same time may potentially lead to many Kafka messages with the same key."
                    + "Normally this is not a problem as Kafka does not enforce or assume message and key uniqueness. Still, setting the demarcator and Kafka key at the same time poses a risk of "
                    + "data loss on Kafka. During a topic compaction on Kafka, messages will be deduplicated based on this key.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor KEY_ATTRIBUTE_ENCODING = new PropertyDescriptor.Builder()
            .name("key-attribute-encoding")
            .displayName("Key Attribute Encoding")
            .description("FlowFiles that are emitted have an attribute named '" + KafkaFlowFileAttribute.KAFKA_KEY + "'. This property dictates how the value of the attribute should be encoded.")
            .required(true)
            .defaultValue(KeyEncoding.UTF8.getValue())
            .allowableValues(KeyEncoding.class)
            .build();

    static final PropertyDescriptor COMPRESSION_CODEC = new PropertyDescriptor.Builder()
            .name("compression.type")
            .displayName("Compression Type")
            .description("This parameter allows you to specify the compression codec for all data generated by this producer.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues("none", "gzip", "snappy", "lz4")
            .defaultValue("none")
            .build();

    static final PropertyDescriptor ATTRIBUTE_NAME_REGEX = new PropertyDescriptor.Builder()
            .name("attribute-name-regex")
            .displayName("Attributes to Send as Headers (Regex)")
            .description("A Regular Expression that is matched against all FlowFile attribute names. "
                    + "Any attribute whose name matches the regex will be added to the Kafka messages as a Header. "
                    + "If not specified, no FlowFile attributes will be added as headers.")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .dependsOn(PUBLISH_STRATEGY, PublishStrategy.USE_VALUE.getValue())
            .required(false)
            .build();

    public static final PropertyDescriptor USE_TRANSACTIONS = new PropertyDescriptor.Builder()
            .name("use-transactions")
            .displayName("Use Transactions")
            .description("Specifies whether or not NiFi should provide Transactional guarantees when communicating with Kafka. If there is a problem sending data to Kafka, "
                    + "and this property is set to false, then the messages that have already been sent to Kafka will continue on and be delivered to consumers. "
                    + "If this is set to true, then the Kafka transaction will be rolled back so that those messages are not available to consumers. Setting this to true "
                    + "requires that the <Delivery Guarantee> property be set to \"Guarantee Replicated Delivery.\"")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();
    static final PropertyDescriptor TRANSACTIONAL_ID_PREFIX = new PropertyDescriptor.Builder()
            .name("transactional-id-prefix")
            .displayName("Transactional Id Prefix")
            .description("When Use Transaction is set to true, KafkaProducer config 'transactional.id' will be a generated UUID and will be prefixed with this string.")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .dependsOn(USE_TRANSACTIONS, "true")
            .required(false)
            .build();

    static final PropertyDescriptor MESSAGE_HEADER_ENCODING = new PropertyDescriptor.Builder()
            .name("message-header-encoding")
            .displayName("Message Header Encoding")
            .description("For any attribute that is added as a message header, as configured via the <Attributes to Send as Headers> property, "
                    + "this property indicates the Character Encoding to use for serializing the headers.")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .required(false)
            .build();

    static final PropertyDescriptor RECORD_KEY_WRITER = new PropertyDescriptor.Builder()
            .name("record-key-writer")
            .displayName("Record Key Writer")
            .description("The Record Key Writer to use for outgoing FlowFiles")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .dependsOn(PUBLISH_STRATEGY, PublishStrategy.USE_WRAPPER.getValue())
            .build();

    static final PropertyDescriptor RECORD_METADATA_STRATEGY = new PropertyDescriptor.Builder()
            .name("Record Metadata Strategy")
            .displayName("Record Metadata Strategy")
            .description("Specifies whether the Record's metadata (topic and partition) should come from the Record's metadata field or if it should come from the configured " +
                    "Topic Name and Partition / Partitioner class properties")
            .required(true)
            .defaultValue(RecordMetadataStrategy.FROM_PROPERTIES.getValue())
            .allowableValues(RecordMetadataStrategy.class)
            .dependsOn(PUBLISH_STRATEGY, PublishStrategy.USE_WRAPPER.getValue())
            .build();

    static final AllowableValue ROUND_ROBIN_PARTITIONING = new AllowableValue("org.apache.nifi.processors.kafka.pubsub.Partitioners.RoundRobinPartitioner",
            "RoundRobinPartitioner",
            "Messages will be assigned partitions in a round-robin fashion, sending the first message to Partition 1, "
                    + "the next Partition to Partition 2, and so on, wrapping as necessary.");
    static final AllowableValue RANDOM_PARTITIONING = new AllowableValue("org.apache.kafka.clients.producer.internals.DefaultPartitioner",
            "DefaultPartitioner", "The default partitioning strategy will choose the sticky partition that changes when the batch is full "
            + "(See KIP-480 for details about sticky partitioning).");
    public static final AllowableValue EXPRESSION_LANGUAGE_PARTITIONING = new AllowableValue("org.apache.nifi.processors.kafka.pubsub.Partitioners.ExpressionLanguagePartitioner",
            "Expression Language Partitioner",
            "Interprets the <Partition> property as Expression Language that will be evaluated against each FlowFile. This Expression will be evaluated once against the FlowFile, " +
                    "so all Records in a given FlowFile will go to the same partition.");

    static final PropertyDescriptor PARTITION_CLASS = new PropertyDescriptor.Builder()
            .name("partitioner.class")
            .displayName("Partitioner class")
            .description("Specifies which class to use to compute a partition id for a message. Corresponds to Kafka's 'partitioner.class' property.")
            .allowableValues(ROUND_ROBIN_PARTITIONING, RANDOM_PARTITIONING, EXPRESSION_LANGUAGE_PARTITIONING)
            .defaultValue(RANDOM_PARTITIONING.getValue())
            .required(false)
            .build();

    static final PropertyDescriptor PARTITION = new PropertyDescriptor.Builder()
            .name("partition")
            .displayName("Partition")
            .description("Specifies which Partition Records will go to. How this value is interpreted is dictated by the <Partitioner class> property.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
            CONNECTION_SERVICE,
            DeliveryGuarantee.DELIVERY_GUARANTEE,
            TOPIC_NAME,
            RECORD_READER,
            RECORD_WRITER,
            PUBLISH_STRATEGY,
            RECORD_KEY_WRITER,
            RECORD_METADATA_STRATEGY,
            MESSAGE_DEMARCATOR,
            FAILURE_STRATEGY,
            KEY,
            KEY_ATTRIBUTE_ENCODING,
            ATTRIBUTE_NAME_REGEX,
            USE_TRANSACTIONS,
            TRANSACTIONAL_ID_PREFIX,
            MESSAGE_HEADER_ENCODING,
            MESSAGE_KEY_FIELD,
            MAX_REQUEST_SIZE,
            COMPRESSION_CODEC,
            PARTITION_CLASS,
            PARTITION
    ));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles for which all content was sent to Kafka.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that cannot be sent to Kafka will be routed to this Relationship")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final List<FlowFile> flowFiles = PublishKafkaUtil.pollFlowFiles(session);
        if (flowFiles.isEmpty()) {
            return;
        }

        final KafkaConnectionService connectionService = context.getProperty(CONNECTION_SERVICE).asControllerService(KafkaConnectionService.class);

        final boolean useTransactions = context.getProperty(USE_TRANSACTIONS).asBoolean();
        final String transactionalIdPrefix = context.getProperty(TRANSACTIONAL_ID_PREFIX).evaluateAttributeExpressions().getValue();
        final String deliveryGuarantee = context.getProperty(DeliveryGuarantee.DELIVERY_GUARANTEE).getValue();
        final String compressionCodec = context.getProperty(COMPRESSION_CODEC).getValue();
        final String partitionClass = context.getProperty(PARTITION_CLASS).getValue();
        final ProducerConfiguration producerConfiguration = new ProducerConfiguration(
                useTransactions, transactionalIdPrefix, deliveryGuarantee, compressionCodec, partitionClass);

        try (final KafkaProducerService producerService = connectionService.getProducerService(producerConfiguration)) {
            publishFlowFiles(context, session, flowFiles, producerService);
        } catch (final Throwable e) {
            getLogger().error(e.getMessage(), e);
            context.yield();
        }
    }

    private void publishFlowFiles(final ProcessContext context, final ProcessSession session,
                                  final List<FlowFile> flowFiles, final KafkaProducerService producerService) {
        producerService.init();
        for (final FlowFile flowFile : flowFiles) {
            publishFlowFile(context, session, flowFile, producerService);
        }
        final RecordSummary recordSummary = producerService.complete();
        if (recordSummary.isFailure()) {
            // might this be a place we want to behave differently?  (route FlowFile to failure only on failure)
            routeFailureStrategy(context, session, flowFiles);
        } else {
            routeResults(session, recordSummary.getFlowFileResults());
        }
    }

    private void routeFailureStrategy(final ProcessContext context, final ProcessSession session, final List<FlowFile> flowFiles) {
        final String strategy = context.getProperty(FAILURE_STRATEGY).getValue();
        if (FailureStrategy.ROLLBACK.getValue().equals(strategy)) {
            session.rollback();
            context.yield();
        } else {
            session.transfer(flowFiles, REL_FAILURE);
        }
    }

    private void routeResults(final ProcessSession session, final List<FlowFileResult> flowFileResults) {
        for (final FlowFileResult flowFileResult : flowFileResults) {
            final Relationship relationship = flowFileResult.getExceptions().isEmpty() ? REL_SUCCESS : REL_FAILURE;
            session.transfer(flowFileResult.getFlowFile(), relationship);
        }
    }

    private void publishFlowFile(final ProcessContext context, final ProcessSession session,
                                 final FlowFile flowFile, final KafkaProducerService producerService) {
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final RecordSetWriterFactory keyWriterFactory = context.getProperty(RECORD_KEY_WRITER).asControllerService(RecordSetWriterFactory.class);

        final String topic = context.getProperty(TOPIC_NAME).evaluateAttributeExpressions(flowFile.getAttributes()).getValue();
        final Integer partition = getPartition(context, flowFile);
        final PublishContext publishContext = new PublishContext(topic, partition, null, flowFile);

        final PropertyValue propertyDemarcator = context.getProperty(MESSAGE_DEMARCATOR);
        final int maxMessageSize = context.getProperty(MAX_REQUEST_SIZE).asDataSize(DataUnit.B).intValue();

        final PublishStrategy publishStrategy = PublishStrategy.valueOf(context.getProperty(PUBLISH_STRATEGY).getValue());
        final RecordMetadataStrategy metadataStrategy = RecordMetadataStrategy.valueOf(context.getProperty(RECORD_METADATA_STRATEGY).getValue());

        final String keyAttribute = context.getProperty(KEY).getValue();
        final String keyAttributeEncoding = context.getProperty(KEY_ATTRIBUTE_ENCODING).getValue();
        final String messageKeyField = context.getProperty(MESSAGE_KEY_FIELD).evaluateAttributeExpressions(flowFile).getValue();
        final KeyFactory keyFactory = ((PublishStrategy.USE_VALUE == publishStrategy) && (messageKeyField != null))
                 ? new MessageKeyFactory(flowFile, messageKeyField, keyWriterFactory, getLogger())
                 : new AttributeKeyFactory(keyAttribute, keyAttributeEncoding);

        final String attributeNameRegex = context.getProperty(ATTRIBUTE_NAME_REGEX).getValue();
        final Pattern attributeNamePattern = (attributeNameRegex == null) ? null : Pattern.compile(attributeNameRegex);
        final String charsetName = context.getProperty(MESSAGE_HEADER_ENCODING).evaluateAttributeExpressions().getValue();
        final Charset charset = Charset.forName(charsetName);
        final HeadersFactory headersFactory = new AttributesHeadersFactory(attributeNamePattern, charset);

        final KafkaRecordConverter kafkaRecordConverter = getKafkaRecordConverterFor(
                publishStrategy, metadataStrategy, readerFactory, writerFactory, keyWriterFactory,
                keyFactory, headersFactory, propertyDemarcator, flowFile, maxMessageSize);
        final PublishCallback callback = new PublishCallback(
                producerService, publishContext, kafkaRecordConverter, flowFile.getAttributes(), flowFile.getSize());
        session.read(flowFile, callback);
    }

    private Integer getPartition(final ProcessContext context, final FlowFile flowFile) {
        final String partitionClass = context.getProperty(PARTITION_CLASS).getValue();
        if (EXPRESSION_LANGUAGE_PARTITIONING.getValue().equals(partitionClass)) {
            final String partition = context.getProperty(PARTITION).evaluateAttributeExpressions(flowFile).getValue();
            return Objects.hashCode(partition);
        }
        return null;
    }

    private KafkaRecordConverter getKafkaRecordConverterFor(
            final PublishStrategy publishStrategy, final RecordMetadataStrategy metadataStrategy,
            final RecordReaderFactory readerFactory, final RecordSetWriterFactory writerFactory,
            final RecordSetWriterFactory keyWriterFactory,
            final KeyFactory keyFactory, final HeadersFactory headersFactory,
            final PropertyValue propertyValueDemarcator, final FlowFile flowFile, final int maxMessageSize) {
        final KafkaRecordConverter kafkaRecordConverter;
        if ((readerFactory != null) && (writerFactory != null)) {
            if (publishStrategy == PublishStrategy.USE_WRAPPER) {
                kafkaRecordConverter = new RecordWrapperStreamKafkaRecordConverter(flowFile, metadataStrategy,
                        readerFactory, writerFactory, keyWriterFactory, maxMessageSize, getLogger());
            } else {
                kafkaRecordConverter = new RecordStreamKafkaRecordConverter(
                        readerFactory, writerFactory, headersFactory, keyFactory, maxMessageSize, getLogger());
            }
        } else if (propertyValueDemarcator.isSet()) {
            final String demarcator = propertyValueDemarcator.evaluateAttributeExpressions(flowFile).getValue();
            kafkaRecordConverter = new DelimitedStreamKafkaRecordConverter(
                    demarcator.getBytes(StandardCharsets.UTF_8), maxMessageSize, headersFactory);
        } else {
            kafkaRecordConverter = new FlowFileStreamKafkaRecordConverter(maxMessageSize, headersFactory);
        }
        return kafkaRecordConverter;
    }

    private static class PublishCallback implements InputStreamCallback {
        private final KafkaProducerService producerService;
        private final PublishContext publishContext;
        private final KafkaRecordConverter kafkaConverter;
        private final Map<String, String> attributes;
        private final long inputLength;

        public PublishCallback(
                final KafkaProducerService producerService,
                final PublishContext publishContext,
                final KafkaRecordConverter kafkaConverter,
                final Map<String, String> attributes,
                final long inputLength) {
            this.producerService = producerService;
            this.publishContext = publishContext;
            this.kafkaConverter = kafkaConverter;
            this.attributes = attributes;
            this.inputLength = inputLength;
        }

        @Override
        public void process(final InputStream in) {
            try (final InputStream is = new BufferedInputStream(in)) {
                final Iterator<KafkaRecord> records = kafkaConverter.convert(attributes, is, inputLength);
                producerService.send(records, publishContext);
            } catch (final Exception e) {
                publishContext.setException(e);  // on data pre-process failure, indicate this to controller service
                producerService.send(Collections.emptyIterator(), publishContext);
            }
        }
    }

    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext context, final ComponentLog verificationLogger,
                                                 final Map<String, String> attributes) {
        final List<ConfigVerificationResult> verificationResults = new ArrayList<>();

        final KafkaConnectionService connectionService = context.getProperty(CONNECTION_SERVICE).asControllerService(KafkaConnectionService.class);

        final boolean useTransactions = context.getProperty(USE_TRANSACTIONS).asBoolean();
        final String transactionalIdPrefix = context.getProperty(TRANSACTIONAL_ID_PREFIX).evaluateAttributeExpressions().getValue();
        final Supplier<String> transactionalIdSupplier = new TransactionIdSupplier(transactionalIdPrefix);
        final String deliveryGuarantee = context.getProperty(DeliveryGuarantee.DELIVERY_GUARANTEE).getValue();
        final String compressionCodec = context.getProperty(COMPRESSION_CODEC).getValue();
        final String partitionClass = context.getProperty(PARTITION_CLASS).getValue();
        final ProducerConfiguration producerConfiguration = new ProducerConfiguration(
                useTransactions, transactionalIdSupplier.get(), deliveryGuarantee, compressionCodec, partitionClass);
        final KafkaProducerService producerService = connectionService.getProducerService(producerConfiguration);

        final ConfigVerificationResult.Builder verificationPartitions = new ConfigVerificationResult.Builder()
                .verificationStepName("Verify Topic Partitions");

        final String topicName = context.getProperty(TOPIC_NAME).evaluateAttributeExpressions(attributes).getValue();
        try {
            final List<PartitionState> partitionStates = producerService.getPartitionStates(topicName);
            verificationPartitions
                .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                .explanation(String.format("Partitions [%d] found for Topic [%s]", partitionStates.size(), topicName));
        } catch (final Exception e) {
            getLogger().error("Topic [%s] Partition verification failed", topicName, e);
            verificationPartitions
                .outcome(ConfigVerificationResult.Outcome.FAILED)
                .explanation(String.format("Topic [%s] Partition access failed: %s", topicName, e));
        }
        verificationResults.add(verificationPartitions.build());

        return verificationResults;
    }
}
