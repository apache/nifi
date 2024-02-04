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
package org.apache.nifi.processors.kafka.pubsub;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.kafka.shared.property.PublishStrategy;
import org.apache.nifi.kafka.shared.transaction.TransactionIdSupplier;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPublishKafkaRecordMockParameterized {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final ObjectMapper mapper = getObjectMapper();

    public static Stream<Arguments> testCaseParametersProvider() {
        return Stream.of(
                arguments("PublishRecord/parameterized/flowfileInput1.json",
                        "account", ".*A.", getAttributes(), PublishStrategy.USE_VALUE,
                        "PublishRecord/parameterized/kafkaOutput1V.json"),
                arguments("PublishRecord/parameterized/flowfileInput1.json",
                        "account", ".*B.", getAttributes(), PublishStrategy.USE_WRAPPER,
                        "PublishRecord/parameterized/kafkaOutput1W.json"),
                arguments("PublishRecord/parameterized/flowfileInputA.json",
                        "key", ".*1", getAttributes(), PublishStrategy.USE_VALUE,
                        "PublishRecord/parameterized/kafkaOutputAV.json"),
                arguments("PublishRecord/parameterized/flowfileInputA.json",
                        "key", ".*2", getAttributes(), PublishStrategy.USE_WRAPPER,
                        "PublishRecord/parameterized/kafkaOutputAW.json"),

                arguments("PublishRecord/parameterized/flowfileInputDoc1V.json",
                        "account", "attribute.*", getAttributesDoc1(), PublishStrategy.USE_VALUE,
                        "PublishRecord/parameterized/kafkaOutputDoc1V.json"),
                arguments("PublishRecord/parameterized/flowfileInputDoc1W.json",
                        null, null, Collections.emptyMap(), PublishStrategy.USE_WRAPPER,
                        "PublishRecord/parameterized/kafkaOutputDoc1W.json"),
                arguments("PublishRecord/parameterized/flowfileInputDoc2W.json",
                        null, null, Collections.emptyMap(), PublishStrategy.USE_WRAPPER,
                        "PublishRecord/parameterized/kafkaOutputDoc2W.json")
        );
    }

    @ParameterizedTest
    @MethodSource("testCaseParametersProvider")
    public void testPublishKafkaRecord(final String flowfileInputResource,
                                       final String messageKeyField,
                                       final String attributeNameRegex,
                                       final Map<String, String> attributes,
                                       final PublishStrategy publishStrategy,
                                       final String kafkaRecordExpectedOutputResource)
            throws IOException, InitializationException {
        final byte[] flowfileData = IOUtils.toByteArray(Objects.requireNonNull(
                getClass().getClassLoader().getResource(flowfileInputResource)));
        logger.trace(new String(flowfileData, UTF_8));
        final MockFlowFile flowFile = new MockFlowFile(1L);
        flowFile.putAttributes(attributes);
        flowFile.setData(flowfileData);

        final Collection<ProducerRecord<byte[], byte[]>> producedRecords = new ArrayList<>();
        final TestRunner runner = getTestRunner(producedRecords);
        runner.setProperty("topic", "test-topic");
        if (attributeNameRegex != null) {
            runner.setProperty("attribute-name-regex", attributeNameRegex);
        }
        if (messageKeyField != null) {
            runner.setProperty("message-key-field", messageKeyField);
        }
        runner.setProperty("publish-strategy", publishStrategy.name());
        runner.enqueue(flowFile);
        runner.run(1);
        // verify results
        runner.assertAllFlowFilesTransferred(PublishKafkaRecord_2_6.REL_SUCCESS, 1);
        assertEquals(1, producedRecords.size());
        final ProducerRecord<byte[], byte[]> kafkaRecord = producedRecords.iterator().next();
        final DefaultPrettyPrinter prettyPrinter = new DefaultPrettyPrinter()
                .withObjectIndenter(new DefaultIndenter().withLinefeed("\n"));
        final String json = mapper.writer(prettyPrinter).writeValueAsString(kafkaRecord);
        logger.trace(json);

        final String kafkaRecordExpected = IOUtils.toString(Objects.requireNonNull(
                getClass().getClassLoader().getResource(kafkaRecordExpectedOutputResource)), UTF_8);
        assertEquals(kafkaRecordExpected, json);
    }

    private static ObjectMapper getObjectMapper() {
        final ObjectMapper objectMapper = new ObjectMapper()
                .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        final SimpleModule simpleModule = new SimpleModule();
        simpleModule.addSerializer(RecordHeader.class, new HeaderSerializer());
        simpleModule.addSerializer(new ProducerRecordBBSerializer(objectMapper));
        objectMapper.registerModule(simpleModule);
        return objectMapper;
    }

    /**
     * Custom {@link com.fasterxml.jackson} serialization for {@link RecordHeader}.
     */
    private static class HeaderSerializer extends JsonSerializer<RecordHeader> {

        @Override
        public void serialize(final RecordHeader recordHeader, final JsonGenerator jsonGenerator,
                              final SerializerProvider serializerProvider) throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeObjectField("RecordHeader-key",
                    (recordHeader.key() == null) ? null : recordHeader.key());
            jsonGenerator.writeObjectField("RecordHeader-value",
                    (recordHeader.value() == null) ? null : new String(recordHeader.value(), StandardCharsets.UTF_8));
            jsonGenerator.writeEndObject();
        }
    }

    /**
     * Custom {@link com.fasterxml.jackson} serialization for {@link ProducerRecord}.
     */
    private static class ProducerRecordBBSerializer extends StdSerializer<ProducerRecord<byte[], byte[]>> {
        private final ObjectMapper objectMapper;

        protected ProducerRecordBBSerializer(ObjectMapper objectMapper) {
            super(ProducerRecord.class, false);
            this.objectMapper = objectMapper;
        }

        @Override
        public void serialize(ProducerRecord<byte[], byte[]> producerRecord, JsonGenerator jsonGenerator,
                              SerializerProvider serializerProvider) throws IOException {
            jsonGenerator.writeStartObject();
            serializeField(jsonGenerator, "ProducerRecord-key", producerRecord.key());
            serializeField(jsonGenerator, "ProducerRecord-value", producerRecord.value());
            jsonGenerator.writeObjectField("ProducerRecord-headers", producerRecord.headers());
            jsonGenerator.writeEndObject();
        }

        private void serializeField(final JsonGenerator jsonGenerator, final String key, final byte[] value) throws IOException {
            if (value == null) {
                jsonGenerator.writeObjectField(key, null);
            } else {
                try {
                    jsonGenerator.writeObjectField(key, objectMapper.readTree(value));
                } catch (final JsonParseException e) {
                    jsonGenerator.writeStringField(key, new String(value, UTF_8));
                }
            }
        }
    }

    private static Map<String, String> getAttributes() {
        final Map<String, String> attributes = new TreeMap<>();
        attributes.put("attrKeyA1", "attrValueA1");
        attributes.put("attrKeyA2", "attrValueA2");
        attributes.put("attrKeyB1", "attrValueB1");
        attributes.put("attrKeyB2", "attrValueB2");
        return attributes;
    }

    private static Map<String, String> getAttributesDoc1() {
        final Map<String, String> attributes = new TreeMap<>();
        attributes.put("attributeA", "valueA");
        attributes.put("attributeB", "valueB");
        attributes.put("otherAttribute", "otherValue");
        return attributes;
    }

    private TestRunner getTestRunner(final Collection<ProducerRecord<byte[], byte[]>> producedRecords)
            throws InitializationException {
        final String readerId = "record-reader";
        final RecordReaderFactory readerService = new JsonTreeReader();
        final String writerId = "record-writer";
        final RecordSetWriterFactory writerService = new JsonRecordSetWriter();
        final String keyWriterId = "record-key-writer";
        final RecordSetWriterFactory keyWriterService = new JsonRecordSetWriter();
        final PublishKafkaRecord_2_6 processor = new PublishKafkaRecord_2_6() {
            @Override
            protected PublisherPool createPublisherPool(final ProcessContext context) {
                return getPublisherPool(producedRecords, context);
            }
        };
        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setValidateExpressionUsage(false);
        runner.addControllerService(readerId, readerService);
        runner.enableControllerService(readerService);
        runner.setProperty(readerId, readerId);
        runner.addControllerService(writerId, writerService);
        runner.enableControllerService(writerService);
        runner.setProperty(writerId, writerId);
        runner.addControllerService(keyWriterId, keyWriterService);
        runner.enableControllerService(keyWriterService);
        runner.setProperty(keyWriterId, keyWriterId);
        return runner;
    }

    private PublisherPool getPublisherPool(final Collection<ProducerRecord<byte[], byte[]>> producedRecords,
                                           final ProcessContext context) {
        final int maxMessageSize = context.getProperty("max.request.size").asDataSize(DataUnit.B).intValue();
        final long maxAckWaitMillis = context.getProperty("ack.wait.time").asTimePeriod(TimeUnit.MILLISECONDS);
        final String attributeNameRegex = context.getProperty("attribute-name-regex").getValue();
        final Pattern attributeNamePattern = attributeNameRegex == null ? null : Pattern.compile(attributeNameRegex);
        final boolean useTransactions = context.getProperty("use-transactions").asBoolean();
        final String transactionalIdPrefix = context.getProperty("transactional-id-prefix").evaluateAttributeExpressions().getValue();
        Supplier<String> transactionalIdSupplier = new TransactionIdSupplier(transactionalIdPrefix);
        final PublishStrategy publishStrategy = PublishStrategy.valueOf(context.getProperty("publish-strategy").getValue());
        final String charsetName = context.getProperty("message-header-encoding").evaluateAttributeExpressions().getValue();
        final Charset charset = Charset.forName(charsetName);
        final RecordSetWriterFactory recordKeyWriterFactory = context.getProperty("record-key-writer").asControllerService(RecordSetWriterFactory.class);

        return new PublisherPool(
                Collections.emptyMap(),
                mock(ComponentLog.class),
                maxMessageSize,
                maxAckWaitMillis,
                useTransactions,
                transactionalIdSupplier,
                attributeNamePattern,
                charset,
                publishStrategy,
                recordKeyWriterFactory) {
            @Override
            public PublisherLease obtainPublisher() {
                return getPublisherLease(producedRecords, context);
            }
        };
    }

    public interface ProducerBB extends Producer<byte[], byte[]> {
    }

    private PublisherLease getPublisherLease(final Collection<ProducerRecord<byte[], byte[]>> producedRecords,
                                             final ProcessContext context) {
        final String attributeNameRegex = context.getProperty("attribute-name-regex").getValue();
        final Pattern patternAttributeName = (attributeNameRegex == null) ? null : Pattern.compile(attributeNameRegex);
        final RecordSetWriterFactory keyWriterFactory = context.getProperty("record-key-writer")
                .asControllerService(RecordSetWriterFactory.class);
        final PublishStrategy publishStrategy = PublishStrategy.valueOf(context.getProperty("publish-strategy").getValue());

        final Producer<byte[], byte[]> producer = mock(ProducerBB.class);
        when(producer.send(any(), any())).then(invocation -> {
            final ProducerRecord<byte[], byte[]> record = invocation.getArgument(0);
            producedRecords.add(record);
            final Callback callback = invocation.getArgument(1);
            callback.onCompletion(new RecordMetadata(new TopicPartition("topic", 0), 0L, 0L, 0L, 0L, 0, 0), null);
            return null;
        });

        return new PublisherLease(
                producer,
                1024,
                1000L,
                mock(ComponentLog.class),
                true,
                patternAttributeName,
                UTF_8,
                publishStrategy,
                keyWriterFactory
        );
    }
}
