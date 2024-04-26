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
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.nifi.kafka.shared.transaction.TransactionIdSupplier;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPublishKafkaMockParameterized {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final ObjectMapper mapper = getObjectMapper();

    public static Stream<Arguments> testCaseParametersProvider() {
        return Stream.of(
                arguments("Publish/parameterized/flowfileInput1.json",
                        "key1A", ".*A.", getAttributes(),
                        "Publish/parameterized/kafkaOutput1A.json"),
                arguments("Publish/parameterized/flowfileInput1.json",
                        "key1B", ".*B.", getAttributes(),
                        "Publish/parameterized/kafkaOutput1B.json"),
                arguments("Publish/parameterized/flowfileInputA.json",
                        "keyA1", ".*1", getAttributes(),
                        "Publish/parameterized/kafkaOutputA1.json"),
                arguments("Publish/parameterized/flowfileInputA.json",
                        "keyA2", ".*2", getAttributes(),
                        "Publish/parameterized/kafkaOutputA2.json")
        );
    }

    @ParameterizedTest
    @MethodSource("testCaseParametersProvider")
    public void testPublishKafka(final String flowfileInputResource,
                                 final String messageKey,
                                 final String attributeNameRegex,
                                 final Map<String, String> attributes,
                                 final String kafkaRecordExpectedOutputResource)
            throws IOException {
        final byte[] flowfileData = IOUtils.toByteArray(Objects.requireNonNull(
                getClass().getClassLoader().getResource(flowfileInputResource)));
        logger.trace(new String(flowfileData, UTF_8));
        final MockFlowFile flowFile = new MockFlowFile(1L);
        flowFile.putAttributes(attributes);
        flowFile.setData(flowfileData);

        final Collection<ProducerRecord<byte[], byte[]>> producedRecords = new ArrayList<>();
        final TestRunner runner = getTestRunner(producedRecords);
        runner.setProperty("topic", "test-topic");
        runner.setProperty("attribute-name-regex", attributeNameRegex);
        runner.setProperty("kafka-key", messageKey);
        runner.enqueue(flowFile);
        runner.run(1);
        // verify results
        runner.assertAllFlowFilesTransferred(PublishKafka_2_6.REL_SUCCESS, 1);
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
            jsonGenerator.writeStringField("RecordHeader-key",
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
            jsonGenerator.writeStringField("ProducerRecord-key",
                    (producerRecord.key() == null) ? null : new String(producerRecord.key(), StandardCharsets.UTF_8));
            jsonGenerator.writeObjectField("ProducerRecord-value",
                    (producerRecord.value() == null) ? null : objectMapper.readTree(producerRecord.value()));
            final List<Header> headers = new ArrayList<>();
            producerRecord.headers().forEach(headers::add);
            final List<Header> headersSorted = headers.stream()
                    .sorted(Comparator.comparing(Header::key)).collect(Collectors.toList());
            jsonGenerator.writeObjectField("ProducerRecord-headers", headersSorted);
            jsonGenerator.writeEndObject();
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

    private TestRunner getTestRunner(final Collection<ProducerRecord<byte[], byte[]>> producedRecords) {
        final PublishKafka_2_6 processor = new PublishKafka_2_6() {
            @Override
            protected PublisherPool createPublisherPool(final ProcessContext context) {
                return getPublisherPool(producedRecords, context);
            }
        };
        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setValidateExpressionUsage(false);
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
        final String charsetName = context.getProperty("message-header-encoding").evaluateAttributeExpressions().getValue();
        final Charset charset = Charset.forName(charsetName);

        return new PublisherPool(
                Collections.emptyMap(),
                mock(ComponentLog.class),
                maxMessageSize,
                maxAckWaitMillis,
                useTransactions,
                transactionalIdSupplier,
                attributeNamePattern,
                charset,
                null,
                null) {
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
                null,
                null
        );
    }
}
