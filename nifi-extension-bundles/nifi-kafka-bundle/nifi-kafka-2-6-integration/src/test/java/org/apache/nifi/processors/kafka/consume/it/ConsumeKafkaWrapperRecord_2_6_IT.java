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
package org.apache.nifi.processors.kafka.consume.it;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.nifi.kafka.shared.property.KeyFormat;
import org.apache.nifi.kafka.shared.property.OutputStrategy;
import org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaRecord_2_6;
import org.apache.nifi.processors.kafka.pubsub.KafkaProcessor;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsumeKafkaWrapperRecord_2_6_IT extends ConsumeKafka_2_6_BaseIT {
    private static final String TEST_RESOURCE = "org/apache/nifi/processors/kafka/publish/ff.json";
    private static final int TEST_RECORD_COUNT = 3;
    private static final String MESSAGE_KEY = "{\"id\": 0,\"name\": \"K\"}";

    static abstract class Verifier {
        abstract void verify(final JsonNode jsonNode);
    }

    static class RecordVerifier extends Verifier {
        void verify(final JsonNode jsonNode) {
            final ObjectNode key = assertInstanceOf(ObjectNode.class, jsonNode);
            assertEquals(2, key.size());
            assertEquals(0, key.get("id").asInt());
            assertEquals("K", key.get("name").asText());
        }
    }

    static class StringVerifier extends Verifier {
        void verify(final JsonNode jsonNode) {
            final TextNode key = assertInstanceOf(TextNode.class, jsonNode);
            assertEquals(MESSAGE_KEY, key.asText());
        }
    }

    static class ByteArrayVerifier extends Verifier {
        void verify(final JsonNode jsonNode) {
            final ArrayNode key = assertInstanceOf(ArrayNode.class, jsonNode);
            assertEquals(MESSAGE_KEY.length(), key.size());
            for (int i = 0; (i < MESSAGE_KEY.length()); ++i) {
                assertEquals(MESSAGE_KEY.charAt(i), key.get(i).asInt());
            }
        }
    }

    public static Stream<Arguments> permutationsKeyFormat() {
        return Stream.of(
                Arguments.arguments(KeyFormat.RECORD, new RecordVerifier()),
                Arguments.arguments(KeyFormat.STRING, new StringVerifier()),
                Arguments.arguments(KeyFormat.BYTE_ARRAY, new ByteArrayVerifier())
        );
    }

    @ParameterizedTest
    @MethodSource("permutationsKeyFormat")
    void testWrapperRecord(final KeyFormat keyFormat, final Verifier verifier)
            throws InterruptedException, ExecutionException, IOException, InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(ConsumeKafkaRecord_2_6.class);
        runner.setValidateExpressionUsage(false);

        final String topic = UUID.randomUUID().toString();
        final String groupId = topic.substring(0, topic.indexOf("-"));

        final URI uri = URI.create(kafka.getBootstrapServers());
        addRecordReaderService(runner);
        addRecordWriterService(runner);
        addKeyRecordReaderService(runner);
        runner.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, String.format("%s:%s", uri.getHost(), uri.getPort()));
        runner.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        runner.setProperty(KafkaProcessor.OUTPUT_STRATEGY, OutputStrategy.USE_WRAPPER);
        runner.setProperty(KafkaProcessor.KEY_FORMAT, keyFormat);
        runner.setProperty("topic", topic);
        runner.setProperty("group.id", groupId);

        runner.run(1, false, true);
        final String message = new String(IOUtils.toByteArray(Objects.requireNonNull(
                getClass().getClassLoader().getResource(TEST_RESOURCE))), StandardCharsets.UTF_8);
        final List<Header> headersPublish = Collections.singletonList(
                new RecordHeader("header1", "value1".getBytes(StandardCharsets.UTF_8)));
        produceOne(topic, 0, MESSAGE_KEY, message, headersPublish);
        final long pollUntil = System.currentTimeMillis() + DURATION_POLL.toMillis();
        while (System.currentTimeMillis() < pollUntil) {
            runner.run(1, false, false);
        }
        runner.run(1, true, false);

        runner.assertAllFlowFilesTransferred(KafkaProcessor.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(KafkaProcessor.REL_SUCCESS);
        assertEquals(1, flowFiles.size());
        final MockFlowFile flowFile = flowFiles.getFirst();
        runner.getLogger().trace(flowFile.getContent());
        flowFile.assertAttributeEquals("record.count", Long.toString(TEST_RECORD_COUNT));

        final ObjectMapper objectMapper = new ObjectMapper();
        final JsonNode jsonNodeTree = objectMapper.readTree(flowFile.getContent());
        final ArrayNode arrayNode = assertInstanceOf(ArrayNode.class, jsonNodeTree);

        final Iterator<JsonNode> elements = arrayNode.elements();
        assertEquals(TEST_RECORD_COUNT, arrayNode.size());
        while (elements.hasNext()) {
            final ObjectNode wrapper = assertInstanceOf(ObjectNode.class, elements.next());

            verifier.verify(wrapper.get("key"));

            final ObjectNode value = assertInstanceOf(ObjectNode.class, wrapper.get("value"));
            assertTrue(Arrays.asList(1, 2, 3).contains(value.get("id").asInt()));
            assertTrue(Arrays.asList("A", "B", "C").contains(value.get("name").asText()));

            final ObjectNode headers = assertInstanceOf(ObjectNode.class, wrapper.get("headers"));
            assertEquals(1, headers.size());
            assertEquals("value1", headers.get("header1").asText());

            final ObjectNode metadata = assertInstanceOf(ObjectNode.class, wrapper.get("metadata"));
            assertEquals(topic, metadata.get("topic").asText());
            assertEquals(0, metadata.get("partition").asInt());
            assertEquals(0, metadata.get("offset").asInt());
            assertTrue(metadata.get("timestamp").isIntegralNumber());

            final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
            assertEquals(1, provenanceEvents.size());
            final ProvenanceEventRecord provenanceEvent = provenanceEvents.getFirst();
            assertEquals(ProvenanceEventType.RECEIVE, provenanceEvent.getEventType());
        }
    }
}
