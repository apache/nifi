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

import org.apache.kafka.common.header.Header;
import org.apache.nifi.kafka.processors.consumer.ProcessingStrategy;
import org.apache.nifi.kafka.service.api.consumer.AutoOffsetReset;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsumeKafkaKeyAttributeIT extends AbstractConsumeKafkaIT {

    private static final String RECORD_KEY = ConsumeKafkaKeyAttributeIT.class.getSimpleName();
    private static final String RECORD_VALUE = ConsumeKafkaKeyAttributeIT.class.getName();

    private TestRunner runner;

    @BeforeEach
    void setRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumeKafka.class);
        addKafkaConnectionService(runner);

        runner.setProperty(ConsumeKafka.CONNECTION_SERVICE, CONNECTION_SERVICE_ID);
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, AutoOffsetReset.EARLIEST.getValue());
    }

    /**
     * Given a processor property and an input, verify the output after the Kafka round-trip.
     */
    public static Stream<Arguments> permutations() {
        return Stream.of(
                Arguments.arguments(KeyEncoding.UTF8, RECORD_KEY, RECORD_KEY),
                Arguments.arguments(KeyEncoding.HEX, RECORD_KEY, HexFormat.of().formatHex(RECORD_KEY.getBytes(StandardCharsets.UTF_8))),
                Arguments.arguments(KeyEncoding.DO_NOT_ADD, RECORD_KEY, null),
                Arguments.arguments(KeyEncoding.UTF8, null, null)
        );
    }

    @ParameterizedTest
    @MethodSource("permutations")
    void testKeyAttributeEncoding(final KeyEncoding keyEncoding, final String kafkaMessageKey, final String expectedValue)
            throws InterruptedException, ExecutionException {
        final String topic = UUID.randomUUID().toString();
        final String groupId = topic.substring(0, topic.indexOf("-"));
        runner.setProperty(ConsumeKafka.GROUP_ID, groupId);
        runner.setProperty(ConsumeKafka.TOPICS, topic);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.FLOW_FILE.getValue());
        runner.setProperty(ConsumeKafka.KEY_ATTRIBUTE_ENCODING, keyEncoding);

        runner.run(1, false, true);
        final List<Header> headers = Collections.emptyList();
        produceOne(topic, 0, kafkaMessageKey, RECORD_VALUE, headers);
        final long pollUntil = System.currentTimeMillis() + DURATION_POLL.toMillis();
        while ((System.currentTimeMillis() < pollUntil) && (runner.getFlowFilesForRelationship("success").isEmpty())) {
            runner.run(1, false, false);
        }
        runner.run(1, true, false);

        final Iterator<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).iterator();
        assertTrue(flowFiles.hasNext());
        final MockFlowFile flowFile = flowFiles.next();
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_KEY, expectedValue);
    }
}
