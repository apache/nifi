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

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.kafka.service.api.KafkaConnectionService;
import org.apache.nifi.kafka.service.api.common.PartitionState;
import org.apache.nifi.kafka.service.api.producer.FlowFileResult;
import org.apache.nifi.kafka.service.api.producer.KafkaProducerService;
import org.apache.nifi.kafka.service.api.producer.PublishContext;
import org.apache.nifi.kafka.service.api.producer.RecordSummary;
import org.apache.nifi.kafka.service.api.record.KafkaRecord;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.kafka.processors.PublishKafka.CONNECTION_SERVICE;
import static org.apache.nifi.kafka.processors.PublishKafka.TOPIC_NAME;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PublishKafkaTest {

    private static final String TEST_TOPIC_NAME = "NiFi-Kafka-Events";

    private static final int FIRST_PARTITION = 0;

    private static final String DYNAMIC_PROPERTY_KEY_PUBLISH = "delivery.timeout.ms";
    private static final String DYNAMIC_PROPERTY_VALUE_PUBLISH = "60000";
    private static final String DYNAMIC_PROPERTY_KEY_CONSUME = "fetch.max.wait.ms";
    private static final String DYNAMIC_PROPERTY_VALUE_CONSUME = "1000";

    private static final String SERVICE_ID = KafkaConnectionService.class.getSimpleName();

    @Mock
    KafkaConnectionService kafkaConnectionService;

    @Mock
    KafkaProducerService kafkaProducerService;

    private TestRunner runner;

    private PublishKafka processor;

    private final List<KafkaRecord> sentRecords = new ArrayList<>();

    private final Map<PublishContext, Long> sentCounts = new LinkedHashMap<>();

    @BeforeEach
    public void setRunner() {
        processor = new PublishKafka();
        runner = TestRunners.newTestRunner(processor);
    }

    @Test
    public void testProperties() throws InitializationException {
        runner.assertNotValid();

        setConnectionService();
        runner.assertNotValid();

        runner.setProperty(TOPIC_NAME, TEST_TOPIC_NAME);
        runner.assertValid();
    }

    @Test
    public void testVerifySuccessful() throws InitializationException {
        final PartitionState firstPartitionState = new PartitionState(TEST_TOPIC_NAME, FIRST_PARTITION);
        final List<PartitionState> partitionStates = Collections.singletonList(firstPartitionState);
        when(kafkaProducerService.getPartitionStates(eq(TEST_TOPIC_NAME))).thenReturn(partitionStates);
        setConnectionService();
        when(kafkaConnectionService.getProducerService(any())).thenReturn(kafkaProducerService);

        runner.setProperty(TOPIC_NAME, TEST_TOPIC_NAME);

        final List<ConfigVerificationResult> results = processor.verify(runner.getProcessContext(), runner.getLogger(), Collections.emptyMap());
        assertEquals(1, results.size());

        final ConfigVerificationResult firstResult = results.iterator().next();
        assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, firstResult.getOutcome());
        assertNotNull(firstResult.getExplanation());
    }

    @Test
    public void testVerifyFailed() throws InitializationException {
        when(kafkaProducerService.getPartitionStates(eq(TEST_TOPIC_NAME))).thenThrow(new IllegalStateException());
        when(kafkaConnectionService.getProducerService(any())).thenReturn(kafkaProducerService);
        setConnectionService();

        runner.setProperty(TOPIC_NAME, TEST_TOPIC_NAME);

        final List<ConfigVerificationResult> results = processor.verify(runner.getProcessContext(), runner.getLogger(), Collections.emptyMap());
        assertEquals(1, results.size());

        final ConfigVerificationResult firstResult = results.iterator().next();
        assertEquals(ConfigVerificationResult.Outcome.FAILED, firstResult.getOutcome());
        assertNotNull(firstResult.getExplanation());
    }

    @Test
    public void testDynamicProperties() throws InitializationException {
        when(kafkaConnectionService.getIdentifier()).thenReturn(SERVICE_ID);
        runner.addControllerService(SERVICE_ID, kafkaConnectionService);
        runner.setProperty(kafkaConnectionService, DYNAMIC_PROPERTY_KEY_PUBLISH, DYNAMIC_PROPERTY_VALUE_PUBLISH);
        runner.setProperty(kafkaConnectionService, DYNAMIC_PROPERTY_KEY_CONSUME, DYNAMIC_PROPERTY_VALUE_CONSUME);
        runner.enableControllerService(kafkaConnectionService);
    }

    @Test
    public void testPublishHexEncodedKey() throws InitializationException {
        setProducerService();
        runner.setProperty(PublishKafka.KEY_ATTRIBUTE_ENCODING, KeyEncoding.HEX.getValue());

        runner.enqueue("value".getBytes(StandardCharsets.UTF_8), Map.of(KafkaFlowFileAttribute.KAFKA_KEY, "0A0b0C"));
        runner.run();

        runner.assertAllFlowFilesTransferred(PublishKafka.REL_SUCCESS, 1);
        assertEquals(1, sentRecords.size());
        assertArrayEquals(new byte[]{0x0a, 0x0b, 0x0c}, sentRecords.getFirst().getKey());
    }

    @Test
    public void testPublishUtf8Key() throws InitializationException {
        setProducerService();

        runner.enqueue("value".getBytes(StandardCharsets.UTF_8), Map.of(KafkaFlowFileAttribute.KAFKA_KEY, "key-1"));
        runner.run();

        runner.assertAllFlowFilesTransferred(PublishKafka.REL_SUCCESS, 1);
        assertEquals(1, sentRecords.size());
        assertArrayEquals("key-1".getBytes(StandardCharsets.UTF_8), sentRecords.getFirst().getKey());
    }

    @Test
    public void testPublishInvalidHexEncodedKey() throws InitializationException {
        setProducerService();
        runner.setProperty(PublishKafka.KEY_ATTRIBUTE_ENCODING, KeyEncoding.HEX.getValue());

        runner.enqueue("value".getBytes(StandardCharsets.UTF_8), Map.of(KafkaFlowFileAttribute.KAFKA_KEY, "0a0"));
        runner.run();

        runner.assertAllFlowFilesTransferred(PublishKafka.REL_FAILURE, 1);
        assertTrue(sentRecords.isEmpty());
    }

    private void setConnectionService() throws InitializationException {
        when(kafkaConnectionService.getIdentifier()).thenReturn(SERVICE_ID);

        runner.addControllerService(SERVICE_ID, kafkaConnectionService);
        runner.enableControllerService(kafkaConnectionService);

        runner.setProperty(CONNECTION_SERVICE, SERVICE_ID);
    }

    private void setProducerService() throws InitializationException {
        setConnectionService();
        when(kafkaConnectionService.getProducerService(any())).thenReturn(kafkaProducerService);
        runner.setProperty(TOPIC_NAME, TEST_TOPIC_NAME);

        doAnswer(invocation -> {
            final Iterator<KafkaRecord> records = invocation.getArgument(0);
            final PublishContext publishContext = invocation.getArgument(1);
            long sentCount = 0;
            while (records.hasNext()) {
                sentRecords.add(records.next());
                sentCount++;
            }
            sentCounts.merge(publishContext, sentCount, Long::sum);
            return null;
        }).when(kafkaProducerService).send(any(), any());

        when(kafkaProducerService.complete()).thenAnswer(invocation -> {
            final RecordSummary recordSummary = new RecordSummary();
            for (final Map.Entry<PublishContext, Long> entry : sentCounts.entrySet()) {
                final PublishContext publishContext = entry.getKey();
                final long sentCount = entry.getValue();
                final Exception exception = publishContext.getException();
                final List<Exception> exceptions = exception == null ? List.of() : List.of(exception);
                recordSummary.getFlowFileResults().add(new FlowFileResult(
                        publishContext.getFlowFile(), sentCount, Map.of(TEST_TOPIC_NAME, sentCount), List.of(), exceptions));
            }
            return recordSummary;
        });
    }
}
