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
import org.apache.nifi.kafka.service.api.consumer.KafkaConsumerService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;

import static org.apache.nifi.kafka.processors.ConsumeKafka.CONNECTION_SERVICE;
import static org.apache.nifi.kafka.processors.ConsumeKafka.GROUP_ID;
import static org.apache.nifi.kafka.processors.ConsumeKafka.TOPICS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ConsumeKafkaTest {

    private static final String TEST_TOPIC_NAME = "NiFi-Kafka-Events";

    private static final int FIRST_PARTITION = 0;

    private static final String SERVICE_ID = KafkaConnectionService.class.getSimpleName();

    private static final String CONSUMER_GROUP_ID = ConsumeKafkaTest.class.getSimpleName();

    @Mock
    KafkaConnectionService kafkaConnectionService;

    @Mock
    KafkaConsumerService kafkaConsumerService;

    private TestRunner runner;

    private ConsumeKafka processor;

    @BeforeEach
    public void setRunner() {
        processor = new ConsumeKafka();
        runner = TestRunners.newTestRunner(processor);
    }

    @Test
    public void testProperties() throws InitializationException {
        runner.assertNotValid();

        setConnectionService();
        runner.assertNotValid();

        runner.setProperty(TOPICS, TEST_TOPIC_NAME);
        runner.setProperty(GROUP_ID, CONSUMER_GROUP_ID);
        runner.assertValid();
    }

    @Test
    public void testVerifySuccessful() throws InitializationException {
        final PartitionState firstPartitionState = new PartitionState(TEST_TOPIC_NAME, FIRST_PARTITION);
        final List<PartitionState> partitionStates = Collections.singletonList(firstPartitionState);
        when(kafkaConsumerService.getPartitionStates(any())).thenReturn(partitionStates);
        setConnectionService();
        when(kafkaConnectionService.getConsumerService(any())).thenReturn(kafkaConsumerService);

        runner.setProperty(TOPICS, TEST_TOPIC_NAME);
        runner.setProperty(GROUP_ID, CONSUMER_GROUP_ID);

        final List<ConfigVerificationResult> results = processor.verify(runner.getProcessContext(), runner.getLogger(), Collections.emptyMap());
        assertEquals(1, results.size());

        final ConfigVerificationResult firstResult = results.iterator().next();
        assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, firstResult.getOutcome());
        assertNotNull(firstResult.getExplanation());
    }

    @Test
    public void testVerifyFailed() throws InitializationException {
        when(kafkaConsumerService.getPartitionStates(any())).thenThrow(new IllegalStateException());
        when(kafkaConnectionService.getConsumerService(any())).thenReturn(kafkaConsumerService);
        setConnectionService();

        runner.setProperty(TOPICS, TEST_TOPIC_NAME);
        runner.setProperty(GROUP_ID, CONSUMER_GROUP_ID);

        final List<ConfigVerificationResult> results = processor.verify(runner.getProcessContext(), runner.getLogger(), Collections.emptyMap());
        assertEquals(1, results.size());

        final ConfigVerificationResult firstResult = results.iterator().next();
        assertEquals(ConfigVerificationResult.Outcome.FAILED, firstResult.getOutcome());
        assertNotNull(firstResult.getExplanation());
    }

    private void setConnectionService() throws InitializationException {
        when(kafkaConnectionService.getIdentifier()).thenReturn(SERVICE_ID);

        runner.addControllerService(SERVICE_ID, kafkaConnectionService);
        runner.enableControllerService(kafkaConnectionService);

        runner.setProperty(CONNECTION_SERVICE, SERVICE_ID);
    }
}
