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

import org.apache.nifi.components.Backlog;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
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

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nifi.kafka.processors.ConsumeKafka.CONNECTION_SERVICE;
import static org.apache.nifi.kafka.processors.ConsumeKafka.GROUP_ID;
import static org.apache.nifi.kafka.processors.ConsumeKafka.TOPICS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ConsumeKafkaTest {

    private static final String TEST_TOPIC_NAME = "NiFi-Kafka-Events";

    private static final int FIRST_PARTITION = 0;

    private static final String DYNAMIC_PROPERTY_KEY_PUBLISH = "delivery.timeout.ms";
    private static final String DYNAMIC_PROPERTY_VALUE_PUBLISH = "60000";
    private static final String DYNAMIC_PROPERTY_KEY_CONSUME = "fetch.max.wait.ms";
    private static final String DYNAMIC_PROPERTY_VALUE_CONSUME = "1000";

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
        when(kafkaConsumerService.getPartitionStates()).thenReturn(partitionStates);
        setConnectionService();
        when(kafkaConnectionService.getConsumerService(any())).thenReturn(kafkaConsumerService);

        runner.setProperty(TOPICS, TEST_TOPIC_NAME);
        runner.setProperty(GROUP_ID, CONSUMER_GROUP_ID);

        final List<ConfigVerificationResult> results = processor.verify(runner.getProcessContext(), runner.getLogger(), Collections.emptyMap());
        final List<ConfigVerificationResult> successResults = results.stream()
            .filter(result -> result.getOutcome() == ConfigVerificationResult.Outcome.SUCCESSFUL)
            .toList();
        assertEquals(1, successResults.size());

        final boolean anyFailures = results.stream().anyMatch(result -> result.getOutcome() == Outcome.FAILED);
        assertFalse(anyFailures, "At least one verification result was a failure: " + results);

        final ConfigVerificationResult firstResult = successResults.getFirst();
        assertNotNull(firstResult.getExplanation());
    }

    @Test
    public void testVerifyFailed() throws InitializationException {
        when(kafkaConsumerService.getPartitionStates()).thenThrow(new IllegalStateException());
        when(kafkaConnectionService.getConsumerService(any())).thenReturn(kafkaConsumerService);
        setConnectionService();

        runner.setProperty(TOPICS, TEST_TOPIC_NAME);
        runner.setProperty(GROUP_ID, CONSUMER_GROUP_ID);

        final List<ConfigVerificationResult> results = processor.verify(runner.getProcessContext(), runner.getLogger(), Collections.emptyMap());
        assertEquals(2, results.size());

        final List<ConfigVerificationResult> failedResults = results.stream()
                .filter(result -> result.getOutcome() == ConfigVerificationResult.Outcome.FAILED)
                .toList();
        assertEquals(1, failedResults.size());

        final ConfigVerificationResult firstResult = failedResults.getFirst();
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
    public void testGetBacklogQueriesAdminClientRegardlessOfPoolState() throws Exception {
        // getBacklog always queries committed/end offsets via the connection service's Admin
        // client. The broker is the source of truth for the consumer group's committed offsets
        // across every assigned partition, so the returned lag covers the entire group rather
        // than only the partitions owned by whichever consumer instances happen to be pooled or
        // checked out at this moment. Per-partition undercounts cannot occur because the consumer
        // pool is not consulted on this path.
        setConnectionService();
        runner.setProperty(TOPICS, TEST_TOPIC_NAME);
        runner.setProperty(GROUP_ID, CONSUMER_GROUP_ID);
        when(kafkaConnectionService.getCommittedOffsetLag(any())).thenReturn(42L);

        // Reflectively place one consumer in the pool and mark one additional consumer as checked
        // out by a running onTrigger task. The Admin-client answer must be returned even when the
        // pool contains a consumer that could be inspected directly.
        final Field consumerServicesField = ConsumeKafka.class.getDeclaredField("consumerServices");
        consumerServicesField.setAccessible(true);
        @SuppressWarnings("unchecked")
        final Queue<KafkaConsumerService> consumerServices = (Queue<KafkaConsumerService>) consumerServicesField.get(processor);
        consumerServices.offer(kafkaConsumerService);

        final Field activeConsumerCountField = ConsumeKafka.class.getDeclaredField("activeConsumerCount");
        activeConsumerCountField.setAccessible(true);
        final AtomicInteger activeConsumerCount = (AtomicInteger) activeConsumerCountField.get(processor);
        activeConsumerCount.set(1);

        final Optional<Backlog> backlog = processor.getBacklog(runner.getProcessContext());

        assertTrue(backlog.isPresent());
        assertEquals(OptionalLong.of(42L), backlog.get().getRecordCount());
        assertEquals(Backlog.Precision.EXACT, backlog.get().getPrecision());
        assertTrue(backlog.get().getLastCaughtUp().isEmpty());
        // The pooled consumer must not be interrogated for partition state or lag; that path is
        // bypassed entirely by the Admin-client design.
        verify(kafkaConsumerService, never()).getPartitionStates();
        verify(kafkaConsumerService, never()).currentLag(any());
    }

    @Test
    public void testGetBacklogPopulatesZeroRecordsOnCaughtUp() throws Exception {
        // A caught-up consumer group has an exactly-known lag of zero. The returned Backlog must
        // populate the records dimension with that zero alongside lastCaughtUp, so that clients
        // can render "0 records" rather than treating the record count as unknown.
        setConnectionService();
        runner.setProperty(TOPICS, TEST_TOPIC_NAME);
        runner.setProperty(GROUP_ID, CONSUMER_GROUP_ID);
        when(kafkaConnectionService.getCommittedOffsetLag(any())).thenReturn(0L);

        final Optional<Backlog> backlog = processor.getBacklog(runner.getProcessContext());

        assertTrue(backlog.isPresent());
        assertEquals(OptionalLong.of(0L), backlog.get().getRecordCount());
        assertEquals(Backlog.Precision.EXACT, backlog.get().getPrecision());
        assertTrue(backlog.get().getLastCaughtUp().isPresent());
    }

    @Test
    public void testOnStoppedPreservesLastCaughtUp() throws Exception {
        setConnectionService();
        runner.setProperty(TOPICS, TEST_TOPIC_NAME);
        runner.setProperty(GROUP_ID, CONSUMER_GROUP_ID);
        when(kafkaConnectionService.getCommittedOffsetLag(any())).thenReturn(0L).thenReturn(5L);

        final Optional<Backlog> initialBacklog = processor.getBacklog(runner.getProcessContext());
        assertTrue(initialBacklog.isPresent());
        final Instant rememberedFirst = initialBacklog.get().getLastCaughtUp().orElse(null);
        assertNotNull(rememberedFirst);

        processor.onStopped();

        final Optional<Backlog> afterStop = processor.getBacklog(runner.getProcessContext());
        assertTrue(afterStop.isPresent());
        // After the processor is stopped while non-zero lag exists, the remembered caught-up
        // timestamp from the prior caught-up observation must still be surfaced — onStopped() must
        // not clear lastCaughtUpTimestamp.
        assertEquals(Optional.of(rememberedFirst), afterStop.get().getLastCaughtUp());
        assertEquals(OptionalLong.of(5L), afterStop.get().getRecordCount());
    }

    private void setConnectionService() throws InitializationException {
        when(kafkaConnectionService.getIdentifier()).thenReturn(SERVICE_ID);

        runner.addControllerService(SERVICE_ID, kafkaConnectionService);
        runner.enableControllerService(kafkaConnectionService);

        runner.setProperty(CONNECTION_SERVICE, SERVICE_ID);
    }
}
