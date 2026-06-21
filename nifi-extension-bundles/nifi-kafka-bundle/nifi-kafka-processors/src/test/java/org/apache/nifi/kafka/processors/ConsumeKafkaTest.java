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
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.kafka.processors.consumer.GroupType;
import org.apache.nifi.kafka.processors.consumer.ProcessingStrategy;
import org.apache.nifi.kafka.service.api.KafkaConnectionService;
import org.apache.nifi.kafka.service.api.common.PartitionState;
import org.apache.nifi.kafka.service.api.consumer.KafkaConsumerService;
import org.apache.nifi.kafka.service.api.consumer.share.KafkaShareConsumerService;
import org.apache.nifi.kafka.service.api.consumer.share.ShareAcknowledgementMode;
import org.apache.nifi.kafka.service.api.consumer.share.ShareGroupContext;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static org.apache.nifi.kafka.processors.ConsumeKafka.ACKNOWLEDGEMENT_MODE;
import static org.apache.nifi.kafka.processors.ConsumeKafka.CONNECTION_SERVICE;
import static org.apache.nifi.kafka.processors.ConsumeKafka.GROUP_ID;
import static org.apache.nifi.kafka.processors.ConsumeKafka.GROUP_TYPE;
import static org.apache.nifi.kafka.processors.ConsumeKafka.PROCESSING_STRATEGY;
import static org.apache.nifi.kafka.processors.ConsumeKafka.TOPICS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
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

    @Mock
    KafkaShareConsumerService kafkaShareConsumerService;

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
    public void testShareGroupSelectionIsValidWithoutClassicProperties() throws InitializationException {
        setConnectionService();
        runner.setProperty(GROUP_TYPE, GroupType.SHARE);
        runner.setProperty(TOPICS, TEST_TOPIC_NAME);
        runner.setProperty(GROUP_ID, CONSUMER_GROUP_ID);

        // Auto Offset Reset, Topic Format, and Commit Offsets depend on GROUP_TYPE = CONSUMER and
        // are intentionally not required when SHARE is selected. Configuration must still be valid.
        runner.assertValid();
    }

    @Test
    public void testShareGroupVerifySuccessful() throws InitializationException {
        when(kafkaShareConsumerService.poll(any(Duration.class))).thenReturn(Collections.emptyList());
        setConnectionService();
        when(kafkaConnectionService.getShareConsumerService(any(ShareGroupContext.class))).thenReturn(kafkaShareConsumerService);

        runner.setProperty(GROUP_TYPE, GroupType.SHARE);
        runner.setProperty(TOPICS, TEST_TOPIC_NAME);
        runner.setProperty(GROUP_ID, CONSUMER_GROUP_ID);

        final List<ConfigVerificationResult> results = processor.verify(runner.getProcessContext(), runner.getLogger(), Collections.emptyMap());
        assertEquals(1, results.size());

        final ConfigVerificationResult result = results.getFirst();
        assertEquals(Outcome.SUCCESSFUL, result.getOutcome());
        assertNotNull(result.getExplanation());
        assertTrue(result.getExplanation().contains(CONSUMER_GROUP_ID));
    }

    @Test
    public void testShareGroupVerifySuccessfulSamplesAreReleased() throws InitializationException {
        final ByteRecord sample = new ByteRecord(TEST_TOPIC_NAME, FIRST_PARTITION, 0L, 0L, Collections.emptyList(), null, "value".getBytes(), 1);
        when(kafkaShareConsumerService.poll(any(Duration.class))).thenReturn(List.of(sample));
        setConnectionService();
        when(kafkaConnectionService.getShareConsumerService(any(ShareGroupContext.class))).thenReturn(kafkaShareConsumerService);

        runner.setProperty(GROUP_TYPE, GroupType.SHARE);
        runner.setProperty(TOPICS, TEST_TOPIC_NAME);
        runner.setProperty(GROUP_ID, CONSUMER_GROUP_ID);

        final List<ConfigVerificationResult> results = processor.verify(runner.getProcessContext(), runner.getLogger(), Collections.emptyMap());
        final ConfigVerificationResult result = results.getFirst();
        assertEquals(Outcome.SUCCESSFUL, result.getOutcome());

        verify(kafkaShareConsumerService).acknowledge(any(ByteRecord.class), any());
        verify(kafkaShareConsumerService).commit();
    }

    @Test
    public void testShareGroupVerifyUnsupportedConnectionService() throws InitializationException {
        setConnectionService();
        when(kafkaConnectionService.getShareConsumerService(any(ShareGroupContext.class)))
                .thenThrow(new UnsupportedOperationException("Share consumer not supported"));

        runner.setProperty(GROUP_TYPE, GroupType.SHARE);
        runner.setProperty(TOPICS, TEST_TOPIC_NAME);
        runner.setProperty(GROUP_ID, CONSUMER_GROUP_ID);

        final List<ConfigVerificationResult> results = processor.verify(runner.getProcessContext(), runner.getLogger(), Collections.emptyMap());
        assertEquals(1, results.size());
        assertEquals(Outcome.FAILED, results.getFirst().getOutcome());
    }

    @Test
    public void testShareGroupVerifyPassesShareGroupIdToConnectionService() throws InitializationException {
        when(kafkaShareConsumerService.poll(any(Duration.class))).thenReturn(Collections.emptyList());
        setConnectionService();
        when(kafkaConnectionService.getShareConsumerService(any(ShareGroupContext.class))).thenReturn(kafkaShareConsumerService);

        runner.setProperty(GROUP_TYPE, GroupType.SHARE);
        runner.setProperty(TOPICS, TEST_TOPIC_NAME);
        runner.setProperty(GROUP_ID, CONSUMER_GROUP_ID);

        processor.verify(runner.getProcessContext(), runner.getLogger(), Collections.emptyMap());

        final ArgumentCaptor<ShareGroupContext> captor = ArgumentCaptor.forClass(ShareGroupContext.class);
        verify(kafkaConnectionService).getShareConsumerService(captor.capture());
        final ShareGroupContext capturedContext = captor.getValue();
        assertEquals(CONSUMER_GROUP_ID, capturedContext.getGroupId());
        assertEquals(List.of(TEST_TOPIC_NAME), List.copyOf(capturedContext.getTopics()));
    }

    @Test
    public void testShareGroupAcknowledgementModeDefaultsToExplicit() throws InitializationException {
        setConnectionService();
        runner.setProperty(GROUP_TYPE, GroupType.SHARE);
        runner.setProperty(TOPICS, TEST_TOPIC_NAME);
        runner.setProperty(GROUP_ID, CONSUMER_GROUP_ID);

        runner.assertValid();
        assertEquals(ShareAcknowledgementMode.EXPLICIT.getValue(),
                runner.getProcessContext().getProperty(ACKNOWLEDGEMENT_MODE).getValue());
    }

    @Test
    public void testShareGroupOnTriggerCommitsAcknowledgements() throws InitializationException {
        setConnectionService();
        runner.setProperty(GROUP_TYPE, GroupType.SHARE);
        runner.setProperty(TOPICS, TEST_TOPIC_NAME);
        runner.setProperty(GROUP_ID, CONSUMER_GROUP_ID);
        runner.setProperty(PROCESSING_STRATEGY, ProcessingStrategy.FLOW_FILE.getValue());

        final String recordValue = "share-record";
        final ByteRecord record = new ByteRecord(TEST_TOPIC_NAME, FIRST_PARTITION, 7L, 0L, Collections.emptyList(), null, recordValue.getBytes(), 1);
        when(kafkaConnectionService.getShareConsumerService(any(ShareGroupContext.class))).thenReturn(kafkaShareConsumerService);
        when(kafkaShareConsumerService.poll(any(Duration.class))).thenReturn(List.of(record));

        runner.run(1, true, true);

        runner.assertTransferCount(ConsumeKafka.SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).getFirst();
        flowFile.assertContentEquals(recordValue);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, TEST_TOPIC_NAME);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, "7");

        verify(kafkaShareConsumerService).commit();
        verify(kafkaShareConsumerService, never()).rollback();
    }

    @Test
    public void testShareGroupOnTriggerNoRecordsRecyclesConsumer() throws InitializationException {
        setConnectionService();
        runner.setProperty(GROUP_TYPE, GroupType.SHARE);
        runner.setProperty(TOPICS, TEST_TOPIC_NAME);
        runner.setProperty(GROUP_ID, CONSUMER_GROUP_ID);
        runner.setProperty(PROCESSING_STRATEGY, ProcessingStrategy.FLOW_FILE.getValue());

        when(kafkaConnectionService.getShareConsumerService(any(ShareGroupContext.class))).thenReturn(kafkaShareConsumerService);
        when(kafkaShareConsumerService.poll(any(Duration.class))).thenReturn(Collections.emptyList());

        runner.run(1, false, true);
        runner.run(1, false, false);
        runner.run(1, true, false);

        runner.assertTransferCount(ConsumeKafka.SUCCESS, 0);
        // The same consumer must be reused across triggers when polls return no records.
        verify(kafkaConnectionService, times(1)).getShareConsumerService(any(ShareGroupContext.class));
        verify(kafkaShareConsumerService, never()).commit();
        verify(kafkaShareConsumerService, never()).rollback();
    }

    @Test
    public void testShareGroupOnTriggerPollExceptionClosesConsumerAndRollsBack() throws Exception {
        setConnectionService();
        runner.setProperty(GROUP_TYPE, GroupType.SHARE);
        runner.setProperty(TOPICS, TEST_TOPIC_NAME);
        runner.setProperty(GROUP_ID, CONSUMER_GROUP_ID);
        runner.setProperty(PROCESSING_STRATEGY, ProcessingStrategy.FLOW_FILE.getValue());

        when(kafkaConnectionService.getShareConsumerService(any(ShareGroupContext.class))).thenReturn(kafkaShareConsumerService);
        when(kafkaShareConsumerService.poll(any(Duration.class))).thenThrow(new RuntimeException("broker unavailable"));
        when(kafkaShareConsumerService.isClosed()).thenReturn(false);

        runner.run(1, true, true);

        runner.assertTransferCount(ConsumeKafka.SUCCESS, 0);
        verify(kafkaShareConsumerService).rollback();
        verify(kafkaShareConsumerService).close();
    }

    @Test
    public void testShareGroupExplicitModePassedToShareGroupContext() throws InitializationException {
        when(kafkaShareConsumerService.poll(any(Duration.class))).thenReturn(Collections.emptyList());
        setConnectionService();
        when(kafkaConnectionService.getShareConsumerService(any(ShareGroupContext.class))).thenReturn(kafkaShareConsumerService);

        runner.setProperty(GROUP_TYPE, GroupType.SHARE);
        runner.setProperty(TOPICS, TEST_TOPIC_NAME);
        runner.setProperty(GROUP_ID, CONSUMER_GROUP_ID);
        runner.setProperty(PROCESSING_STRATEGY, ProcessingStrategy.FLOW_FILE.getValue());

        runner.run(1, true, true);

        final ArgumentCaptor<ShareGroupContext> captor = ArgumentCaptor.forClass(ShareGroupContext.class);
        verify(kafkaConnectionService).getShareConsumerService(captor.capture());
        assertEquals(ShareAcknowledgementMode.EXPLICIT, captor.getValue().getAcknowledgementMode());
    }

    @Test
    public void testShareGroupImplicitModePassedToShareGroupContext() throws InitializationException {
        when(kafkaShareConsumerService.poll(any(Duration.class))).thenReturn(Collections.emptyList());
        setConnectionService();
        when(kafkaConnectionService.getShareConsumerService(any(ShareGroupContext.class))).thenReturn(kafkaShareConsumerService);

        runner.setProperty(GROUP_TYPE, GroupType.SHARE);
        runner.setProperty(TOPICS, TEST_TOPIC_NAME);
        runner.setProperty(GROUP_ID, CONSUMER_GROUP_ID);
        runner.setProperty(ACKNOWLEDGEMENT_MODE, ShareAcknowledgementMode.IMPLICIT.getValue());
        runner.setProperty(PROCESSING_STRATEGY, ProcessingStrategy.FLOW_FILE.getValue());

        runner.run(1, true, true);

        final ArgumentCaptor<ShareGroupContext> captor = ArgumentCaptor.forClass(ShareGroupContext.class);
        verify(kafkaConnectionService).getShareConsumerService(captor.capture());
        assertEquals(ShareAcknowledgementMode.IMPLICIT, captor.getValue().getAcknowledgementMode());
    }

    @Test
    public void testShareGroupVerificationAlwaysUsesExplicitMode() throws InitializationException {
        when(kafkaShareConsumerService.poll(any(Duration.class))).thenReturn(Collections.emptyList());
        setConnectionService();
        when(kafkaConnectionService.getShareConsumerService(any(ShareGroupContext.class))).thenReturn(kafkaShareConsumerService);

        runner.setProperty(GROUP_TYPE, GroupType.SHARE);
        runner.setProperty(TOPICS, TEST_TOPIC_NAME);
        runner.setProperty(GROUP_ID, CONSUMER_GROUP_ID);
        runner.setProperty(ACKNOWLEDGEMENT_MODE, ShareAcknowledgementMode.IMPLICIT.getValue());

        processor.verify(runner.getProcessContext(), runner.getLogger(), Collections.emptyMap());

        final ArgumentCaptor<ShareGroupContext> captor = ArgumentCaptor.forClass(ShareGroupContext.class);
        verify(kafkaConnectionService).getShareConsumerService(captor.capture());
        // Verification must use EXPLICIT regardless of the user-selected mode so sampled records
        // can be released back to the share group.
        assertEquals(ShareAcknowledgementMode.EXPLICIT, captor.getValue().getAcknowledgementMode());
    }

    private void setConnectionService() throws InitializationException {
        when(kafkaConnectionService.getIdentifier()).thenReturn(SERVICE_ID);

        runner.addControllerService(SERVICE_ID, kafkaConnectionService);
        runner.enableControllerService(kafkaConnectionService);

        runner.setProperty(CONNECTION_SERVICE, SERVICE_ID);
    }
}
