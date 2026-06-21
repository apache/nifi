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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.clients.admin.ShareMemberDescription;
import org.apache.kafka.clients.admin.SharePartitionOffsetInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.kafka.processors.consumer.GroupType;
import org.apache.nifi.kafka.processors.consumer.ProcessingStrategy;
import org.apache.nifi.kafka.service.api.consumer.share.ShareAcknowledgementMode;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsumeKafkaShareGroupIT extends AbstractConsumeKafkaIT {

    private static final String RECORD_VALUE = "share-group-test-value";

    private TestRunner runner;

    @BeforeEach
    void setRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumeKafka.class);
        addKafkaConnectionService(runner);

        runner.setProperty(ConsumeKafka.CONNECTION_SERVICE, CONNECTION_SERVICE_ID);
        runner.setProperty(ConsumeKafka.GROUP_TYPE, GroupType.SHARE);
    }

    private static final long ASSIGNMENT_TIMEOUT_MS = 60_000L;

    private static final long DELIVERY_TIMEOUT_MS = 60_000L;

    @Test
    @Timeout(120)
    void testShareGroupConsumesProducedRecord() throws ExecutionException, InterruptedException, TimeoutException {
        final String topic = "share-group-topic-" + UUID.randomUUID();
        final String shareGroupId = "share-group-" + UUID.randomUUID();

        runner.setProperty(ConsumeKafka.GROUP_ID, shareGroupId);
        runner.setProperty(ConsumeKafka.TOPICS, topic);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.FLOW_FILE.getValue());

        // The share-group starting position is fixed at the partition's high watermark when the
        // partition is first assigned to the group (auto offset reset defaults to "latest"). Drive
        // empty poll iterations until the broker reports a non-empty assignment, then produce, so
        // the record's offset is not before the group's starting offset.
        runner.run(1, false, true);
        waitForShareGroupAssignment(shareGroupId, topic);

        produceOne(topic, null, null, RECORD_VALUE, Collections.emptyList());

        waitForFlowFile();

        runner.run(1, true, false);

        final Iterator<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).iterator();
        assertTrue(flowFiles.hasNext());

        final MockFlowFile flowFile = flowFiles.next();
        flowFile.assertContentEquals(RECORD_VALUE);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        flowFile.assertAttributeExists(KafkaFlowFileAttribute.KAFKA_PARTITION);
        flowFile.assertAttributeExists(KafkaFlowFileAttribute.KAFKA_OFFSET);
        flowFile.assertAttributeExists(KafkaFlowFileAttribute.KAFKA_TIMESTAMP);
    }

    private void waitForFlowFile() throws TimeoutException {
        final long deadline = System.currentTimeMillis() + DELIVERY_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            runner.run(1, false, false);
            if (!runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).isEmpty()) {
                return;
            }
        }
        throw new TimeoutException("Did not receive a FlowFile from the share group within %d ms".formatted(DELIVERY_TIMEOUT_MS));
    }

    /**
     * Drive {@link TestRunner#run} iterations against the share-group consumer until the broker
     * reports the share group is ready to deliver records for {@code topic}. Two conditions must
     * hold before producing the test record:
     * <ol>
     *   <li>The group coordinator has assigned the partition to a share-group member.</li>
     *   <li>The share-group state machine has recorded a starting offset for the partition,
     *   which only happens after the consumer's first ShareFetch lands on the leader.</li>
     * </ol>
     * Without the second check there is a race where the test produces before the consumer's
     * first fetch, the broker locks the share-group's starting offset to the post-produce
     * high-watermark, and the produced record sits below the starting offset and is never
     * delivered. The race is rare locally but more likely on slow CI runners.
     */
    private void waitForShareGroupAssignment(final String shareGroupId, final String topic) throws TimeoutException {
        final Properties adminProperties = new Properties();
        adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        try (Admin admin = Admin.create(adminProperties)) {
            final long deadline = System.currentTimeMillis() + ASSIGNMENT_TIMEOUT_MS;
            boolean assigned = false;
            while (System.currentTimeMillis() < deadline) {
                runner.run(1, false, false);
                if (!assigned) {
                    assigned = hasShareGroupAssignment(admin, shareGroupId, topic);
                }
                if (assigned && hasShareGroupStartingOffset(admin, shareGroupId, topic)) {
                    return;
                }
            }
        }
        throw new TimeoutException("Share group %s never became ready to deliver records for topic %s".formatted(shareGroupId, topic));
    }

    private boolean hasShareGroupAssignment(final Admin admin, final String shareGroupId, final String topic) {
        try {
            final Map<String, ShareGroupDescription> descriptions = admin.describeShareGroups(Collections.singletonList(shareGroupId)).all().get();
            final ShareGroupDescription description = descriptions.get(shareGroupId);
            if (description == null) {
                return false;
            }
            for (final ShareMemberDescription member : description.members()) {
                if (member.assignment().topicPartitions().stream().anyMatch(tp -> topic.equals(tp.topic()))) {
                    return true;
                }
            }
            return false;
        } catch (final Exception ignored) {
            return false;
        }
    }

    private boolean hasShareGroupStartingOffset(final Admin admin, final String shareGroupId, final String topic) {
        try {
            final Map<TopicPartition, SharePartitionOffsetInfo> offsets = admin
                    .listShareGroupOffsets(Collections.singletonMap(shareGroupId, new ListShareGroupOffsetsSpec()))
                    .partitionsToOffsetInfo(shareGroupId).get();
            return offsets != null && offsets.keySet().stream().anyMatch(tp -> topic.equals(tp.topic()));
        } catch (final Exception ignored) {
            return false;
        }
    }

    @Test
    @Timeout(120)
    void testShareGroupConsumesProducedRecordWithImplicitAcknowledgement() throws ExecutionException, InterruptedException, TimeoutException {
        final String topic = "share-group-implicit-topic-" + UUID.randomUUID();
        final String shareGroupId = "share-group-implicit-" + UUID.randomUUID();

        runner.setProperty(ConsumeKafka.GROUP_ID, shareGroupId);
        runner.setProperty(ConsumeKafka.TOPICS, topic);
        runner.setProperty(ConsumeKafka.ACKNOWLEDGEMENT_MODE, ShareAcknowledgementMode.IMPLICIT.getValue());
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.FLOW_FILE.getValue());

        runner.run(1, false, true);
        waitForShareGroupAssignment(shareGroupId, topic);

        produceOne(topic, null, null, RECORD_VALUE, Collections.emptyList());

        waitForFlowFile();
        runner.run(1, true, false);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).getFirst();
        flowFile.assertContentEquals(RECORD_VALUE);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
    }

    @Test
    @Timeout(60)
    void testShareGroupVerifySucceeds() throws InitializationException {
        final String topic = "share-group-verify-" + UUID.randomUUID();
        final String shareGroupId = "share-group-verify-" + UUID.randomUUID();

        runner.setProperty(ConsumeKafka.GROUP_ID, shareGroupId);
        runner.setProperty(ConsumeKafka.TOPICS, topic);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.FLOW_FILE.getValue());

        final ConsumeKafka processor = (ConsumeKafka) runner.getProcessor();
        final List<ConfigVerificationResult> results = processor.verify(runner.getProcessContext(), runner.getLogger(), Collections.emptyMap());
        assertEquals(1, results.size());

        final ConfigVerificationResult result = results.getFirst();
        assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, result.getOutcome());
        assertNotNull(result.getExplanation());
        assertTrue(result.getExplanation().contains(shareGroupId));
    }
}
