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
package org.apache.nifi.processors.azure.eventhub.checkpoint;

import com.azure.messaging.eventhubs.models.Checkpoint;
import com.azure.messaging.eventhubs.models.PartitionOwnership;
import org.junit.jupiter.api.BeforeEach;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

abstract class AbstractCheckpointStoreTest {

    static final String EVENT_HUB_NAMESPACE = "my-event-hub-namespace";
    static final String EVENT_HUB_NAME = "my-event-hub-name";
    static final String CONSUMER_GROUP = "my-consumer-group";

    static final String PARTITION_ID_1 = "1";
    static final String PARTITION_ID_2 = "2";

    static final String CLIENT_ID_1 = "client-id-1";
    static final String CLIENT_ID_2 = "client-id-2";

    static final Long LAST_MODIFIED_TIME = 1234567890L;
    static final String ETAG = "my-etag";

    static final Long OFFSET = 10L;
    static final Long SEQUENCE_NUMBER = 1L;

    PartitionOwnership partitionOwnership1;
    PartitionOwnership partitionOwnership2;

    Checkpoint checkpoint1;
    Checkpoint checkpoint2;

    @BeforeEach
    void initTestData() {
        partitionOwnership1 = createPartitionOwnership(PARTITION_ID_1, CLIENT_ID_1);
        partitionOwnership2 = createPartitionOwnership(PARTITION_ID_2, CLIENT_ID_2);

        checkpoint1 = createCheckpoint(PARTITION_ID_1, OFFSET, SEQUENCE_NUMBER);
        checkpoint2 = createCheckpoint(PARTITION_ID_2, OFFSET, SEQUENCE_NUMBER);
    }

    PartitionOwnership createPartitionOwnership(String partitionId, String ownerId) {
        return createPartitionOwnership(
                EVENT_HUB_NAMESPACE,
                EVENT_HUB_NAME,
                CONSUMER_GROUP,
                partitionId,
                ownerId
        );
    }

    PartitionOwnership createPartitionOwnership(
            String fullyQualifiedNamespace,
            String eventHubName,
            String consumerGroup,
            String partitionId,
            String ownerId) {
        return new TestablePartitionOwnership()
                .setFullyQualifiedNamespace(fullyQualifiedNamespace)
                .setEventHubName(eventHubName)
                .setConsumerGroup(consumerGroup)
                .setPartitionId(partitionId)
                .setOwnerId(ownerId)
                .setLastModifiedTime(null)
                .setETag(null);
    }

    Checkpoint createCheckpoint(String partitionId, Long offset, Long sequenceNumber) {
        return createCheckpoint(
                EVENT_HUB_NAMESPACE,
                EVENT_HUB_NAME,
                CONSUMER_GROUP,
                partitionId,
                offset,
                sequenceNumber
        );
    }

    Checkpoint createCheckpoint(
            String fullyQualifiedNamespace,
            String eventHubName,
            String consumerGroup,
            String partitionId,
            Long offset,
            Long sequenceNumber) {
        return new TestableCheckpoint()
                .setFullyQualifiedNamespace(fullyQualifiedNamespace)
                .setEventHubName(eventHubName)
                .setConsumerGroup(consumerGroup)
                .setPartitionId(partitionId)
                .setOffset(offset)
                .setSequenceNumber(sequenceNumber);
    }

    PartitionOwnership copy(PartitionOwnership original) {
        return convertToTestable(original);
    }

    Checkpoint copy(Checkpoint original) {
        return convertToTestable(original);
    }

    PartitionOwnership convertToTestable(PartitionOwnership original) {
        return new TestablePartitionOwnership()
                .setFullyQualifiedNamespace(original.getFullyQualifiedNamespace())
                .setEventHubName(original.getEventHubName())
                .setConsumerGroup(original.getConsumerGroup())
                .setPartitionId(original.getPartitionId())
                .setOwnerId(original.getOwnerId())
                .setLastModifiedTime(original.getLastModifiedTime())
                .setETag(original.getETag());
    }

    Checkpoint convertToTestable(Checkpoint original) {
        return new TestableCheckpoint()
                .setFullyQualifiedNamespace(original.getFullyQualifiedNamespace())
                .setEventHubName(original.getEventHubName())
                .setConsumerGroup(original.getConsumerGroup())
                .setPartitionId(original.getPartitionId())
                .setOffset(original.getOffset())
                .setSequenceNumber(original.getSequenceNumber());
    }

    List<PartitionOwnership> convertToTestablePartitionOwnerships(List<PartitionOwnership> partitionOwnerships) {
        return partitionOwnerships.stream()
                .map(this::convertToTestable)
                .collect(Collectors.toList());
    }

    List<Checkpoint> convertToTestableCheckpoints(List<Checkpoint> checkpoints) {
        return checkpoints.stream()
                .map(this::convertToTestable)
                .collect(Collectors.toList());
    }

    static class TestablePartitionOwnership extends PartitionOwnership {

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestablePartitionOwnership that = (TestablePartitionOwnership) o;
            return Objects.equals(getFullyQualifiedNamespace(), that.getFullyQualifiedNamespace())
                    && Objects.equals(getEventHubName(), that.getEventHubName())
                    && Objects.equals(getConsumerGroup(), that.getConsumerGroup())
                    && Objects.equals(getPartitionId(), that.getPartitionId())
                    && Objects.equals(getOwnerId(), that.getOwnerId())
                    && Objects.equals(getLastModifiedTime(), that.getLastModifiedTime())
                    && Objects.equals(getETag(), that.getETag());
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    getFullyQualifiedNamespace(),
                    getEventHubName(),
                    getConsumerGroup(),
                    getPartitionId(),
                    getOwnerId(),
                    getLastModifiedTime(),
                    getETag()
            );
        }
    }

    static class TestableCheckpoint extends Checkpoint {

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestableCheckpoint that = (TestableCheckpoint) o;
            return Objects.equals(getFullyQualifiedNamespace(), that.getFullyQualifiedNamespace())
                    && Objects.equals(getEventHubName(), that.getEventHubName())
                    && Objects.equals(getConsumerGroup(), that.getConsumerGroup())
                    && Objects.equals(getPartitionId(), that.getPartitionId())
                    && Objects.equals(getOffset(), that.getOffset())
                    && Objects.equals(getSequenceNumber(), that.getSequenceNumber());
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    getFullyQualifiedNamespace(),
                    getEventHubName(),
                    getConsumerGroup(),
                    getPartitionId(),
                    getOffset(),
                    getSequenceNumber()
            );
        }
    }

}
