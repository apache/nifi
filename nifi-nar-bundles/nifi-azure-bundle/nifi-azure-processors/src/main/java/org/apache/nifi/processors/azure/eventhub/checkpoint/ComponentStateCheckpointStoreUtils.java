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
import com.azure.messaging.eventhubs.models.PartitionContext;
import com.azure.messaging.eventhubs.models.PartitionOwnership;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.nifi.processors.azure.eventhub.checkpoint.CheckpointStoreKeyPrefix.CHECKPOINT;
import static org.apache.nifi.processors.azure.eventhub.checkpoint.CheckpointStoreKeyPrefix.OWNERSHIP;

final class ComponentStateCheckpointStoreUtils {

    private ComponentStateCheckpointStoreUtils() {
    }

    static PartitionOwnership convertOwnership(String key, String value) {
        PartitionContext context = convertPartitionContext(key);

        final String[] parts = value.split("/", 3);
        if (parts.length != 3) {
            throw new ProcessException(String.format("Invalid ownership value: %s", value));
        }

        return new PartitionOwnership()
                .setFullyQualifiedNamespace(context.getFullyQualifiedNamespace())
                .setEventHubName(context.getEventHubName())
                .setConsumerGroup(context.getConsumerGroup())
                .setPartitionId(context.getPartitionId())
                .setOwnerId(parts[0])
                .setLastModifiedTime(Long.parseLong(parts[1]))
                .setETag(parts[2]);
    }

    static Checkpoint convertCheckpoint(String key, String value) {
        PartitionContext context = convertPartitionContext(key);

        final String[] parts = value.split("/", 2);
        if (parts.length != 2) {
            throw new ProcessException(String.format("Invalid checkpoint value: %s", value));
        }

        return new Checkpoint()
                .setFullyQualifiedNamespace(context.getFullyQualifiedNamespace())
                .setEventHubName(context.getEventHubName())
                .setConsumerGroup(context.getConsumerGroup())
                .setPartitionId(context.getPartitionId())
                .setOffset(StringUtils.isNotEmpty(parts[0]) ? Long.parseLong(parts[0]) : null)
                .setSequenceNumber(StringUtils.isNotEmpty(parts[1]) ? Long.parseLong(parts[1]): null);
    }

    static PartitionContext convertPartitionContext(String key) {
        final String[] parts = key.split("/", 5);
        if (parts.length != 5) {
            throw new ProcessException(String.format("Invalid entry key: %s", key));
        }

        final String fullyQualifiedNamespace = parts[1];
        final String eventHubName = parts[2];
        final String consumerGroup = parts[3];
        final String partitionId = parts[4];

        return new PartitionContext(
                fullyQualifiedNamespace,
                eventHubName,
                consumerGroup,
                partitionId
        );
    }

    static String createOwnershipKey(PartitionOwnership partitionOwnership) {
        return createKey(
                OWNERSHIP.keyPrefix(),
                partitionOwnership.getFullyQualifiedNamespace(),
                partitionOwnership.getEventHubName(),
                partitionOwnership.getConsumerGroup(),
                partitionOwnership.getPartitionId()
        );
    }

    static String createCheckpointKey(Checkpoint checkpoint) {
        return createKey(
                CHECKPOINT.keyPrefix(),
                checkpoint.getFullyQualifiedNamespace(),
                checkpoint.getEventHubName(),
                checkpoint.getConsumerGroup(),
                checkpoint.getPartitionId()
        );
    }

    private static String createKey(String kind, String fullyQualifiedNamespace, String eventHubName, String consumerGroup, String partitionId) {
        return String.format(
                "%s/%s/%s/%s/%s",
                kind,
                fullyQualifiedNamespace,
                eventHubName,
                consumerGroup,
                partitionId
        );
    }

    static String createOwnershipValue(PartitionOwnership partitionOwnership) {
        return String.format("%s/%s/%s",
                partitionOwnership.getOwnerId(),
                partitionOwnership.getLastModifiedTime(),
                partitionOwnership.getETag());
    }

    static String createCheckpointValue(Checkpoint checkpoint) {
        return String.format("%s/%s",
                checkpoint.getOffset() != null ? checkpoint.getOffset().toString() : "",
                checkpoint.getSequenceNumber() != null ? checkpoint.getSequenceNumber().toString() : "");
    }

    static String ownershipToString(PartitionOwnership partitionOwnership) {
        return "PartitionOwnership{" +
                "fullyQualifiedNamespace='" + partitionOwnership.getFullyQualifiedNamespace() + '\'' +
                ", eventHubName='" + partitionOwnership.getEventHubName() + '\'' +
                ", consumerGroup='" + partitionOwnership.getConsumerGroup() + '\'' +
                ", partitionId='" + partitionOwnership.getPartitionId() + '\'' +
                ", ownerId='" + partitionOwnership.getOwnerId() + '\'' +
                ", lastModifiedTime=" + partitionOwnership.getLastModifiedTime() +
                ", eTag='" + partitionOwnership.getETag() + '\'' +
                '}';
    }

    static List<String> ownershipListToString(List<PartitionOwnership> partitionOwnershipList) {
        return partitionOwnershipList.stream()
                .map(ComponentStateCheckpointStoreUtils::ownershipToString)
                .collect(Collectors.toList());
    }

    static String checkpointToString(Checkpoint checkpoint) {
        return "Checkpoint{" +
                "fullyQualifiedNamespace='" + checkpoint.getFullyQualifiedNamespace() + '\'' +
                ", eventHubName='" + checkpoint.getEventHubName() + '\'' +
                ", consumerGroup='" + checkpoint.getConsumerGroup() + '\'' +
                ", partitionId='" + checkpoint.getPartitionId() + '\'' +
                ", offset=" + checkpoint.getOffset() +
                ", sequenceNumber=" + checkpoint.getSequenceNumber() +
                '}';
    }

}
