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
import org.junit.jupiter.api.Test;

import static org.apache.nifi.processors.azure.eventhub.checkpoint.ComponentStateCheckpointStoreUtils.convertCheckpoint;
import static org.apache.nifi.processors.azure.eventhub.checkpoint.ComponentStateCheckpointStoreUtils.convertOwnership;
import static org.apache.nifi.processors.azure.eventhub.checkpoint.ComponentStateCheckpointStoreUtils.convertPartitionContext;
import static org.apache.nifi.processors.azure.eventhub.checkpoint.ComponentStateCheckpointStoreUtils.createCheckpointKey;
import static org.apache.nifi.processors.azure.eventhub.checkpoint.ComponentStateCheckpointStoreUtils.createCheckpointValue;
import static org.apache.nifi.processors.azure.eventhub.checkpoint.ComponentStateCheckpointStoreUtils.createOwnershipKey;
import static org.apache.nifi.processors.azure.eventhub.checkpoint.ComponentStateCheckpointStoreUtils.createOwnershipValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ComponentStateCheckpointStoreUtilsTest extends AbstractCheckpointStoreTest {

    private static final String OWNERSHIP_KEY = "ownership/my-event-hub-namespace/my-event-hub-name/my-consumer-group/1";
    private static final String OWNERSHIP_VALUE = "client-id-1/1234567890/my-etag";

    private static final String CHECKPOINT_KEY = "checkpoint/my-event-hub-namespace/my-event-hub-name/my-consumer-group/1";
    private static final String CHECKPOINT_VALUE = "10/1";

    @Test
    void testConvertOwnership() {
        PartitionOwnership partitionOwnership = convertOwnership(OWNERSHIP_KEY, OWNERSHIP_VALUE);

        assertNotNull(partitionOwnership);

        assertEquals(EVENT_HUB_NAMESPACE, partitionOwnership.getFullyQualifiedNamespace());
        assertEquals(EVENT_HUB_NAME, partitionOwnership.getEventHubName());
        assertEquals(CONSUMER_GROUP, partitionOwnership.getConsumerGroup());
        assertEquals(PARTITION_ID_1, partitionOwnership.getPartitionId());

        assertEquals(CLIENT_ID_1, partitionOwnership.getOwnerId());
        assertEquals(LAST_MODIFIED_TIME, partitionOwnership.getLastModifiedTime());
        assertEquals(ETAG, partitionOwnership.getETag());
    }

    @Test
    void testConvertCheckpoint() {
        Checkpoint checkpoint = convertCheckpoint(CHECKPOINT_KEY, CHECKPOINT_VALUE);

        assertNotNull(checkpoint);

        assertEquals(EVENT_HUB_NAMESPACE, checkpoint.getFullyQualifiedNamespace());
        assertEquals(EVENT_HUB_NAME, checkpoint.getEventHubName());
        assertEquals(CONSUMER_GROUP, checkpoint.getConsumerGroup());
        assertEquals(PARTITION_ID_1, checkpoint.getPartitionId());

        assertEquals(OFFSET, checkpoint.getOffset());
        assertEquals(SEQUENCE_NUMBER, checkpoint.getSequenceNumber());
    }

    @Test
    void testConvertPartitionContextFromOwnershipKey() {
        PartitionContext partitionContext = convertPartitionContext(OWNERSHIP_KEY);

        assertNotNull(partitionContext);

        assertEquals(EVENT_HUB_NAMESPACE, partitionContext.getFullyQualifiedNamespace());
        assertEquals(EVENT_HUB_NAME, partitionContext.getEventHubName());
        assertEquals(CONSUMER_GROUP, partitionContext.getConsumerGroup());
        assertEquals(PARTITION_ID_1, partitionContext.getPartitionId());
    }

    @Test
    void testConvertPartitionContextFromCheckpointKey() {
        PartitionContext partitionContext = convertPartitionContext(CHECKPOINT_KEY);

        assertNotNull(partitionContext);

        assertEquals(EVENT_HUB_NAMESPACE, partitionContext.getFullyQualifiedNamespace());
        assertEquals(EVENT_HUB_NAME, partitionContext.getEventHubName());
        assertEquals(CONSUMER_GROUP, partitionContext.getConsumerGroup());
        assertEquals(PARTITION_ID_1, partitionContext.getPartitionId());
    }

    @Test
    void testOwnershipKey() {
        String ownershipKey = createOwnershipKey(partitionOwnership1);

        assertEquals(OWNERSHIP_KEY, ownershipKey);
    }

    @Test
    void testOwnershipValue() {
        partitionOwnership1
                .setLastModifiedTime(LAST_MODIFIED_TIME)
                .setETag(ETAG);

        String ownershipValue = createOwnershipValue(partitionOwnership1);

        assertEquals(OWNERSHIP_VALUE, ownershipValue);
    }

    @Test
    void testCheckpointKey() {
        String checkpointKey = createCheckpointKey(checkpoint1);

        assertEquals(CHECKPOINT_KEY, checkpointKey);
    }

    @Test
    void testCheckpointValue() {
        String checkpointValue = createCheckpointValue(checkpoint1);

        assertEquals(CHECKPOINT_VALUE, checkpointValue);
    }

}
