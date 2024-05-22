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

import com.azure.core.util.CoreUtils;
import com.azure.messaging.eventhubs.models.PartitionOwnership;
import org.apache.nifi.components.state.StateManager;
import org.junit.jupiter.api.BeforeEach;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

abstract class AbstractComponentStateCheckpointStoreTest extends AbstractCheckpointStoreTest {

    ComponentStateCheckpointStore checkpointStore;

    @BeforeEach
    void initCheckpointStore() {
        checkpointStore = new ComponentStateCheckpointStore(CLIENT_ID_1, getStateManager());
    }

    abstract StateManager getStateManager();

    PartitionOwnership setETagAndLastModified(PartitionOwnership partitionOwnership) {
        return partitionOwnership.setETag(CoreUtils.randomUuid().toString())
                .setLastModifiedTime(System.currentTimeMillis() - 1000);
    }

    void assertClaimedOwnership(PartitionOwnership requestedOwnership, PartitionOwnership claimedOwnership) {
        assertEquals(requestedOwnership.getFullyQualifiedNamespace(), claimedOwnership.getFullyQualifiedNamespace());
        assertEquals(requestedOwnership.getEventHubName(), claimedOwnership.getEventHubName());
        assertEquals(requestedOwnership.getConsumerGroup(), claimedOwnership.getConsumerGroup());
        assertEquals(requestedOwnership.getPartitionId(), claimedOwnership.getPartitionId());

        assertEquals(requestedOwnership.getOwnerId(), claimedOwnership.getOwnerId());

        assertNotNull(claimedOwnership.getLastModifiedTime());
        assertTrue(claimedOwnership.getLastModifiedTime() > (requestedOwnership.getLastModifiedTime() != null ? requestedOwnership.getLastModifiedTime() : 0));

        assertNotNull(claimedOwnership.getETag());
        assertNotEquals(requestedOwnership.getETag(), claimedOwnership.getETag());
    }

    Map<String, String> initMap(PartitionOwnership... partitionOwnerships) {
        return Stream.of(partitionOwnerships)
                .map(this::copy)
                .map(this::setETagAndLastModified)
                .collect(Collectors.toMap(ComponentStateCheckpointStoreUtils::createOwnershipKey, ComponentStateCheckpointStoreUtils::createOwnershipValue));
    }

}
