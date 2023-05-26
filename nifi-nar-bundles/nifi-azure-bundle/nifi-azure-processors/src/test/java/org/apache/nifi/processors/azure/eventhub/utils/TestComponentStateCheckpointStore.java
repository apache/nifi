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
package org.apache.nifi.processors.azure.eventhub.utils;

import com.azure.messaging.eventhubs.models.Checkpoint;
import com.azure.messaging.eventhubs.models.PartitionOwnership;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.processors.azure.eventhub.utils.ComponentStateCheckpointStore.State;
import org.apache.nifi.state.MockStateMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

class TestComponentStateCheckpointStore {
    private static final String EVENT_HUB_NAMESPACE = "NAMESPACE";
    private static final String EVENT_HUB_NAME = "NAME";
    private static final String CONSUMER_GROUP = "CONSUMER";
    private static final String PARTITION_ID = "1";
    private static final long OFFSET = 1234;

    final static String IDENTIFIER = "id";
    ComponentStateCheckpointStore checkpointStore;
    Checkpoint checkpoint;
    PartitionOwnership partitionOwnership;

    boolean failToReplaceState = false;
    boolean throwIOExceptionOnGetState = false;
    boolean throwIOExceptionOnReplaceState = false;

    @Test
    public void testClaimOwnershipSuccess() {
        var claimed = checkpointStore.claimOwnership(List.of(
            partitionOwnership
        )).blockFirst();
        var listed = checkpointStore.listOwnership(
                EVENT_HUB_NAMESPACE,
                EVENT_HUB_NAME,
                CONSUMER_GROUP
        ).blockFirst();
        assertEquals(listed.getETag(), claimed.getETag());
    }

    @Test
    public void testClaimOwnershipReplaceStateFailure() {
        failToReplaceState = true;
        var claimed = checkpointStore.claimOwnership(List.of(
                partitionOwnership
        )).blockFirst();
        assertNull(claimed);
    }

    @Test
    public void testClaimOwnershipGetStateIOException() {
        throwIOExceptionOnGetState = true;
        assertThrows(RuntimeException.class, () ->
            checkpointStore.claimOwnership(List.of(
                partitionOwnership
        )).blockFirst());
    }

    @Test
    public void testClaimOwnershipReplaceStateIOException() {
        throwIOExceptionOnReplaceState = true;
        assertThrows(RuntimeException.class, () ->
            checkpointStore.claimOwnership(List.of(
                partitionOwnership
        )).blockFirst());
    }

    @Test
    public void testUpdateCheckpointSuccess() {
        checkpointStore.updateCheckpoint(checkpoint).block();
        var checkpoint = checkpointStore.listCheckpoints(
                EVENT_HUB_NAMESPACE,
                EVENT_HUB_NAME,
                CONSUMER_GROUP
        ).blockFirst();
        assert checkpoint.getOffset().equals(this.checkpoint.getOffset());
    }
    @BeforeEach
    public void setupProcessor() {
        final Map<String, String> store = new HashMap<>();
        final Lock lock = new ReentrantLock();
        final LongAdder version = new LongAdder();
        final State state = new State() {
            public StateMap getState() throws IOException {
                if (throwIOExceptionOnGetState) {
                    throw new IOException();
                }
                lock.lock();
                try {
                    return new MockStateMap(store, version.longValue());
                } finally {
                    lock.unlock();
                }
            }

            public boolean replaceState(StateMap oldValue, Map<String, String> newValue) throws IOException {
                if (failToReplaceState) {
                    return false;
                }
                if (throwIOExceptionOnReplaceState) {
                    throw new IOException();
                }
                lock.lock();
                try {
                    if (oldValue.toMap().equals(store)) {
                        store.clear();
                        store.putAll(newValue);
                        version.increment();
                        return true;
                    }
                    return false;
                } finally {
                    lock.unlock();
                }
            }
        };
        checkpointStore = new ComponentStateCheckpointStore(IDENTIFIER, state);
        partitionOwnership = new PartitionOwnership()
                .setFullyQualifiedNamespace(EVENT_HUB_NAMESPACE)
                .setEventHubName(EVENT_HUB_NAME)
                .setConsumerGroup(CONSUMER_GROUP);
        checkpoint = new Checkpoint()
                .setFullyQualifiedNamespace(EVENT_HUB_NAMESPACE)
                .setEventHubName(EVENT_HUB_NAME)
                .setConsumerGroup(CONSUMER_GROUP)
                .setPartitionId(PARTITION_ID)
                .setOffset(OFFSET);
    }
}