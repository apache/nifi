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
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.state.MockStateManager;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.azure.eventhub.checkpoint.ComponentStateCheckpointStore.createCheckpointKey;
import static org.apache.nifi.processors.azure.eventhub.checkpoint.ComponentStateCheckpointStore.createCheckpointValue;
import static org.apache.nifi.processors.azure.eventhub.checkpoint.ComponentStateCheckpointStore.createOwnershipKey;
import static org.apache.nifi.processors.azure.eventhub.checkpoint.ComponentStateCheckpointStore.createOwnershipValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ComponentStateCheckpointStoreTest extends AbstractComponentStateCheckpointStoreTest {

    private MockStateManager stateManager;

    @Override
    StateManager getStateManager() {
        stateManager = new MockStateManager(new Object());
        stateManager.setIgnoreAnnotations(true);
        return stateManager;
    }

    @Test
    void testOwnershipKey() {
        String ownershipKey = createOwnershipKey(partitionOwnership1);

        String expectedOwnershipKey = "ownership/my-event-hub-namespace/my-event-hub-name/my-consumer-group/1";
        assertEquals(expectedOwnershipKey, ownershipKey);
    }

    @Test
    void testOwnershipValue() {
        partitionOwnership1
                .setLastModifiedTime(1234567890L)
                .setETag("my-etag");

        String ownershipValue = createOwnershipValue(partitionOwnership1);

        String expectedOwnershipValue = "client-id-1/1234567890/my-etag";
        assertEquals(expectedOwnershipValue, ownershipValue);
    }

    @Test
    void testCheckpointKey() {
        String checkpointKey = createCheckpointKey(checkpoint1);

        String expectedCheckpointKey = "checkpoint/my-event-hub-namespace/my-event-hub-name/my-consumer-group/1";
        assertEquals(expectedCheckpointKey, checkpointKey);
    }

    @Test
    void testCheckpointValue() {
        String checkpointValue = createCheckpointValue(checkpoint1);

        String expectedCheckpointValue = "10/1";
        assertEquals(expectedCheckpointValue, checkpointValue);
    }

    @Test
    void testOwnershipStoredInState() throws IOException {
        checkpointStore.claimOwnership(Collections.singletonList(partitionOwnership1)).blockLast();

        String expectedOwnershipKey = "ownership/my-event-hub-namespace/my-event-hub-name/my-consumer-group/1";
        stateManager.assertStateSet(expectedOwnershipKey, Scope.CLUSTER);

        String ownershipValue = stateManager.getState(Scope.CLUSTER).get(expectedOwnershipKey);
        assertTrue(ownershipValue.matches("client-id-1/\\d+/.+"));
    }

    @Test
    void testCheckpointStoredInState() {
        checkpointStore.updateCheckpoint(checkpoint1).block();

        String expectedCheckpointKey = "checkpoint/my-event-hub-namespace/my-event-hub-name/my-consumer-group/1";
        String expectedCheckpointValue = "10/1";
        stateManager.assertStateEquals(expectedCheckpointKey, expectedCheckpointValue, Scope.CLUSTER);
    }

    @Test
    void testListing() throws IOException {
        addToState(partitionOwnership1);
        addToState(partitionOwnership2);

        addToState(checkpoint1);
        addToState(checkpoint2);

        addToState(createPartitionOwnership(EVENT_HUB_NAMESPACE + "-2", EVENT_HUB_NAME, CONSUMER_GROUP, PARTITION_ID_1, CLIENT_ID_1));
        addToState(createPartitionOwnership(EVENT_HUB_NAMESPACE, EVENT_HUB_NAME + "-2", CONSUMER_GROUP, PARTITION_ID_1, CLIENT_ID_1));
        addToState(createPartitionOwnership(EVENT_HUB_NAMESPACE, EVENT_HUB_NAME, CONSUMER_GROUP + "-2", PARTITION_ID_1, CLIENT_ID_1));

        addToState(createCheckpoint(EVENT_HUB_NAMESPACE + "-2", EVENT_HUB_NAME, CONSUMER_GROUP, PARTITION_ID_1, OFFSET, SEQUENCE_NUMBER));
        addToState(createCheckpoint(EVENT_HUB_NAMESPACE, EVENT_HUB_NAME + "-2", CONSUMER_GROUP, PARTITION_ID_1, OFFSET, SEQUENCE_NUMBER));
        addToState(createCheckpoint(EVENT_HUB_NAMESPACE, EVENT_HUB_NAME, CONSUMER_GROUP + "-2", PARTITION_ID_1, OFFSET, SEQUENCE_NUMBER));

        testListOwnerships(partitionOwnership1, partitionOwnership2);
        testListCheckpoints(checkpoint1, checkpoint2);
    }

    @Test
    void testClaimOwnership() throws IOException {
        addToState(partitionOwnership1);

        addToState(checkpoint1);

        List<PartitionOwnership> requestedOwnerships = Collections.singletonList(partitionOwnership2);

        List<PartitionOwnership> claimedOwnerships = checkpointStore.claimOwnership(requestedOwnerships).collectList().block();

        assertNotNull(claimedOwnerships);
        assertEquals(1, claimedOwnerships.size());

        PartitionOwnership claimedOwnership = claimedOwnerships.get(0);
        assertClaimedOwnership(partitionOwnership2, claimedOwnership);

        assertStoredOwnerships(partitionOwnership1, convertToTestable(claimedOwnership));
        assertStoredCheckpoints(checkpoint1);
    }

    @Test
    void testRenewOwnership() throws IOException {
        addToState(partitionOwnership1);
        addToState(partitionOwnership2);

        addToState(checkpoint1);
        addToState(checkpoint2);

        List<PartitionOwnership> requestedOwnerships = Collections.singletonList(partitionOwnership2);

        List<PartitionOwnership> claimedOwnerships = checkpointStore.claimOwnership(requestedOwnerships).collectList().block();

        assertNotNull(claimedOwnerships);
        assertEquals(1, claimedOwnerships.size());

        PartitionOwnership claimedOwnership = claimedOwnerships.get(0);
        assertClaimedOwnership(partitionOwnership2, claimedOwnership);

        assertStoredOwnerships(partitionOwnership1, convertToTestable(claimedOwnership));
        assertStoredCheckpoints(checkpoint1, checkpoint2);
    }

    @Test
    void testStealOwnership() throws IOException {
        addToState(partitionOwnership1);

        addToState(checkpoint1);

        PartitionOwnership newOwnership = copy(partitionOwnership1)
                .setOwnerId(CLIENT_ID_2);

        List<PartitionOwnership> requestedOwnerships = Collections.singletonList(newOwnership);

        List<PartitionOwnership> claimedOwnerships = checkpointStore.claimOwnership(requestedOwnerships).collectList().block();

        assertNotNull(claimedOwnerships);
        assertEquals(1, claimedOwnerships.size());

        PartitionOwnership claimedOwnership = claimedOwnerships.get(0);
        assertClaimedOwnership(newOwnership, claimedOwnership);

        assertStoredOwnerships(convertToTestable(claimedOwnership));
        assertStoredCheckpoints(checkpoint1);
    }

    @Test
    void testClaimMultipleOwnerships() {
        partitionOwnership2.setOwnerId(CLIENT_ID_1);

        List<PartitionOwnership> requestedOwnerships = Arrays.asList(partitionOwnership1, partitionOwnership2);

        List<PartitionOwnership> claimedOwnerships = checkpointStore.claimOwnership(requestedOwnerships).collectList().block();

        assertNotNull(claimedOwnerships);
        assertEquals(2, claimedOwnerships.size());

        PartitionOwnership claimedOwnership1;
        PartitionOwnership claimedOwnership2;

        if (claimedOwnerships.get(0).getPartitionId().equals(PARTITION_ID_1)) {
            claimedOwnership1 = claimedOwnerships.get(0);
            claimedOwnership2 = claimedOwnerships.get(1);
        } else {
            claimedOwnership1 = claimedOwnerships.get(1);
            claimedOwnership2 = claimedOwnerships.get(2);
        }

        assertClaimedOwnership(partitionOwnership1, claimedOwnership1);
        assertClaimedOwnership(partitionOwnership2, claimedOwnership2);

        assertStoredOwnerships(convertToTestable(claimedOwnership1), convertToTestable(claimedOwnership2));
    }

    @Test
    void testClaimOwnershipUnsuccessful() {
        List<PartitionOwnership> requestedOwnershipsA = Collections.singletonList(partitionOwnership1);
        List<PartitionOwnership> requestedOwnershipsB = Collections.singletonList(partitionOwnership1);

        List<PartitionOwnership> claimedOwnershipsA = checkpointStore.claimOwnership(requestedOwnershipsA).collectList().block();

        List<PartitionOwnership> claimedOwnershipsB = checkpointStore.claimOwnership(requestedOwnershipsB).collectList().block();

        assertNotNull(claimedOwnershipsB);
        assertEquals(0, claimedOwnershipsB.size());

        assertStoredOwnerships(convertToTestable(claimedOwnershipsA.get(0)));
    }

    @Test
    void testClaimMultipleOwnershipsPartialSuccess() {
        List<PartitionOwnership> requestedOwnershipsA = Collections.singletonList(partitionOwnership1);
        List<PartitionOwnership> requestedOwnershipsB = Arrays.asList(partitionOwnership1, partitionOwnership2);

        List<PartitionOwnership> claimedOwnershipsA = checkpointStore.claimOwnership(requestedOwnershipsA).collectList().block();

        List<PartitionOwnership> claimedOwnershipsB = checkpointStore.claimOwnership(requestedOwnershipsB).collectList().block();

        assertNotNull(claimedOwnershipsB);
        assertEquals(1, claimedOwnershipsB.size());

        assertStoredOwnerships(convertToTestable(claimedOwnershipsA.get(0)), convertToTestable(claimedOwnershipsB.get(0)));
    }

    @Test
    void testUnclaimOwnership() throws IOException {
        addToState(partitionOwnership1);

        addToState(checkpoint1);

        PartitionOwnership ownershipToUnclaim = copy(partitionOwnership1)
                .setOwnerId("");

        List<PartitionOwnership> requestedOwnerships = Collections.singletonList(ownershipToUnclaim);

        List<PartitionOwnership> returnedOwnerships = checkpointStore.claimOwnership(requestedOwnerships).collectList().block();

        assertNotNull(returnedOwnerships);
        assertEquals(1, returnedOwnerships.size());

        PartitionOwnership unclaimedOwnership = returnedOwnerships.get(0);
        assertClaimedOwnership(ownershipToUnclaim, unclaimedOwnership);

        assertStoredOwnerships(convertToTestable(unclaimedOwnership));
        assertStoredCheckpoints(checkpoint1);
    }

    @Test
    void testNewCheckpoint() throws IOException {
        addToState(partitionOwnership1);
        addToState(partitionOwnership2);

        addToState(checkpoint1);

        Checkpoint newCheckpoint = createCheckpoint(PARTITION_ID_2, 20L, 2L);

        checkpointStore.updateCheckpoint(newCheckpoint).block();

        assertStoredCheckpoints(checkpoint1, newCheckpoint);
        assertStoredOwnerships(partitionOwnership1, partitionOwnership2);
    }

    @Test
    void testUpdatedCheckpoint() throws IOException {
        addToState(partitionOwnership1);
        addToState(partitionOwnership2);

        addToState(checkpoint1);
        addToState(checkpoint2);

        Checkpoint updatedCheckpoint = copy(checkpoint2)
                .setOffset(20L)
                .setSequenceNumber(2L);

        checkpointStore.updateCheckpoint(updatedCheckpoint).block();

        assertStoredCheckpoints(checkpoint1, updatedCheckpoint);
        assertStoredOwnerships(partitionOwnership1, partitionOwnership2);
    }

    @Test
    void testCheckpointWithNullOffset() {
        checkpoint1.setOffset(null);

        checkpointStore.updateCheckpoint(checkpoint1).block();

        assertStoredCheckpoints(checkpoint1);
    }

    @Test
    void testCheckpointWithNullSequenceNumber() {
        checkpoint1.setSequenceNumber(null);

        checkpointStore.updateCheckpoint(checkpoint1).block();

        assertStoredCheckpoints(checkpoint1);
    }

    private void testListOwnerships(PartitionOwnership... expectedPartitionOwnerships) {
        List<PartitionOwnership> partitionOwnerships = checkpointStore.listOwnership(EVENT_HUB_NAMESPACE, EVENT_HUB_NAME, CONSUMER_GROUP).collectList().block();

        assertNotNull(partitionOwnerships);
        assertEquals(expectedPartitionOwnerships.length, partitionOwnerships.size());
        assertThat(convertToTestablePartitionOwnerships(partitionOwnerships), containsInAnyOrder(expectedPartitionOwnerships));
    }

    private void testListCheckpoints(Checkpoint... expectedCheckpoints) {
        List<Checkpoint> checkpoints = checkpointStore.listCheckpoints(EVENT_HUB_NAMESPACE, EVENT_HUB_NAME, CONSUMER_GROUP).collectList().block();

        assertNotNull(checkpoints);
        assertEquals(expectedCheckpoints.length, checkpoints.size());
        assertThat(convertToTestableCheckpoints(checkpoints), containsInAnyOrder(expectedCheckpoints));
    }

    private void assertStoredOwnerships(PartitionOwnership... expectedPartitionOwnerships) {
        testListOwnerships(expectedPartitionOwnerships);
    }

    private void assertStoredCheckpoints(Checkpoint... expectedCheckpoints) {
        testListCheckpoints(expectedCheckpoints);
    }

    private void addToState(PartitionOwnership partitionOwnership) throws IOException {
        setETagAndLastModified(partitionOwnership);

        Map<String, String> map = new HashMap<>(stateManager.getState(Scope.CLUSTER).toMap());

        map.put(createOwnershipKey(partitionOwnership), createOwnershipValue(partitionOwnership));

        stateManager.setState(map, Scope.CLUSTER);
    }

    private void addToState(Checkpoint checkpoint) throws IOException {
        Map<String, String> map = new HashMap<>(stateManager.getState(Scope.CLUSTER).toMap());

        map.put(createCheckpointKey(checkpoint), createCheckpointValue(checkpoint));

        stateManager.setState(map, Scope.CLUSTER);
    }

}
