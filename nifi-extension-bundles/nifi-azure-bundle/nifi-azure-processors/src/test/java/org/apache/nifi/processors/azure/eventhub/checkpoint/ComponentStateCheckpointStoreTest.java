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
import org.apache.nifi.processors.azure.eventhub.checkpoint.exception.ClusterNodeDisconnectedException;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.state.MockStateManager.ExecutionMode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.azure.eventhub.checkpoint.CheckpointStoreKey.CLIENT_ID;
import static org.apache.nifi.processors.azure.eventhub.checkpoint.CheckpointStoreKey.CLUSTERED;
import static org.apache.nifi.processors.azure.eventhub.checkpoint.ComponentStateCheckpointStoreUtils.createCheckpointKey;
import static org.apache.nifi.processors.azure.eventhub.checkpoint.ComponentStateCheckpointStoreUtils.createCheckpointValue;
import static org.apache.nifi.processors.azure.eventhub.checkpoint.ComponentStateCheckpointStoreUtils.createOwnershipKey;
import static org.apache.nifi.processors.azure.eventhub.checkpoint.ComponentStateCheckpointStoreUtils.createOwnershipValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ComponentStateCheckpointStoreTest extends AbstractComponentStateCheckpointStoreTest {

    private MockStateManager stateManager;

    @Override
    StateManager getStateManager() {
        stateManager = new MockStateManager(new Object());
        stateManager.setIgnoreAnnotations(true);
        return stateManager;
    }

    @BeforeEach
    void beforeEach() throws IOException {
        stateManager.setExecutionMode(ExecutionMode.CLUSTERED);

        addToState(CLIENT_ID.key(), CLIENT_ID_1, Scope.LOCAL);
        addToState(CLUSTERED.key(), "true", Scope.LOCAL);
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
        initStateWithAllItems();

        testListOwnerships(partitionOwnership1, partitionOwnership2);
        testListCheckpoints(checkpoint1, checkpoint2);
    }

    @ParameterizedTest
    @EnumSource(ExecutionMode.class)
    void testCleanUp(ExecutionMode executionMode) throws IOException {
        setExecutionMode(executionMode);

        initStateWithAllItems();

        int expectedBefore = executionMode == ExecutionMode.CLUSTERED ? 10 : 12;
        assertEquals(expectedBefore, stateManager.getState(Scope.CLUSTER).toMap().size());

        checkpointStore.cleanUp(EVENT_HUB_NAMESPACE, EVENT_HUB_NAME, CONSUMER_GROUP);

        int expectedAfter = executionMode == ExecutionMode.CLUSTERED ? 4 : 6;
        assertEquals(expectedAfter, stateManager.getState(Scope.CLUSTER).toMap().size());

        testListOwnerships(partitionOwnership1, partitionOwnership2);
        testListCheckpoints(checkpoint1, checkpoint2);

        assertLocalScope(executionMode);
    }

    @ParameterizedTest
    @EnumSource(ExecutionMode.class)
    void testClaimOwnership(ExecutionMode executionMode) throws IOException {
        setExecutionMode(executionMode);

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

        assertLocalScope(executionMode);
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

    @ParameterizedTest
    @EnumSource(ExecutionMode.class)
    void testNewCheckpoint(ExecutionMode executionMode) throws IOException {
        setExecutionMode(executionMode);

        addToState(partitionOwnership1);
        addToState(partitionOwnership2);

        addToState(checkpoint1);

        Checkpoint newCheckpoint = createCheckpoint(PARTITION_ID_2, 20L, 2L);

        checkpointStore.updateCheckpoint(newCheckpoint).block();

        assertStoredCheckpoints(checkpoint1, newCheckpoint);
        assertStoredOwnerships(partitionOwnership1, partitionOwnership2);

        assertLocalScope(executionMode);
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

    @Test
    void testDisconnectedNode() {
        stateManager.setExecutionMode(ExecutionMode.STANDALONE);

        assertThrows(ClusterNodeDisconnectedException.class, () -> checkpointStore.claimOwnership(Collections.singletonList(partitionOwnership1)).collectList().block());

        assertThrows(ClusterNodeDisconnectedException.class, () -> checkpointStore.updateCheckpoint(checkpoint1).block());
    }

    private void setExecutionMode(ExecutionMode executionMode) throws IOException {
        stateManager.setExecutionMode(executionMode);
        addToState(CLUSTERED.key(), Boolean.toString(executionMode == ExecutionMode.CLUSTERED), Scope.LOCAL);
    }

    private void testListOwnerships(PartitionOwnership... expectedPartitionOwnerships) {
        List<PartitionOwnership> partitionOwnerships = checkpointStore.listOwnership(EVENT_HUB_NAMESPACE, EVENT_HUB_NAME, CONSUMER_GROUP).collectList().block();

        assertNotNull(partitionOwnerships);
        assertEquals(expectedPartitionOwnerships.length, partitionOwnerships.size());

        assertTrue(convertToTestablePartitionOwnerships(partitionOwnerships).containsAll(Arrays.asList(expectedPartitionOwnerships)));
    }

    private void testListCheckpoints(Checkpoint... expectedCheckpoints) {
        List<Checkpoint> checkpoints = checkpointStore.listCheckpoints(EVENT_HUB_NAMESPACE, EVENT_HUB_NAME, CONSUMER_GROUP).collectList().block();

        assertNotNull(checkpoints);
        assertEquals(expectedCheckpoints.length, checkpoints.size());
        assertTrue(convertToTestableCheckpoints(checkpoints).containsAll(Arrays.asList(expectedCheckpoints)));
    }

    private void assertStoredOwnerships(PartitionOwnership... expectedPartitionOwnerships) {
        testListOwnerships(expectedPartitionOwnerships);
    }

    private void assertStoredCheckpoints(Checkpoint... expectedCheckpoints) {
        testListCheckpoints(expectedCheckpoints);
    }

    private void assertLocalScope(ExecutionMode executionMode) {
        stateManager.assertStateEquals(CLIENT_ID.key(), CLIENT_ID_1, Scope.LOCAL);
        stateManager.assertStateEquals(CLUSTERED.key(), Boolean.toString(executionMode == ExecutionMode.CLUSTERED), Scope.LOCAL);
    }

    private void addToState(PartitionOwnership partitionOwnership) throws IOException {
        setETagAndLastModified(partitionOwnership);

        addToState(createOwnershipKey(partitionOwnership), createOwnershipValue(partitionOwnership), Scope.CLUSTER);
    }

    private void addToState(Checkpoint checkpoint) throws IOException {
        addToState(createCheckpointKey(checkpoint), createCheckpointValue(checkpoint), Scope.CLUSTER);
    }

    private void addToState(String key, String value, Scope scope) throws IOException {
        Map<String, String> map = new HashMap<>(stateManager.getState(scope).toMap());

        map.put(key, value);

        stateManager.setState(map, scope);
    }

    private void initStateWithAllItems() throws IOException {
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
    }

}
