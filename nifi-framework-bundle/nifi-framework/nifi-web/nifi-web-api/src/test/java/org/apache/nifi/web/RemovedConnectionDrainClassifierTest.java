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
package org.apache.nifi.web;

import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.controller.ScheduledState;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RemovedConnectionDrainClassifierTest {
    private static final String ROOT_GROUP_ID = "root-group";

    private final RemovedConnectionDrainClassifier classifier = new RemovedConnectionDrainClassifier();

    @Test
    public void testEmptyRemovedConnectionIsNoDrainRegardlessOfUnsupportedTopology() {
        final RemovedConnectionDescriptor removedConnection = createDescriptor("c-empty", "c-empty-v", ROOT_GROUP_ID,
                "remote-source", "remote-source-v", ROOT_GROUP_ID, ConnectableType.REMOTE_OUTPUT_PORT,
                "funnel-destination", "funnel-destination-v", ROOT_GROUP_ID, ConnectableType.FUNNEL,
                RemovalReason.SOURCE_CHANGED);

        final TestContext context = new TestContext()
                .addGroup(ROOT_GROUP_ID, null)
                .addConnection("c-empty", "remote-source", "funnel-destination", true)
                .addConnectable("remote-source", ConnectableType.REMOTE_OUTPUT_PORT, ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID, true)
                .addConnectable("funnel-destination", ConnectableType.FUNNEL, ROOT_GROUP_ID, null, null, false);

        final RemovedConnectionDrainClassifier.BatchResult result = classifier.classify(createImpact(Set.of(removedConnection), Set.of(), Set.of()), context);

        assertTrue(result.isSupported());
        assertEquals(RemovedConnectionDrainClassifier.Classification.NO_DRAIN, result.connectionResults().getFirst().classification());
    }

    @Test
    public void testUnknownQueueStateDoesNotSkipDrain() {
        final RemovedConnectionDescriptor removedConnection = createDescriptor("c-unknown-queue", "c-unknown-queue-v", ROOT_GROUP_ID,
                "source", "source-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                "destination", "destination-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                RemovalReason.COMPONENT_REMOVED);

        final TestContext context = new TestContext()
                .addGroup(ROOT_GROUP_ID, null)
                .addConnection("c-unknown-queue", "source", "destination", false)
                .addProcessor("source", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("destination", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID);

        final RemovedConnectionDrainClassifier.BatchResult result = classifier.classify(createImpact(Set.of(removedConnection), Set.of(), Set.of()), context);

        assertTrue(result.isSupported());
        assertEquals(RemovedConnectionDrainClassifier.Classification.CANDIDATE, result.connectionResults().getFirst().classification());
    }

    @Test
    public void testNonEmptySourceChangedRemovalIsUnsupported() {
        final RemovedConnectionDescriptor removedConnection = createDescriptor("c-source-changed", "c-source-changed-v", ROOT_GROUP_ID,
                "source", "source-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                "destination", "destination-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                RemovalReason.SOURCE_CHANGED);

        final TestContext context = new TestContext()
                .addGroup(ROOT_GROUP_ID, null)
                .addConnection("c-source-changed", "source", "destination", false)
                .addProcessor("source", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("destination", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID);

        final RemovedConnectionDrainClassifier.BatchResult result = classifier.classify(createImpact(Set.of(removedConnection), Set.of(), Set.of()), context);

        assertFalse(result.isSupported());
        assertEquals(RemovedConnectionDrainClassifier.UnsupportedReason.SOURCE_CHANGED_REMOVAL,
                result.getFirstUnsupportedConnectionResult().orElseThrow().unsupportedReason());
    }

    @Test
    public void testNullContainingGroupIsUnsupported() {
        final RemovedConnectionDescriptor removedConnection = createDescriptor("c-null-group", "c-null-group-v", null,
                "source", "source-v", null, ConnectableType.PROCESSOR,
                "destination", "destination-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                RemovalReason.COMPONENT_REMOVED);

        final TestContext context = new TestContext()
                .addGroup(ROOT_GROUP_ID, null)
                .addConnection("c-null-group", "source", "destination", false)
                .addProcessor("source", null, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("destination", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID);

        final RemovedConnectionDrainClassifier.BatchResult result = classifier.classify(
                createImpact(Set.of(removedConnection), Set.of(), Set.of()), context);

        assertFalse(result.isSupported());
        assertEquals(RemovedConnectionDrainClassifier.UnsupportedReason.CONNECTION_IN_REMOVED_GROUP,
                result.getFirstUnsupportedConnectionResult().orElseThrow().unsupportedReason());
    }

    @Test
    public void testMissingProducerUsesSourceAgnosticReason() {
        final RemovedConnectionDescriptor removedConnection = createDescriptor("c-missing-producer", "c-missing-producer-v", ROOT_GROUP_ID,
                "missing-source", "missing-source-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                "destination", "destination-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                RemovalReason.COMPONENT_REMOVED);

        final TestContext context = new TestContext()
                .addGroup(ROOT_GROUP_ID, null)
                .addConnection("c-missing-producer", "missing-source", "destination", false)
                .addProcessor("destination", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID);

        final RemovedConnectionDrainClassifier.BatchResult result = classifier.classify(
                createImpact(Set.of(removedConnection), Set.of(), Set.of()), context);

        assertFalse(result.isSupported());
        assertEquals(RemovedConnectionDrainClassifier.UnsupportedReason.NO_SUPPORTED_PRODUCER_FOUND,
                result.getFirstUnsupportedConnectionResult().orElseThrow().unsupportedReason());
    }

    @Test
    public void testRemovedGroupAndRemovedEndpointAreUnsupportedWhenQueueNotEmpty() {
        final RemovedConnectionDescriptor removedInGroup = createDescriptor("c-removed-group", "c-removed-group-v", "child-group",
                "source-a", "source-a-v", "child-group", ConnectableType.PROCESSOR,
                "destination-a", "destination-a-v", "child-group", ConnectableType.PROCESSOR,
                RemovalReason.COMPONENT_REMOVED);
        final RemovedConnectionDescriptor removedEndpoint = createDescriptor("c-removed-endpoint", "c-removed-endpoint-v", ROOT_GROUP_ID,
                "source-b", "source-b-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                "destination-b", "destination-b-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                RemovalReason.COMPONENT_REMOVED);

        final TestContext context = new TestContext()
                .addGroup(ROOT_GROUP_ID, null)
                .addGroup("child-group", ROOT_GROUP_ID)
                .addConnection("c-removed-group", "source-a", "destination-a", false)
                .addConnection("c-removed-endpoint", "source-b", "destination-b", false)
                .addProcessor("source-a", "child-group", ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("destination-a", "child-group", ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("source-b", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("destination-b", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID);

        final RemovedConnectionDrainClassifier.BatchResult result = classifier.classify(
                createImpact(List.of(removedInGroup, removedEndpoint), Set.of("child-group"), Set.of("destination-b")), context);

        final Map<String, RemovedConnectionDrainClassifier.UnsupportedReason> reasonsByConnection = result.connectionResults().stream()
                .collect(Collectors.toMap(r -> r.connection().getConnectionInstanceId(), RemovedConnectionDrainClassifier.ConnectionResult::unsupportedReason));
        assertEquals(RemovedConnectionDrainClassifier.UnsupportedReason.CONNECTION_IN_REMOVED_GROUP, reasonsByConnection.get("c-removed-group"));
        assertEquals(RemovedConnectionDrainClassifier.UnsupportedReason.DESTINATION_COMPONENT_REMOVED, reasonsByConnection.get("c-removed-endpoint"));
    }

    @Test
    public void testDirectSelfLoopRemovedSourceRemoteSourceAndFunnelDestinationAreUnsupported() {
        final RemovedConnectionDescriptor selfLoop = createDescriptor("c-self-loop", "c-self-loop-v", ROOT_GROUP_ID,
                "self-loop", "self-loop-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                "self-loop", "self-loop-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                RemovalReason.COMPONENT_REMOVED);
        final RemovedConnectionDescriptor removedSource = createDescriptor("c-removed-source", "c-removed-source-v", ROOT_GROUP_ID,
                "removed-source", "removed-source-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                "destination-removed-source", "destination-removed-source-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                RemovalReason.COMPONENT_REMOVED);
        final RemovedConnectionDescriptor remoteSource = createDescriptor("c-remote-source", "c-remote-source-v", ROOT_GROUP_ID,
                "remote-source", "remote-source-v", ROOT_GROUP_ID, ConnectableType.REMOTE_OUTPUT_PORT,
                "destination-remote-source", "destination-remote-source-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                RemovalReason.COMPONENT_REMOVED);
        final RemovedConnectionDescriptor funnelDestination = createDescriptor("c-funnel-destination", "c-funnel-destination-v", ROOT_GROUP_ID,
                "source-funnel-destination", "source-funnel-destination-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                "funnel-destination", "funnel-destination-v", ROOT_GROUP_ID, ConnectableType.FUNNEL,
                RemovalReason.COMPONENT_REMOVED);

        final TestContext context = new TestContext()
                .addGroup(ROOT_GROUP_ID, null)
                .addConnection("c-self-loop", "self-loop", "self-loop", false)
                .addConnection("c-removed-source", "removed-source", "destination-removed-source", false)
                .addConnection("c-remote-source", "remote-source", "destination-remote-source", false)
                .addConnection("c-funnel-destination", "source-funnel-destination", "funnel-destination", false)
                .addProcessor("self-loop", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("removed-source", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("destination-removed-source", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addConnectable("remote-source", ConnectableType.REMOTE_OUTPUT_PORT, ROOT_GROUP_ID, null, null, true)
                .addProcessor("destination-remote-source", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("source-funnel-destination", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addConnectable("funnel-destination", ConnectableType.FUNNEL, ROOT_GROUP_ID, null, null, false);

        final RemovedConnectionDrainClassifier.BatchResult result = classifier.classify(
                createImpact(List.of(selfLoop, removedSource, remoteSource, funnelDestination), Set.of(), Set.of("removed-source")), context);

        final Map<String, RemovedConnectionDrainClassifier.UnsupportedReason> reasonsByConnection = result.connectionResults().stream()
                .collect(Collectors.toMap(r -> r.connection().getConnectionInstanceId(), RemovedConnectionDrainClassifier.ConnectionResult::unsupportedReason));
        assertEquals(RemovedConnectionDrainClassifier.UnsupportedReason.SELF_LOOP, reasonsByConnection.get("c-self-loop"));
        assertEquals(RemovedConnectionDrainClassifier.UnsupportedReason.SOURCE_COMPONENT_REMOVED, reasonsByConnection.get("c-removed-source"));
        assertEquals(RemovedConnectionDrainClassifier.UnsupportedReason.UNSUPPORTED_SOURCE_TYPE, reasonsByConnection.get("c-remote-source"));
        assertEquals(RemovedConnectionDrainClassifier.UnsupportedReason.UNSUPPORTED_DESTINATION_TYPE, reasonsByConnection.get("c-funnel-destination"));
    }

    @Test
    public void testProcessorDestinationMustBePhysicallyRunningAndValid() {
        final RemovedConnectionDescriptor invalidDestination = createDescriptor("c-invalid-destination", "c-invalid-destination-v", ROOT_GROUP_ID,
                "source-invalid", "source-invalid-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                "destination-invalid", "destination-invalid-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                RemovalReason.COMPONENT_REMOVED);
        final RemovedConnectionDescriptor startingDestination = createDescriptor("c-starting-destination", "c-starting-destination-v", ROOT_GROUP_ID,
                "source-starting", "source-starting-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                "destination-starting", "destination-starting-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                RemovalReason.COMPONENT_REMOVED);

        final TestContext context = new TestContext()
                .addGroup(ROOT_GROUP_ID, null)
                .addConnection("c-invalid-destination", "source-invalid", "destination-invalid", false)
                .addConnection("c-starting-destination", "source-starting", "destination-starting", false)
                .addProcessor("source-invalid", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("destination-invalid", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.INVALID)
                .addProcessor("source-starting", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("destination-starting", ROOT_GROUP_ID, ScheduledState.STARTING, ValidationStatus.VALID);

        final RemovedConnectionDrainClassifier.BatchResult result = classifier.classify(createImpact(List.of(invalidDestination, startingDestination), Set.of(), Set.of()), context);

        final Map<String, RemovedConnectionDrainClassifier.UnsupportedReason> reasonsByConnection = result.connectionResults().stream()
                .collect(Collectors.toMap(r -> r.connection().getConnectionInstanceId(), RemovedConnectionDrainClassifier.ConnectionResult::unsupportedReason));
        assertEquals(RemovedConnectionDrainClassifier.UnsupportedReason.DESTINATION_NOT_VALID, reasonsByConnection.get("c-invalid-destination"));
        assertEquals(RemovedConnectionDrainClassifier.UnsupportedReason.DESTINATION_NOT_RUNNING, reasonsByConnection.get("c-starting-destination"));
    }

    @Test
    public void testPortDestinationUsesRunningState() {
        final RemovedConnectionDescriptor removedConnection = createDescriptor("c-port", "c-port-v", ROOT_GROUP_ID,
                "source-port", "source-port-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                "destination-port", "destination-port-v", ROOT_GROUP_ID, ConnectableType.OUTPUT_PORT,
                RemovalReason.COMPONENT_REMOVED);

        final TestContext context = new TestContext()
                .addGroup(ROOT_GROUP_ID, null)
                .addConnection("c-port", "source-port", "destination-port", false)
                .addProcessor("source-port", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addPort("destination-port", ConnectableType.OUTPUT_PORT, ROOT_GROUP_ID, false);

        final RemovedConnectionDrainClassifier.BatchResult result = classifier.classify(createImpact(Set.of(removedConnection), Set.of(), Set.of()), context);

        assertEquals(RemovedConnectionDrainClassifier.UnsupportedReason.DESTINATION_NOT_RUNNING,
                result.getFirstUnsupportedConnectionResult().orElseThrow().unsupportedReason());
    }

    @Test
    public void testSharedProducerIsDeduplicatedAndSharedDestinationSupported() {
        final RemovedConnectionDescriptor sharedProducerA = createDescriptor("c-shared-producer-a", "c-shared-producer-a-v", ROOT_GROUP_ID,
                "shared-source", "shared-source-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                "destination-a", "destination-a-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                RemovalReason.COMPONENT_REMOVED);
        final RemovedConnectionDescriptor sharedProducerB = createDescriptor("c-shared-producer-b", "c-shared-producer-b-v", ROOT_GROUP_ID,
                "shared-source", "shared-source-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                "destination-b", "destination-b-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                RemovalReason.COMPONENT_REMOVED);
        final RemovedConnectionDescriptor sharedDestination = createDescriptor("c-shared-destination", "c-shared-destination-v", ROOT_GROUP_ID,
                "source-c", "source-c-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                "destination-a", "destination-a-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                RemovalReason.COMPONENT_REMOVED);

        final TestContext context = new TestContext()
                .addGroup(ROOT_GROUP_ID, null)
                .addConnection("c-shared-producer-a", "shared-source", "destination-a", false)
                .addConnection("c-shared-producer-b", "shared-source", "destination-b", false)
                .addConnection("c-shared-destination", "source-c", "destination-a", false)
                .addProcessor("shared-source", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("source-c", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("destination-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("destination-b", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID);

        final RemovedConnectionDrainClassifier.BatchResult result = classifier.classify(
                createImpact(List.of(sharedProducerA, sharedProducerB, sharedDestination), Set.of(), Set.of()), context);

        assertTrue(result.isSupported());
        assertEquals(Set.of("shared-source", "source-c"), result.producerBarrierComponentIds());
        assertTrue(result.connectionResults().stream().allMatch(r -> r.classification() == RemovedConnectionDrainClassifier.Classification.CANDIDATE));
    }

    @Test
    public void testChainAndCycleTopologiesAreUnsupported() {
        final RemovedConnectionDescriptor chainFirst = createDescriptor("c-chain-a", "c-chain-a-v", ROOT_GROUP_ID,
                "producer-a", "producer-a-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                "middle", "middle-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                RemovalReason.COMPONENT_REMOVED);
        final RemovedConnectionDescriptor chainSecond = createDescriptor("c-chain-b", "c-chain-b-v", ROOT_GROUP_ID,
                "middle", "middle-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                "consumer", "consumer-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                RemovalReason.COMPONENT_REMOVED);
        final RemovedConnectionDescriptor cycleFirst = createDescriptor("c-cycle-a", "c-cycle-a-v", ROOT_GROUP_ID,
                "cycle-a", "cycle-a-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                "cycle-b", "cycle-b-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                RemovalReason.COMPONENT_REMOVED);
        final RemovedConnectionDescriptor cycleSecond = createDescriptor("c-cycle-b", "c-cycle-b-v", ROOT_GROUP_ID,
                "cycle-b", "cycle-b-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                "cycle-a", "cycle-a-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                RemovalReason.COMPONENT_REMOVED);

        final TestContext context = new TestContext()
                .addGroup(ROOT_GROUP_ID, null)
                .addConnection("c-chain-a", "producer-a", "middle", false)
                .addConnection("c-chain-b", "middle", "consumer", false)
                .addConnection("c-cycle-a", "cycle-a", "cycle-b", false)
                .addConnection("c-cycle-b", "cycle-b", "cycle-a", false)
                .addProcessor("producer-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("middle", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("consumer", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("cycle-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("cycle-b", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID);

        final RemovedConnectionDrainClassifier.BatchResult result = classifier.classify(
                createImpact(List.of(chainFirst, chainSecond, cycleFirst, cycleSecond), Set.of(), Set.of()), context);

        final Map<String, RemovedConnectionDrainClassifier.Classification> classificationByConnection = result.connectionResults().stream()
                .collect(Collectors.toMap(r -> r.connection().getConnectionInstanceId(), RemovedConnectionDrainClassifier.ConnectionResult::classification));
        final Map<String, RemovedConnectionDrainClassifier.UnsupportedReason> reasonsByConnection = result.connectionResults().stream()
                .filter(r -> r.classification() == RemovedConnectionDrainClassifier.Classification.UNSUPPORTED)
                .collect(Collectors.toMap(r -> r.connection().getConnectionInstanceId(), RemovedConnectionDrainClassifier.ConnectionResult::unsupportedReason));
        assertEquals(RemovedConnectionDrainClassifier.Classification.CANDIDATE, classificationByConnection.get("c-chain-a"));
        assertEquals(RemovedConnectionDrainClassifier.UnsupportedReason.PRODUCER_BARRIER_IS_REMOVED_DESTINATION, reasonsByConnection.get("c-chain-b"));
        assertEquals(RemovedConnectionDrainClassifier.UnsupportedReason.PRODUCER_BARRIER_IS_REMOVED_DESTINATION, reasonsByConnection.get("c-cycle-a"));
        assertEquals(RemovedConnectionDrainClassifier.UnsupportedReason.PRODUCER_BARRIER_IS_REMOVED_DESTINATION, reasonsByConnection.get("c-cycle-b"));
    }

    @Test
    public void testFunnelTraversalIsCycleSafeAndRejectsFunnelSelfLoop() {
        final RemovedConnectionDescriptor supportedFunnel = createDescriptor("c-funnel", "c-funnel-v", ROOT_GROUP_ID,
                "funnel", "funnel-v", ROOT_GROUP_ID, ConnectableType.FUNNEL,
                "destination", "destination-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                RemovalReason.COMPONENT_REMOVED);
        final RemovedConnectionDescriptor selfLoopFunnel = createDescriptor("c-funnel-self-loop", "c-funnel-self-loop-v", ROOT_GROUP_ID,
                "funnel-self-loop", "funnel-self-loop-v", ROOT_GROUP_ID, ConnectableType.FUNNEL,
                "loop-destination", "loop-destination-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                RemovalReason.COMPONENT_REMOVED);

        final TestContext context = new TestContext()
                .addGroup(ROOT_GROUP_ID, null)
                .addConnection("c-funnel", "funnel", "destination", false)
                .addConnection("c-funnel-self-loop", "funnel-self-loop", "loop-destination", false)
                .addConnection("incoming-a", "upstream-a", "funnel", false)
                .addConnection("incoming-cycle-1", "funnel-cycle", "funnel", false)
                .addConnection("incoming-cycle-2", "funnel", "funnel-cycle", false)
                .addConnection("loop-incoming", "loop-destination", "funnel-self-loop", false)
                .addProcessor("upstream-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("destination", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("loop-destination", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addConnectable("funnel", ConnectableType.FUNNEL, ROOT_GROUP_ID, null, null, false)
                .addConnectable("funnel-cycle", ConnectableType.FUNNEL, ROOT_GROUP_ID, null, null, false)
                .addConnectable("funnel-self-loop", ConnectableType.FUNNEL, ROOT_GROUP_ID, null, null, false);

        context.linkIncoming("funnel", Set.of("incoming-a", "incoming-cycle-1"));
        context.linkOutgoing("funnel", Set.of("c-funnel", "incoming-cycle-2"));
        context.linkIncoming("funnel-cycle", Set.of("incoming-cycle-2"));
        context.linkOutgoing("funnel-cycle", Set.of("incoming-cycle-1"));
        context.linkIncoming("funnel-self-loop", Set.of("loop-incoming"));
        context.linkOutgoing("funnel-self-loop", Set.of("c-funnel-self-loop"));
        context.linkOutgoing("upstream-a", Set.of("incoming-a"));
        context.linkOutgoing("loop-destination", Set.of("loop-incoming"));

        final RemovedConnectionDrainClassifier.BatchResult result = classifier.classify(
                createImpact(List.of(supportedFunnel, selfLoopFunnel), Set.of(), Set.of()), context);

        final Map<String, RemovedConnectionDrainClassifier.Classification> classificationByConnection = result.connectionResults().stream()
                .collect(Collectors.toMap(r -> r.connection().getConnectionInstanceId(), RemovedConnectionDrainClassifier.ConnectionResult::classification));
        final Map<String, RemovedConnectionDrainClassifier.UnsupportedReason> reasonsByConnection = result.connectionResults().stream()
                .filter(r -> r.classification() == RemovedConnectionDrainClassifier.Classification.UNSUPPORTED)
                .collect(Collectors.toMap(r -> r.connection().getConnectionInstanceId(), RemovedConnectionDrainClassifier.ConnectionResult::unsupportedReason));
        assertEquals(RemovedConnectionDrainClassifier.Classification.CANDIDATE, classificationByConnection.get("c-funnel"));
        assertEquals(RemovedConnectionDrainClassifier.Classification.UNSUPPORTED, classificationByConnection.get("c-funnel-self-loop"));
        assertEquals(RemovedConnectionDrainClassifier.UnsupportedReason.SELF_LOOP, reasonsByConnection.get("c-funnel-self-loop"));
        assertEquals(Set.of("upstream-a"), result.connectionResults().stream()
                .filter(r -> "c-funnel".equals(r.connection().getConnectionInstanceId()))
                .findFirst().orElseThrow().producerBarrierComponentIds());
    }

    @Test
    public void testRetainedFeedbackPathIsUnsupported() {
        final RemovedConnectionDescriptor removedConnection = createDescriptor("c-feedback", "c-feedback-v", ROOT_GROUP_ID,
                "producer", "producer-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                "destination", "destination-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                RemovalReason.COMPONENT_REMOVED);

        final TestContext context = new TestContext()
                .addGroup(ROOT_GROUP_ID, null)
                .addConnection("c-feedback", "producer", "destination", false)
                .addConnection("retained-1", "destination", "retained-mid", false)
                .addConnection("retained-2", "retained-mid", "producer", false)
                .addProcessor("producer", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("destination", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("retained-mid", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID);

        context.linkOutgoing("destination", Set.of("retained-1"));
        context.linkOutgoing("retained-mid", Set.of("retained-2"));

        final RemovedConnectionDrainClassifier.BatchResult result = classifier.classify(createImpact(Set.of(removedConnection), Set.of(), Set.of()), context);

        assertFalse(result.isSupported());
        assertEquals(RemovedConnectionDrainClassifier.UnsupportedReason.RETAINED_FEEDBACK_PATH,
                result.getFirstUnsupportedConnectionResult().orElseThrow().unsupportedReason());
    }

    @Test
    public void testBatchUnsupportedResultIsDeterministic() {
        final RemovedConnectionDescriptor laterUnsupported = createDescriptor("z-unsupported", "z-unsupported-v", ROOT_GROUP_ID,
                "source-z", "source-z-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                "dest-z", "dest-z-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                RemovalReason.COMPONENT_REMOVED);
        final RemovedConnectionDescriptor earlierUnsupported = createDescriptor("a-unsupported", "a-unsupported-v", ROOT_GROUP_ID,
                "source-a", "source-a-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                "dest-a", "dest-a-v", ROOT_GROUP_ID, ConnectableType.PROCESSOR,
                RemovalReason.SOURCE_CHANGED);

        final TestContext context = new TestContext()
                .addGroup(ROOT_GROUP_ID, null)
                .addConnection("z-unsupported", "source-z", "dest-z", false)
                .addConnection("a-unsupported", "source-a", "dest-a", false)
                .addProcessor("source-z", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("dest-z", ROOT_GROUP_ID, ScheduledState.STOPPED, ValidationStatus.VALID)
                .addProcessor("source-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("dest-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID);

        final RemovedConnectionDrainClassifier.BatchResult result = classifier.classify(
                createImpact(List.of(laterUnsupported, earlierUnsupported), Set.of(), Set.of()), context);

        assertEquals("a-unsupported", result.getFirstUnsupportedConnectionResult().orElseThrow().connection().getConnectionInstanceId());
        assertEquals(RemovedConnectionDrainClassifier.UnsupportedReason.SOURCE_CHANGED_REMOVAL,
                result.getFirstUnsupportedConnectionResult().orElseThrow().unsupportedReason());
    }

    private FlowUpdateImpact createImpact(final Iterable<RemovedConnectionDescriptor> removedConnections,
                                          final Set<String> removedProcessGroupIds,
                                          final Set<String> removedEndpointIds) {
        final Set<RemovedConnectionDescriptor> descriptors = new LinkedHashSet<>();
        removedConnections.forEach(descriptors::add);
        return new FlowUpdateImpact(Set.of(), descriptors, removedProcessGroupIds, removedEndpointIds);
    }

    private RemovedConnectionDescriptor createDescriptor(final String connectionInstanceId, final String connectionVersionedId,
                                                         final String containingProcessGroupId,
                                                         final String sourceInstanceId, final String sourceVersionedId,
                                                         final String sourceProcessGroupId, final ConnectableType sourceType,
                                                         final String destinationInstanceId, final String destinationVersionedId,
                                                         final String destinationProcessGroupId, final ConnectableType destinationType,
                                                         final RemovalReason removalReason) {
        return new RemovedConnectionDescriptor(connectionInstanceId, connectionVersionedId, containingProcessGroupId,
                sourceInstanceId, sourceVersionedId, sourceProcessGroupId, sourceType,
                destinationInstanceId, destinationVersionedId, destinationProcessGroupId, destinationType,
                removalReason);
    }

    private static final class TestContext implements RemovedConnectionDrainClassifier.Context {
        private final Map<String, RemovedConnectionDrainClassifier.LiveConnection> connections = new LinkedHashMap<>();
        private final Map<String, RemovedConnectionDrainClassifier.LiveConnectable> connectables = new LinkedHashMap<>();
        private final Map<String, RemovedConnectionDrainClassifier.LiveProcessGroup> groups = new LinkedHashMap<>();

        TestContext addGroup(final String groupId, final String parentGroupId) {
            groups.put(groupId, new RemovedConnectionDrainClassifier.LiveProcessGroup(groupId, parentGroupId));
            return this;
        }

        TestContext addConnection(final String connectionId, final String sourceId, final String destinationId, final boolean queueEmpty) {
            connections.put(connectionId, new RemovedConnectionDrainClassifier.LiveConnection(connectionId, sourceId, destinationId, queueEmpty));
            return this;
        }

        TestContext addProcessor(final String connectableId, final String groupId,
                                 final ScheduledState physicalScheduledState, final ValidationStatus validationStatus) {
            connectables.put(connectableId, new RemovedConnectionDrainClassifier.LiveConnectable(connectableId, ConnectableType.PROCESSOR,
                    groupId, physicalScheduledState, validationStatus, false, Set.of(), Set.of()));
            return this;
        }

        TestContext addPort(final String connectableId, final ConnectableType connectableType, final String groupId, final boolean running) {
            connectables.put(connectableId, new RemovedConnectionDrainClassifier.LiveConnectable(connectableId, connectableType,
                    groupId, null, null, running, Set.of(), Set.of()));
            return this;
        }

        TestContext addConnectable(final String connectableId, final ConnectableType connectableType, final String groupId,
                                   final ScheduledState physicalScheduledState, final ValidationStatus validationStatus,
                                   final boolean running) {
            connectables.put(connectableId, new RemovedConnectionDrainClassifier.LiveConnectable(connectableId, connectableType,
                    groupId, physicalScheduledState, validationStatus, running, Set.of(), Set.of()));
            return this;
        }

        TestContext linkIncoming(final String connectableId, final Set<String> incomingConnectionIds) {
            final RemovedConnectionDrainClassifier.LiveConnectable connectable = connectables.get(connectableId);
            connectables.put(connectableId, new RemovedConnectionDrainClassifier.LiveConnectable(connectable.id(), connectable.type(), connectable.processGroupId(),
                    connectable.physicalScheduledState(), connectable.validationStatus(), connectable.running(), incomingConnectionIds,
                    connectable.outgoingConnectionIds()));
            return this;
        }

        TestContext linkOutgoing(final String connectableId, final Set<String> outgoingConnectionIds) {
            final RemovedConnectionDrainClassifier.LiveConnectable connectable = connectables.get(connectableId);
            connectables.put(connectableId, new RemovedConnectionDrainClassifier.LiveConnectable(connectable.id(), connectable.type(), connectable.processGroupId(),
                    connectable.physicalScheduledState(), connectable.validationStatus(), connectable.running(), connectable.incomingConnectionIds(),
                    outgoingConnectionIds));
            return this;
        }

        @Override
        public RemovedConnectionDrainClassifier.LiveConnection getConnection(final String connectionId) {
            return connections.get(connectionId);
        }

        @Override
        public RemovedConnectionDrainClassifier.LiveConnectable getConnectable(final String connectableId) {
            return connectables.get(connectableId);
        }

        @Override
        public RemovedConnectionDrainClassifier.LiveProcessGroup getProcessGroup(final String processGroupId) {
            return groups.get(processGroupId);
        }
    }
}
