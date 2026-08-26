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
import org.apache.nifi.web.api.dto.AffectedComponentDTO;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.util.ComponentLifecycle;
import org.apache.nifi.web.util.InvalidComponentAction;
import org.apache.nifi.web.util.LifecycleManagementException;
import org.apache.nifi.web.util.Pause;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RemovedConnectionDrainCoordinatorTest {
    private static final String ROOT_GROUP_ID = "root-group";
    private static final URI REQUEST_URI = URI.create("http://localhost:8080/nifi-api");

    @Test
    void testCoordinateDrainReturnsImmediatelyWhenNoCandidateConnections() throws Exception {
        final FlowUpdateImpact impact = createImpact(Set.of(), Set.of(affectedProcessor("source", "Running", 1)));
        final TestComponentLifecycle lifecycle = new TestComponentLifecycle();
        final TestCancellationHandle cancellationHandle = new TestCancellationHandle();

        final RemovedConnectionDrainCoordinator coordinator = new RemovedConnectionDrainCoordinator(
                new RemovedConnectionDrainClassifier(), new TestPauseFactory(), Duration.ofSeconds(30));

        final RemovedConnectionDrainCoordinator.DrainResult result = coordinator.coordinateDrain(
                impact, new TestContext().addGroup(ROOT_GROUP_ID, null), lifecycle, REQUEST_URI, ROOT_GROUP_ID, cancellationHandle);

        assertTrue(result.candidateConnectionIds().isEmpty());
        assertTrue(result.drainStoppedComponents().isEmpty());
        assertFalse(result.cancelled());
        assertTrue(lifecycle.scheduleCalls.isEmpty());
        assertTrue(lifecycle.queueWaitCalls.isEmpty());
    }

    @Test
    void testCoordinateDrainUsesSingleSharedProducerBarrierAndLeavesDestinationRunning() throws Exception {
        final RemovedConnectionDescriptor first = descriptor("connection-a", "shared-source", ConnectableType.PROCESSOR, "destination-a", ConnectableType.PROCESSOR, RemovalReason.COMPONENT_REMOVED);
        final RemovedConnectionDescriptor second = descriptor("connection-b", "shared-source", ConnectableType.PROCESSOR, "destination-b", ConnectableType.PROCESSOR, RemovalReason.COMPONENT_REMOVED);
        final FlowUpdateImpact impact = createImpact(Set.of(first, second), Set.of(
                affectedProcessor("shared-source", "Running", 1),
                affectedProcessor("destination-a", "Running", 2),
                affectedProcessor("destination-b", "Running", 3)));
        final TestContext context = new TestContext()
                .addGroup(ROOT_GROUP_ID, null)
                .addConnection("connection-a", "shared-source", "destination-a", false)
                .addConnection("connection-b", "shared-source", "destination-b", false)
                .addProcessor("shared-source", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("destination-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("destination-b", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID);
        final TestComponentLifecycle lifecycle = new TestComponentLifecycle();
        lifecycle.stoppedResultById.put("shared-source", affectedProcessor("shared-source", "Stopped", 0));
        lifecycle.queueWaitResult = true;

        final RemovedConnectionDrainCoordinator coordinator = new RemovedConnectionDrainCoordinator(
                new RemovedConnectionDrainClassifier(), new TestPauseFactory(), Duration.ofSeconds(30));

        final RemovedConnectionDrainCoordinator.DrainResult result = coordinator.coordinateDrain(
                impact, context, lifecycle, REQUEST_URI, ROOT_GROUP_ID, new TestCancellationHandle());

        assertFalse(result.cancelled());
        assertEquals(Set.of("connection-a", "connection-b"), result.candidateConnectionIds());
        assertEquals(List.of(new ScheduleCall(ScheduledState.STOPPED, Set.of("shared-source"))), lifecycle.scheduleCalls);
        assertEquals(List.of(Set.of("connection-a", "connection-b")), lifecycle.queueWaitCalls);
        assertEquals(Set.of("shared-source"), ids(result.drainStoppedComponents()));
    }

    @Test
    void testCoordinateDrainCancelsDuringProducerStopAndRestoresOnlyActuallyStoppedComponents() throws Exception {
        final RemovedConnectionDescriptor first = descriptor("connection-a", "source-a", ConnectableType.PROCESSOR, "destination-a", ConnectableType.PROCESSOR, RemovalReason.COMPONENT_REMOVED);
        final RemovedConnectionDescriptor second = descriptor("connection-b", "source-b", ConnectableType.PROCESSOR, "destination-b", ConnectableType.PROCESSOR, RemovalReason.COMPONENT_REMOVED);
        final FlowUpdateImpact impact = createImpact(Set.of(first, second), Set.of(
                affectedProcessor("source-a", "Running", 1),
                affectedProcessor("source-b", "Running", 1),
                affectedProcessor("destination-a", "Running", 1),
                affectedProcessor("destination-b", "Running", 1)));
        final TestContext context = new TestContext()
                .addGroup(ROOT_GROUP_ID, null)
                .addConnection("connection-a", "source-a", "destination-a", false)
                .addConnection("connection-b", "source-b", "destination-b", false)
                .addProcessor("source-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("source-b", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("destination-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("destination-b", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID);
        final TestComponentLifecycle lifecycle = new TestComponentLifecycle();
        lifecycle.cancelAfterStop = true;
        lifecycle.stoppedResultById.put("source-a", affectedProcessor("source-a", "Stopped", 0));
        lifecycle.stoppedResultById.put("source-b", affectedProcessor("source-b", "Running", 1));
        lifecycle.runningResultById.put("source-a", affectedProcessor("source-a", "Running", 1));
        lifecycle.runningResultById.put("source-b", affectedProcessor("source-b", "Running", 1));
        final TestCancellationHandle cancellationHandle = new TestCancellationHandle();
        lifecycle.cancellationHandle = cancellationHandle;

        final RemovedConnectionDrainCoordinator coordinator = new RemovedConnectionDrainCoordinator(
                new RemovedConnectionDrainClassifier(), new TestPauseFactory(), Duration.ofSeconds(30));

        final RemovedConnectionDrainCoordinator.DrainResult result = coordinator.coordinateDrain(
                impact, context, lifecycle, REQUEST_URI, ROOT_GROUP_ID, cancellationHandle);

        assertTrue(result.cancelled());
        assertEquals(List.of(
                new ScheduleCall(ScheduledState.STOPPED, Set.of("source-a", "source-b")),
                new ScheduleCall(ScheduledState.RUNNING, Set.of("source-a"))
        ), lifecycle.scheduleCalls);
        assertTrue(lifecycle.queueWaitCalls.isEmpty());
    }

    @Test
    void testCoordinateDrainCancelsDuringQueueWaitAndRestoresStoppedComponents() throws Exception {
        final RemovedConnectionDescriptor removedConnection = descriptor(
                "connection-a", "source-a", ConnectableType.PROCESSOR,
                "destination-a", ConnectableType.PROCESSOR, RemovalReason.COMPONENT_REMOVED);
        final FlowUpdateImpact impact = createImpact(Set.of(removedConnection), Set.of(
                affectedProcessor("source-a", "Running", 1),
                affectedProcessor("destination-a", "Running", 1)));
        final TestContext context = new TestContext()
                .addGroup(ROOT_GROUP_ID, null)
                .addConnection("connection-a", "source-a", "destination-a", false)
                .addProcessor("source-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("destination-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID);
        final TestComponentLifecycle lifecycle = new TestComponentLifecycle();
        lifecycle.stoppedResultById.put("source-a", affectedProcessor("source-a", "Stopped", 0));
        lifecycle.runningResultById.put("source-a", affectedProcessor("source-a", "Running", 1));
        lifecycle.cancelDuringQueueWait = true;
        final TestCancellationHandle cancellationHandle = new TestCancellationHandle();
        lifecycle.cancellationHandle = cancellationHandle;

        final RemovedConnectionDrainCoordinator coordinator = new RemovedConnectionDrainCoordinator(
                new RemovedConnectionDrainClassifier(), new TestPauseFactory(), Duration.ofSeconds(30));

        final RemovedConnectionDrainCoordinator.DrainResult result = coordinator.coordinateDrain(
                impact, context, lifecycle, REQUEST_URI, ROOT_GROUP_ID, cancellationHandle);

        assertTrue(result.cancelled());
        assertEquals(List.of(
                new ScheduleCall(ScheduledState.STOPPED, Set.of("source-a")),
                new ScheduleCall(ScheduledState.RUNNING, Set.of("source-a"))
        ), lifecycle.scheduleCalls);
        assertEquals(List.of(Set.of("connection-a")), lifecycle.queueWaitCalls);
    }

    @Test
    void testCoordinateDrainCancelsWhenQueueWaitReportsSuccessAndRestoresStoppedComponents() throws Exception {
        final RemovedConnectionDescriptor removedConnection = descriptor(
                "connection-a", "source-a", ConnectableType.PROCESSOR,
                "destination-a", ConnectableType.PROCESSOR, RemovalReason.COMPONENT_REMOVED);
        final FlowUpdateImpact impact = createImpact(Set.of(removedConnection), Set.of(
                affectedProcessor("source-a", "Running", 1),
                affectedProcessor("destination-a", "Running", 1)));
        final TestContext context = new TestContext()
                .addGroup(ROOT_GROUP_ID, null)
                .addConnection("connection-a", "source-a", "destination-a", false)
                .addProcessor("source-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("destination-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID);
        final TestComponentLifecycle lifecycle = new TestComponentLifecycle();
        lifecycle.stoppedResultById.put("source-a", affectedProcessor("source-a", "Stopped", 0));
        lifecycle.runningResultById.put("source-a", affectedProcessor("source-a", "Running", 1));
        lifecycle.cancelDuringQueueWait = true;
        lifecycle.queueWaitResult = true;
        final TestCancellationHandle cancellationHandle = new TestCancellationHandle();
        lifecycle.cancellationHandle = cancellationHandle;

        final RemovedConnectionDrainCoordinator coordinator = new RemovedConnectionDrainCoordinator(
                new RemovedConnectionDrainClassifier(), new TestPauseFactory(), Duration.ofSeconds(30));

        final RemovedConnectionDrainCoordinator.DrainResult result = coordinator.coordinateDrain(
                impact, context, lifecycle, REQUEST_URI, ROOT_GROUP_ID, cancellationHandle);

        assertTrue(result.cancelled());
        assertEquals(List.of(
                new ScheduleCall(ScheduledState.STOPPED, Set.of("source-a")),
                new ScheduleCall(ScheduledState.RUNNING, Set.of("source-a"))
        ), lifecycle.scheduleCalls);
    }

    @Test
    void testCoordinateDrainCancellationRetainsRestorationFailureWithoutRetry() throws Exception {
        final RemovedConnectionDescriptor removedConnection = descriptor(
                "connection-a", "source-a", ConnectableType.PROCESSOR,
                "destination-a", ConnectableType.PROCESSOR, RemovalReason.COMPONENT_REMOVED);
        final FlowUpdateImpact impact = createImpact(Set.of(removedConnection), Set.of(
                affectedProcessor("source-a", "Running", 1),
                affectedProcessor("destination-a", "Running", 1)));
        final TestContext context = new TestContext()
                .addGroup(ROOT_GROUP_ID, null)
                .addConnection("connection-a", "source-a", "destination-a", false)
                .addProcessor("source-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("destination-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID);
        final TestComponentLifecycle lifecycle = new TestComponentLifecycle();
        lifecycle.stoppedResultById.put("source-a", affectedProcessor("source-a", "Stopped", 0));
        lifecycle.cancelDuringQueueWait = true;
        lifecycle.restoreException = new LifecycleManagementException("Failed to restore stopped producers");
        final TestCancellationHandle cancellationHandle = new TestCancellationHandle();
        lifecycle.cancellationHandle = cancellationHandle;

        final RemovedConnectionDrainCoordinator coordinator = new RemovedConnectionDrainCoordinator(
                new RemovedConnectionDrainClassifier(), new TestPauseFactory(), Duration.ofSeconds(30));

        final RemovedConnectionDrainCoordinator.DrainResult result = coordinator.coordinateDrain(
                impact, context, lifecycle, REQUEST_URI, ROOT_GROUP_ID, cancellationHandle);

        assertTrue(result.cancelled());
        assertEquals("Failed to restore stopped producers", result.restorationFailure().getMessage());
        assertEquals(List.of(
                new ScheduleCall(ScheduledState.STOPPED, Set.of("source-a")),
                new ScheduleCall(ScheduledState.RUNNING, Set.of("source-a"))
        ), lifecycle.scheduleCalls);
    }

    @Test
    void testCoordinateDrainCancelDuringStopThrowReturnsCancellationResultWithRestorationFailure() throws Exception {
        final RemovedConnectionDescriptor removedConnection = descriptor(
                "connection-a", "funnel-source", ConnectableType.FUNNEL,
                "destination-a", ConnectableType.PROCESSOR, RemovalReason.COMPONENT_REMOVED);
        final FlowUpdateImpact impact = createImpact(Set.of(removedConnection), Set.of(
                affectedProcessor("upstream-a", "Running", 1),
                affectedProcessor("upstream-b", "Running", 1),
                affectedProcessor("destination-a", "Running", 1)));
        final TestContext context = new TestContext()
                .addGroup(ROOT_GROUP_ID, null)
                .addConnection("connection-a", "funnel-source", "destination-a", false)
                .addConnection("incoming-a", "upstream-a", "funnel-source", false)
                .addConnection("incoming-b", "upstream-b", "funnel-source", false)
                .addProcessor("upstream-a", ROOT_GROUP_ID, ScheduledState.STOPPED, ValidationStatus.VALID)
                .addProcessor("upstream-b", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("destination-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addConnectable("funnel-source", ConnectableType.FUNNEL, ROOT_GROUP_ID, null, null, false);
        context.linkIncoming("funnel-source", Set.of("incoming-a", "incoming-b"));
        context.linkOutgoing("upstream-a", Set.of("incoming-a"));
        context.linkOutgoing("upstream-b", Set.of("incoming-b"));

        final TestComponentLifecycle lifecycle = new TestComponentLifecycle();
        lifecycle.cancelAfterStop = true;
        lifecycle.stopException = new LifecycleManagementException("Failed while waiting for components to transition to state of STOPPED");
        lifecycle.restoreException = new LifecycleManagementException("Failed to restore stopped producers");
        final TestCancellationHandle cancellationHandle = new TestCancellationHandle();
        lifecycle.cancellationHandle = cancellationHandle;

        final RemovedConnectionDrainCoordinator coordinator = new RemovedConnectionDrainCoordinator(
                new RemovedConnectionDrainClassifier(), new TestPauseFactory(), Duration.ofSeconds(30));

        final RemovedConnectionDrainCoordinator.DrainResult result = coordinator.coordinateDrain(
                impact, context, lifecycle, REQUEST_URI, ROOT_GROUP_ID, cancellationHandle);

        assertTrue(result.cancelled());
        assertEquals(Set.of("connection-a"), result.candidateConnectionIds());
        assertEquals("Failed to restore stopped producers", result.restorationFailure().getMessage());
        assertEquals(List.of(
                new ScheduleCall(ScheduledState.STOPPED, Set.of("upstream-a", "upstream-b")),
                new ScheduleCall(ScheduledState.RUNNING, Set.of("upstream-a"))
        ), lifecycle.scheduleCalls);
        assertTrue(lifecycle.queueWaitCalls.isEmpty());
    }

    @Test
    void testCoordinateDrainTimesOutAcrossStopAndQueueWaitUsingSingleDeadline() {
        final RemovedConnectionDescriptor removedConnection = descriptor(
                "connection-a", "source-a", ConnectableType.PROCESSOR,
                "destination-a", ConnectableType.PROCESSOR, RemovalReason.COMPONENT_REMOVED);
        final FlowUpdateImpact impact = createImpact(Set.of(removedConnection), Set.of(
                affectedProcessor("source-a", "Running", 1),
                affectedProcessor("destination-a", "Running", 1)));
        final TestContext context = new TestContext()
                .addGroup(ROOT_GROUP_ID, null)
                .addConnection("connection-a", "source-a", "destination-a", false)
                .addProcessor("source-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("destination-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID);
        final SequencedPauseFactory pauseFactory = new SequencedPauseFactory();
        final RecordingDeadlinePause deadlinePause = new RecordingDeadlinePause(List.of(true, false));
        pauseFactory.drainPause = deadlinePause;
        final TestComponentLifecycle lifecycle = new TestComponentLifecycle();
        lifecycle.stoppedResultById.put("source-a", affectedProcessor("source-a", "Stopped", 0));
        lifecycle.queueWaitUsesPause = true;

        final RemovedConnectionDrainCoordinator coordinator = new RemovedConnectionDrainCoordinator(
                new RemovedConnectionDrainClassifier(), pauseFactory, Duration.ofSeconds(30));

        final LifecycleManagementException exception = assertThrows(LifecycleManagementException.class, () -> coordinator.coordinateDrain(
                impact, context, lifecycle, REQUEST_URI, ROOT_GROUP_ID, new TestCancellationHandle()));

        assertTrue(exception.getMessage().contains("Removed connection drain timed out"));
        assertTrue(exception.getMessage().contains("connectionIds=[connection-a]"));
        assertSame(deadlinePause, lifecycle.queueWaitPause);
        assertEquals(3, deadlinePause.pauseInvocations);
        assertEquals(1, pauseFactory.restorationPauseCreations);
        assertEquals(List.of(
                new ScheduleCall(ScheduledState.STOPPED, Set.of("source-a")),
                new ScheduleCall(ScheduledState.RUNNING, Set.of("source-a"))
        ), lifecycle.scheduleCalls);
    }

    @Test
    void testCoordinateDrainRestoresStoppedComponentsWhenQueueWaitFails() {
        final RemovedConnectionDescriptor removedConnection = descriptor(
                "connection-a", "source-a", ConnectableType.PROCESSOR,
                "destination-a", ConnectableType.PROCESSOR, RemovalReason.COMPONENT_REMOVED);
        final FlowUpdateImpact impact = createImpact(Set.of(removedConnection), Set.of(
                affectedProcessor("source-a", "Running", 1),
                affectedProcessor("destination-a", "Running", 1)));
        final TestContext context = new TestContext()
                .addGroup(ROOT_GROUP_ID, null)
                .addConnection("connection-a", "source-a", "destination-a", false)
                .addProcessor("source-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("destination-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID);
        final TestComponentLifecycle lifecycle = new TestComponentLifecycle();
        lifecycle.stoppedResultById.put("source-a", affectedProcessor("source-a", "Stopped", 0));
        lifecycle.runningResultById.put("source-a", affectedProcessor("source-a", "Running", 1));
        lifecycle.queueWaitException = new LifecycleManagementException("Interrupted while waiting for connection queues to empty");

        final RemovedConnectionDrainCoordinator coordinator = new RemovedConnectionDrainCoordinator(
                new RemovedConnectionDrainClassifier(), new TestPauseFactory(), Duration.ofSeconds(30));

        final LifecycleManagementException exception = assertThrows(LifecycleManagementException.class, () -> coordinator.coordinateDrain(
                impact, context, lifecycle, REQUEST_URI, ROOT_GROUP_ID, new TestCancellationHandle()));

        assertTrue(exception.getMessage().contains("Removed connection drain failed while waiting for connections [connection-a]"));
        assertEquals(List.of(
                new ScheduleCall(ScheduledState.STOPPED, Set.of("source-a")),
                new ScheduleCall(ScheduledState.RUNNING, Set.of("source-a"))
        ), lifecycle.scheduleCalls);
    }

    @Test
    void testCoordinateDrainAddsRestorationFailureAsSuppressed() {
        final RemovedConnectionDescriptor removedConnection = descriptor(
                "connection-a", "source-a", ConnectableType.PROCESSOR,
                "destination-a", ConnectableType.PROCESSOR, RemovalReason.COMPONENT_REMOVED);
        final FlowUpdateImpact impact = createImpact(Set.of(removedConnection), Set.of(
                affectedProcessor("source-a", "Running", 1),
                affectedProcessor("destination-a", "Running", 1)));
        final TestContext context = new TestContext()
                .addGroup(ROOT_GROUP_ID, null)
                .addConnection("connection-a", "source-a", "destination-a", false)
                .addProcessor("source-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("destination-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID);
        final TestComponentLifecycle lifecycle = new TestComponentLifecycle();
        lifecycle.stoppedResultById.put("source-a", affectedProcessor("source-a", "Stopped", 0));
        lifecycle.queueWaitResult = false;
        lifecycle.restoreException = new LifecycleManagementException("Failed to restore stopped producers");

        final RemovedConnectionDrainCoordinator coordinator = new RemovedConnectionDrainCoordinator(
                new RemovedConnectionDrainClassifier(), new TestPauseFactory(), Duration.ofSeconds(30));

        final LifecycleManagementException exception = assertThrows(LifecycleManagementException.class, () -> coordinator.coordinateDrain(
                impact, context, lifecycle, REQUEST_URI, ROOT_GROUP_ID, new TestCancellationHandle()));

        assertEquals(1, exception.getSuppressed().length);
        assertEquals("Failed to restore stopped producers", exception.getSuppressed()[0].getMessage());
        assertTrue(exception.getMessage().contains("Removed connection drain timed out"));
    }

    @Test
    void testCoordinateDrainRestoresStoppedComponentsWhenQueueWaitThrowsRuntimeException() {
        final RemovedConnectionDescriptor removedConnection = descriptor(
                "connection-a", "source-a", ConnectableType.PROCESSOR,
                "destination-a", ConnectableType.PROCESSOR, RemovalReason.COMPONENT_REMOVED);
        final FlowUpdateImpact impact = createImpact(Set.of(removedConnection), Set.of(
                affectedProcessor("source-a", "Running", 1),
                affectedProcessor("destination-a", "Running", 1)));
        final TestContext context = new TestContext()
                .addGroup(ROOT_GROUP_ID, null)
                .addConnection("connection-a", "source-a", "destination-a", false)
                .addProcessor("source-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("destination-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID);
        final TestComponentLifecycle lifecycle = new TestComponentLifecycle();
        lifecycle.stoppedResultById.put("source-a", affectedProcessor("source-a", "Stopped", 0));
        lifecycle.runningResultById.put("source-a", affectedProcessor("source-a", "Running", 1));
        lifecycle.queueWaitRuntimeException = new IllegalStateException("Cluster membership changed");

        final RemovedConnectionDrainCoordinator coordinator = new RemovedConnectionDrainCoordinator(
                new RemovedConnectionDrainClassifier(), new TestPauseFactory(), Duration.ofSeconds(30));

        final LifecycleManagementException exception = assertThrows(LifecycleManagementException.class, () -> coordinator.coordinateDrain(
                impact, context, lifecycle, REQUEST_URI, ROOT_GROUP_ID, new TestCancellationHandle()));

        assertSame(lifecycle.queueWaitRuntimeException, exception.getCause());
        assertTrue(exception.getMessage().contains("Removed connection drain failed for connections [connection-a]"));
        assertEquals(List.of(
                new ScheduleCall(ScheduledState.STOPPED, Set.of("source-a")),
                new ScheduleCall(ScheduledState.RUNNING, Set.of("source-a"))
        ), lifecycle.scheduleCalls);
    }

    @Test
    void testCoordinateDrainRetainsRestorationFailureWhenQueueWaitThrowsRuntimeException() {
        final RemovedConnectionDescriptor removedConnection = descriptor(
                "connection-a", "source-a", ConnectableType.PROCESSOR,
                "destination-a", ConnectableType.PROCESSOR, RemovalReason.COMPONENT_REMOVED);
        final FlowUpdateImpact impact = createImpact(Set.of(removedConnection), Set.of(
                affectedProcessor("source-a", "Running", 1),
                affectedProcessor("destination-a", "Running", 1)));
        final TestContext context = new TestContext()
                .addGroup(ROOT_GROUP_ID, null)
                .addConnection("connection-a", "source-a", "destination-a", false)
                .addProcessor("source-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("destination-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID);
        final TestComponentLifecycle lifecycle = new TestComponentLifecycle();
        lifecycle.stoppedResultById.put("source-a", affectedProcessor("source-a", "Stopped", 0));
        lifecycle.queueWaitRuntimeException = new IllegalStateException("Cluster membership changed");
        lifecycle.restoreException = new LifecycleManagementException("Failed to restore stopped producers");

        final RemovedConnectionDrainCoordinator coordinator = new RemovedConnectionDrainCoordinator(
                new RemovedConnectionDrainClassifier(), new TestPauseFactory(), Duration.ofSeconds(30));

        final LifecycleManagementException exception = assertThrows(LifecycleManagementException.class, () -> coordinator.coordinateDrain(
                impact, context, lifecycle, REQUEST_URI, ROOT_GROUP_ID, new TestCancellationHandle()));

        assertEquals(1, exception.getSuppressed().length);
        assertEquals("Failed to restore stopped producers", exception.getSuppressed()[0].getMessage());
        assertEquals(List.of(
                new ScheduleCall(ScheduledState.STOPPED, Set.of("source-a")),
                new ScheduleCall(ScheduledState.RUNNING, Set.of("source-a"))
        ), lifecycle.scheduleCalls);
    }

    @Test
    void testCoordinateDrainRestoresOnlyLiveStoppedProducerWhenStopThrowsAfterPartialTransition() {
        final RemovedConnectionDescriptor removedConnection = descriptor(
                "connection-a", "funnel-source", ConnectableType.FUNNEL,
                "destination-a", ConnectableType.PROCESSOR, RemovalReason.COMPONENT_REMOVED);
        final FlowUpdateImpact impact = createImpact(Set.of(removedConnection), Set.of(
                affectedProcessor("upstream-a", "Running", 1),
                affectedProcessor("upstream-b", "Running", 1),
                affectedProcessor("destination-a", "Running", 1)));
        final TestContext context = new TestContext()
                .addGroup(ROOT_GROUP_ID, null)
                .addConnection("connection-a", "funnel-source", "destination-a", false)
                .addConnection("incoming-a", "upstream-a", "funnel-source", false)
                .addConnection("incoming-b", "upstream-b", "funnel-source", false)
                .addProcessor("upstream-a", ROOT_GROUP_ID, ScheduledState.STOPPED, ValidationStatus.VALID)
                .addProcessor("upstream-b", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("destination-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addConnectable("funnel-source", ConnectableType.FUNNEL, ROOT_GROUP_ID, null, null, false);
        context.linkIncoming("funnel-source", Set.of("incoming-a", "incoming-b"));
        context.linkOutgoing("upstream-a", Set.of("incoming-a"));
        context.linkOutgoing("upstream-b", Set.of("incoming-b"));

        final TestComponentLifecycle lifecycle = new TestComponentLifecycle();
        lifecycle.stopException = new LifecycleManagementException("Failed while waiting for components to transition to state of STOPPED");
        lifecycle.runningResultById.put("upstream-a", affectedProcessor("upstream-a", "Running", 1));

        final RemovedConnectionDrainCoordinator coordinator = new RemovedConnectionDrainCoordinator(
                new RemovedConnectionDrainClassifier(), new TestPauseFactory(), Duration.ofSeconds(30));

        final LifecycleManagementException exception = assertThrows(LifecycleManagementException.class, () -> coordinator.coordinateDrain(
                impact, context, lifecycle, REQUEST_URI, ROOT_GROUP_ID, new TestCancellationHandle()));

        assertEquals("Removed connection drain failed while stopping producer barriers [upstream-a, upstream-b]: Failed while waiting for components to transition to state of STOPPED",
                exception.getMessage());
        assertSame(lifecycle.stopException, exception.getCause());
        assertEquals(List.of(
                new ScheduleCall(ScheduledState.STOPPED, Set.of("upstream-a", "upstream-b")),
                new ScheduleCall(ScheduledState.RUNNING, Set.of("upstream-a"))
        ), lifecycle.scheduleCalls);
        assertTrue(lifecycle.queueWaitCalls.isEmpty());
    }

    @Test
    void testCoordinateDrainRestoresStoppingProcessorWhenStopThrowsAfterPartialTransition() {
        final RemovedConnectionDescriptor removedConnection = descriptor(
                "connection-a", "source-a", ConnectableType.PROCESSOR,
                "destination-a", ConnectableType.PROCESSOR, RemovalReason.COMPONENT_REMOVED);
        final FlowUpdateImpact impact = createImpact(Set.of(removedConnection), Set.of(
                affectedProcessor("source-a", "Running", 1),
                affectedProcessor("destination-a", "Running", 1)));
        final TestContext context = new TestContext()
                .addGroup(ROOT_GROUP_ID, null)
                .addConnection("connection-a", "source-a", "destination-a", false)
                .addProcessor("source-a", ROOT_GROUP_ID, ScheduledState.STOPPING, ValidationStatus.VALID)
                .addProcessor("destination-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID);

        final TestComponentLifecycle lifecycle = new TestComponentLifecycle();
        lifecycle.stopException = new LifecycleManagementException("Failed while waiting for components to transition to state of STOPPED");
        lifecycle.runningResultById.put("source-a", affectedProcessor("source-a", "Running", 1));

        final RemovedConnectionDrainCoordinator coordinator = new RemovedConnectionDrainCoordinator(
                new RemovedConnectionDrainClassifier(), new TestPauseFactory(), Duration.ofSeconds(30));

        final LifecycleManagementException exception = assertThrows(LifecycleManagementException.class, () -> coordinator.coordinateDrain(
                impact, context, lifecycle, REQUEST_URI, ROOT_GROUP_ID, new TestCancellationHandle()));

        assertTrue(exception.getMessage().contains("Removed connection drain failed while stopping producer barriers [source-a]"));
        assertEquals(List.of(
                new ScheduleCall(ScheduledState.STOPPED, Set.of("source-a")),
                new ScheduleCall(ScheduledState.RUNNING, Set.of("source-a"))
        ), lifecycle.scheduleCalls);
        assertTrue(lifecycle.queueWaitCalls.isEmpty());
    }

    @Test
    void testCoordinateDrainRestoresStoppedInputPortWhenStopThrowsAfterPartialTransition() {
        final RemovedConnectionDescriptor removedConnection = descriptor(
                "connection-a", "input-port-a", ConnectableType.INPUT_PORT,
                "destination-a", ConnectableType.PROCESSOR, RemovalReason.COMPONENT_REMOVED);
        final FlowUpdateImpact impact = createImpact(Set.of(removedConnection), Set.of(
                affectedPort("input-port-a", AffectedComponentDTO.COMPONENT_TYPE_INPUT_PORT, "Running"),
                affectedProcessor("destination-a", "Running", 1)));
        final TestContext context = new TestContext()
                .addGroup(ROOT_GROUP_ID, null)
                .addConnection("connection-a", "input-port-a", "destination-a", false)
                .addConnectable("input-port-a", ConnectableType.INPUT_PORT, ROOT_GROUP_ID, null, null, false)
                .addProcessor("destination-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID);

        final TestComponentLifecycle lifecycle = new TestComponentLifecycle();
        lifecycle.stopException = new LifecycleManagementException("Failed while waiting for components to transition to state of STOPPED");
        lifecycle.runningResultById.put("input-port-a", affectedPort("input-port-a", AffectedComponentDTO.COMPONENT_TYPE_INPUT_PORT, "Running"));

        final RemovedConnectionDrainCoordinator coordinator = new RemovedConnectionDrainCoordinator(
                new RemovedConnectionDrainClassifier(), new TestPauseFactory(), Duration.ofSeconds(30));

        final LifecycleManagementException exception = assertThrows(LifecycleManagementException.class, () -> coordinator.coordinateDrain(
                impact, context, lifecycle, REQUEST_URI, ROOT_GROUP_ID, new TestCancellationHandle()));

        assertTrue(exception.getMessage().contains("Removed connection drain failed while stopping producer barriers [input-port-a]"));
        assertEquals(List.of(
                new ScheduleCall(ScheduledState.STOPPED, Set.of("input-port-a")),
                new ScheduleCall(ScheduledState.RUNNING, Set.of("input-port-a"))
        ), lifecycle.scheduleCalls);
        assertTrue(lifecycle.queueWaitCalls.isEmpty());
    }

    @Test
    void testAlreadyStoppedProducerIsNotStoppedOrRestarted() throws Exception {
        final RemovedConnectionDescriptor removedConnection = descriptor(
                "connection-a", "source-a", ConnectableType.PROCESSOR,
                "destination-a", ConnectableType.PROCESSOR, RemovalReason.COMPONENT_REMOVED);
        final FlowUpdateImpact impact = createImpact(Set.of(removedConnection), Set.of(
                affectedProcessor("source-a", "Stopped", 0),
                affectedProcessor("destination-a", "Running", 1)));
        final TestContext context = new TestContext()
                .addGroup(ROOT_GROUP_ID, null)
                .addConnection("connection-a", "source-a", "destination-a", false)
                .addProcessor("source-a", ROOT_GROUP_ID, ScheduledState.STOPPED, ValidationStatus.VALID)
                .addProcessor("destination-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID);
        final TestComponentLifecycle lifecycle = new TestComponentLifecycle();
        lifecycle.queueWaitResult = true;

        final RemovedConnectionDrainCoordinator coordinator = new RemovedConnectionDrainCoordinator(
                new RemovedConnectionDrainClassifier(), new TestPauseFactory(), Duration.ofSeconds(30));

        final RemovedConnectionDrainCoordinator.DrainResult result = coordinator.coordinateDrain(
                impact, context, lifecycle, REQUEST_URI, ROOT_GROUP_ID, new TestCancellationHandle());

        assertTrue(lifecycle.scheduleCalls.isEmpty());
        assertEquals(Set.of("connection-a"), result.candidateConnectionIds());
        assertTrue(result.drainStoppedComponents().isEmpty());
    }

    @Test
    void testUnsupportedClassificationFailsWithoutAnyMutation() {
        final RemovedConnectionDescriptor removedConnection = descriptor(
                "connection-a", "source-a", ConnectableType.PROCESSOR,
                "destination-a", ConnectableType.PROCESSOR, RemovalReason.SOURCE_CHANGED);
        final FlowUpdateImpact impact = createImpact(Set.of(removedConnection), Set.of(
                affectedProcessor("source-a", "Running", 1),
                affectedProcessor("destination-a", "Running", 1)));
        final TestContext context = new TestContext()
                .addGroup(ROOT_GROUP_ID, null)
                .addConnection("connection-a", "source-a", "destination-a", false)
                .addProcessor("source-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID)
                .addProcessor("destination-a", ROOT_GROUP_ID, ScheduledState.RUNNING, ValidationStatus.VALID);
        final TestComponentLifecycle lifecycle = new TestComponentLifecycle();

        final RemovedConnectionDrainCoordinator coordinator = new RemovedConnectionDrainCoordinator(
                new RemovedConnectionDrainClassifier(), new TestPauseFactory(), Duration.ofSeconds(30));

        final LifecycleManagementException exception = assertThrows(LifecycleManagementException.class, () -> coordinator.coordinateDrain(
                impact, context, lifecycle, REQUEST_URI, ROOT_GROUP_ID, new TestCancellationHandle()));

        assertTrue(exception.getMessage().contains("connection-a[reason=SOURCE_CHANGED_REMOVAL]"));
        assertTrue(lifecycle.scheduleCalls.isEmpty());
        assertTrue(lifecycle.queueWaitCalls.isEmpty());
    }

    private FlowUpdateImpact createImpact(final Set<RemovedConnectionDescriptor> removedConnections, final Set<AffectedComponentEntity> affectedComponents) {
        return new FlowUpdateImpact(affectedComponents, removedConnections, Set.of(), Set.of());
    }

    private RemovedConnectionDescriptor descriptor(final String connectionId, final String sourceId, final ConnectableType sourceType,
                                                   final String destinationId, final ConnectableType destinationType,
                                                   final RemovalReason removalReason) {
        return new RemovedConnectionDescriptor(connectionId, connectionId + "-v", ROOT_GROUP_ID,
                sourceId, sourceId + "-v", ROOT_GROUP_ID, sourceType,
                destinationId, destinationId + "-v", ROOT_GROUP_ID, destinationType,
                removalReason);
    }

    private AffectedComponentEntity affectedProcessor(final String id, final String state, final int activeThreadCount) {
        final AffectedComponentDTO dto = new AffectedComponentDTO();
        dto.setId(id);
        dto.setName(id);
        dto.setProcessGroupId(ROOT_GROUP_ID);
        dto.setReferenceType(AffectedComponentDTO.COMPONENT_TYPE_PROCESSOR);
        dto.setState(state);
        dto.setActiveThreadCount(activeThreadCount);

        final AffectedComponentEntity entity = new AffectedComponentEntity();
        entity.setId(id);
        entity.setReferenceType(AffectedComponentDTO.COMPONENT_TYPE_PROCESSOR);
        entity.setComponent(dto);
        return entity;
    }

    private AffectedComponentEntity affectedPort(final String id, final String referenceType, final String state) {
        final AffectedComponentDTO dto = new AffectedComponentDTO();
        dto.setId(id);
        dto.setName(id);
        dto.setProcessGroupId(ROOT_GROUP_ID);
        dto.setReferenceType(referenceType);
        dto.setState(state);

        final AffectedComponentEntity entity = new AffectedComponentEntity();
        entity.setId(id);
        entity.setReferenceType(referenceType);
        entity.setComponent(dto);
        return entity;
    }

    private Set<String> ids(final Set<AffectedComponentEntity> components) {
        final Set<String> ids = new LinkedHashSet<>();
        for (final AffectedComponentEntity component : components) {
            ids.add(component.getId());
        }
        return ids;
    }

    private record ScheduleCall(ScheduledState state, Set<String> componentIds) {
        ScheduleCall {
            componentIds = Set.copyOf(componentIds);
        }
    }

    private static final class TestComponentLifecycle implements ComponentLifecycle {
        private final List<ScheduleCall> scheduleCalls = new ArrayList<>();
        private final List<Set<String>> queueWaitCalls = new ArrayList<>();
        private final Map<String, AffectedComponentEntity> stoppedResultById = new LinkedHashMap<>();
        private final Map<String, AffectedComponentEntity> runningResultById = new LinkedHashMap<>();
        private final Set<String> initiallyEmptyConnectionIds = new LinkedHashSet<>();
        private boolean queueWaitResult;
        private boolean cancelAfterStop;
        private boolean cancelDuringQueueWait;
        private boolean queueWaitUsesPause;
        private LifecycleManagementException stopException;
        private LifecycleManagementException queueWaitException;
        private RuntimeException queueWaitRuntimeException;
        private LifecycleManagementException restoreException;
        private TestCancellationHandle cancellationHandle;
        private Pause queueWaitPause;

        @Override
        public Set<AffectedComponentEntity> scheduleComponents(final URI exampleUri, final String groupId, final Set<AffectedComponentEntity> components,
                                                               final ScheduledState desiredState, final Pause pause,
                                                               final InvalidComponentAction invalidComponentAction) throws LifecycleManagementException {
            final Set<String> componentIds = ids(components);
            scheduleCalls.add(new ScheduleCall(desiredState, componentIds));

            if (desiredState == ScheduledState.RUNNING && restoreException != null) {
                throw restoreException;
            }

            if (desiredState == ScheduledState.STOPPED && cancelAfterStop && cancellationHandle != null) {
                cancellationHandle.cancel();
            }

            final Map<String, AffectedComponentEntity> resultsById = desiredState == ScheduledState.STOPPED ? stoppedResultById : runningResultById;
            final Set<AffectedComponentEntity> results = new LinkedHashSet<>();
            for (final AffectedComponentEntity component : components) {
                results.add(resultsById.getOrDefault(component.getId(), component));
            }

            if (desiredState == ScheduledState.STOPPED && stopException != null) {
                throw stopException;
            }

            return results;
        }

        @Override
        public Set<AffectedComponentEntity> activateControllerServices(final URI exampleUri, final String groupId,
                                                                       final Set<AffectedComponentEntity> servicesToUpdate,
                                                                       final Set<AffectedComponentEntity> servicesRequiringDesiredState,
                                                                       final org.apache.nifi.controller.service.ControllerServiceState desiredState,
                                                                       final Pause pause,
                                                                       final InvalidComponentAction invalidComponentAction) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean waitForConnectionQueuesEmpty(final URI exampleUri, final Set<String> connectionIds, final Pause pause) throws LifecycleManagementException {
            if (connectionIds.size() == 1 && !pause.pause()) {
                return initiallyEmptyConnectionIds.contains(connectionIds.iterator().next());
            }

            queueWaitCalls.add(Set.copyOf(connectionIds));
            queueWaitPause = pause;

            if (queueWaitException != null) {
                throw queueWaitException;
            }

            if (queueWaitRuntimeException != null) {
                throw queueWaitRuntimeException;
            }

            if (cancelDuringQueueWait && cancellationHandle != null) {
                cancellationHandle.cancel();
            }

            if (queueWaitUsesPause) {
                pause.pause();
                return pause.pause();
            }

            return queueWaitResult;
        }

        private Set<String> ids(final Set<AffectedComponentEntity> components) {
            final Set<String> ids = new LinkedHashSet<>();
            for (final AffectedComponentEntity component : components) {
                ids.add(component.getId());
            }
            return ids;
        }
    }

    private static final class TestCancellationHandle implements RemovedConnectionDrainCoordinator.CancellationHandle {
        private boolean cancelled;
        private Runnable cancelCallback;

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public void setCancelCallback(final Runnable runnable) {
            this.cancelCallback = runnable;
        }

        private void cancel() {
            cancelled = true;
            if (cancelCallback != null) {
                cancelCallback.run();
            }
        }
    }

    private static class TestPauseFactory implements RemovedConnectionDrainCoordinator.PauseFactory {
        @Override
        public RemovedConnectionDrainCoordinator.DeadlinePause createDrainPause(final Duration timeout) {
            return new RecordingDeadlinePause(List.of(true));
        }

        @Override
        public Pause createRestorationPause() {
            return () -> true;
        }
    }

    private static final class SequencedPauseFactory extends TestPauseFactory {
        private RecordingDeadlinePause drainPause;
        private int restorationPauseCreations;

        @Override
        public RemovedConnectionDrainCoordinator.DeadlinePause createDrainPause(final Duration timeout) {
            return drainPause;
        }

        @Override
        public Pause createRestorationPause() {
            restorationPauseCreations++;
            return () -> true;
        }
    }

    private static final class RecordingDeadlinePause implements RemovedConnectionDrainCoordinator.DeadlinePause {
        private final List<Boolean> decisions;
        private int index;
        private int pauseInvocations;
        private boolean cancelled;

        private RecordingDeadlinePause(final List<Boolean> decisions) {
            this.decisions = decisions;
        }

        @Override
        public void cancel() {
            cancelled = true;
        }

        @Override
        public boolean pause() {
            pauseInvocations++;
            if (cancelled) {
                return false;
            }

            if (index >= decisions.size()) {
                return false;
            }

            return decisions.get(index++);
        }
    }

    private static final class TestContext implements RemovedConnectionDrainClassifier.Context {
        private final Map<String, RemovedConnectionDrainClassifier.LiveConnection> connections = new LinkedHashMap<>();
        private final Map<String, RemovedConnectionDrainClassifier.LiveConnectable> connectables = new LinkedHashMap<>();
        private final Map<String, RemovedConnectionDrainClassifier.LiveProcessGroup> groups = new LinkedHashMap<>();

        private TestContext addGroup(final String groupId, final String parentGroupId) {
            groups.put(groupId, new RemovedConnectionDrainClassifier.LiveProcessGroup(groupId, parentGroupId));
            return this;
        }

        private TestContext addConnection(final String connectionId, final String sourceId, final String destinationId, final boolean queueEmpty) {
            connections.put(connectionId, new RemovedConnectionDrainClassifier.LiveConnection(connectionId, sourceId, destinationId, queueEmpty));
            return this;
        }

        private TestContext addProcessor(final String connectableId, final String groupId,
                                         final ScheduledState physicalScheduledState, final ValidationStatus validationStatus) {
            connectables.put(connectableId, new RemovedConnectionDrainClassifier.LiveConnectable(connectableId, ConnectableType.PROCESSOR,
                    groupId, physicalScheduledState, validationStatus, false, Set.of(), Set.of()));
            return this;
        }

        private TestContext addConnectable(final String connectableId, final ConnectableType connectableType, final String groupId,
                                           final ScheduledState physicalScheduledState, final ValidationStatus validationStatus,
                                           final boolean running) {
            connectables.put(connectableId, new RemovedConnectionDrainClassifier.LiveConnectable(connectableId, connectableType,
                    groupId, physicalScheduledState, validationStatus, running, Set.of(), Set.of()));
            return this;
        }

        private TestContext linkIncoming(final String connectableId, final Set<String> incomingConnectionIds) {
            final RemovedConnectionDrainClassifier.LiveConnectable connectable = connectables.get(connectableId);
            connectables.put(connectableId, new RemovedConnectionDrainClassifier.LiveConnectable(connectable.id(), connectable.type(), connectable.processGroupId(),
                    connectable.physicalScheduledState(), connectable.validationStatus(), connectable.running(), incomingConnectionIds,
                    connectable.outgoingConnectionIds()));
            return this;
        }

        private TestContext linkOutgoing(final String connectableId, final Set<String> outgoingConnectionIds) {
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
