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
package org.apache.nifi.web.api;

import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.web.FlowUpdateImpact;
import org.apache.nifi.web.RemovedConnectionDescriptor;
import org.apache.nifi.web.RemovedConnectionDrainCoordinator;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.concurrent.AsynchronousWebRequest;
import org.apache.nifi.web.api.concurrent.StandardAsynchronousWebRequest;
import org.apache.nifi.web.api.concurrent.UpdateStep;
import org.apache.nifi.web.api.dto.AffectedComponentDTO;
import org.apache.nifi.web.api.dto.FlowUpdateRequestDTO;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.Entity;
import org.apache.nifi.web.api.entity.FlowUpdateRequestEntity;
import org.apache.nifi.web.api.entity.ProcessGroupDescriptorEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.util.ComponentLifecycle;
import org.apache.nifi.web.util.InvalidComponentAction;
import org.apache.nifi.web.util.LifecycleManagementException;
import org.apache.nifi.web.util.Pause;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FlowUpdateResourceTest {
    private static final String GROUP_ID = "group-id";
    private static final URI REQUEST_URI = URI.create("http://localhost:8080/nifi-api/process-groups/group-id");
    private static final List<String> STANDARD_STEPS = List.of(
            "Stopping Affected Processors",
            "Disabling Affected Controller Services",
            "Updating Flow",
            "Re-Enabling Controller Services",
            "Restarting Affected Processors");

    @Test
    void testRegistryUpdateHasRemovedConnectionDrainStep() {
        assertEquals(List.of(
                "Draining Removed Connections",
                "Stopping Affected Processors",
                "Disabling Affected Controller Services",
                "Updating Flow",
                "Re-Enabling Controller Services",
                "Restarting Affected Processors"), getStepDescriptions(FlowUpdateResource.UPDATE_REQUEST_TYPE));
    }

    @Test
    void testNonUpdateRequestsRetainStandardSteps() {
        assertEquals(STANDARD_STEPS, getStepDescriptions("revert-requests"));
        assertEquals(STANDARD_STEPS, getStepDescriptions("rebase-requests"));
        assertEquals(STANDARD_STEPS, getStepDescriptions("replace-requests"));
    }

    @Test
    void testCancelledBeforeStopAllRestoresExactDrainStoppedComponents() throws Exception {
        final TestFlowUpdateResource resource = new TestFlowUpdateResource();
        final TrackingComponentLifecycle componentLifecycle = new TrackingComponentLifecycle();
        final AffectedComponentEntity affectedRunning = activeProcessor("affected-running");
        final AffectedComponentEntity overlapDrainStopped = activeProcessor("overlap-drain-stopped");
        final AffectedComponentEntity drainOnlyStopped = activeProcessor("drain-only-stopped");
        resource.drainResult = new RemovedConnectionDrainCoordinator.DrainResult(Set.of("connection-1"),
                orderedSet(overlapDrainStopped, drainOnlyStopped), false, null);

        final StandardAsynchronousWebRequest<TestProcessGroupDescriptorEntity, TestProcessGroupDescriptorEntity> asyncRequest =
                createAsyncRequest(FlowUpdateResource.UPDATE_REQUEST_TYPE);
        asyncRequest.cancel();

        resource.invokeUpdateFlow(componentLifecycle, new FlowUpdateImpact(orderedSet(affectedRunning, overlapDrainStopped), Set.<RemovedConnectionDescriptor>of(), Set.of(), Set.of()),
                asyncRequest, FlowUpdateResource.UPDATE_REQUEST_TYPE);

        assertEquals(List.of(
                new ScheduleInvocation(ScheduledState.RUNNING, Set.of("overlap-drain-stopped", "drain-only-stopped"))),
                componentLifecycle.scheduleInvocations);
        assertEquals("Request cancelled by user", asyncRequest.getFailureReason());
    }

    @Test
    void testCancelledDuringStopAllRetainsStopAndRestorationFailures() throws Exception {
        final TestFlowUpdateResource resource = new TestFlowUpdateResource();
        final TrackingComponentLifecycle componentLifecycle = new TrackingComponentLifecycle();
        final AffectedComponentEntity affectedRunning = activeProcessor("affected-running");
        final AffectedComponentEntity overlapDrainStopped = activeProcessor("overlap-drain-stopped");
        final AffectedComponentEntity drainOnlyStopped = activeProcessor("drain-only-stopped");
        resource.drainResult = new RemovedConnectionDrainCoordinator.DrainResult(Set.of("connection-1"),
                orderedSet(overlapDrainStopped, drainOnlyStopped), false, null);

        final StandardAsynchronousWebRequest<TestProcessGroupDescriptorEntity, TestProcessGroupDescriptorEntity> asyncRequest =
                createAsyncRequest(FlowUpdateResource.UPDATE_REQUEST_TYPE);
        componentLifecycle.requestToCancel = asyncRequest;
        componentLifecycle.cancelDuringStop = true;
        componentLifecycle.stopRuntimeException = new RuntimeException("stop failed");
        componentLifecycle.restoreRuntimeException = new RuntimeException("restore failed");

        resource.invokeUpdateFlow(componentLifecycle, new FlowUpdateImpact(orderedSet(affectedRunning, overlapDrainStopped), Set.<RemovedConnectionDescriptor>of(), Set.of(), Set.of()),
                asyncRequest, FlowUpdateResource.UPDATE_REQUEST_TYPE);

        assertEquals(List.of(
                new ScheduleInvocation(ScheduledState.STOPPED, Set.of("affected-running", "overlap-drain-stopped", "drain-only-stopped")),
                new ScheduleInvocation(ScheduledState.RUNNING, Set.of("overlap-drain-stopped", "drain-only-stopped"))),
                componentLifecycle.scheduleInvocations);
        assertEquals("Request cancelled by user; stop failed: stop failed; restoration failed: restore failed", asyncRequest.getFailureReason());
    }

    @Test
    void testStopAllRuntimeFailureRestoresDrainStoppedComponentsAndRethrowsOriginalFailure() throws Exception {
        final TestFlowUpdateResource resource = new TestFlowUpdateResource();
        final TrackingComponentLifecycle componentLifecycle = new TrackingComponentLifecycle();
        final AffectedComponentEntity affectedRunning = activeProcessor("affected-running");
        final AffectedComponentEntity overlapDrainStopped = activeProcessor("overlap-drain-stopped");
        final AffectedComponentEntity drainOnlyStopped = activeProcessor("drain-only-stopped");
        resource.drainResult = new RemovedConnectionDrainCoordinator.DrainResult(Set.of("connection-1"),
                orderedSet(overlapDrainStopped, drainOnlyStopped), false, null);

        final RuntimeException stopFailure = new RuntimeException("stop failed");
        final RuntimeException restoreFailure = new RuntimeException("restore failed");
        componentLifecycle.stopRuntimeException = stopFailure;
        componentLifecycle.restoreRuntimeException = restoreFailure;

        final StandardAsynchronousWebRequest<TestProcessGroupDescriptorEntity, TestProcessGroupDescriptorEntity> asyncRequest =
                createAsyncRequest(FlowUpdateResource.UPDATE_REQUEST_TYPE);
        final RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> resource.invokeUpdateFlow(componentLifecycle,
                        new FlowUpdateImpact(orderedSet(affectedRunning, overlapDrainStopped), Set.<RemovedConnectionDescriptor>of(), Set.of(), Set.of()),
                        asyncRequest, FlowUpdateResource.UPDATE_REQUEST_TYPE));

        assertSame(stopFailure, thrown);
        assertEquals(1, thrown.getSuppressed().length);
        assertSame(restoreFailure, thrown.getSuppressed()[0]);
        assertEquals(List.of(
                new ScheduleInvocation(ScheduledState.STOPPED, Set.of("affected-running", "overlap-drain-stopped", "drain-only-stopped")),
                new ScheduleInvocation(ScheduledState.RUNNING, Set.of("overlap-drain-stopped", "drain-only-stopped"))),
                componentLifecycle.scheduleInvocations);
    }

    private List<String> getStepDescriptions(final String requestType) {
        return FlowUpdateResource.getUpdateFlowSteps(requestType).stream()
                .map(UpdateStep::getDescription)
                .toList();
    }

    private static StandardAsynchronousWebRequest<TestProcessGroupDescriptorEntity, TestProcessGroupDescriptorEntity> createAsyncRequest(final String requestType) {
        return new StandardAsynchronousWebRequest<>("request-id", new TestProcessGroupDescriptorEntity(), GROUP_ID, null,
                FlowUpdateResource.getUpdateFlowSteps(requestType));
    }

    @SafeVarargs
    private static <T> Set<T> orderedSet(final T... values) {
        return new LinkedHashSet<>(List.of(values));
    }

    private static AffectedComponentEntity activeProcessor(final String id) {
        final AffectedComponentDTO component = new AffectedComponentDTO();
        component.setId(id);
        component.setReferenceType(AffectedComponentDTO.COMPONENT_TYPE_PROCESSOR);
        component.setState("Running");

        final AffectedComponentEntity entity = new AffectedComponentEntity();
        entity.setId(id);
        entity.setComponent(component);
        entity.setReferenceType(AffectedComponentDTO.COMPONENT_TYPE_PROCESSOR);
        return entity;
    }

    private record ScheduleInvocation(ScheduledState state, Set<String> componentIds) {
    }

    private static final class TrackingComponentLifecycle implements ComponentLifecycle {
        private final List<ScheduleInvocation> scheduleInvocations = new ArrayList<>();
        private StandardAsynchronousWebRequest<TestProcessGroupDescriptorEntity, TestProcessGroupDescriptorEntity> requestToCancel;
        private boolean cancelDuringStop;
        private RuntimeException stopRuntimeException;
        private RuntimeException restoreRuntimeException;

        @Override
        public Set<AffectedComponentEntity> scheduleComponents(final URI exampleUri, final String groupId, final Set<AffectedComponentEntity> components,
                                                               final ScheduledState desiredState, final Pause pause,
                                                               final InvalidComponentAction invalidComponentAction) throws LifecycleManagementException {
            scheduleInvocations.add(new ScheduleInvocation(desiredState, components.stream()
                    .map(AffectedComponentEntity::getId)
                    .collect(Collectors.toCollection(LinkedHashSet::new))));
            if (desiredState == ScheduledState.STOPPED && cancelDuringStop && requestToCancel != null) {
                requestToCancel.cancel();
            }

            if (desiredState == ScheduledState.STOPPED && stopRuntimeException != null) {
                throw stopRuntimeException;
            }

            if (desiredState == ScheduledState.RUNNING && restoreRuntimeException != null) {
                throw restoreRuntimeException;
            }

            return components;
        }

        @Override
        public Set<AffectedComponentEntity> activateControllerServices(final URI exampleUri, final String groupId,
                                                                       final Set<AffectedComponentEntity> servicesToUpdate,
                                                                       final Set<AffectedComponentEntity> servicesRequiringDesiredState,
                                                                       final ControllerServiceState desiredState,
                                                                       final Pause pause,
                                                                       final InvalidComponentAction invalidComponentAction) {
            throw new AssertionError("Controller service activation should not be reached in these tests");
        }

        @Override
        public boolean waitForConnectionQueuesEmpty(final URI exampleUri, final Set<String> connectionIds, final Pause pause) {
            throw new AssertionError("Connection draining should be stubbed in these tests");
        }
    }

    private static final class TestFlowUpdateResource extends FlowUpdateResource<TestProcessGroupDescriptorEntity, TestFlowUpdateRequestEntity> {
        private RemovedConnectionDrainCoordinator.DrainResult drainResult = new RemovedConnectionDrainCoordinator.DrainResult(Set.of(), Set.of(), false, null);

        private void invokeUpdateFlow(final TrackingComponentLifecycle componentLifecycle, final FlowUpdateImpact flowUpdateImpact,
                                      final AsynchronousWebRequest<TestProcessGroupDescriptorEntity, TestProcessGroupDescriptorEntity> asyncRequest,
                                      final String requestType) throws Exception {
            updateFlow(GROUP_ID, componentLifecycle, REQUEST_URI, flowUpdateImpact, false, "/versions/update-requests",
                    null, new TestProcessGroupDescriptorEntity(), null, asyncRequest, null, false, requestType);
        }

        @Override
        protected ProcessGroupEntity performUpdateFlow(final String groupId, final Revision revision,
                                                       final TestProcessGroupDescriptorEntity requestEntity,
                                                       final RegisteredFlowSnapshot flowSnapshot, final String idGenerationSeed,
                                                       final boolean verifyNotModified, final boolean updateDescendantVersionedFlows) {
            throw new AssertionError("Flow update should not be reached in these tests");
        }

        @Override
        protected Entity createReplicateUpdateFlowEntity(final Revision revision,
                                                         final TestProcessGroupDescriptorEntity requestEntity,
                                                         final RegisteredFlowSnapshot flowSnapshot) {
            throw new AssertionError("Replication should not be reached in these tests");
        }

        @Override
        protected TestFlowUpdateRequestEntity createUpdateRequestEntity() {
            return new TestFlowUpdateRequestEntity();
        }

        @Override
        protected void finalizeCompletedUpdateRequest(final TestFlowUpdateRequestEntity updateRequestEntity) {
        }

        @Override
        protected RemovedConnectionDrainCoordinator.DrainResult preDrainRemovedConnections(final FlowUpdateImpact flowUpdateImpact,
                                                                                           final ComponentLifecycle componentLifecycle,
                                                                                           final URI requestUri,
                                                                                           final String groupId,
                                                                                           final AsynchronousWebRequest<TestProcessGroupDescriptorEntity,
                                                                                                   TestProcessGroupDescriptorEntity> asyncRequest,
                                                                                           final AtomicReference<Runnable> stopComponentsCancellationCallback) {
            return drainResult;
        }
    }

    private static final class TestProcessGroupDescriptorEntity extends ProcessGroupDescriptorEntity {
    }

    private static final class TestFlowUpdateRequestEntity extends FlowUpdateRequestEntity<TestFlowUpdateRequestDTO> {
        @Override
        public TestFlowUpdateRequestDTO getRequest() {
            return request;
        }

        @Override
        public void setRequest(final TestFlowUpdateRequestDTO request) {
            this.request = request;
        }
    }

    private static final class TestFlowUpdateRequestDTO extends FlowUpdateRequestDTO {
    }
}
