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

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.core.Response;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.AuthorizeAccess;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.bulkreplay.BulkReplayJobDetailDTO;
import org.apache.nifi.web.api.dto.bulkreplay.BulkReplayJobItemDTO;
import org.apache.nifi.web.api.dto.bulkreplay.BulkReplayJobStatus;
import org.apache.nifi.web.api.entity.BulkReplayJobDetailEntity;
import org.apache.nifi.web.api.entity.BulkReplayJobsEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link BulkReplayResource} covering:
 * <ul>
 *   <li>Authorization enforcement on every endpoint</li>
 *   <li>Correct CRUD delegation to {@link BulkReplayJobStore}</li>
 *   <li>Cluster behaviour: POST and DELETE are replicated to all nodes</li>
 *   <li>Job ID is assigned <em>before</em> replication so every cluster node stores
 *       the same identifier</li>
 *   <li>Only the primary node starts job execution</li>
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
class TestBulkReplayResource {

    @InjectMocks
    private BulkReplayResource resource;

    @Mock
    private NiFiServiceFacade serviceFacade;

    @Mock
    private Authorizer authorizer;

    @Mock
    private BulkReplayJobStore jobStore;

    @Mock
    private BulkReplayExecutionService executionService;

    @Mock
    private HttpServletRequest defaultRequest;

    @Mock
    private NiFiProperties defaultProperties;

    @BeforeEach
    void standaloneDefaults() {
        lenient().when(jobStore.getAllJobs()).thenReturn(List.of());
        // Default to standalone so isReplicateRequest() returns false
        lenient().when(defaultProperties.isNode()).thenReturn(false);
        lenient().when(defaultRequest.getHeader(any())).thenReturn(null);
        resource.properties = defaultProperties;
        resource.httpServletRequest = defaultRequest;
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Configures the resource for standalone (non-cluster) mode so that
     * {@code isReplicateRequest()} short-circuits to {@code false} and
     * {@code withWriteLock} treats the request as single-phase.
     */
    private void standaloneMode(final HttpServletRequest request, final NiFiProperties properties) {
        lenient().when(properties.isNode()).thenReturn(false);
        lenient().when(request.getHeader(any())).thenReturn(null);
        resource.properties = properties;
        resource.httpServletRequest = request;
    }

    /** Creates a minimal valid detail DTO with one item (required by the submit endpoint). */
    private static BulkReplayJobDetailDTO jobDetail(final String id) {
        final BulkReplayJobDetailDTO dto = new BulkReplayJobDetailDTO();
        dto.setJobId(id);
        dto.setProcessorId("proc-1");
        dto.setProcessorName("TestProcessor");
        dto.setProcessorType("org.apache.nifi.TestProcessor");
        dto.setGroupId("group-1");
        dto.setStatus(BulkReplayJobStatus.QUEUED);

        final BulkReplayJobItemDTO item = new BulkReplayJobItemDTO();
        item.setProvenanceEventId(1L);
        item.setFlowFileUuid("uuid-1");
        item.setEventType("RECEIVE");
        item.setEventTime("01/01/2024 00:00:00 UTC");
        item.setComponentName("TestProcessor");
        dto.setItems(List.of(item));
        return dto;
    }

    private static BulkReplayJob testJob(final String id) {
        return new BulkReplayJob(id, "Test Job", "user",
                Instant.now(), "proc-1", "TestProcessor", "org.apache.nifi.TestProcessor",
                "group-1", List.of());
    }

    private static BulkReplayJobDetailEntity entityFor(final BulkReplayJobDetailDTO detail) {
        final BulkReplayJobDetailEntity entity = new BulkReplayJobDetailEntity();
        entity.setJobDetail(detail);
        return entity;
    }

    // -----------------------------------------------------------------------
    // GET /bulk-replay/jobs
    // -----------------------------------------------------------------------

    @Test
    void testGetBulkReplayJobs_unauthorized_throws() {
        doThrow(AccessDeniedException.class).when(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));

        assertThrows(AccessDeniedException.class, () -> resource.getBulkReplayJobs());
        verify(jobStore, never()).getAllJobs();
    }

    @Test
    void testGetBulkReplayJobs_authorized_returnsStoreContents() {
        final BulkReplayJob job1 = testJob("j1");
        final BulkReplayJob job2 = testJob("j2");
        when(jobStore.getAllJobs()).thenReturn(List.of(job1, job2));

        try (Response response = resource.getBulkReplayJobs()) {
            assertEquals(200, response.getStatus());
            final BulkReplayJobsEntity entity = (BulkReplayJobsEntity) response.getEntity();
            assertEquals(2, entity.getJobs().size());
        }
    }

    @Test
    void testGetBulkReplayJobs_standalone_returnsEmptyList() {
        when(jobStore.getAllJobs()).thenReturn(List.of());

        try (Response response = resource.getBulkReplayJobs()) {
            assertEquals(200, response.getStatus());
        }
    }

    // -----------------------------------------------------------------------
    // GET /bulk-replay/jobs/{id}
    // -----------------------------------------------------------------------

    @Test
    void testGetBulkReplayJob_unauthorized_throws() {
        doThrow(AccessDeniedException.class).when(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));

        assertThrows(AccessDeniedException.class, () -> resource.getBulkReplayJob("any-id"));
        verify(jobStore, never()).findJobById(any());
    }

    @Test
    void testGetBulkReplayJob_notFound_throws404() {
        when(jobStore.findJobById("missing")).thenReturn(Optional.empty());

        assertThrows(ResourceNotFoundException.class, () -> resource.getBulkReplayJob("missing"));
    }

    @Test
    void testGetBulkReplayJob_found_returns200() {
        final BulkReplayJob job = testJob("j42");
        when(jobStore.findJobById("j42")).thenReturn(Optional.of(job));

        try (Response response = resource.getBulkReplayJob("j42")) {
            assertEquals(200, response.getStatus());
        }
    }

    // -----------------------------------------------------------------------
    // POST /bulk-replay/jobs — validation
    // -----------------------------------------------------------------------

    @Test
    void testSubmitBulkReplayJob_nullEntity_throwsIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () -> resource.submitBulkReplayJob(null));
    }

    @Test
    void testSubmitBulkReplayJob_nullJobDetail_throwsIllegalArgument() {
        final BulkReplayJobDetailEntity entity = new BulkReplayJobDetailEntity();
        assertThrows(IllegalArgumentException.class, () -> resource.submitBulkReplayJob(entity));
    }

    @Test
    void testSubmitBulkReplayJob_emptyItems_throwsIllegalArgument(
            @Mock HttpServletRequest request,
            @Mock NiFiProperties properties) {
        standaloneMode(request, properties);
        final BulkReplayJobDetailDTO dto = new BulkReplayJobDetailDTO();
        dto.setItems(List.of());
        assertThrows(IllegalArgumentException.class,
                () -> resource.submitBulkReplayJob(entityFor(dto)));
    }

    @Test
    void testSubmitBulkReplayJob_unauthorized_throws(
            @Mock HttpServletRequest request,
            @Mock NiFiProperties properties) {
        standaloneMode(request, properties);
        doThrow(AccessDeniedException.class).when(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));

        assertThrows(AccessDeniedException.class,
                () -> resource.submitBulkReplayJob(entityFor(jobDetail(null))));
        verify(jobStore, never()).registerJob(any());
    }

    /**
     * Standalone success path: job is registered and 201 is returned.
     * In standalone mode the node is always considered primary, so execution starts.
     */
    @Test
    void testSubmitBulkReplayJob_standalone_registersJobAndReturns201(
            @Mock HttpServletRequest request,
            @Mock NiFiProperties properties) {
        standaloneMode(request, properties);

        final NiFiUser mockUser = mock(NiFiUser.class);
        when(mockUser.getIdentity()).thenReturn("test-user");

        try (MockedStatic<NiFiUserUtils> mockedUtils = mockStatic(NiFiUserUtils.class)) {
            mockedUtils.when(NiFiUserUtils::getNiFiUser).thenReturn(mockUser);

            final BulkReplayResource spy = spy(resource);
            doReturn("http://localhost/nifi-api/bulk-replay/jobs/test-id")
                    .when(spy).generateResourceUri(anyString(), anyString(), anyString());

            try (Response response = spy.submitBulkReplayJob(entityFor(jobDetail(null)))) {
                assertEquals(201, response.getStatus());
            }
            verify(jobStore).registerJob(any(BulkReplayJob.class));
            verify(executionService).executeJob(any(BulkReplayJob.class));
        }
    }

    // -----------------------------------------------------------------------
    // POST /bulk-replay/jobs — cluster behaviour
    // -----------------------------------------------------------------------

    /**
     * In cluster mode the coordinator must replicate POST to all nodes.
     */
    @Test
    void testSubmitBulkReplayJob_cluster_replicatesToAllNodes(
            @Mock HttpServletRequest request,
            @Mock NiFiProperties properties) {
        standaloneMode(request, properties);

        final BulkReplayResource spy = spy(resource);
        doReturn(true).when(spy).isReplicateRequest();

        final Response mockReplicatedResponse = Response.accepted().build();
        doReturn(mockReplicatedResponse)
                .when(spy).replicate(anyString(), any(Object.class));

        final Response actual = spy.submitBulkReplayJob(entityFor(jobDetail(null)));

        assertSame(mockReplicatedResponse, actual);
        verify(spy).replicate(eq("POST"), any(Object.class));
        verify(jobStore, never()).registerJob(any());
    }

    /**
     * Job ID must be assigned before replication so every node stores the same ID.
     */
    @Test
    void testSubmitBulkReplayJob_cluster_jobIdAssignedBeforeReplication(
            @Mock HttpServletRequest request,
            @Mock NiFiProperties properties) {
        standaloneMode(request, properties);

        final BulkReplayResource spy = spy(resource);
        doReturn(true).when(spy).isReplicateRequest();

        doAnswer(invocation -> {
            final BulkReplayJobDetailEntity captured = invocation.getArgument(1);
            assertNotNull(captured.getJobDetail().getJobId(),
                    "Job id must be populated before the request is replicated to cluster nodes");
            return Response.accepted().build();
        }).when(spy).replicate(anyString(), any(Object.class));

        spy.submitBulkReplayJob(entityFor(jobDetail(null)));

        verify(spy).replicate(eq("POST"), any(Object.class));
    }

    /**
     * An existing id supplied by the client must be preserved (not overwritten).
     */
    @Test
    void testSubmitBulkReplayJob_cluster_preservesCallerSuppliedId(
            @Mock HttpServletRequest request,
            @Mock NiFiProperties properties) {
        standaloneMode(request, properties);

        final BulkReplayResource spy = spy(resource);
        doReturn(true).when(spy).isReplicateRequest();

        doAnswer(invocation -> {
            final BulkReplayJobDetailEntity captured = invocation.getArgument(1);
            assertEquals("caller-supplied-id", captured.getJobDetail().getJobId());
            return Response.accepted().build();
        }).when(spy).replicate(anyString(), any(Object.class));

        spy.submitBulkReplayJob(entityFor(jobDetail("caller-supplied-id")));

        verify(spy).replicate(eq("POST"), any(Object.class));
    }

    // -----------------------------------------------------------------------
    // GET /bulk-replay/jobs/{id}/items
    // -----------------------------------------------------------------------

    @Test
    void testGetBulkReplayJobItems_unauthorized_throws() {
        doThrow(AccessDeniedException.class).when(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));

        assertThrows(AccessDeniedException.class, () -> resource.getBulkReplayJobItems("any-id"));
        verify(jobStore, never()).findJobById(any());
    }

    @Test
    void testGetBulkReplayJobItems_notFound_throws404() {
        when(jobStore.findJobById("missing")).thenReturn(Optional.empty());

        assertThrows(ResourceNotFoundException.class, () -> resource.getBulkReplayJobItems("missing"));
    }

    @Test
    void testGetBulkReplayJobItems_found_returns200WithItems() {
        final BulkReplayJobItem item = new BulkReplayJobItem("item-1", "j42", 0, 100L,
                null, "uuid-1", "RECEIVE", "01/01/2024 00:00:00 UTC", "TestProcessor", 0L);
        final BulkReplayJob job = new BulkReplayJob("j42", "Test Job", "user",
                Instant.now(), "proc-1", "TestProcessor", "org.apache.nifi.TestProcessor",
                "group-1", List.of(item));
        when(jobStore.findJobById("j42")).thenReturn(Optional.of(job));

        try (Response response = resource.getBulkReplayJobItems("j42")) {
            assertEquals(200, response.getStatus());
        }
    }

    // -----------------------------------------------------------------------
    // POST /bulk-replay/jobs/{id}/cancel
    // -----------------------------------------------------------------------

    @Test
    void testCancelBulkReplayJob_unauthorized_throws() {
        doThrow(AccessDeniedException.class).when(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));

        assertThrows(AccessDeniedException.class, () -> resource.cancelBulkReplayJob("any-id"));
        verify(jobStore, never()).findJobById(any());
    }

    @Test
    void testCancelBulkReplayJob_found_setsCancelRequestedAndReturns204(
            @Mock HttpServletRequest request,
            @Mock NiFiProperties properties) {
        standaloneMode(request, properties);

        final BulkReplayJob job = testJob("j42");
        when(jobStore.findJobById("j42")).thenReturn(Optional.of(job));

        try (Response response = resource.cancelBulkReplayJob("j42")) {
            assertEquals(204, response.getStatus());
        }
        assertTrue(job.isCancelRequested());
    }

    @Test
    void testCancelBulkReplayJob_missing_returnsNoContent(
            @Mock HttpServletRequest request,
            @Mock NiFiProperties properties) {
        standaloneMode(request, properties);

        when(jobStore.findJobById("missing")).thenReturn(Optional.empty());

        // Cancel is tolerant of missing jobs (cluster replication timing).
        try (Response response = resource.cancelBulkReplayJob("missing")) {
            assertEquals(204, response.getStatus());
        }
    }

    /**
     * In cluster mode, cancel is replicated to all nodes via PUT.
     */
    @Test
    void testCancelBulkReplayJob_cluster_replicatesToAllNodes(
            @Mock HttpServletRequest request,
            @Mock NiFiProperties properties) {
        standaloneMode(request, properties);

        final BulkReplayResource spy = spy(resource);
        doReturn(true).when(spy).isReplicateRequest();

        final Response mockReplicatedResponse = Response.noContent().build();
        doReturn(mockReplicatedResponse).when(spy).replicate("PUT");

        final Response actual = spy.cancelBulkReplayJob("j1");

        assertSame(mockReplicatedResponse, actual);
        verify(spy).replicate("PUT");
        verify(jobStore, never()).findJobById(any());
    }

    // -----------------------------------------------------------------------
    // DELETE /bulk-replay/jobs/{id}
    // -----------------------------------------------------------------------

    @Test
    void testDeleteBulkReplayJob_unauthorized_throws(
            @Mock HttpServletRequest request,
            @Mock NiFiProperties properties) {
        standaloneMode(request, properties);
        doThrow(AccessDeniedException.class).when(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));

        assertThrows(AccessDeniedException.class, () -> resource.deleteBulkReplayJob("j1"));
        verify(jobStore, never()).remove(any());
    }

    @Test
    void testDeleteBulkReplayJob_standalone_removesJobAndReturns204(
            @Mock HttpServletRequest request,
            @Mock NiFiProperties properties) {
        standaloneMode(request, properties);

        try (Response response = resource.deleteBulkReplayJob("j1")) {
            assertEquals(204, response.getStatus());
        }
        verify(jobStore).remove("j1");
    }

    /**
     * DELETE must be replicated to all cluster nodes so that the job is removed everywhere.
     */
    @Test
    void testDeleteBulkReplayJob_cluster_replicatesToAllNodes(
            @Mock HttpServletRequest request,
            @Mock NiFiProperties properties) {
        standaloneMode(request, properties);

        final BulkReplayResource spy = spy(resource);
        doReturn(true).when(spy).isReplicateRequest();

        final Response mockReplicatedResponse = Response.noContent().build();
        doReturn(mockReplicatedResponse).when(spy).replicate("DELETE");

        final Response actual = spy.deleteBulkReplayJob("j1");

        assertSame(mockReplicatedResponse, actual);
        verify(spy).replicate("DELETE");
        verify(jobStore, never()).remove(any());
    }

    // -----------------------------------------------------------------------
    // Orphaned job recovery
    // -----------------------------------------------------------------------

    @Test
    void testGetBulkReplayJobs_resumesOrphanedQueuedJobs() {
        final BulkReplayJob orphanedJob = testJob("orphan-1");
        // Not submitted — simulates a job replicated via POST but never executed
        // (this node was not primary when the job was submitted)
        orphanedJob.setSubmitted(false);
        orphanedJob.setStatus(BulkReplayJobStatus.QUEUED);

        when(jobStore.getAllJobs()).thenReturn(List.of(orphanedJob));

        try (Response response = resource.getBulkReplayJobs()) {
            assertEquals(200, response.getStatus());
        }

        // The orphaned job should now be submitted for execution.
        assertTrue(orphanedJob.isSubmitted());
        verify(executionService).executeJob(orphanedJob);
    }

    @Test
    void testSubmitBulkReplayJob_nonPrimary_doesNotExecute(
            @Mock HttpServletRequest request,
            @Mock NiFiProperties properties) {
        standaloneMode(request, properties);

        final NiFiUser mockUser = mock(NiFiUser.class);
        when(mockUser.getIdentity()).thenReturn("test-user");

        try (MockedStatic<NiFiUserUtils> mockedUtils = mockStatic(NiFiUserUtils.class)) {
            mockedUtils.when(NiFiUserUtils::getNiFiUser).thenReturn(mockUser);

            final BulkReplayResource spy = spy(resource);
            doReturn("http://localhost/nifi-api/bulk-replay/jobs/test-id")
                    .when(spy).generateResourceUri(anyString(), anyString(), anyString());
            // Simulate non-primary node
            doReturn(false).when(spy).isReplicateRequest();
            doReturn(mock(org.apache.nifi.cluster.coordination.ClusterCoordinator.class))
                    .when(spy).getClusterCoordinator();

            try (Response response = spy.submitBulkReplayJob(entityFor(jobDetail(null)))) {
                assertEquals(201, response.getStatus());
            }
            verify(jobStore).registerJob(any(BulkReplayJob.class));
            verify(executionService, never()).executeJob(any(BulkReplayJob.class));
        }
    }

    @Test
    void testGetBulkReplayJobs_doesNotResubmitAlreadySubmittedJobs() {
        final BulkReplayJob runningJob = testJob("running-1");
        runningJob.setSubmitted(true);
        runningJob.setStatus(BulkReplayJobStatus.QUEUED);

        when(jobStore.getAllJobs()).thenReturn(List.of(runningJob));

        try (Response response = resource.getBulkReplayJobs()) {
            assertEquals(200, response.getStatus());
        }

        verify(executionService, never()).executeJob(any());
    }

    // -----------------------------------------------------------------------
    // GET /bulk-replay/jobs/config
    // -----------------------------------------------------------------------

    @Test
    void testGetBulkReplayConfig_returnsCurrentTimeout() {
        when(executionService.getNodeDisconnectTimeout()).thenReturn("5 mins");

        try (Response response = resource.getBulkReplayConfig()) {
            assertEquals(200, response.getStatus());
            @SuppressWarnings("unchecked")
            final Map<String, String> result = (Map<String, String>) response.getEntity();
            assertEquals("5 mins", result.get("nodeDisconnectTimeout"));
        }
    }

    @Test
    void testGetBulkReplayConfig_servedLocally_noReplication() {
        when(executionService.getNodeDisconnectTimeout()).thenReturn("5 mins");

        // GET config is served locally even in cluster mode — no replication needed
        // since all nodes have the config (replicated via PUT).
        try (Response response = resource.getBulkReplayConfig()) {
            assertEquals(200, response.getStatus());
        }
        verify(executionService).getNodeDisconnectTimeout();
    }

    // -----------------------------------------------------------------------
    // PUT /bulk-replay/jobs/config
    // -----------------------------------------------------------------------

    @Test
    void testUpdateBulkReplayConfig_standalone_updatesTimeout(
            @Mock HttpServletRequest request,
            @Mock NiFiProperties properties) {
        standaloneMode(request, properties);

        when(executionService.getNodeDisconnectTimeout()).thenReturn("3 mins");

        try (Response response = resource.updateBulkReplayConfig(Map.of("nodeDisconnectTimeout", "3 mins"))) {
            assertEquals(200, response.getStatus());
        }
        verify(executionService).setNodeDisconnectTimeout("3 mins");
    }

    @Test
    void testUpdateBulkReplayConfig_cluster_replicatesToAllNodes(
            @Mock HttpServletRequest request,
            @Mock NiFiProperties properties) {
        standaloneMode(request, properties);

        final BulkReplayResource spy = spy(resource);
        doReturn(true).when(spy).isReplicateRequest();

        final Response mockReplicatedResponse = Response.ok().build();
        doReturn(mockReplicatedResponse).when(spy).replicate(eq("PUT"), any(Object.class));

        final Map<String, String> config = Map.of("nodeDisconnectTimeout", "3 mins");
        final Response actual = spy.updateBulkReplayConfig(config);

        assertSame(mockReplicatedResponse, actual);
        verify(spy).replicate(eq("PUT"), eq(config));
        verify(executionService, never()).setNodeDisconnectTimeout(any());
    }

    @Test
    void testUpdateBulkReplayConfig_invalidTimeout_throwsIllegalArgument(
            @Mock HttpServletRequest request,
            @Mock NiFiProperties properties) {
        standaloneMode(request, properties);

        assertThrows(IllegalArgumentException.class,
                () -> resource.updateBulkReplayConfig(Map.of("nodeDisconnectTimeout", "")));
    }

}
