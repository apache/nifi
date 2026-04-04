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

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HttpMethod;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.bulkreplay.BulkReplayJobDetailDTO;
import org.apache.nifi.web.api.dto.bulkreplay.BulkReplayJobItemDTO;
import org.apache.nifi.web.api.dto.bulkreplay.BulkReplayJobStatus;
import org.apache.nifi.web.api.dto.bulkreplay.BulkReplayJobSummaryDTO;
import org.apache.nifi.web.api.entity.BulkReplayJobDetailEntity;
import org.apache.nifi.web.api.entity.BulkReplayJobItemsEntity;
import org.apache.nifi.web.api.entity.BulkReplayJobSummaryEntity;
import org.apache.nifi.web.api.entity.BulkReplayJobsEntity;
import org.apache.nifi.web.api.entity.ComponentEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import java.net.URI;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.nifi.web.api.BulkReplayDateFormat.format;

/**
 * RESTful endpoint for managing bulk provenance-event replay jobs.
 *
 * <h2>Primary-node model with cluster-wide state</h2>
 * <p>All bulk replay jobs are replicated to every cluster node on submission (POST).
 * Only the primary node executes the replay worker. If the primary node changes,
 * the new primary detects orphaned QUEUED jobs and resumes execution automatically.
 * The worker checks primary-node status between items and stops if the role is lost,
 * preventing duplicate replays.</p>
 *
 * <p>In standalone mode, all operations execute locally.</p>
 */
@Controller
@Path("/bulk-replay/jobs")
@Tag(name = "BulkReplay", description = "Endpoints for managing bulk provenance-event replay jobs.")
public class BulkReplayResource extends ApplicationResource {

    private static final Logger log = LoggerFactory.getLogger(BulkReplayResource.class);

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;
    private BulkReplayJobStore jobStore;
    private BulkReplayExecutionService executionService;

    // ---------------------------------------------------------------------------
    // Authorization
    // ---------------------------------------------------------------------------

    private void authorizeProvenanceRequest(final RequestAction action) {
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable provenance = lookup.getProvenance();
            provenance.authorize(authorizer, action, NiFiUserUtils.getNiFiUser());
        });
    }

    // ---------------------------------------------------------------------------
    // Cluster helpers
    // ---------------------------------------------------------------------------

    /**
     * Returns the primary node identifier, or throws if no primary is elected.
     */
    private NodeIdentifier getPrimaryNodeOrThrow() {
        return getPrimaryNodeId()
                .orElseThrow(() -> new IllegalStateException(
                        "There is currently no Primary Node elected. Bulk replay operations require a Primary Node."));
    }

    /**
     * Returns {@code true} if this node is the primary node or if running standalone (no cluster).
     */
    private boolean isLocalNodePrimary() {
        final ClusterCoordinator coordinator = getClusterCoordinator();
        if (coordinator == null) {
            return true;
        }
        final NodeIdentifier primary = coordinator.getPrimaryNode();
        final NodeIdentifier local = coordinator.getLocalNodeIdentifier();
        return primary != null && primary.equals(local);
    }

    /**
     * Checks for QUEUED or INTERRUPTED jobs that have not been submitted to the executor.
     * This handles primary-node failover: the new primary picks up orphaned jobs
     * that were replicated during POST but never executed because this node was
     * not the primary at submission time, as well as jobs that were interrupted
     * when the previous primary lost its role.
     */
    private void resumeOrphanedJobs() {
        if (!isLocalNodePrimary()) {
            return;
        }
        for (final BulkReplayJob job : jobStore.getAllJobs()) {
            final BulkReplayJobStatus jobStatus = job.getStatus();
            if (!job.isDeleteRequested()
                    && (jobStatus == BulkReplayJobStatus.QUEUED || jobStatus == BulkReplayJobStatus.INTERRUPTED)
                    && job.tryMarkSubmitted()) {
                log.info("Resuming orphaned bulk replay job {} (status={}) after primary node change",
                        job.getJobId(), jobStatus);
                job.setStatus(BulkReplayJobStatus.QUEUED);
                executionService.executeJob(job);
            }
        }
    }

    // ---------------------------------------------------------------------------
    // Mapping helpers
    // ---------------------------------------------------------------------------

    private BulkReplayJobSummaryDTO toSummary(final BulkReplayJob job) {
        final BulkReplayJobSummaryDTO dto = new BulkReplayJobSummaryDTO();
        dto.setJobId(job.getJobId());
        dto.setJobName(job.getJobName());
        dto.setProcessorId(job.getProcessorId());
        dto.setProcessorName(job.getProcessorName());
        dto.setProcessorType(job.getProcessorType());
        dto.setGroupId(job.getGroupId());
        dto.setSubmittedBy(job.getSubmittedBy());
        dto.setSubmissionTime(format(job.getSubmissionTime()));
        dto.setStartTime(format(job.getStartTime()));
        dto.setEndTime(format(job.getEndTime()));
        dto.setLastUpdated(format(job.getLastUpdated()));
        dto.setStatus(job.getStatus());
        dto.setStatusMessage(job.getStatusMessage());
        dto.setTotalItems(job.getTotalItems());
        dto.setProcessedItems(job.getProcessedItems());
        dto.setSucceededItems(job.getSucceededItems());
        dto.setFailedItems(job.getFailedItems());
        dto.setPercentComplete(job.getPercentComplete());
        // Use ISO-8601 for the deadline so JavaScript Date() can reliably parse it for countdown math.
        final Instant deadline = job.getDisconnectWaitDeadline();
        dto.setDisconnectWaitDeadline(deadline != null ? deadline.toString() : null);
        return dto;
    }

    private BulkReplayJobItemDTO toItemDTO(final BulkReplayJobItem item) {
        final BulkReplayJobItemDTO dto = new BulkReplayJobItemDTO();
        dto.setItemId(item.getItemId());
        dto.setItemIndex(item.getItemIndex());
        dto.setProvenanceEventId(item.getProvenanceEventId());
        dto.setClusterNodeId(item.getClusterNodeId());
        dto.setFlowFileUuid(item.getFlowFileUuid());
        dto.setEventType(item.getEventType());
        dto.setEventTime(item.getEventTime());
        dto.setComponentName(item.getComponentName());
        dto.setStatus(item.getStatus());
        dto.setErrorMessage(item.getErrorMessage());
        dto.setFileSizeBytes(item.getFileSizeBytes());
        dto.setStartTime(format(item.getStartTime()));
        dto.setEndTime(format(item.getEndTime()));
        dto.setLastUpdated(format(item.getLastUpdated()));
        return dto;
    }

    // ---------------------------------------------------------------------------
    // GET /bulk-replay/jobs
    // ---------------------------------------------------------------------------

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Gets all bulk replay jobs",
            description = "Returns job summaries from the primary node. In a cluster, non-primary nodes " +
                    "forward the request to the primary automatically.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = BulkReplayJobsEntity.class))),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request.")
            },
            security = {@SecurityRequirement(name = "Read - /provenance")}
    )
    public Response getBulkReplayJobs() {
        authorizeProvenanceRequest(RequestAction.READ);

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET, getPrimaryNodeOrThrow());
        }

        // On the primary, check for orphaned jobs left by a previous primary.
        resumeOrphanedJobs();

        final List<BulkReplayJobSummaryDTO> summaries = jobStore.getAllJobs().stream()
                .map(this::toSummary)
                .collect(Collectors.toList());
        final BulkReplayJobsEntity entity = new BulkReplayJobsEntity();
        entity.setJobs(summaries);
        return noCache(generateOkResponse(entity)).build();
    }

    // ---------------------------------------------------------------------------
    // GET /bulk-replay/jobs/{id}
    // ---------------------------------------------------------------------------

    @GET
    @Path("/{id}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Gets a bulk replay job summary",
            description = "Returns live job status from the primary node.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = BulkReplayJobSummaryEntity.class))),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found.")
            },
            security = {@SecurityRequirement(name = "Read - /provenance")}
    )
    public Response getBulkReplayJob(
            @Parameter(description = "The bulk replay job id.", required = true)
            @PathParam("id") final String id) {

        authorizeProvenanceRequest(RequestAction.READ);

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET, getPrimaryNodeOrThrow());
        }

        resumeOrphanedJobs();

        final BulkReplayJob job = jobStore.findJobById(id)
                .orElseThrow(() -> new ResourceNotFoundException("No bulk replay job with id: " + id));

        final BulkReplayJobSummaryEntity entity = new BulkReplayJobSummaryEntity();
        entity.setJob(toSummary(job));
        return noCache(generateOkResponse(entity)).build();
    }

    // ---------------------------------------------------------------------------
    // GET /bulk-replay/jobs/{id}/items
    // ---------------------------------------------------------------------------

    @GET
    @Path("/{id}/items")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Gets item-level status for a bulk replay job",
            description = "Returns the per-item replay status from the primary node.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = BulkReplayJobItemsEntity.class))),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found.")
            },
            security = {@SecurityRequirement(name = "Read - /provenance")}
    )
    public Response getBulkReplayJobItems(
            @Parameter(description = "The bulk replay job id.", required = true)
            @PathParam("id") final String id) {

        authorizeProvenanceRequest(RequestAction.READ);

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET, getPrimaryNodeOrThrow());
        }

        resumeOrphanedJobs();

        final BulkReplayJob job = jobStore.findJobById(id)
                .orElseThrow(() -> new ResourceNotFoundException("No bulk replay job with id: " + id));

        final List<BulkReplayJobItemDTO> itemDtos = job.getItems().stream()
                .map(this::toItemDTO)
                .collect(Collectors.toList());

        final BulkReplayJobItemsEntity entity = new BulkReplayJobItemsEntity();
        entity.setItems(itemDtos);
        return noCache(generateOkResponse(entity)).build();
    }

    // ---------------------------------------------------------------------------
    // PUT /bulk-replay/jobs/{id}/items/{itemIndex}/status  (internal cluster sync)
    // ---------------------------------------------------------------------------

    @PUT
    @Path("/{id}/items/{itemIndex}/status")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Updates item status (internal cluster sync)",
            description = "Called by the primary node to replicate per-item progress to other nodes. " +
                    "Not intended for external use.",
            responses = {
                    @ApiResponse(responseCode = "204", description = "Item status updated."),
                    @ApiResponse(responseCode = "404", description = "Job not found on this node.")
            },
            security = {@SecurityRequirement(name = "Write - /provenance")}
    )
    public Response updateItemStatus(
            @Parameter(description = "The bulk replay job id.", required = true)
            @PathParam("id") final String id,
            @Parameter(description = "The zero-based item index.", required = true)
            @PathParam("itemIndex") final int itemIndex,
            final BulkReplayJobItemDTO itemDto) {

        authorizeProvenanceRequest(RequestAction.WRITE);

        final BulkReplayJob job = jobStore.findJobById(id)
                .orElseThrow(() -> new ResourceNotFoundException("No bulk replay job with id: " + id));

        final List<BulkReplayJobItem> items = job.getItems();
        if (itemIndex < 0 || itemIndex >= items.size()) {
            throw new ResourceNotFoundException("No bulk replay item with index " + itemIndex + " for job: " + id);
        }

        final BulkReplayJobItem item = items.get(itemIndex);
        if (itemDto.getStatus() != null) {
            item.setStatus(itemDto.getStatus());
        }
        if (itemDto.getErrorMessage() != null) {
            item.setErrorMessage(itemDto.getErrorMessage());
        }

        return noCache(Response.noContent()).build();
    }

    // ---------------------------------------------------------------------------
    // POST /bulk-replay/jobs
    // ---------------------------------------------------------------------------

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Submits a new bulk replay job",
            description = "In a cluster, the request is replicated to all nodes. Each node stores the " +
                    "job, but only the primary node starts execution. If the primary changes, the " +
                    "new primary automatically resumes orphaned QUEUED jobs.",
            responses = {
                    @ApiResponse(responseCode = "201", content = @Content(schema = @Schema(implementation = BulkReplayJobSummaryEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request.")
            },
            security = {@SecurityRequirement(name = "Write - /provenance")}
    )
    public Response submitBulkReplayJob(
            @Parameter(description = "The bulk replay job request.", required = true)
            final BulkReplayJobDetailEntity requestEntity) {

        if (requestEntity == null || requestEntity.getJobDetail() == null) {
            throw new IllegalArgumentException("Bulk replay job detail is required.");
        }

        final BulkReplayJobDetailDTO detail = requestEntity.getJobDetail();

        // Assign stable id before replication so every cluster node stores the same id.
        if (detail.getJobId() == null || detail.getJobId().isBlank()) {
            detail.setJobId(generateUuid());
        }

        // Replicate to ALL cluster nodes so every node has the job definition.
        if (isReplicateRequest()) {
            return replicate("POST", requestEntity);
        }

        return withWriteLock(
                serviceFacade,
                requestEntity,
                lookup -> authorizeProvenanceRequest(RequestAction.WRITE),
                null,
                (entity) -> {
                    final BulkReplayJobDetailDTO d = entity.getJobDetail();
                    if (d.getItems() == null || d.getItems().isEmpty()) {
                        throw new IllegalArgumentException("At least one item is required.");
                    }
                    d.setSubmittedBy(NiFiUserUtils.getNiFiUser().getIdentity());
                    final Instant submissionTime = Instant.now();
                    d.setSubmissionTime(format(submissionTime));
                    d.setStatus(BulkReplayJobStatus.QUEUED);
                    d.setStatusMessage("Queued");

                    // Default job name to "processorName YYYY-MM-DDThh:mm:ss" if not provided.
                    if (d.getJobName() == null || d.getJobName().isBlank()) {
                        final String ts = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
                                .withZone(ZoneId.of("UTC"))
                                .format(submissionTime);
                        d.setJobName(d.getProcessorName() + " " + ts);
                    }

                    final List<BulkReplayJobItemDTO> itemDtos = d.getItems();

                    final List<BulkReplayJobItem> items = new ArrayList<>(itemDtos.size());
                    for (int i = 0; i < itemDtos.size(); i++) {
                        final BulkReplayJobItemDTO dto = itemDtos.get(i);
                        if (dto.getProvenanceEventId() == null) {
                            throw new IllegalArgumentException("provenanceEventId is required for item at index " + i);
                        }
                        items.add(new BulkReplayJobItem(
                                UUID.randomUUID().toString(),
                                d.getJobId(),
                                i,
                                dto.getProvenanceEventId(),
                                dto.getClusterNodeId(),
                                dto.getFlowFileUuid(),
                                dto.getEventType(),
                                dto.getEventTime(),
                                dto.getComponentName(),
                                dto.getFileSizeBytes() != null ? dto.getFileSizeBytes() : 0L
                        ));
                    }

                    final BulkReplayJob job = new BulkReplayJob(
                            d.getJobId(),
                            d.getJobName(),
                            d.getSubmittedBy(),
                            submissionTime,
                            d.getProcessorId(),
                            d.getProcessorName(),
                            d.getProcessorType(),
                            d.getGroupId(),
                            items
                    );

                    // Every node stores the job for failover recovery.
                    jobStore.registerJob(job);

                    // Only the primary node (or standalone) starts the worker.
                    if (isLocalNodePrimary()) {
                        job.tryMarkSubmitted();
                        executionService.executeJob(job);
                    }

                    final BulkReplayJobSummaryEntity responseEntity = new BulkReplayJobSummaryEntity();
                    responseEntity.setJob(toSummary(job));

                    final URI location = URI.create(generateResourceUri("bulk-replay", "jobs", d.getJobId()));
                    return generateCreatedResponse(location, responseEntity).build();
                }
        );
    }

    // ---------------------------------------------------------------------------
    // POST /bulk-replay/jobs/{id}/cancel
    // ---------------------------------------------------------------------------

    @PUT
    @Path("/{id}/cancel")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Requests cancellation of a running bulk replay job",
            description = "In a cluster, replicated to all nodes. The worker stops after the current item completes.",
            responses = {
                    @ApiResponse(responseCode = "204", description = "The cancellation request was accepted."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found.")
            },
            security = {@SecurityRequirement(name = "Write - /provenance")}
    )
    public Response cancelBulkReplayJob(
            @Parameter(description = "The bulk replay job id.", required = true)
            @PathParam("id") final String id) {

        authorizeProvenanceRequest(RequestAction.WRITE);

        // Replicate to all nodes so every copy is marked cancelled.
        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT);
        }

        final ComponentEntity requestEntity = new ComponentEntity();
        requestEntity.setId(id);

        return withWriteLock(
                serviceFacade,
                requestEntity,
                lookup -> authorizeProvenanceRequest(RequestAction.WRITE),
                null,
                (entity) -> {
                    // Job may not exist on this node yet if replication is still in flight.
                    jobStore.findJobById(entity.getId()).ifPresent(job -> job.setCancelRequested(true));
                    return noCache(Response.noContent()).build();
                }
        );
    }

    // ---------------------------------------------------------------------------
    // DELETE /bulk-replay/jobs/{id}
    // ---------------------------------------------------------------------------

    @DELETE
    @Path("/{id}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Deletes a bulk replay job",
            description = "Removes the job from all cluster nodes.",
            responses = {
                    @ApiResponse(responseCode = "204", description = "The job was successfully deleted."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found.")
            },
            security = {@SecurityRequirement(name = "Write - /provenance")}
    )
    public Response deleteBulkReplayJob(
            @Parameter(description = "The bulk replay job id.", required = true)
            @PathParam("id") final String id) {

        // Replicate to all nodes so the job is removed everywhere.
        if (isReplicateRequest()) {
            return replicate("DELETE");
        }

        final ComponentEntity requestEntity = new ComponentEntity();
        requestEntity.setId(id);

        return withWriteLock(
                serviceFacade,
                requestEntity,
                lookup -> authorizeProvenanceRequest(RequestAction.WRITE),
                null,
                (entity) -> {
                    jobStore.findJobById(entity.getId()).ifPresent(job -> {
                        job.setCancelRequested(true);
                        job.setDeleteRequested(true);

                        final BulkReplayJobStatus status = job.getStatus();
                        final boolean active = status == BulkReplayJobStatus.QUEUED
                                || status == BulkReplayJobStatus.RUNNING
                                || status == BulkReplayJobStatus.INTERRUPTED;
                        if (!active) {
                            jobStore.remove(entity.getId());
                        }
                    });
                    return noCache(Response.noContent()).build();
                }
        );
    }

    // ---------------------------------------------------------------------------
    // GET /bulk-replay/jobs/config
    // ---------------------------------------------------------------------------

    @GET
    @Path("/config")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Gets bulk replay configuration",
            description = "Returns runtime-tunable bulk replay settings such as the node disconnect timeout.",
            responses = {
                    @ApiResponse(responseCode = "200"),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request.")
            },
            security = {@SecurityRequirement(name = "Read - /provenance")}
    )
    public Response getBulkReplayConfig() {
        authorizeProvenanceRequest(RequestAction.READ);

        // Served locally — all nodes have the config (replicated via PUT).
        final Map<String, String> config = Collections.singletonMap(
                "nodeDisconnectTimeout", executionService.getNodeDisconnectTimeout());
        return noCache(generateOkResponse(config)).build();
    }

    // ---------------------------------------------------------------------------
    // POST /bulk-replay/jobs/config
    // ---------------------------------------------------------------------------

    @PUT
    @Path("/config")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Updates bulk replay configuration",
            description = "Updates runtime-tunable settings on all cluster nodes. Changes are not persisted " +
                    "to nifi.properties and will revert on restart.",
            responses = {
                    @ApiResponse(responseCode = "200"),
                    @ApiResponse(responseCode = "400", description = "Invalid configuration value."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request.")
            },
            security = {@SecurityRequirement(name = "Write - /provenance")}
    )
    public Response updateBulkReplayConfig(final Map<String, String> configMap) {
        authorizeProvenanceRequest(RequestAction.WRITE);

        // Replicate to all nodes so every node has the updated config.
        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, configMap);
        }

        // withWriteLock handles the two-stage commit protocol for cluster-safe writes.
        final ComponentEntity requestEntity = new ComponentEntity();
        requestEntity.setId("bulk-replay-config");

        return withWriteLock(
                serviceFacade,
                requestEntity,
                lookup -> authorizeProvenanceRequest(RequestAction.WRITE),
                null,
                (entity) -> {
                    final String timeout = configMap.get("nodeDisconnectTimeout");
                    if (timeout == null || timeout.isBlank()) {
                        throw new IllegalArgumentException("nodeDisconnectTimeout is required.");
                    }
                    try {
                        executionService.setNodeDisconnectTimeout(timeout.trim());
                    } catch (final Exception e) {
                        throw new IllegalArgumentException("Invalid timeout value: " + timeout + ". " +
                                "Use a NiFi duration string such as '5 mins' or '30 secs'.");
                    }

                    final Map<String, String> result = Collections.singletonMap(
                            "nodeDisconnectTimeout", executionService.getNodeDisconnectTimeout());
                    return noCache(generateOkResponse(result)).build();
                }
        );
    }

    // ---------------------------------------------------------------------------
    // Spring wiring
    // ---------------------------------------------------------------------------

    @Autowired
    public void setServiceFacade(final NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    @Autowired
    public void setAuthorizer(final Authorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Autowired
    public void setJobStore(final BulkReplayJobStore jobStore) {
        this.jobStore = jobStore;
    }

    @Autowired
    public void setExecutionService(final BulkReplayExecutionService executionService) {
        this.executionService = executionService;
    }
}
