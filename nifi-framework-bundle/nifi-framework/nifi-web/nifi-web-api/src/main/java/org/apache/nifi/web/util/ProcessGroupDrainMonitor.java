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
package org.apache.nifi.web.util;

import jakarta.ws.rs.HttpMethod;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response.Status;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.http.replication.RequestReplicator;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.exception.NoClusterCoordinatorException;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.groups.ProcessGroupDrainState;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.ProcessGroupDrainRequestDTO;
import org.apache.nifi.web.api.entity.ProcessGroupDrainRequestEntity;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.context.SecurityContextHolder;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Ends a drain of a Process Group once the data has left the group, so that the drain ends even if no client is watching it. Only the node that
 * received the request to begin the drain watches it, acting as the user who began it. A FlowFile that is being moved between nodes by a
 * load-balanced connection can be missed by a single check, so the drain is ended only after two empty checks in a row. A node that is not
 * connected does not report its data, so no check counts while any node is not connected.
 */
public class ProcessGroupDrainMonitor {
    private static final Logger logger = LoggerFactory.getLogger(ProcessGroupDrainMonitor.class);

    private static final long CHECK_INTERVAL_SECONDS = 2;
    private static final int EMPTY_CHECKS_REQUIRED = 2;

    private final ClusterCoordinator clusterCoordinator;
    private final RequestReplicator requestReplicator;
    private final NiFiServiceFacade serviceFacade;
    private final ScheduledExecutorService executor;

    /**
     * @param clusterCoordinator the cluster coordinator, or null if this instance is not part of a cluster
     * @param requestReplicator the request replicator, or null if this instance is not part of a cluster
     */
    public ProcessGroupDrainMonitor(final ClusterCoordinator clusterCoordinator, final RequestReplicator requestReplicator, final NiFiServiceFacade serviceFacade) {
        this.clusterCoordinator = clusterCoordinator;
        this.requestReplicator = requestReplicator;
        this.serviceFacade = serviceFacade;
        this.executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            final Thread thread = new Thread(runnable, "Process Group Drain Monitor");
            thread.setDaemon(true);
            return thread;
        });
    }

    public void watch(final String processGroupId, final String drainRequestId, final NiFiUser user) {
        schedule(new DrainCheck(null, processGroupId, drainRequestId, user));
    }

    /**
     * @param drainRequestUri the URI of the drain request on this node, which is replicated across the cluster to check and end the drain
     */
    public void watch(final URI drainRequestUri, final String processGroupId, final String drainRequestId, final NiFiUser user) {
        schedule(new DrainCheck(drainRequestUri, processGroupId, drainRequestId, user));
    }

    public void shutdown() {
        executor.shutdownNow();
    }

    private void schedule(final DrainCheck check) {
        if (!executor.isShutdown()) {
            executor.schedule(check, CHECK_INTERVAL_SECONDS, TimeUnit.SECONDS);
        }
    }

    /**
     * Each check is run by the single monitor thread, so the number of empty checks needs no synchronization.
     */
    class DrainCheck implements Runnable {
        private final URI drainRequestUri;
        private final String processGroupId;
        private final String drainRequestId;
        private final NiFiUser user;
        private int emptyCheckCount = 0;

        DrainCheck(final URI drainRequestUri, final String processGroupId, final String drainRequestId, final NiFiUser user) {
            this.drainRequestUri = drainRequestUri;
            this.processGroupId = processGroupId;
            this.drainRequestId = drainRequestId;
            this.user = user;
        }

        @Override
        public void run() {
            if (check()) {
                schedule(this);
            }
        }

        /**
         * @return true if the drain should be checked again
         */
        boolean check() {
            SecurityContextHolder.getContext().setAuthentication(new NiFiAuthenticationToken(new NiFiUserDetails(user)));
            try {
                if (drainRequestUri != null && !clusterCoordinator.getNodeIdentifiers(NodeConnectionState.CONNECTING, NodeConnectionState.DISCONNECTING,
                        NodeConnectionState.DISCONNECTED, NodeConnectionState.OFFLOADING).isEmpty()) {
                    logger.debug("Not checking drain {} of Process Group {} because not every node is connected", drainRequestId, processGroupId);
                    emptyCheckCount = 0;
                    return true;
                }

                final Optional<ProcessGroupDrainRequestDTO> drainRequest = getDrainRequest();
                if (drainRequest.isPresent() && ProcessGroupDrainState.FAILED.name().equals(drainRequest.get().getState())) {
                    logger.info("Ending drain {} of Process Group {} because it failed: {}", drainRequestId, processGroupId, drainRequest.get().getFailureReason());
                    endDrain();
                    return false;
                }

                if (drainRequest.isEmpty() || !ProcessGroupDrainState.DRAINING.name().equals(drainRequest.get().getState())) {
                    logger.debug("Stopped watching drain {} of Process Group {} because it is no longer draining", drainRequestId, processGroupId);
                    return false;
                }

                if (drainRequest.get().getCurrentCount() > 0 || !Boolean.TRUE.equals(drainRequest.get().getPrepared())) {
                    emptyCheckCount = 0;
                    return true;
                }

                emptyCheckCount++;
                if (emptyCheckCount < EMPTY_CHECKS_REQUIRED) {
                    return true;
                }

                logger.info("Ending drain {} of Process Group {} because no FlowFiles were queued in the group for {} checks in a row", drainRequestId, processGroupId, emptyCheckCount);
                endDrain();
                return false;
            } catch (final ResourceNotFoundException e) {
                logger.info("Stopped watching drain {} because Process Group {} no longer exists", drainRequestId, processGroupId);
                return false;
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } catch (final Throwable t) {
                emptyCheckCount = 0;
                logger.warn("Failed to check drain {} of Process Group {}; will check again in {} seconds", drainRequestId, processGroupId, CHECK_INTERVAL_SECONDS, t);
                return true;
            } finally {
                SecurityContextHolder.getContext().setAuthentication(null);
            }
        }

        private Optional<ProcessGroupDrainRequestDTO> getDrainRequest() throws IOException, InterruptedException {
            if (drainRequestUri == null) {
                return serviceFacade.getProcessGroupDrainRequest(processGroupId, drainRequestId);
            }

            final NodeResponse response = replicate(HttpMethod.GET);
            if (response.getStatus() == Status.NOT_FOUND.getStatusCode()) {
                throw new ResourceNotFoundException("Process Group " + processGroupId + " was not found");
            }

            final ProcessGroupDrainRequestEntity entity = (ProcessGroupDrainRequestEntity) response.getUpdatedEntity();
            if (entity == null) {
                return Optional.ofNullable(response.getClientResponse().readEntity(ProcessGroupDrainRequestEntity.class).getDrainRequest());
            }

            return Optional.ofNullable(entity.getDrainRequest());
        }

        private void endDrain() throws IOException, InterruptedException {
            if (drainRequestUri == null) {
                serviceFacade.endProcessGroupDrain(processGroupId, drainRequestId);
                return;
            }

            replicate(HttpMethod.DELETE);
        }

        private NodeResponse replicate(final String method) throws IOException, InterruptedException {
            final Map<String, String> headers = new HashMap<>();
            headers.put("content-type", MediaType.APPLICATION_JSON);

            final NodeResponse response;
            if (clusterCoordinator.isActiveClusterCoordinator()) {
                response = requestReplicator.replicate(user, method, drainRequestUri, Collections.emptyMap(), headers).awaitMergedResponse();
            } else {
                final NodeIdentifier coordinatorNode = clusterCoordinator.getElectedActiveCoordinatorNode();
                if (coordinatorNode == null) {
                    throw new NoClusterCoordinatorException();
                }

                response = requestReplicator.forwardToCoordinator(coordinatorNode, user, method, drainRequestUri, Collections.emptyMap(), headers).awaitMergedResponse();
            }

            final int status = response.getStatus();
            if (status != Status.OK.getStatusCode() && status != Status.NOT_FOUND.getStatusCode()) {
                throw new IOException("Request to " + method + " " + drainRequestUri + " returned status " + status);
            }

            return response;
        }
    }
}
