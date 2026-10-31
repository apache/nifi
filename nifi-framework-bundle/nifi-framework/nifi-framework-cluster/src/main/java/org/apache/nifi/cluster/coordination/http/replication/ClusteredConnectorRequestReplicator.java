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

package org.apache.nifi.cluster.coordination.http.replication;

import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import jakarta.ws.rs.core.Response.Status.Family;
import jakarta.ws.rs.core.Response.StatusType;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.node.DisconnectionCode;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.exception.NoConnectedNodesException;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.components.connector.ConnectorRequestReplicator;
import org.apache.nifi.components.connector.ConnectorState;
import org.apache.nifi.components.connector.FlowEvolutionStatus;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.Entity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public class ClusteredConnectorRequestReplicator implements ConnectorRequestReplicator {
    private static final Logger logger = LoggerFactory.getLogger(ClusteredConnectorRequestReplicator.class);
    private static final String GET = "GET";

    private final Supplier<RequestReplicator> requestReplicatorSupplier;
    private final Supplier<ClusterCoordinator> clusterCoordinatorSupplier;
    private final String replicationScheme;

    public ClusteredConnectorRequestReplicator(final Supplier<RequestReplicator> requestReplicatorSupplier, final Supplier<ClusterCoordinator> clusterCoordinatorSupplier,
            final boolean httpsEnabled) {
        this.requestReplicatorSupplier = Objects.requireNonNull(requestReplicatorSupplier, "Request Replicator Supplier required");
        this.clusterCoordinatorSupplier = Objects.requireNonNull(clusterCoordinatorSupplier, "Cluster Coordinator Supplier required");
        this.replicationScheme = httpsEnabled ? "https" : "http";
    }

    @Override
    public ConnectorState getState(final String connectorId) throws IOException {
        final ConnectorEntity connectorEntity = getConnectorEntity(connectorId, false);
        final String stateName = connectorEntity.getComponent().getState();
        logger.debug("getState: Connector [{}] — state from response: [{}]", connectorId, stateName);
        try {
            return ConnectorState.valueOf(stateName);
        } catch (final IllegalArgumentException e) {
            throw new IOException("Received unknown Connector state '" + stateName + "' for Connector with ID " + connectorId);
        }
    }

    @Override
    public FlowEvolutionStatus getFlowEvolutionStatus(final String connectorId) throws IOException {
        final ConnectorEntity connectorEntity = getConnectorEntity(connectorId, true);
        final String statusName = connectorEntity.getComponent().getFlowEvolutionStatus();
        if (statusName == null) {
            return FlowEvolutionStatus.INACTIVE;
        }

        try {
            return FlowEvolutionStatus.valueOf(statusName);
        } catch (final IllegalArgumentException e) {
            throw new IOException("Received unknown flow evolution status '" + statusName + "' for Connector with ID " + connectorId);
        }
    }

    private ConnectorEntity getConnectorEntity(final String connectorId, final boolean requireAllConnected) throws IOException {
        final RequestReplicator requestReplicator = getRequestReplicator();
        verifyClusterMembership(connectorId, requireAllConnected);

        final URI uri = URI.create(replicationScheme + "://localhost/nifi-api/connectors/" + connectorId);

        logger.debug("Connector [{}] — replicating GET to URI [{}]", connectorId, uri);

        // User is null. GET /connectors/{id} API endpoint has special logic for authorizing requests made by nifi cluster nodes based on mTLS cert SANs
        final AsyncClusterResponse asyncResponse = requestReplicator.replicate(null, GET, uri, Map.of(), Map.of());

        try {
            final NodeResponse mergedNodeResponse = asyncResponse.awaitMergedResponse();
            if (mergedNodeResponse == null) {
                throw new IOException("No merged response received while retrieving Connector " + connectorId);
            }
            final Entity updatedEntity = mergedNodeResponse.getUpdatedEntity();
            final Response response = mergedNodeResponse.getClientResponse();
            if (response != null) {
                final int statusCode = response.getStatusInfo().getStatusCode();
                logger.debug("Connector [{}] — merged response status [{}] ({})", connectorId, statusCode, response.getStatusInfo().getReasonPhrase());
                verifyResponse(response.getStatusInfo(), connectorId);
            } else if (mergedNodeResponse.hasThrowable()) {
                throw new IOException("Failed to retrieve Connector state for " + connectorId, mergedNodeResponse.getThrowable());
            } else if (!(updatedEntity instanceof ConnectorEntity)) {
                throw new IOException("Received neither response nor merged Connector entity while retrieving state for Connector " + connectorId);
            }

            // Use the merged/updated entity if available, otherwise fall back to reading from the raw response.
            // The updatedEntity contains the properly merged state from all nodes, while readEntity() would
            // only return the state from whichever single node was selected as the "client response".
            final ConnectorEntity connectorEntity;
            if (updatedEntity instanceof final ConnectorEntity mergedConnectorEntity) {
                logger.debug("Connector [{}] - using merged updatedEntity", connectorId);
                connectorEntity = mergedConnectorEntity;
            } else {
                logger.debug("Connector [{}] — updatedEntity is [{}], falling back to readEntity()", connectorId,
                        updatedEntity == null ? "null" : updatedEntity.getClass().getSimpleName());
                connectorEntity = response.readEntity(ConnectorEntity.class);
            }

            if (connectorEntity == null) {
                throw new IOException("Received null ConnectorEntity for Connector with ID " + connectorId);
            }
            if (connectorEntity.getComponent() == null) {
                throw new IOException("Received ConnectorEntity with null component for Connector with ID " + connectorId
                        + ". Permissions canRead=" + (connectorEntity.getPermissions() != null ? connectorEntity.getPermissions().getCanRead() : "null"));
            }

            if (requireAllConnected) {
                for (final NodeIdentifier node : asyncResponse.getNodesInvolved()) {
                    final NodeResponse nodeResponse = asyncResponse.getNodeResponse(node);
                    if (nodeResponse == null || !nodeResponse.is2xx()) {
                        throw new IOException("Failed to retrieve flow evolution status for Connector " + connectorId + " from node " + node);
                    }
                }
            }
            return connectorEntity;
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for response for status of Connector with ID " + connectorId, e);
        }
    }

    private void verifyClusterMembership(final String connectorId, final boolean requireAllConnected) throws IOException {
        final ClusterCoordinator clusterCoordinator = clusterCoordinatorSupplier.get();
        if (clusterCoordinator == null || !clusterCoordinator.isConnected()) {
            throw new NoConnectedNodesException();
        }

        for (final NodeConnectionStatus connectionStatus : clusterCoordinator.getConnectionStatuses()) {
            final NodeConnectionState state = connectionStatus.getState();
            if (state == NodeConnectionState.CONNECTING
                    || (state == NodeConnectionState.DISCONNECTED && connectionStatus.getDisconnectCode() == DisconnectionCode.NOT_YET_CONNECTED)) {
                throw new NoConnectedNodesException();
            }
            if (requireAllConnected && state != NodeConnectionState.CONNECTED && state != NodeConnectionState.REMOVED) {
                throw new IOException("Cannot retrieve flow evolution status for Connector " + connectorId
                        + " because node " + connectionStatus.getNodeIdentifier() + " is " + state);
            }
        }
    }

    @Override
    public void setFlowManager(final FlowManager flowManager) {
    }

    private RequestReplicator getRequestReplicator() {
        final RequestReplicator requestReplicator = requestReplicatorSupplier.get();
        return Objects.requireNonNull(requestReplicator, "Request Replicator required");
    }

    private void verifyResponse(final StatusType responseStatusType, final String connectorId) throws IOException {
        final int statusCode = responseStatusType.getStatusCode();
        final String reason = responseStatusType.getReasonPhrase();

        if (responseStatusType.getStatusCode() == Status.NOT_FOUND.getStatusCode()) {
            throw new IllegalArgumentException("Connector with ID " + connectorId + " does not exist");
        }

        final Family responseFamily = responseStatusType.getFamily();
        if (responseFamily == Family.SERVER_ERROR) {
            throw new IOException("Server-side error requesting State for Connector with ID " + connectorId + ". Status code: " + statusCode + ", reason: " + reason);
        }
        if (responseFamily == Family.CLIENT_ERROR) {
            throw new IOException("Client-side error requesting State for Connector with ID " + connectorId + ". Status code: " + statusCode + ", reason: " + reason);
        }
        if (responseFamily != Family.SUCCESSFUL) {
            throw new IOException("Unexpected response code " + statusCode + " returned when requesting State for Connector with ID " + connectorId + ": " + reason);
        }
    }
}
