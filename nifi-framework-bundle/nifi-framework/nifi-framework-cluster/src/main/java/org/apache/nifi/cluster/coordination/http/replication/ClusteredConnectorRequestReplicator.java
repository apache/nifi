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
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.StandardNiFiUser;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.components.connector.ConnectorRequestReplicator;
import org.apache.nifi.components.connector.ConnectorState;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.Entity;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

public class ClusteredConnectorRequestReplicator implements ConnectorRequestReplicator {
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
        final RequestReplicator requestReplicator = getRequestReplicator();
        final NiFiUser nodeUser = getNodeUser();
        final URI uri = URI.create(replicationScheme + "://localhost/nifi-api/connectors/" + connectorId);
        final AsyncClusterResponse asyncResponse = requestReplicator.replicate(nodeUser, GET, uri, Map.of(), Map.of());

        try {
            final NodeResponse mergedNodeResponse = asyncResponse.awaitMergedResponse();
            final Response response = mergedNodeResponse.getClientResponse();
            verifyResponse(response.getStatusInfo(), connectorId);

            // Use the merged/updated entity if available, otherwise fall back to reading from the raw response.
            // The updatedEntity contains the properly merged state from all nodes, while readEntity() would
            // only return the state from whichever single node was selected as the "client response".
            final ConnectorEntity connectorEntity;
            final Entity updatedEntity = mergedNodeResponse.getUpdatedEntity();
            if (updatedEntity instanceof ConnectorEntity mergedConnectorEntity) {
                connectorEntity = mergedConnectorEntity;
            } else {
                connectorEntity = response.readEntity(ConnectorEntity.class);
            }

            final String stateName = connectorEntity.getComponent().getState();
            try {
                return ConnectorState.valueOf(stateName);
            } catch (final IllegalArgumentException e) {
                throw new IOException("Received unknown Connector state '" + stateName + "' for Connector with ID " + connectorId);
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for response for status of Connector with ID " + connectorId, e);
        }
    }

    @Override
    public void setFlowManager(final FlowManager flowManager) {
    }

    private RequestReplicator getRequestReplicator() {
        final RequestReplicator requestReplicator = requestReplicatorSupplier.get();
        return Objects.requireNonNull(requestReplicator, "Request Replicator required");
    }

    private NiFiUser getNodeUser() {
        final ClusterCoordinator clusterCoordinator = clusterCoordinatorSupplier.get();
        Objects.requireNonNull(clusterCoordinator, "Cluster Coordinator required");

        final NodeIdentifier localNodeIdentifier = clusterCoordinator.getLocalNodeIdentifier();
        Objects.requireNonNull(localNodeIdentifier, "Local Node Identifier required");

        final Set<String> nodeIdentities = localNodeIdentifier.getNodeIdentities();
        final String nodeIdentity = nodeIdentities.isEmpty() ? localNodeIdentifier.getApiAddress() : nodeIdentities.iterator().next();
        return new StandardNiFiUser.Builder().identity(nodeIdentity).build();
    }

    private void verifyResponse(final StatusType responseStatusType, final String connectorId) throws IOException {
        final int statusCode = responseStatusType.getStatusCode();
        final String reason = responseStatusType.getReasonPhrase();

        if (responseStatusType.getStatusCode() == Status.NOT_FOUND.getStatusCode()) {
            throw new IllegalArgumentException("Connector with ID + " + connectorId + " does not exist");
        }

        final Family responseFamily = responseStatusType.getFamily();
        if (responseFamily == Family.SERVER_ERROR) {
            throw new IOException("Server-side error requesting State for Connector with ID + " + connectorId + ". Status code: " + statusCode + ", reason: " + reason);
        }
        if (responseFamily == Family.CLIENT_ERROR) {
            throw new IOException("Client-side error requesting State for Connector with ID " + connectorId + ". Status code: " + statusCode + ", reason: " + reason);
        }
        if (responseFamily != Family.SUCCESSFUL) {
            throw new IOException("Unexpected response code " + statusCode + " returned when requesting State for Connector with ID " + connectorId + ": " + reason);
        }
    }
}
