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
import org.apache.nifi.components.connector.ConnectorRequestReplicator;
import org.apache.nifi.components.connector.ConnectorState;
import org.apache.nifi.controller.flow.FlowManager;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public class ClusteredConnectorRequestReplicator implements ConnectorRequestReplicator {
    private static final String GET = "GET";

    private final Supplier<RequestReplicator> requestReplicatorSupplier;

    public ClusteredConnectorRequestReplicator(final Supplier<RequestReplicator> requestReplicatorSupplier) {
        this.requestReplicatorSupplier = Objects.requireNonNull(requestReplicatorSupplier, "Request Replicator Supplier required");
    }

    @Override
    public ConnectorState getState(final String connectorId) throws IOException {
        final RequestReplicator requestReplicator = getRequestReplicator();
        final AsyncClusterResponse asyncResponse = requestReplicator.replicate(GET, URI.create("/nifi-api/connectors/" + connectorId + "/status"), null, Map.of());
        try {
            final Response response = asyncResponse.awaitMergedResponse().getClientResponse();
            verifyResponse(response.getStatusInfo(), connectorId);

            // TODO: Need a ConnectorStatusEntity? Need correct URI.
            return response.readEntity(ConnectorState.class);
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
