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
package org.apache.nifi.toolkit.cli.impl.client.nifi.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ConnectionClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.RequestConfig;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.DropRequestEntity;
import org.apache.nifi.web.api.entity.FlowFileEntity;
import org.apache.nifi.web.api.entity.ListingRequestEntity;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.InputStream;

public class JerseyConnectionClient extends AbstractJerseyClient implements ConnectionClient {
    private final WebTarget connectionTarget;
    private final WebTarget processGroupTarget;
    private final WebTarget flowFileQueueTarget;

    public JerseyConnectionClient(final WebTarget baseTarget) {
        this(baseTarget, null);
    }

    public JerseyConnectionClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
        super(requestConfig);

        this.connectionTarget = baseTarget.path("/connections/{id}");
        this.processGroupTarget = baseTarget.path("/process-groups/{pgId}");
        this.flowFileQueueTarget = baseTarget.path("/flowfile-queues/{id}");
    }


    @Override
    public ConnectionEntity getConnection(final String id) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Connection id cannot be null or blank");
        }

        return executeAction("Error getting connection", () -> {
            final WebTarget target = connectionTarget
                .resolveTemplate("id", id);

            return getRequestBuilder(target).get(ConnectionEntity.class);
        });
    }

    @Override
    public ConnectionEntity deleteConnection(final String id, final String clientId, final long version) throws NiFiClientException, IOException {
        if (id == null) {
            throw new IllegalArgumentException("Connection id cannot be null");
        }

        return executeAction("Error deleting Connection", () -> {
            final WebTarget target = connectionTarget
                .queryParam("version", version)
                .queryParam("clientId", clientId)
                .resolveTemplate("id", id);

            return getRequestBuilder(target).delete(ConnectionEntity.class);
        });
    }

    @Override
    public ConnectionEntity deleteConnection(final ConnectionEntity connectionEntity) throws NiFiClientException, IOException {
        if (connectionEntity == null) {
            throw new IllegalArgumentException("Connection Entity cannot be null");
        }
        if (connectionEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision cannot be null");
        }

        return deleteConnection(connectionEntity.getId(), connectionEntity.getRevision().getClientId(), connectionEntity.getRevision().getVersion());
    }

    @Override
    public ConnectionEntity createConnection(final String parentGroupdId, final ConnectionEntity connectionEntity) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(parentGroupdId)) {
            throw new IllegalArgumentException("Parent process group id cannot be null or blank");
        }

        if (connectionEntity == null) {
            throw new IllegalArgumentException("Connection entity cannot be null");
        }

        return executeAction("Error creating Connection", () -> {
            final WebTarget target = processGroupTarget
                .path("/connections")
                .resolveTemplate("pgId", parentGroupdId);

            return getRequestBuilder(target).post(
                Entity.entity(connectionEntity, MediaType.APPLICATION_JSON_TYPE),
                ConnectionEntity.class
            );
        });
    }

    @Override
    public ConnectionEntity updateConnection(final ConnectionEntity connectionEntity) throws NiFiClientException, IOException {
        if (connectionEntity == null) {
            throw new IllegalArgumentException("Connection entity cannot be null");
        }

        return executeAction("Error updating Connection", () -> {
            final WebTarget target = connectionTarget
                .resolveTemplate("id", connectionEntity.getId());

            return getRequestBuilder(target).put(
                Entity.entity(connectionEntity, MediaType.APPLICATION_JSON_TYPE),
                ConnectionEntity.class
            );
        });
    }

    @Override
    public DropRequestEntity emptyQueue(final String connectionId) throws NiFiClientException, IOException {
        if (connectionId == null) {
            throw new IllegalArgumentException("Connection ID cannot be null");
        }

        return executeAction("Error empty queue for Connection", () -> {
            final WebTarget target = flowFileQueueTarget
                .path("drop-requests")
                .resolveTemplate("id", connectionId);

            return getRequestBuilder(target).post(
                Entity.entity(connectionId, MediaType.TEXT_PLAIN),
                DropRequestEntity.class
            );
        });
    }

    @Override
    public DropRequestEntity getDropRequest(final String connectionId, final String dropRequestId) throws NiFiClientException, IOException {
        if (connectionId == null) {
            throw new IllegalArgumentException("Connection ID cannot be null");
        }
        if (dropRequestId == null) {
            throw new IllegalArgumentException("Drop Request ID cannot be null");
        }

        return executeAction("Error retrieving Drop Request", () -> {
            final WebTarget target = flowFileQueueTarget
                .path("drop-requests/{requestId}")
                .resolveTemplate("id", connectionId)
                .resolveTemplate("requestId", dropRequestId);

            return getRequestBuilder(target).get(DropRequestEntity.class);
        });
    }

    @Override
    public DropRequestEntity deleteDropRequest(final String connectionId, final String dropRequestId) throws NiFiClientException, IOException {
        if (connectionId == null) {
            throw new IllegalArgumentException("Connection ID cannot be null");
        }
        if (dropRequestId == null) {
            throw new IllegalArgumentException("Drop Request ID cannot be null");
        }

        return executeAction("Error retrieving Drop Request", () -> {
            final WebTarget target = flowFileQueueTarget
                .path("drop-requests/{requestId}")
                .resolveTemplate("id", connectionId)
                .resolveTemplate("requestId", dropRequestId);

            return getRequestBuilder(target).delete(DropRequestEntity.class);
        });
    }

    @Override
    public ListingRequestEntity listQueue(final String connectionId) throws NiFiClientException, IOException {
        if (connectionId == null) {
            throw new IllegalArgumentException("Connection ID cannot be null");
        }

        return executeAction("Error listing queue for Connection", () -> {
            final WebTarget target = flowFileQueueTarget
                .path("listing-requests")
                .resolveTemplate("id", connectionId);

            return getRequestBuilder(target).post(
                Entity.entity(connectionId, MediaType.TEXT_PLAIN),
                ListingRequestEntity.class
            );
        });
    }

    @Override
    public ListingRequestEntity getListingRequest(final String connectionId, final String listingRequestId) throws NiFiClientException, IOException {
        if (connectionId == null) {
            throw new IllegalArgumentException("Connection ID cannot be null");
        }
        if (listingRequestId == null) {
            throw new IllegalArgumentException("Listing Request ID cannot be null");
        }

        return executeAction("Error retrieving Listing Request", () -> {
            final WebTarget target = flowFileQueueTarget
                .path("listing-requests/{requestId}")
                .resolveTemplate("id", connectionId)
                .resolveTemplate("requestId", listingRequestId);

            return getRequestBuilder(target).get(ListingRequestEntity.class);
        });
    }

    @Override
    public ListingRequestEntity deleteListingRequest(final String connectionId, final String listingRequestId) throws NiFiClientException, IOException {
        if (connectionId == null) {
            throw new IllegalArgumentException("Connection ID cannot be null");
        }
        if (listingRequestId == null) {
            throw new IllegalArgumentException("Listing Request ID cannot be null");
        }

        return executeAction("Error retrieving Listing Request", () -> {
            final WebTarget target = flowFileQueueTarget
                .path("listing-requests/{requestId}")
                .resolveTemplate("id", connectionId)
                .resolveTemplate("requestId", listingRequestId);

            return getRequestBuilder(target).delete(ListingRequestEntity.class);
        });
    }


    @Override
    public FlowFileEntity getFlowFile(final String connectionId, final String flowFileUuid) throws NiFiClientException, IOException {
        return getFlowFile(connectionId, flowFileUuid, null);
    }


    @Override
    public FlowFileEntity getFlowFile(final String connectionId, final String flowFileUuid, final String nodeId) throws NiFiClientException, IOException {
        if (connectionId == null) {
            throw new IllegalArgumentException("Connection ID cannot be null");
        }
        if (flowFileUuid == null) {
            throw new IllegalArgumentException("FlowFile UUID cannot be null");
        }

        return executeAction("Error retrieving FlowFile", () -> {
            WebTarget target = flowFileQueueTarget
                .path("flowfiles/{uuid}")
                .resolveTemplate("id", connectionId)
                .resolveTemplate("uuid", flowFileUuid);

            if (nodeId != null) {
                target = target.queryParam("clusterNodeId", nodeId);
            }

            return getRequestBuilder(target).get(FlowFileEntity.class);
        });
    }

    @Override
    public InputStream getFlowFileContent(final String connectionId, final String flowFileUuid, final String nodeId) throws NiFiClientException, IOException {
        if (connectionId == null) {
            throw new IllegalArgumentException("Connection ID cannot be null");
        }
        if (flowFileUuid == null) {
            throw new IllegalArgumentException("FlowFile UUID cannot be null");
        }

        return executeAction("Error retrieving FlowFile Content", () -> {
            WebTarget target = flowFileQueueTarget
                .path("flowfiles/{uuid}/content")
                .resolveTemplate("id", connectionId)
                .resolveTemplate("uuid", flowFileUuid);

            if (nodeId != null) {
                target = target.queryParam("clusterNodeId", nodeId);
            }

            return getRequestBuilder(target).get(InputStream.class);
        });
    }
}
