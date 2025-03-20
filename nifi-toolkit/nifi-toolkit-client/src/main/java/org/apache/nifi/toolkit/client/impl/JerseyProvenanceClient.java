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
package org.apache.nifi.toolkit.client.impl;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.toolkit.client.ProvenanceClient;
import org.apache.nifi.toolkit.client.RequestConfig;
import org.apache.nifi.web.api.entity.LatestProvenanceEventsEntity;
import org.apache.nifi.web.api.entity.LineageEntity;
import org.apache.nifi.web.api.entity.ProvenanceEntity;
import org.apache.nifi.web.api.entity.ReplayLastEventRequestEntity;
import org.apache.nifi.web.api.entity.ReplayLastEventResponseEntity;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public class JerseyProvenanceClient extends AbstractJerseyClient implements ProvenanceClient {
    private final WebTarget provenanceTarget;
    private final WebTarget provenanceEventsTarget;

    public JerseyProvenanceClient(final WebTarget baseTarget) {
        this(baseTarget, null);
    }

    public JerseyProvenanceClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
        super(requestConfig);
        this.provenanceTarget = baseTarget.path("/provenance");
        this.provenanceEventsTarget = baseTarget.path("/provenance-events");
    }

    @Override
    public ProvenanceEntity submitProvenanceQuery(final ProvenanceEntity provenanceEntity) throws NiFiClientException, IOException {
        if (provenanceEntity == null) {
            throw new IllegalArgumentException("Provenance entity cannot be null");
        }

        return executeAction("Error submitting Provenance Query", () -> getRequestBuilder(provenanceTarget).post(
                Entity.entity(provenanceEntity, MediaType.APPLICATION_JSON_TYPE),
                ProvenanceEntity.class));
    }

    @Override
    public ProvenanceEntity getProvenanceQuery(final String queryId) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(queryId)) {
            throw new IllegalArgumentException("Query ID cannot be null");
        }

        return executeAction("Error retrieving status of Provenance Query", () -> {
            final WebTarget target = provenanceTarget.path("/{id}").resolveTemplate("id", queryId);
            return getRequestBuilder(target).get(ProvenanceEntity.class);
        });
    }

    @Override
    public ProvenanceEntity deleteProvenanceQuery(final String provenanceQueryId) throws NiFiClientException, IOException {
        if (provenanceQueryId == null) {
            throw new IllegalArgumentException("Provenance Query ID cannot be null");
        }

        return executeAction("Error deleting Provenance Query", () -> {
            final WebTarget target = provenanceTarget.path("/{id}")
                    .resolveTemplate("id", provenanceQueryId);

            return getRequestBuilder(target).delete(ProvenanceEntity.class);
        });
    }

    @Override
    public LineageEntity submitLineageRequest(final LineageEntity lineageEntity) throws NiFiClientException, IOException {
        if (lineageEntity == null) {
            throw new IllegalArgumentException("Lineage entity cannot be null");
        }

        return executeAction("Error submitting Provenance Lineage Request", () -> getRequestBuilder(provenanceTarget.path("lineage")).post(
                Entity.entity(lineageEntity, MediaType.APPLICATION_JSON_TYPE),
                LineageEntity.class));
    }

    @Override
    public LineageEntity getLineageRequest(final String lineageRequestId) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(lineageRequestId)) {
            throw new IllegalArgumentException("Lineage Request ID cannot be null");
        }

        return executeAction("Error retrieving status of Provenance Lineage Request", () -> {
            final WebTarget target = provenanceTarget.path("/lineage/{id}").resolveTemplate("id", lineageRequestId);
            return getRequestBuilder(target).get(LineageEntity.class);
        });
    }

    @Override
    public LineageEntity deleteLineageRequest(final String lineageRequestId) throws NiFiClientException, IOException {
        if (lineageRequestId == null) {
            throw new IllegalArgumentException("Lineage Request ID cannot be null");
        }

        return executeAction("Error deleting Provenance Lineage Request", () -> {
            final WebTarget target = provenanceTarget.path("/lineage/{id}")
                    .resolveTemplate("id", lineageRequestId);

            return getRequestBuilder(target).delete(LineageEntity.class);
        });

    }

    @Override
    public ReplayLastEventResponseEntity replayLastEvent(final String processorId, final ReplayEventNodes nodes) throws NiFiClientException, IOException {
        Objects.requireNonNull(processorId, "Processor ID required");
        Objects.requireNonNull(nodes, "Nodes must be specified");

        final ReplayLastEventRequestEntity requestEntity = new ReplayLastEventRequestEntity();
        requestEntity.setComponentId(processorId);
        requestEntity.setNodes(nodes.name());

        return executeAction("Error replaying last event for Processor " + processorId, () -> {
            final WebTarget target = provenanceEventsTarget.path("/latest/replays");
            return getRequestBuilder(target).post(
                    Entity.entity(requestEntity, MediaType.APPLICATION_JSON_TYPE),
                    ReplayLastEventResponseEntity.class);
        });
    }

    @Override
    public LatestProvenanceEventsEntity getLatestEvents(final String processorId) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(processorId)) {
            throw new IllegalArgumentException("Processor ID must be specified");
        }

        return executeAction("Error getting latest events for Processor " + processorId, () -> {
            final WebTarget target = provenanceEventsTarget.path("/latest/").path(processorId);
            return getRequestBuilder(target).get(LatestProvenanceEventsEntity.class);
        });
    }

    @Override
    public InputStream getInputFlowFileContent(final String provenanceEventId, final String nodeId) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(provenanceEventId)) {
            throw new IllegalArgumentException("Provenance Event ID must be specified");
        }

        return executeAction("Error retrieving Input FlowFile Content from provenance event", () -> {
            WebTarget target = provenanceEventsTarget
                    .path("/{id}/content/input")
                    .resolveTemplate("id", provenanceEventId);

            if (nodeId != null) {
                target = target.queryParam("clusterNodeId", nodeId);
            }

            return getRequestBuilder(target).get(InputStream.class);
        });
    }

    @Override
    public InputStream getOutputFlowFileContent(String provenanceEventId, final String nodeId) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(provenanceEventId)) {
            throw new IllegalArgumentException("Provenance Event ID must be specified");
        }

        return executeAction("Error retrieving Output FlowFile Content from provenance event", () -> {
            WebTarget target = provenanceEventsTarget
                    .path("/{id}/content/output")
                    .resolveTemplate("id", provenanceEventId);

            if (nodeId != null) {
                target = target.queryParam("clusterNodeId", nodeId);
            }

            return getRequestBuilder(target).get(InputStream.class);
        });
    }
}
