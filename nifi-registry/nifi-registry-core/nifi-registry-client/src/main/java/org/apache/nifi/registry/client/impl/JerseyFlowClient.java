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
package org.apache.nifi.registry.client.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.client.FlowClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.RequestConfig;
import org.apache.nifi.registry.diff.VersionedFlowDifference;
import org.apache.nifi.registry.field.Fields;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.revision.entity.RevisionInfo;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Jersey implementation of FlowClient.
 */
public class JerseyFlowClient extends AbstractJerseyClient  implements FlowClient {

    private final WebTarget flowsTarget;
    private final WebTarget bucketFlowsTarget;

    public JerseyFlowClient(final WebTarget baseTarget) {
        this(baseTarget, null);
    }

    public JerseyFlowClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
        super(requestConfig);
        this.flowsTarget = baseTarget.path("/flows");
        this.bucketFlowsTarget = baseTarget.path("/buckets/{bucketId}/flows");
    }

    @Override
    public VersionedFlow create(final VersionedFlow flow) throws NiFiRegistryException, IOException {
        if (flow == null) {
            throw new IllegalArgumentException("VersionedFlow cannot be null");
        }

        final String bucketId = flow.getBucketIdentifier();
        if (StringUtils.isBlank(bucketId)) {
            throw new IllegalArgumentException("Bucket Identifier cannot be blank");
        }

        return executeAction("Error creating flow", () -> {
            final WebTarget target = bucketFlowsTarget
                    .resolveTemplate("bucketId", bucketId);

            return getRequestBuilder(target)
                    .post(
                            Entity.entity(flow, MediaType.APPLICATION_JSON),
                            VersionedFlow.class
                    );
        });
    }

    @Override
    public VersionedFlow get(final String bucketId, final String flowId) throws NiFiRegistryException, IOException {
        if (StringUtils.isBlank(bucketId)) {
            throw new IllegalArgumentException("Bucket Identifier cannot be blank");
        }

        if (StringUtils.isBlank(flowId)) {
            throw new IllegalArgumentException("Flow Identifier cannot be blank");
        }

        return executeAction("Error retrieving flow", () -> {
            final WebTarget target = bucketFlowsTarget
                    .path("/{flowId}")
                    .resolveTemplate("bucketId", bucketId)
                    .resolveTemplate("flowId", flowId);

            return  getRequestBuilder(target).get(VersionedFlow.class);
        });
    }

    @Override
    public VersionedFlow get(final String flowId) throws NiFiRegistryException, IOException {
        if (StringUtils.isBlank(flowId)) {
            throw new IllegalArgumentException("Flow Identifier cannot be blank");
        }

        // this uses the flowsTarget because its calling /flows/{flowId} without knowing a bucketId
        return executeAction("Error retrieving flow", () -> {
            final WebTarget target = flowsTarget
                    .path("/{flowId}")
                    .resolveTemplate("flowId", flowId);

            return  getRequestBuilder(target).get(VersionedFlow.class);
        });
    }

    @Override
    public VersionedFlow update(final String bucketId, final VersionedFlow flow) throws NiFiRegistryException, IOException {
        if (StringUtils.isBlank(bucketId)) {
            throw new IllegalArgumentException("Bucket Identifier cannot be blank");
        }

        if (flow == null) {
            throw new IllegalArgumentException("VersionedFlow cannot be null");
        }

        if (StringUtils.isBlank(flow.getIdentifier())) {
            throw new IllegalArgumentException("VersionedFlow identifier must be provided");
        }

        return executeAction("Error updating flow", () -> {
            final WebTarget target = bucketFlowsTarget
                    .path("/{flowId}")
                    .resolveTemplate("bucketId", bucketId)
                    .resolveTemplate("flowId", flow.getIdentifier());

            return  getRequestBuilder(target)
                    .put(
                            Entity.entity(flow, MediaType.APPLICATION_JSON),
                            VersionedFlow.class
                    );
        });
    }

    @Override
    public VersionedFlow delete(final String bucketId, final String flowId) throws NiFiRegistryException, IOException {
        return delete(bucketId, flowId, null);
    }

    @Override
    public VersionedFlow delete(final String bucketId, final String flowId, final RevisionInfo revision)
            throws NiFiRegistryException, IOException {
        if (StringUtils.isBlank(bucketId)) {
            throw new IllegalArgumentException("Bucket Identifier cannot be blank");
        }

        if (StringUtils.isBlank(flowId)) {
            throw new IllegalArgumentException("Flow Identifier cannot be blank");
        }

        return executeAction("Error deleting flow", () -> {
            WebTarget target = bucketFlowsTarget
                    .path("/{flowId}")
                    .resolveTemplate("bucketId", bucketId)
                    .resolveTemplate("flowId", flowId);

            target = addRevisionQueryParams(target, revision);

            return getRequestBuilder(target).delete(VersionedFlow.class);
        });
    }

    @Override
    public Fields getFields() throws NiFiRegistryException, IOException {
        return executeAction("Error retrieving fields info for flows", () -> {
            final WebTarget target = flowsTarget.path("/fields");
            return getRequestBuilder(target).get(Fields.class);
        });
    }

    @Override
    public List<VersionedFlow> getByBucket(final String bucketId) throws NiFiRegistryException, IOException {
        if (StringUtils.isBlank(bucketId)) {
            throw new IllegalArgumentException("Bucket Identifier cannot be blank");
        }

        return executeAction("Error getting flows for bucket", () -> {
            WebTarget target = bucketFlowsTarget;
            target = target.resolveTemplate("bucketId", bucketId);

            final VersionedFlow[] versionedFlows = getRequestBuilder(target).get(VersionedFlow[].class);
            return  versionedFlows == null ? Collections.emptyList() : Arrays.asList(versionedFlows);
        });
    }

    @Override
    public VersionedFlowDifference diff(final String bucketId, final String flowId,
                                        final Integer versionA, final Integer versionB) throws NiFiRegistryException, IOException {
        if (StringUtils.isBlank(bucketId)) {
            throw new IllegalArgumentException("Bucket Identifier cannot be blank");
        }

        if (StringUtils.isBlank(flowId)) {
            throw new IllegalArgumentException("Flow Identifier cannot be blank");
        }

        return executeAction("Error retrieving flow", () -> {
            final WebTarget target = bucketFlowsTarget
                    .path("/{flowId}/diff/{versionA}/{versionB}")
                    .resolveTemplate("bucketId", bucketId)
                    .resolveTemplate("flowId", flowId)
                    .resolveTemplate("versionA", versionA)
                    .resolveTemplate("versionB", versionB);

            return  getRequestBuilder(target).get(VersionedFlowDifference.class);
        });
    }
}
