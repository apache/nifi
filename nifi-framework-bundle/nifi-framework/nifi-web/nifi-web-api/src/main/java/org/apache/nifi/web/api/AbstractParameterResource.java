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

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.util.LifecycleManagementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.ws.rs.HttpMethod;

import java.net.URI;
import java.util.Map;

public abstract class AbstractParameterResource extends ApplicationResource {
    private static final Logger logger = LoggerFactory.getLogger(AbstractParameterResource.class);

    public NodeResponse updateParameterContext(final ParameterContextEntity parameterContext, final URI updateUri,
                                               final Map<String, String> headers, final NiFiUser user) throws LifecycleManagementException {
        final NodeResponse clusterResponse;
        try {
            logger.debug("Replicating PUT request to {} for user {}", updateUri, user);

            if (getReplicationTarget() == ReplicationTarget.CLUSTER_NODES) {
                clusterResponse = getRequestReplicator().replicate(user, HttpMethod.PUT, updateUri, parameterContext, headers).awaitMergedResponse();
            } else {
                clusterResponse = getRequestReplicator().forwardToCoordinator(
                        getClusterCoordinatorNode(), user, HttpMethod.PUT, updateUri, parameterContext, headers).awaitMergedResponse();
            }
        } catch (final InterruptedException ie) {
            logger.warn("Interrupted while replicating PUT request to {} for user {}", updateUri, user);
            Thread.currentThread().interrupt();
            throw new LifecycleManagementException("Interrupted while updating flows across cluster", ie);
        }
        return clusterResponse;
    }
}
