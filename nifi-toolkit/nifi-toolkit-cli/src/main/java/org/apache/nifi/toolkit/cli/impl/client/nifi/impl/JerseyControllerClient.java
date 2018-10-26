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
import org.apache.nifi.toolkit.cli.impl.client.nifi.ControllerClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.web.api.entity.ClusterEntity;
import org.apache.nifi.web.api.entity.NodeEntity;
import org.apache.nifi.web.api.entity.RegistryClientEntity;
import org.apache.nifi.web.api.entity.RegistryClientsEntity;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Jersey implementation of ControllerClient.
 */
public class JerseyControllerClient extends AbstractJerseyClient implements ControllerClient {

    private final WebTarget controllerTarget;

    public JerseyControllerClient(final WebTarget baseTarget) {
        this(baseTarget, Collections.emptyMap());
    }

    public JerseyControllerClient(final WebTarget baseTarget, final Map<String,String> headers) {
        super(headers);
        this.controllerTarget = baseTarget.path("/controller");
    }

    @Override
    public RegistryClientsEntity getRegistryClients() throws NiFiClientException, IOException {
        return executeAction("Error retrieving registry clients", () -> {
            final WebTarget target = controllerTarget.path("registry-clients");
            return getRequestBuilder(target).get(RegistryClientsEntity.class);
        });
    }

    @Override
    public RegistryClientEntity getRegistryClient(final String id) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Registry client id cannot be null");
        }

        final WebTarget target = controllerTarget
                .path("registry-clients/{id}")
                .resolveTemplate("id", id);

        return getRequestBuilder(target).get(RegistryClientEntity.class);
    }

    @Override
    public RegistryClientEntity createRegistryClient(final RegistryClientEntity registryClient) throws NiFiClientException, IOException {
        if (registryClient == null) {
            throw new IllegalArgumentException("Registry client entity cannot be null");
        }

        return executeAction("Error creating registry client", () -> {
            final WebTarget target = controllerTarget.path("registry-clients");

            return getRequestBuilder(target).post(
                    Entity.entity(registryClient, MediaType.APPLICATION_JSON),
                    RegistryClientEntity.class
            );
        });
    }

    @Override
    public RegistryClientEntity updateRegistryClient(final RegistryClientEntity registryClient) throws NiFiClientException, IOException {
        if (registryClient == null) {
            throw new IllegalArgumentException("Registry client entity cannot be null");
        }

        if (StringUtils.isBlank(registryClient.getId())) {
            throw new IllegalArgumentException("Registry client entity must contain an id");
        }

        return executeAction("Error updating registry client", () -> {
            final WebTarget target = controllerTarget
                    .path("registry-clients/{id}")
                    .resolveTemplate("id", registryClient.getId());

            return getRequestBuilder(target).put(
                    Entity.entity(registryClient, MediaType.APPLICATION_JSON),
                    RegistryClientEntity.class
            );
        });
    }

    @Override
    public NodeEntity deleteNode(final String nodeId) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(nodeId)) {
            throw new IllegalArgumentException("Node ID cannot be null or empty");
        }

        return executeAction("Error deleting node", () -> {
            final WebTarget target = controllerTarget.path("cluster/nodes/" + nodeId);

            return getRequestBuilder(target).delete(NodeEntity.class);
        });
    }

    @Override
    public NodeEntity connectNode(final String nodeId, final NodeEntity nodeEntity) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(nodeId)) {
            throw new IllegalArgumentException("Node ID cannot be null or empty");
        }

        if (nodeEntity == null) {
            throw new IllegalArgumentException("Node entity cannot be null");
        }

        return executeAction("Error connecting node", () -> {
            final WebTarget target = controllerTarget.path("cluster/nodes/" + nodeId);

            return getRequestBuilder(target).put(Entity.entity(nodeEntity, MediaType.APPLICATION_JSON), NodeEntity.class);
        });
    }

    @Override
    public NodeEntity offloadNode(final String nodeId, final NodeEntity nodeEntity) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(nodeId)) {
            throw new IllegalArgumentException("Node ID cannot be null or empty");
        }

        if (nodeEntity == null) {
            throw new IllegalArgumentException("Node entity cannot be null");
        }

        return executeAction("Error offloading node", () -> {
            final WebTarget target = controllerTarget.path("cluster/nodes/" + nodeId);

            return getRequestBuilder(target).put(Entity.entity(nodeEntity, MediaType.APPLICATION_JSON), NodeEntity.class);
        });
    }

    @Override
    public NodeEntity disconnectNode(final String nodeId, final NodeEntity nodeEntity) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(nodeId)) {
            throw new IllegalArgumentException("Node ID cannot be null or empty");
        }

        if (nodeEntity == null) {
            throw new IllegalArgumentException("Node entity cannot be null");
        }

        return executeAction("Error disconnecting node", () -> {
            final WebTarget target = controllerTarget.path("cluster/nodes/" + nodeId);

            return getRequestBuilder(target).put(Entity.entity(nodeEntity, MediaType.APPLICATION_JSON), NodeEntity.class);
        });
    }

    @Override
    public NodeEntity getNode(String nodeId) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(nodeId)) {
            throw new IllegalArgumentException("Node ID cannot be null or empty");
        }

        return executeAction("Error retrieving node status", () -> {
            final WebTarget target = controllerTarget.path("cluster/nodes/" + nodeId);

            return getRequestBuilder(target).get(NodeEntity.class);
        });
    }

    @Override
    public ClusterEntity getNodes() throws NiFiClientException, IOException {
        return executeAction("Error retrieving node status", () -> {
            final WebTarget target = controllerTarget.path("cluster");

            return getRequestBuilder(target).get(ClusterEntity.class);
        });
    }
}
