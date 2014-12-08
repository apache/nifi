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
package org.apache.nifi.cluster.manager;

import org.apache.nifi.cluster.manager.exception.DisconnectedNodeMutableRequestException;
import org.apache.nifi.cluster.manager.exception.UriConstructionException;
import org.apache.nifi.cluster.manager.exception.ConnectingNodeMutableRequestException;
import org.apache.nifi.cluster.manager.exception.NoConnectedNodesException;
import org.apache.nifi.cluster.manager.exception.NoResponseFromNodesException;
import org.apache.nifi.cluster.manager.exception.SafeModeMutableRequestException;
import org.apache.nifi.cluster.protocol.NodeIdentifier;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Extends the ClusterManager interface to define how requests issued to the
 * cluster manager are federated to the nodes. Specifically, the HTTP protocol
 * is used for communicating requests to the cluster manager and to the nodes.
 *
 * @author unattributed
 */
public interface HttpClusterManager extends ClusterManager {

    /**
     * Federates the HTTP request to all connected nodes in the cluster. The
     * given URI's host and port will not be used and instead will be adjusted
     * for each node's host and port. The node URIs are guaranteed to be
     * constructed before issuing any requests, so if a UriConstructionException
     * is thrown, then it is guaranteed that no request was issued.
     *
     * @param method the HTTP method (e.g., GET, POST, PUT, DELETE, HEAD)
     * @param uri the base request URI (up to, but not including, the query
     * string)
     * @param parameters the request parameters
     * @param headers the request headers
     *
     * @return the client response
     *
     * @throws NoConnectedNodesException if no nodes are connected as results of
     * the request
     * @throws NoResponseFromNodesException if no response could be obtained
     * @throws UriConstructionException if there was an issue constructing the
     * URIs tailored for each individual node
     * @throws ConnectingNodeMutableRequestException if the request was a PUT,
     * POST, DELETE and a node is connecting to the cluster
     * @throws DisconnectedNodeMutableRequestException if the request was a PUT,
     * POST, DELETE and a node is disconnected from the cluster
     * @throws SafeModeMutableRequestException if the request was a PUT, POST,
     * DELETE and a the cluster is in safe mode
     */
    NodeResponse applyRequest(String method, URI uri, Map<String, List<String>> parameters, Map<String, String> headers)
            throws NoConnectedNodesException, NoResponseFromNodesException, UriConstructionException, ConnectingNodeMutableRequestException,
            DisconnectedNodeMutableRequestException, SafeModeMutableRequestException;

    /**
     * Federates the HTTP request to the nodes specified. The given URI's host
     * and port will not be used and instead will be adjusted for each node's
     * host and port. The node URIs are guaranteed to be constructed before
     * issuing any requests, so if a UriConstructionException is thrown, then it
     * is guaranteed that no request was issued.
     *
     * @param method the HTTP method (e.g., GET, POST, PUT, DELETE, HEAD)
     * @param uri the base request URI (up to, but not including, the query
     * string)
     * @param parameters the request parameters
     * @param headers the request headers
     * @param nodeIdentifiers the NodeIdentifier for each node that the request
     * should be replaced to
     *
     * @return the client response
     *
     * @throws NoConnectedNodesException if no nodes are connected as results of
     * the request
     * @throws NoResponseFromNodesException if no response could be obtained
     * @throws UriConstructionException if there was an issue constructing the
     * URIs tailored for each individual node
     * @throws ConnectingNodeMutableRequestException if the request was a PUT,
     * POST, DELETE and a node is connecting to the cluster
     * @throws DisconnectedNodeMutableRequestException if the request was a PUT,
     * POST, DELETE and a node is disconnected from the cluster
     * @throws SafeModeMutableRequestException if the request was a PUT, POST,
     * DELETE and a the cluster is in safe mode
     */
    NodeResponse applyRequest(String method, URI uri, Map<String, List<String>> parameters, Map<String, String> headers,
            Set<NodeIdentifier> nodeIdentifiers)
            throws NoConnectedNodesException, NoResponseFromNodesException, UriConstructionException, ConnectingNodeMutableRequestException,
            DisconnectedNodeMutableRequestException, SafeModeMutableRequestException;

    /**
     * Federates the HTTP request to all connected nodes in the cluster. The
     * given URI's host and port will not be used and instead will be adjusted
     * for each node's host and port. The node URIs are guaranteed to be
     * constructed before issuing any requests, so if a UriConstructionException
     * is thrown, then it is guaranteed that no request was issued.
     *
     * @param method the HTTP method (e.g., GET, POST, PUT, DELETE, HEAD)
     * @param uri the base request URI (up to, but not including, the query
     * string)
     * @param entity the HTTP request entity
     * @param headers the request headers
     *
     * @return the client response
     *
     * @throws NoConnectedNodesException if no nodes are connected as results of
     * the request
     * @throws NoResponseFromNodesException if no response could be obtained
     * @throws UriConstructionException if there was an issue constructing the
     * URIs tailored for each individual node
     * @throws ConnectingNodeMutableRequestException if the request was a PUT,
     * POST, DELETE and a node is connecting to the cluster
     * @throws DisconnectedNodeMutableRequestException if the request was a PUT,
     * POST, DELETE and a node is disconnected from the cluster
     * @throws SafeModeMutableRequestException if the request was a PUT, POST,
     * DELETE and a the cluster is in safe mode
     */
    NodeResponse applyRequest(String method, URI uri, Object entity, Map<String, String> headers)
            throws NoConnectedNodesException, NoResponseFromNodesException, UriConstructionException, ConnectingNodeMutableRequestException,
            DisconnectedNodeMutableRequestException, SafeModeMutableRequestException;

    /**
     * Federates the HTTP request to the nodes specified. The given URI's host
     * and port will not be used and instead will be adjusted for each node's
     * host and port. The node URIs are guaranteed to be constructed before
     * issuing any requests, so if a UriConstructionException is thrown, then it
     * is guaranteed that no request was issued.
     *
     * @param method the HTTP method (e.g., GET, POST, PUT, DELETE, HEAD)
     * @param uri the base request URI (up to, but not including, the query
     * string)
     * @param entity the HTTP request entity
     * @param headers the request headers
     * @param nodeIdentifiers the NodeIdentifier for each node that the request
     * should be replaced to
     *
     * @return the client response
     *
     * @throws NoConnectedNodesException if no nodes are connected as results of
     * the request
     * @throws NoResponseFromNodesException if no response could be obtained
     * @throws UriConstructionException if there was an issue constructing the
     * URIs tailored for each individual node
     * @throws ConnectingNodeMutableRequestException if the request was a PUT,
     * POST, DELETE and a node is connecting to the cluster
     * @throws DisconnectedNodeMutableRequestException if the request was a PUT,
     * POST, DELETE and a node is disconnected from the cluster
     * @throws SafeModeMutableRequestException if the request was a PUT, POST,
     * DELETE and a the cluster is in safe mode
     */
    NodeResponse applyRequest(String method, URI uri, Object entity, Map<String, String> headers, Set<NodeIdentifier> nodeIdentifiers)
            throws NoConnectedNodesException, NoResponseFromNodesException, UriConstructionException, ConnectingNodeMutableRequestException,
            DisconnectedNodeMutableRequestException, SafeModeMutableRequestException;
}
