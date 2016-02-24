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

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.cluster.manager.exception.UriConstructionException;
import org.apache.nifi.cluster.protocol.NodeIdentifier;

/**
 * A service for managing the replication of requests to nodes. It is up to the implementing class to decide if requests are sent concurrently or serially.
 *
 * Clients must call start() and stop() to initialize and shutdown the instance. The instance must be started before issuing any replication requests.
 *
 */
public interface HttpRequestReplicator {

    /**
     * Starts the instance for replicating requests. Start may only be called if the instance is not running.
     */
    void start();

    /**
     * Stops the instance from replicating requests. Stop may only be called if the instance is running.
     */
    void stop();

    /**
     * @return true if the instance is started; false otherwise.
     */
    boolean isRunning();

    /**
     * Requests are sent to each node in the cluster. If the request results in an exception, then the NodeResourceResponse will contain the exception.
     *
     * HTTP DELETE and OPTIONS methods must supply an empty parameters map or else and IllegalArgumentException is thrown.
     *
     * @param nodeIds the node identifiers
     * @param method the HTTP method (e.g., GET, POST, PUT, DELETE, HEAD, OPTIONS)
     * @param uri the base request URI (up to, but not including, the query string)
     * @param parameters any request parameters
     * @param headers any HTTP headers
     *
     * @return the set of node responses
     *
     * @throws UriConstructionException if a request for a node failed to be constructed from the given prototype URI. If thrown, it is guaranteed that no request was sent.
     */
    Set<NodeResponse> replicate(Set<NodeIdentifier> nodeIds, String method, URI uri, Map<String, List<String>> parameters, Map<String, String> headers) throws UriConstructionException;

    /**
     * Requests are sent to each node in the cluster. If the request results in an exception, then the NodeResourceResponse will contain the exception.
     *
     * HTTP DELETE, GET, HEAD, and OPTIONS methods will throw an IllegalArgumentException if used.
     *
     * @param nodeIds the node identifiers
     * @param method the HTTP method (e.g., POST, PUT)
     * @param uri the base request URI (up to, but not including, the query string)
     * @param entity an entity
     * @param headers any HTTP headers
     *
     * @return the set of node responses
     *
     * @throws UriConstructionException if a request for a node failed to be constructed from the given prototype URI. If thrown, it is guaranteed that no request was sent.
     */
    Set<NodeResponse> replicate(Set<NodeIdentifier> nodeIds, String method, URI uri, Object entity, Map<String, String> headers) throws UriConstructionException;

}
