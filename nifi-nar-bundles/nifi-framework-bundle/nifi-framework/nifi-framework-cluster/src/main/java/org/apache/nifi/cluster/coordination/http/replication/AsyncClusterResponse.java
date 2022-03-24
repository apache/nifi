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

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;

/**
 * A response that is provided when an HTTP Request is made that must be federated to nodes in the cluster
 */
public interface AsyncClusterResponse {

    /**
     * @return the unique identifier of the request
     */
    String getRequestIdentifier();

    /**
     * @return the HTTP Method that was used for the request
     */
    String getMethod();

    /**
     * @return the Path of the URI that was used for the request
     */
    String getURIPath();

    /**
     * @return a Set that contains the Node Identifier of each node to which the request was replicated
     */
    Set<NodeIdentifier> getNodesInvolved();

    /**
     * @return a Set that contains the Node Identifier of each node for which the request has completed (either with
     *         a successful response or a timeout)
     */
    Set<NodeIdentifier> getCompletedNodeIdentifiers();

    /**
     * @return a Set that contains the Node Response of each node for which the request has completed (either with a
     *         successful response or a timeout)
     */
    Set<NodeResponse> getCompletedNodeResponses();

    /**
     * @return <code>true</code> if all nodes have responded (or timed out), <code>false</code> if still waiting on a response
     *         from one or more nodes
     */
    boolean isComplete();

    /**
     * Indicates whether or not the request was created more than the specified amount of time ago.
     * For example, if called via
     * <code>isOlderThan(3, TimeUnit.SECONDS)</code>
     * the method will return <code>true</code> if the request was created more than 3 seconds ago.
     *
     * @param time the amount of time to check
     * @param timeUnit the associated time unit
     * @return <code>true</code> if the request was created before (now - time)
     */
    boolean isOlderThan(long time, TimeUnit timeUnit);

    /**
     * @return the {@link NodeResponse} that represents the merged result from all nodes, or <code>null</code> if this request
     *         is not yet complete
     *
     * @throws RuntimeException if the request could not be completed for some reason, a
     *             RuntimeException will be thrown that indicates why the request failed
     */
    NodeResponse getMergedResponse();

    /**
     * Blocks until the request has completed and then returns the merged response
     *
     * @return the NodeResponse that represents the merged result from all nodes
     *
     * @throws InterruptedException if the thread is interrupted while waiting
     * @throws RuntimeException if the request could not be completed for some reason, a
     *             RuntimeException will be thrown that indicates why the request failed
     */
    NodeResponse awaitMergedResponse() throws InterruptedException;

    /**
     * Blocks until the request has completed or until the given timeout elapses. At that point, if the request has completed, then
     * the merged response is returned. If the timeout elapses before the request completes, then <code>null</code> will be returned.
     *
     * @return the NodeResponse that represents the merged result from all nodes, or <code>null</code> if the given timeout elapses
     *         before the request completes
     *
     * @throws InterruptedException if the thread is interrupted while waiting
     * @throws RuntimeException if the request could not be completed for some reason, a
     *             RuntimeException will be thrown that indicates why the request failed
     *
     */
    NodeResponse awaitMergedResponse(long timeout, TimeUnit timeUnit) throws InterruptedException;

    /**
     * Returns the NodeResponse that represents the individual response from the node with the given identifier
     *
     * @param nodeId the ID of the node whose response is to be returned
     * @return the NodeResponse from the node with the given identifier, or <code>null</code> if there is no response yet from the given node
     */
    NodeResponse getNodeResponse(NodeIdentifier nodeId);
}
