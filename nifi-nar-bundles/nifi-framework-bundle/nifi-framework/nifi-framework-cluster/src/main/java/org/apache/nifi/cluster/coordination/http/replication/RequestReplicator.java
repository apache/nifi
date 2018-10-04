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

import java.net.URI;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.cluster.protocol.NodeIdentifier;

public interface RequestReplicator {

    public static final String REQUEST_TRANSACTION_ID_HEADER = "X-RequestTransactionId";
    public static final String CLUSTER_ID_GENERATION_SEED_HEADER = "X-Cluster-Id-Generation-Seed";

    /**
     * The HTTP header that the requestor specifies to ask a node if they are able to process a given request.
     * The value is always 202-Accepted. The node will respond with 202 ACCEPTED if it is able to
     * process the request, 417 EXPECTATION_FAILED otherwise.
     */
    public static final String REQUEST_VALIDATION_HTTP_HEADER = "X-Validation-Expects";
    public static final String NODE_CONTINUE = "202-Accepted";
    public static final int NODE_CONTINUE_STATUS_CODE = 202;

    /**
     * Indicates that the request is intended to cancel a transaction that was previously created without performing the action
     */
    public static final String REQUEST_TRANSACTION_CANCELATION_HTTP_HEADER = "X-Cancel-Transaction";

    /**
     * Indicates that this is the second phase of the two phase commit and the execution of the action should proceed.
     */
    public static final String REQUEST_EXECUTION_HTTP_HEADER = "X-Execution-Continue";

    /**
     * When we replicate a request across the cluster, we replicate it only from the cluster coordinator.
     * If the request needs to be replicated by another node, it first replicates the request to the coordinator,
     * which then replicates the request on the node's behalf. This header name and value are used to denote
     * that the request has already been to the cluster coordinator, and the cluster coordinator is the one replicating
     * the request. This allows us to know that the request should be serviced, rather than proxied back to the
     * cluster coordinator.
     */
    public static final String REPLICATION_INDICATOR_HEADER = "X-Request-Replicated";

    /**
     * When replicating a request to the cluster coordinator, it may be useful to denote that the request should
     * be replicated only to a single node. This happens, for instance, when retrieving a Provenance Event that
     * we know lives on a specific node. This request must still be replicated through the cluster coordinator.
     * This header tells the cluster coordinator the UUID's (comma-separated list, possibly with spaces between)
     * of the nodes that the request should be replicated to.
     */
    public static final String REPLICATION_TARGET_NODE_UUID_HEADER = "X-Replication-Target-Id";

    /**
     * Stops the instance from replicating requests. Calling this method on a stopped instance has no effect.
     */
    void shutdown();

    /**
     * Replicates a request to each node in the cluster. If the request attempts to modify the flow and there is a node
     * that is not currently connected, an Exception will be thrown. Otherwise, the returned AsyncClusterResponse object
     * will contain the results that are immediately available, as well as an identifier for obtaining an updated result
     * later. NOTE: This method will ALWAYS indicate that the request has been replicated.
     *
     * @param method  the HTTP method (e.g., POST, PUT)
     * @param uri     the base request URI (up to, but not including, the query string)
     * @param entity  an entity
     * @param headers any HTTP headers
     * @return an AsyncClusterResponse that indicates the current status of the request and provides an identifier for obtaining an updated response later
     * @throws ConnectingNodeMutableRequestException   if the request attempts to modify the flow and there is a node that is in the CONNECTING state
     * @throws DisconnectedNodeMutableRequestException if the request attempts to modify the flow and there is a node that is in the DISCONNECTED state
     */
    AsyncClusterResponse replicate(String method, URI uri, Object entity, Map<String, String> headers);

    /**
     * Replicates a request to each node in the cluster. If the request attempts to modify the flow and there is a node
     * that is not currently connected, an Exception will be thrown. Otherwise, the returned AsyncClusterResponse object
     * will contain the results that are immediately available, as well as an identifier for obtaining an updated result
     * later. NOTE: This method will ALWAYS indicate that the request has been replicated.
     *
     * @param user the user making the request
     * @param method the HTTP method (e.g., POST, PUT)
     * @param uri the base request URI (up to, but not including, the query string)
     * @param entity an entity
     * @param headers any HTTP headers
     * @return an AsyncClusterResponse that indicates the current status of the request and provides an identifier for obtaining an updated response later
     * @throws ConnectingNodeMutableRequestException if the request attempts to modify the flow and there is a node that is in the CONNECTING state
     * @throws DisconnectedNodeMutableRequestException if the request attempts to modify the flow and there is a node that is in the DISCONNECTED state
     */
    AsyncClusterResponse replicate(NiFiUser user, String method, URI uri, Object entity, Map<String, String> headers);

    /**
     * Requests are sent to each node in the given set of Node Identifiers. The returned AsyncClusterResponse object will contain
     * the results that are immediately available, as well as an identifier for obtaining an updated result later.
     * <p>
     * HTTP DELETE, GET, HEAD, and OPTIONS methods will throw an IllegalArgumentException if used.
     *
     * @param nodeIds the node identifiers
     * @param user the user making the request
     * @param method the HTTP method (e.g., POST, PUT)
     * @param uri the base request URI (up to, but not including, the query string)
     * @param entity an entity
     * @param headers any HTTP headers
     * @param indicateReplicated if <code>true</code>, will add a header indicating to the receiving nodes that the request
     *            has already been replicated, so the receiving node will not replicate the request itself.
     * @param performVerification if <code>true</code>, and the request is mutable, will verify that all nodes are connected before
     *            making the request and that all nodes are able to perform the request before acutally attempting to perform the task.
     *            If false, will perform no such verification
     * @return an AsyncClusterResponse that indicates the current status of the request and provides an identifier for obtaining an updated response later
     */
    AsyncClusterResponse replicate(Set<NodeIdentifier> nodeIds, NiFiUser user, String method, URI uri, Object entity, Map<String, String> headers, boolean indicateReplicated,
        boolean performVerification);

    /**
     * Requests are sent to each node in the given set of Node Identifiers. The returned AsyncClusterResponse object will contain
     * the results that are immediately available, as well as an identifier for obtaining an updated result later.
     * <p>
     * HTTP DELETE, GET, HEAD, and OPTIONS methods will throw an IllegalArgumentException if used.
     *
     * @param nodeIds the node identifiers
     * @param method the HTTP method (e.g., POST, PUT)
     * @param uri the base request URI (up to, but not including, the query string)
     * @param entity an entity
     * @param headers any HTTP headers
     * @param indicateReplicated if <code>true</code>, will add a header indicating to the receiving nodes that the request
     *            has already been replicated, so the receiving node will not replicate the request itself.
     * @param performVerification if <code>true</code>, and the request is mutable, will verify that all nodes are connected before
     *            making the request and that all nodes are able to perform the request before acutally attempting to perform the task.
     *            If false, will perform no such verification
     * @return an AsyncClusterResponse that indicates the current status of the request and provides an identifier for obtaining an updated response later
     */
    AsyncClusterResponse replicate(Set<NodeIdentifier> nodeIds, String method, URI uri, Object entity, Map<String, String> headers, boolean indicateReplicated,
        boolean performVerification);


    /**
     * Forwards a request to the Cluster Coordinator so that it is able to replicate the request to all nodes in the cluster.
     *
     * @param coordinatorNodeId the node identifier of the Cluster Coordinator
     * @param method            the HTTP method (e.g., POST, PUT)
     * @param uri               the base request URI (up to, but not including, the query string)
     * @param entity            an entity
     * @param headers           any HTTP headers
     * @return an AsyncClusterResponse that indicates the current status of the request and provides an identifier for obtaining an updated response later
     */
    AsyncClusterResponse forwardToCoordinator(NodeIdentifier coordinatorNodeId, String method, URI uri, Object entity, Map<String, String> headers);

    /**
     * Forwards a request to the Cluster Coordinator so that it is able to replicate the request to all nodes in the cluster.
     *
     * @param coordinatorNodeId the node identifier of the Cluster Coordinator
     * @param user the user making the request
     * @param method the HTTP method (e.g., POST, PUT)
     * @param uri the base request URI (up to, but not including, the query string)
     * @param entity an entity
     * @param headers any HTTP headers
     * @return an AsyncClusterResponse that indicates the current status of the request and provides an identifier for obtaining an updated response later
     */
    AsyncClusterResponse forwardToCoordinator(NodeIdentifier coordinatorNodeId, NiFiUser user, String method, URI uri, Object entity, Map<String, String> headers);

    /**
     * <p>
     * Returns an AsyncClusterResponse that provides the most up-to-date status of the request with the given identifier.
     * If the request is finished, meaning that all nodes in the cluster have reported back their status or have timed out,
     * then the response will be removed and any subsequent calls to obtain the response with the same identifier will return
     * <code>null</code>. If the response is not complete, the method may be called again at some point in the future in order
     * to check again if the request has completed.
     * </p>
     *
     * @param requestIdentifier the identifier of the request to obtain a response for
     * @return an AsyncClusterResponse that provides the most up-to-date status of the request with the given identifier, or <code>null</code> if
     *         no request exists with the given identifier
     */
    AsyncClusterResponse getClusterResponse(String requestIdentifier);
}
