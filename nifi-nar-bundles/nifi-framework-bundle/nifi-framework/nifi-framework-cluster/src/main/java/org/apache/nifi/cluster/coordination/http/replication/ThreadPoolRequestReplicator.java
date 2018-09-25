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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.http.HttpResponseMapper;
import org.apache.nifi.cluster.coordination.http.StandardHttpResponseMapper;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.exception.ConnectingNodeMutableRequestException;
import org.apache.nifi.cluster.manager.exception.DisconnectedNodeMutableRequestException;
import org.apache.nifi.cluster.manager.exception.IllegalClusterStateException;
import org.apache.nifi.cluster.manager.exception.NoConnectedNodesException;
import org.apache.nifi.cluster.manager.exception.OffloadedNodeMutableRequestException;
import org.apache.nifi.cluster.manager.exception.UnknownNodeException;
import org.apache.nifi.cluster.manager.exception.UriConstructionException;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.util.ComponentIdGenerator;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.ProxiedEntitiesUtils;
import org.apache.nifi.web.security.jwt.JwtAuthenticationFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ThreadPoolRequestReplicator implements RequestReplicator {

    private static final Logger logger = LoggerFactory.getLogger(ThreadPoolRequestReplicator.class);

    private final int maxConcurrentRequests; // maximum number of concurrent requests
    private final HttpResponseMapper responseMapper;
    private final EventReporter eventReporter;
    private final RequestCompletionCallback callback;
    private final ClusterCoordinator clusterCoordinator;
    private final NiFiProperties nifiProperties;

    private ThreadPoolExecutor executorService;
    private ScheduledExecutorService maintenanceExecutor;

    private final ConcurrentMap<String, StandardAsyncClusterResponse> responseMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<NodeIdentifier, AtomicInteger> sequentialLongRequestCounts = new ConcurrentHashMap<>();

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    private HttpReplicationClient httpClient;


    /**
     * Creates an instance.
     *
     * @param corePoolSize core size of the thread pool
     * @param maxPoolSize the max number of threads in the thread pool
     * @param maxConcurrentRequests maximum number of concurrent requests
     * @param client a client for making requests
     * @param clusterCoordinator the cluster coordinator to use for interacting with node statuses
     * @param callback a callback that will be called whenever all of the responses have been gathered for a request. May be null.
     * @param eventReporter an EventReporter that can be used to notify users of interesting events. May be null.
     * @param nifiProperties properties
     */
    public ThreadPoolRequestReplicator(final int corePoolSize, final int maxPoolSize, final int maxConcurrentRequests, final HttpReplicationClient client,
        final ClusterCoordinator clusterCoordinator, final RequestCompletionCallback callback, final EventReporter eventReporter, final NiFiProperties nifiProperties) {
        if (corePoolSize <= 0) {
            throw new IllegalArgumentException("The Core Pool Size must be greater than zero.");
        } else if (maxPoolSize < corePoolSize) {
            throw new IllegalArgumentException("Max Pool Size must be >= Core Pool Size.");
        } else if (client == null) {
            throw new IllegalArgumentException("Client may not be null.");
        }

        this.clusterCoordinator = clusterCoordinator;
        this.maxConcurrentRequests = maxConcurrentRequests;
        this.responseMapper = new StandardHttpResponseMapper(nifiProperties);
        this.eventReporter = eventReporter;
        this.callback = callback;
        this.nifiProperties = nifiProperties;
        this.httpClient = client;

        final AtomicInteger threadId = new AtomicInteger(0);
        final ThreadFactory threadFactory = r -> {
            final Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setDaemon(true);
            t.setName("Replicate Request Thread-" + threadId.incrementAndGet());
            return t;
        };

        executorService = new ThreadPoolExecutor(corePoolSize, maxPoolSize, 5, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), threadFactory);

        maintenanceExecutor = Executors.newScheduledThreadPool(1, new ThreadFactory() {
            @Override
            public Thread newThread(final Runnable r) {
                final Thread t = Executors.defaultThreadFactory().newThread(r);
                t.setDaemon(true);
                t.setName(ThreadPoolRequestReplicator.class.getSimpleName() + " Maintenance Thread");
                return t;
            }
        });

        maintenanceExecutor.scheduleWithFixedDelay(() -> purgeExpiredRequests(), 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public void shutdown() {
        executorService.shutdown();
        maintenanceExecutor.shutdown();
    }


    @Override
    public AsyncClusterResponse replicate(String method, URI uri, Object entity, Map<String, String> headers) {
        return replicate(NiFiUserUtils.getNiFiUser(), method, uri, entity, headers);
    }

    @Override
    public AsyncClusterResponse replicate(NiFiUser user, String method, URI uri, Object entity, Map<String, String> headers) {
        final Map<NodeConnectionState, List<NodeIdentifier>> stateMap = clusterCoordinator.getConnectionStates();
        final boolean mutable = isMutableRequest(method, uri.getPath());

        // If the request is mutable, ensure that all nodes are connected.
        if (mutable) {
            final List<NodeIdentifier> offloaded = stateMap.get(NodeConnectionState.OFFLOADED);
            if (offloaded != null && !offloaded.isEmpty()) {
                if (offloaded.size() == 1) {
                    throw new OffloadedNodeMutableRequestException("Node " + offloaded.iterator().next() + " is currently offloaded");
                } else {
                    throw new OffloadedNodeMutableRequestException(offloaded.size() + " Nodes are currently offloaded");
                }
            }

            final List<NodeIdentifier> offloading = stateMap.get(NodeConnectionState.OFFLOADING);
            if (offloading != null && !offloading.isEmpty()) {
                if (offloading.size() == 1) {
                    throw new OffloadedNodeMutableRequestException("Node " + offloading.iterator().next() + " is currently offloading");
                } else {
                    throw new OffloadedNodeMutableRequestException(offloading.size() + " Nodes are currently offloading");
                }
            }

            final List<NodeIdentifier> disconnected = stateMap.get(NodeConnectionState.DISCONNECTED);
            if (disconnected != null && !disconnected.isEmpty()) {
                if (disconnected.size() == 1) {
                    throw new DisconnectedNodeMutableRequestException("Node " + disconnected.iterator().next() + " is currently disconnected");
                } else {
                    throw new DisconnectedNodeMutableRequestException(disconnected.size() + " Nodes are currently disconnected");
                }
            }

            final List<NodeIdentifier> disconnecting = stateMap.get(NodeConnectionState.DISCONNECTING);
            if (disconnecting != null && !disconnecting.isEmpty()) {
                if (disconnecting.size() == 1) {
                    throw new DisconnectedNodeMutableRequestException("Node " + disconnecting.iterator().next() + " is currently disconnecting");
                } else {
                    throw new DisconnectedNodeMutableRequestException(disconnecting.size() + " Nodes are currently disconnecting");
                }
            }

            final List<NodeIdentifier> connecting = stateMap.get(NodeConnectionState.CONNECTING);
            if (connecting != null && !connecting.isEmpty()) {
                if (connecting.size() == 1) {
                    throw new ConnectingNodeMutableRequestException("Node " + connecting.iterator().next() + " is currently connecting");
                } else {
                    throw new ConnectingNodeMutableRequestException(connecting.size() + " Nodes are currently connecting");
                }
            }
        }

        final List<NodeIdentifier> nodeIds = stateMap.get(NodeConnectionState.CONNECTED);
        if (nodeIds == null || nodeIds.isEmpty()) {
            throw new NoConnectedNodesException();
        }

        final Set<NodeIdentifier> nodeIdSet = new HashSet<>(nodeIds);

        return replicate(nodeIdSet, user, method, uri, entity, headers, true, true);
    }

    void updateRequestHeaders(final Map<String, String> headers, final NiFiUser user) {
        if (user == null) {
            throw new AccessDeniedException("Unknown user");
        }

        // Add the user as a proxied entity so that when the receiving NiFi receives the request,
        // it knows that we are acting as a proxy on behalf of the current user.
        final String proxiedEntitiesChain = ProxiedEntitiesUtils.buildProxiedEntitiesChainString(user);
        headers.put(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN, proxiedEntitiesChain);

        // remove the access token if present, since the user is already authenticated... authorization
        // will happen when the request is replicated using the proxy chain above
        headers.remove(JwtAuthenticationFilter.AUTHORIZATION);

        // if knox sso cookie name is set, remove any authentication cookie since this user is already authenticated
        // and will be included in the proxied entities chain above... authorization will happen when the
        // request is replicated
        final String knoxCookieName = nifiProperties.getKnoxCookieName();
        if (headers.containsKey("Cookie") && StringUtils.isNotBlank(knoxCookieName)) {
            final String rawCookies = headers.get("Cookie");
            final String[] rawCookieParts = rawCookies.split(";");
            final Set<String> filteredCookieParts = Stream.of(rawCookieParts).map(String::trim).filter(cookie -> !cookie.startsWith(knoxCookieName + "=")).collect(Collectors.toSet());

            // if that was the only cookie, remove it
            if (filteredCookieParts.isEmpty()) {
                headers.remove("Cookie");
            } else {
                // otherwise rebuild the cookies without the knox token
                headers.put("Cookie", StringUtils.join(filteredCookieParts, "; "));
            }
        }

        // remove the host header
        headers.remove("Host");
    }

    @Override
    public AsyncClusterResponse replicate(Set<NodeIdentifier> nodeIds, String method, URI uri, Object entity, Map<String, String> headers,
                final boolean indicateReplicated, final boolean performVerification) {

        return replicate(nodeIds, NiFiUserUtils.getNiFiUser(), method, uri, entity, headers, indicateReplicated, performVerification);
    }

    @Override
    public AsyncClusterResponse replicate(Set<NodeIdentifier> nodeIds, final NiFiUser user, String method, URI uri, Object entity, Map<String, String> headers,
                                          final boolean indicateReplicated, final boolean performVerification) {
        final Map<String, String> updatedHeaders = new HashMap<>(headers);

        updatedHeaders.put(RequestReplicator.CLUSTER_ID_GENERATION_SEED_HEADER, ComponentIdGenerator.generateId().toString());
        if (indicateReplicated) {
            updatedHeaders.put(RequestReplicator.REPLICATION_INDICATOR_HEADER, "true");
        }

        // include the proxied entities header
        updateRequestHeaders(updatedHeaders, user);

        if (indicateReplicated) {
            // If we are replicating a request and indicating that it is replicated, then this means that we are
            // performing an action, rather than simply proxying the request to the cluster coordinator. In this case,
            // we need to ensure that we use proper locking. We don't want two requests modifying the flow at the same
            // time, so we use a write lock if the request is mutable and a read lock otherwise.
            final Lock lock = isMutableRequest(method, uri.getPath()) ? writeLock : readLock;
            logger.debug("Obtaining lock {} in order to replicate request {} {}", lock, method, uri);
            lock.lock();
            try {
                logger.debug("Lock {} obtained in order to replicate request {} {}", lock, method, uri);

                // Unlocking of the lock is performed within the replicate method, as we need to ensure that it is unlocked only after
                // the entire request has completed.
                final Object monitor = new Object();
                synchronized (monitor) {
                    final AsyncClusterResponse response = replicate(nodeIds, method, uri, entity, updatedHeaders, performVerification, null, !performVerification, true, monitor);

                    try {
                        monitor.wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }

                    return response;
                }
            } finally {
                lock.unlock();
                logger.debug("Unlocked {} after replication completed for {} {}", lock, method, uri);
            }
        } else {
            return replicate(nodeIds, method, uri, entity, updatedHeaders, performVerification, null, !performVerification, true, null);
        }
    }


    @Override
    public AsyncClusterResponse forwardToCoordinator(final NodeIdentifier coordinatorNodeId, final String method, final URI uri, final Object entity, final Map<String, String> headers) {
        return forwardToCoordinator(coordinatorNodeId, NiFiUserUtils.getNiFiUser(), method, uri, entity, headers);
    }

    @Override
    public AsyncClusterResponse forwardToCoordinator(final NodeIdentifier coordinatorNodeId, final NiFiUser user, final String method,
                final URI uri, final Object entity, final Map<String, String> headers) {
        final Map<String, String> updatedHeaders = new HashMap<>(headers);

        // include the proxied entities header
        updateRequestHeaders(updatedHeaders, user);

        return replicate(Collections.singleton(coordinatorNodeId), method, uri, entity, updatedHeaders, false, null, false, false, null);
    }

    /**
     * Replicates the request to all nodes in the given set of node identifiers
     *
     * @param nodeIds             the NodeIdentifiers that identify which nodes to send the request to
     * @param method              the HTTP method to use
     * @param uri                 the URI to send the request to
     * @param entity              the entity to use
     * @param headers             the HTTP Headers
     * @param performVerification whether or not to verify that all nodes in the cluster are connected and that all nodes can perform request. Ignored if request is not mutable.
     * @param response            the response to update with the results
     * @param executionPhase      <code>true</code> if this is the execution phase, <code>false</code> otherwise
     * @param monitor             a monitor that will be notified when the request completes (successfully or otherwise)
     * @return an AsyncClusterResponse that can be used to obtain the result
     */
    AsyncClusterResponse replicate(final Set<NodeIdentifier> nodeIds, final String method, final URI uri, final Object entity, final Map<String, String> headers,
        final boolean performVerification, StandardAsyncClusterResponse response, final boolean executionPhase, final boolean merge, final Object monitor) {
        try {
            // state validation
            Objects.requireNonNull(nodeIds);
            Objects.requireNonNull(method);
            Objects.requireNonNull(uri);
            Objects.requireNonNull(entity);
            Objects.requireNonNull(headers);

            if (nodeIds.isEmpty()) {
                throw new IllegalArgumentException("Cannot replicate request to 0 nodes");
            }

            // verify all of the nodes exist and are in the proper state
            for (final NodeIdentifier nodeId : nodeIds) {
                final NodeConnectionStatus status = clusterCoordinator.getConnectionStatus(nodeId);
                if (status == null) {
                    throw new UnknownNodeException("Node " + nodeId + " does not exist in this cluster");
                }

                if (status.getState() != NodeConnectionState.CONNECTED) {
                    throw new IllegalClusterStateException("Cannot replicate request to Node " + nodeId + " because the node is not connected");
                }
            }

            logger.debug("Replicating request {} {} with entity {} to {}; response is {}", method, uri, entity, nodeIds, response);

            // Update headers to indicate the current revision so that we can
            // prevent multiple users changing the flow at the same time
            final Map<String, String> updatedHeaders = new HashMap<>(headers);
            final String requestId = updatedHeaders.computeIfAbsent(REQUEST_TRANSACTION_ID_HEADER, key -> UUID.randomUUID().toString());

            long verifyClusterStateNanos = -1;
            if (performVerification) {
                final long start = System.nanoTime();
                verifyClusterState(method, uri.getPath());
                verifyClusterStateNanos = System.nanoTime() - start;
            }

            int numRequests = responseMap.size();
            if (numRequests >= maxConcurrentRequests) {
                numRequests = purgeExpiredRequests();
            }

            if (numRequests >= maxConcurrentRequests) {
                final Map<String, Long> countsByUri = responseMap.values().stream().collect(
                        Collectors.groupingBy(
                                StandardAsyncClusterResponse::getURIPath,
                                Collectors.counting()));

                logger.error("Cannot replicate request {} {} because there are {} outstanding HTTP Requests already. Request Counts Per URI = {}", method, uri.getPath(), numRequests, countsByUri);
                throw new IllegalStateException("There are too many outstanding HTTP requests with a total " + numRequests + " outstanding requests");
            }

            // create a response object if one was not already passed to us
            if (response == null) {
                // create the request objects and replicate to all nodes.
                // When the request has completed, we need to ensure that we notify the monitor, if there is one.
                final CompletionCallback completionCallback = clusterResponse -> {
                    try {
                        onCompletedResponse(requestId);
                    } finally {
                        if (monitor != null) {
                            synchronized (monitor) {
                                monitor.notify();
                            }

                            logger.debug("Notified monitor {} because request {} {} has completed", monitor, method, uri);
                        }
                    }
                };

                final Runnable responseConsumedCallback = () -> onResponseConsumed(requestId);

                response = new StandardAsyncClusterResponse(requestId, uri, method, nodeIds,
                        responseMapper, completionCallback, responseConsumedCallback, merge);
                responseMap.put(requestId, response);
            }

            if (verifyClusterStateNanos > -1) {
                response.addTiming("Verify Cluster State", "All Nodes", verifyClusterStateNanos);
            }

            logger.debug("For Request ID {}, response object is {}", requestId, response);

            // if mutable request, we have to do a two-phase commit where we ask each node to verify
            // that the request can take place and then, if all nodes agree that it can, we can actually
            // issue the request. This is all handled by calling performVerification, which will replicate
            // the 'vote' request to all nodes and then if successful will call back into this method to
            // replicate the actual request.
            final boolean mutableRequest = isMutableRequest(method, uri.getPath());
            if (mutableRequest && performVerification) {
                logger.debug("Performing verification (first phase of two-phase commit) for Request ID {}", requestId);
                performVerification(nodeIds, method, uri, entity, updatedHeaders, response, merge, monitor);
                return response;
            } else if (mutableRequest) {
                response.setPhase(StandardAsyncClusterResponse.COMMIT_PHASE);
            }

            // Callback function for generating a NodeHttpRequestCallable that can be used to perform the work
            final StandardAsyncClusterResponse finalResponse = response;
            NodeRequestCompletionCallback nodeCompletionCallback = nodeResponse -> {
                logger.debug("Received response from {} for {} {}", nodeResponse.getNodeId(), method, uri.getPath());
                finalResponse.add(nodeResponse);
            };

            // instruct the node to actually perform the underlying action
            if (mutableRequest && executionPhase) {
                updatedHeaders.put(REQUEST_EXECUTION_HTTP_HEADER, "true");
            }

            // replicate the request to all nodes
            final PreparedRequest request = httpClient.prepareRequest(method, updatedHeaders, entity);
            final Function<NodeIdentifier, NodeHttpRequest> requestFactory =
                nodeId -> new NodeHttpRequest(request, nodeId, createURI(uri, nodeId), nodeCompletionCallback, finalResponse);

            submitAsyncRequest(nodeIds, uri.getScheme(), uri.getPath(), requestFactory, updatedHeaders);

            return response;
        } catch (final Throwable t) {
            if (monitor != null) {
                synchronized (monitor) {
                    monitor.notify();
                }
                logger.debug("Notified monitor {} because request {} {} has failed with Throwable {}", monitor, method, uri, t);
            }

            if (response != null) {
                final RuntimeException failure = (t instanceof RuntimeException) ? (RuntimeException) t : new RuntimeException("Failed to submit Replication Request to background thread", t);
                response.setFailure(failure, new NodeIdentifier());
            }

            throw t;
        }
    }


    private void performVerification(final Set<NodeIdentifier> nodeIds, final String method, final URI uri, final Object entity, final Map<String, String> headers,
        final StandardAsyncClusterResponse clusterResponse, final boolean merge, final Object monitor) {
        logger.debug("Verifying that mutable request {} {} can be made", method, uri.getPath());

        final Map<String, String> validationHeaders = new HashMap<>(headers);
        validationHeaders.put(REQUEST_VALIDATION_HTTP_HEADER, NODE_CONTINUE);

        final long startNanos = System.nanoTime();
        final int numNodes = nodeIds.size();
        final NodeRequestCompletionCallback completionCallback = new NodeRequestCompletionCallback() {
            final Set<NodeResponse> nodeResponses = Collections.synchronizedSet(new HashSet<>());

            @Override
            public void onCompletion(final NodeResponse nodeResponse) {
                // Add the node response to our collection. We later need to know whether or
                // not this is the last node response, so we add the response and then check
                // the size within a synchronized block to ensure that those two things happen
                // atomically. Otherwise, we could have multiple threads checking the sizes of
                // the sets at the same time, which could result in multiple threads performing
                // the 'all nodes are complete' logic.
                final boolean allNodesResponded;
                synchronized (nodeResponses) {
                    nodeResponses.add(nodeResponse);
                    allNodesResponded = nodeResponses.size() == numNodes;
                }

                try {
                    final long nanos = System.nanoTime() - startNanos;
                    clusterResponse.addTiming("Completed Verification", nodeResponse.getNodeId().toString(), nanos);

                    // If we have all of the node responses, then we can verify the responses
                    // and if good replicate the original request to all of the nodes.
                    if (allNodesResponded) {
                        clusterResponse.addTiming("Verification Completed", "All Nodes", nanos);

                        // Check if we have any requests that do not have a 202-Accepted status code.
                        final long dissentingCount = nodeResponses.stream().filter(p -> p.getStatus() != NODE_CONTINUE_STATUS_CODE).count();

                        // If all nodes responded with 202-Accepted, then we can replicate the original request
                        // to all nodes and we are finished.
                        if (dissentingCount == 0) {
                            logger.debug("Received verification from all {} nodes that mutable request {} {} can be made", numNodes, method, uri.getPath());
                            replicate(nodeIds, method, uri, entity, headers, false, clusterResponse, true, merge, monitor);
                            return;
                        }

                        try {
                            final Map<String, String> cancelLockHeaders = new HashMap<>(headers);
                            cancelLockHeaders.put(REQUEST_TRANSACTION_CANCELATION_HTTP_HEADER, "true");
                            final Thread cancelLockThread = new Thread(new Runnable() {
                                @Override
                                public void run() {
                                    logger.debug("Found {} dissenting nodes for {} {}; canceling claim request", dissentingCount, method, uri.getPath());

                                    final PreparedRequest request = httpClient.prepareRequest(method, cancelLockHeaders, entity);
                                    final Function<NodeIdentifier, NodeHttpRequest> requestFactory =
                                        nodeId -> new NodeHttpRequest(request, nodeId, createURI(uri, nodeId), null, clusterResponse);

                                    submitAsyncRequest(nodeIds, uri.getScheme(), uri.getPath(), requestFactory, cancelLockHeaders);
                                }
                            });
                            cancelLockThread.setName("Cancel Flow Locks");
                            cancelLockThread.start();

                            // Add a NodeResponse for each node to the Cluster Response
                            // Check that all nodes responded successfully.
                            for (final NodeResponse response : nodeResponses) {
                                if (response.getStatus() != NODE_CONTINUE_STATUS_CODE) {
                                    final Response clientResponse = response.getClientResponse();

                                    final String message;
                                    if (clientResponse == null) {
                                        message = "Node " + response.getNodeId() + " is unable to fulfill this request due to: Unexpected Response Code " + response.getStatus();

                                        logger.info("Received a status of {} from {} for request {} {} when performing first stage of two-stage commit. The action will not occur",
                                                response.getStatus(), response.getNodeId(), method, uri.getPath());
                                    } else {
                                        final String nodeExplanation = clientResponse.readEntity(String.class);
                                        message = "Node " + response.getNodeId() + " is unable to fulfill this request due to: " + nodeExplanation;

                                        logger.info("Received a status of {} from {} for request {} {} when performing first stage of two-stage commit. "
                                            + "The action will not occur. Node explanation: {}", response.getStatus(), response.getNodeId(), method, uri.getPath(), nodeExplanation);
                                    }

                                    // if a node reports forbidden, use that as the response failure
                                    final RuntimeException failure;
                                    if (response.getStatus() == Status.FORBIDDEN.getStatusCode()) {
                                        if (response.hasThrowable()) {
                                            failure = new AccessDeniedException(message, response.getThrowable());
                                        } else {
                                            failure = new AccessDeniedException(message);
                                        }
                                    } else {
                                        if (response.hasThrowable()) {
                                            failure = new IllegalClusterStateException(message, response.getThrowable());
                                        } else {
                                            failure = new IllegalClusterStateException(message);
                                        }
                                    }

                                    clusterResponse.setFailure(failure, response.getNodeId());
                                }
                            }
                        } finally {
                            if (monitor != null) {
                                synchronized (monitor) {
                                    monitor.notify();
                                }

                                logger.debug("Notified monitor {} because request {} {} has failed due to at least 1 dissenting node", monitor, method, uri);
                            }
                        }
                    }
                } catch (final Exception e) {
                    clusterResponse.add(new NodeResponse(nodeResponse.getNodeId(), method, uri, e));

                    // If there was a problem, we need to ensure that we add all of the other nodes' responses
                    // to the Cluster Response so that the Cluster Response is complete.
                    for (final NodeResponse otherResponse : nodeResponses) {
                        if (otherResponse.getNodeId().equals(nodeResponse.getNodeId())) {
                            continue;
                        }

                        clusterResponse.add(otherResponse);
                    }
                }
            }
        };

        // Callback function for generating a NodeHttpRequestCallable that can be used to perform the work
        final PreparedRequest request = httpClient.prepareRequest(method, validationHeaders, entity);
        final Function<NodeIdentifier, NodeHttpRequest> requestFactory =
            nodeId -> new NodeHttpRequest(request, nodeId, createURI(uri, nodeId), completionCallback, clusterResponse);

        // replicate the 'verification request' to all nodes
        submitAsyncRequest(nodeIds, uri.getScheme(), uri.getPath(), requestFactory, validationHeaders);
    }


    @Override
    public AsyncClusterResponse getClusterResponse(final String identifier) {
        final AsyncClusterResponse response = responseMap.get(identifier);
        if (response == null) {
            return null;
        }

        return response;
    }

    // Visible for testing - overriding this method makes it easy to verify behavior without actually making any web requests
    protected NodeResponse replicateRequest(final PreparedRequest request, final NodeIdentifier nodeId, final URI uri, final String requestId,
            final StandardAsyncClusterResponse clusterResponse) throws IOException {

        final Response response;
        final long startNanos = System.nanoTime();
        logger.debug("Replicating request to {} {}, request ID = {}, headers = {}", request.getMethod(), uri, requestId, request.getHeaders());

        // invoke the request
        response = httpClient.replicate(request, uri.toString());

        final long nanos = System.nanoTime() - startNanos;
        clusterResponse.addTiming("Perform HTTP Request", nodeId.toString(), nanos);
        final NodeResponse nodeResponse = new NodeResponse(nodeId, request.getMethod(), uri, response, System.nanoTime() - startNanos, requestId);
        if (nodeResponse.is2xx()) {
            final int length = nodeResponse.getClientResponse().getLength();
            if (length > 0) {
                final boolean canBufferResponse = clusterResponse.requestBuffer(length);
                if (canBufferResponse) {
                    nodeResponse.bufferResponse();
                }
            }
        }

        return nodeResponse;
    }

    private boolean isMutableRequest(final String method, final String uriPath) {
        switch (method.toUpperCase()) {
            case HttpMethod.GET:
            case HttpMethod.HEAD:
            case HttpMethod.OPTIONS:
                return false;
            default:
                return true;
        }
    }

    /**
     * Verifies that the cluster is in a state that will allow requests to be made using the given HTTP Method and URI path
     *
     * @param httpMethod the HTTP Method
     * @param uriPath    the URI Path
     * @throw IllegalClusterStateException if the cluster is not in a state that allows a request to made to the given URI Path using the given HTTP Method
     */
    private void verifyClusterState(final String httpMethod, final String uriPath) {
        final boolean mutableRequest = HttpMethod.DELETE.equals(httpMethod) || HttpMethod.POST.equals(httpMethod) || HttpMethod.PUT.equals(httpMethod);

        // check that the request can be applied
        if (mutableRequest) {
            final Map<NodeConnectionState, List<NodeIdentifier>> connectionStates = clusterCoordinator.getConnectionStates();
            if (connectionStates.containsKey(NodeConnectionState.DISCONNECTED) || connectionStates.containsKey(NodeConnectionState.DISCONNECTING)) {
                throw new DisconnectedNodeMutableRequestException("Received a mutable request [" + httpMethod + " " + uriPath + "] while a node is disconnected from the cluster");
            }

            if (connectionStates.containsKey(NodeConnectionState.CONNECTING)) {
                // if any node is connecting and a request can change the flow, then we throw an exception
                throw new ConnectingNodeMutableRequestException("Received a mutable request [" + httpMethod + " " + uriPath + "] while a node is trying to connect to the cluster");
            }
        }
    }

    /**
     * Removes the AsyncClusterResponse with the given ID from the map and handles any cleanup
     * or post-processing related to the request after the client has consumed the response
     *
     * @param requestId the ID of the request that has been consumed by the client
     */
    private void onResponseConsumed(final String requestId) {
        responseMap.remove(requestId);
    }

    /**
     * When all nodes have completed a request and provided a response (or have timed out), this method will be invoked
     * to handle calling the Callback that was provided for the request, if any, and handle any cleanup or post-processing
     * related to the request
     *
     * @param requestId the ID of the request that has completed
     */
    private void onCompletedResponse(final String requestId) {
        final AsyncClusterResponse response = responseMap.get(requestId);

        if (response != null && callback != null) {
            try {
                callback.afterRequest(response.getURIPath(), response.getMethod(), response.getCompletedNodeResponses());
            } catch (final Exception e) {
                logger.warn("Completed request {} {} but failed to properly handle the Request Completion Callback due to {}",
                        response.getMethod(), response.getURIPath(), e.toString());
                logger.warn("", e);
            }
        }

        if (response != null && logger.isDebugEnabled()) {
            logTimingInfo(response);
        }

        // If we have any nodes that are slow to respond, keep track of this. If the same node is slow 3 times in
        // a row, log a warning to indicate that the node is responding slowly.
        final Set<NodeIdentifier> slowResponseNodes = ResponseUtils.findLongResponseTimes(response, 1.5D);
        for (final NodeIdentifier nodeId : response.getNodesInvolved()) {
            final AtomicInteger counter = sequentialLongRequestCounts.computeIfAbsent(nodeId, id -> new AtomicInteger(0));
            if (slowResponseNodes.contains(nodeId)) {
                final int sequentialLongRequests = counter.incrementAndGet();
                if (sequentialLongRequests >= 3) {
                    final String message = "Response time from " + nodeId + " was slow for each of the last 3 requests made. "
                            + "To see more information about timing, enable DEBUG logging for " + logger.getName();

                    logger.warn(message);
                    if (eventReporter != null) {
                        eventReporter.reportEvent(Severity.WARNING, "Node Response Time", message);
                    }

                    counter.set(0);
                }
            } else {
                counter.set(0);
            }
        }
    }

    private void logTimingInfo(final AsyncClusterResponse response) {
        // Calculate min, max, mean for the requests
        final LongSummaryStatistics stats = response.getNodesInvolved().stream()
                .map(p -> response.getNodeResponse(p).getRequestDuration(TimeUnit.MILLISECONDS))
                .collect(Collectors.summarizingLong(Long::longValue));

        final StringBuilder sb = new StringBuilder();
        sb.append("Node Responses for ").append(response.getMethod()).append(" ").append(response.getURIPath()).append(" (Request ID ").append(response.getRequestIdentifier()).append("):\n");
        for (final NodeIdentifier node : response.getNodesInvolved()) {
            sb.append(node).append(": ").append(response.getNodeResponse(node).getRequestDuration(TimeUnit.MILLISECONDS)).append(" millis\n");
        }

        logger.debug("For {} {} (Request ID {}), minimum response time = {}, max = {}, average = {} ms",
                response.getMethod(), response.getURIPath(), response.getRequestIdentifier(), stats.getMin(), stats.getMax(), stats.getAverage());
        logger.debug(sb.toString());
    }


    private void submitAsyncRequest(final Set<NodeIdentifier> nodeIds, final String scheme, final String path,
                                  final Function<NodeIdentifier, NodeHttpRequest> callableFactory, final Map<String, String> headers) {

        if (nodeIds.isEmpty()) {
            return; // return quickly for trivial case
        }

        // submit the requests to the nodes
        for (final NodeIdentifier nodeId : nodeIds) {
            final NodeHttpRequest callable = callableFactory.apply(nodeId);
            executorService.submit(callable);
        }
    }


    private URI createURI(final URI exampleUri, final NodeIdentifier nodeId) {
        return createURI(exampleUri.getScheme(), nodeId.getApiAddress(), nodeId.getApiPort(), exampleUri.getPath(), exampleUri.getQuery());
    }

    private URI createURI(final String scheme, final String nodeApiAddress, final int nodeApiPort, final String path, final String query) {
        try {
            return new URI(scheme, null, nodeApiAddress, nodeApiPort, path, query, null);
        } catch (final URISyntaxException e) {
            throw new UriConstructionException(e);
        }
    }


    /**
     * A Callable for making an HTTP request to a single node and returning its response.
     */
    private class NodeHttpRequest implements Runnable {
        private final NodeIdentifier nodeId;
        private final String method;
        private final URI uri;
        private final NodeRequestCompletionCallback callback;
        private final StandardAsyncClusterResponse clusterResponse;
        private final long creationNanos = System.nanoTime();
        private final PreparedRequest request;

        private NodeHttpRequest(final PreparedRequest request, final NodeIdentifier nodeId, final URI uri,
                final NodeRequestCompletionCallback callback, final StandardAsyncClusterResponse clusterResponse) {
            this.request = request;
            this.nodeId = nodeId;
            this.method = request.getMethod();
            this.uri = uri;
            this.callback = callback;
            this.clusterResponse = clusterResponse;
        }


        @Override
        public void run() {
            final long waitForScheduleNanos = System.nanoTime() - creationNanos;
            clusterResponse.addTiming("Wait for HTTP Request Replication to be triggered", nodeId.toString(), waitForScheduleNanos);

            NodeResponse nodeResponse;

            try {
                // create and send the request
                final String requestId = request.getHeaders().get("x-nifi-request-id");
                logger.debug("Replicating request {} {} to {}", method, uri.getPath(), nodeId);

                nodeResponse = replicateRequest(request, nodeId, uri, requestId, clusterResponse);
            } catch (final Exception e) {
                nodeResponse = new NodeResponse(nodeId, method, uri, e);
                logger.warn("Failed to replicate request {} {} to {} due to {}", method, uri.getPath(), nodeId, e.toString());
                logger.warn("", e);
            }

            if (callback != null) {
                logger.debug("Request {} {} completed for {}", method, uri.getPath(), nodeId);
                callback.onCompletion(nodeResponse);
            }
        }
    }

    private static interface NodeRequestCompletionCallback {
        void onCompletion(NodeResponse nodeResponse);
    }

    private synchronized int purgeExpiredRequests() {
        final Set<String> expiredRequestIds = ThreadPoolRequestReplicator.this.responseMap.entrySet().stream()
                .filter(entry -> entry.getValue().isOlderThan(30, TimeUnit.SECONDS)) // older than 30 seconds
                .filter(entry -> entry.getValue().isComplete()) // is complete
                .map(entry -> entry.getKey()) // get the request id
                .collect(Collectors.toSet());

        expiredRequestIds.forEach(id -> onResponseConsumed(id));
        return responseMap.size();
    }
}
