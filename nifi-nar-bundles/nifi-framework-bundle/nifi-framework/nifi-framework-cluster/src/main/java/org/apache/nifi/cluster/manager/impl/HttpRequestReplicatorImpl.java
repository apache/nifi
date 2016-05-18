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
package org.apache.nifi.cluster.manager.impl;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.cluster.coordination.http.StandardHttpResponseMerger;
import org.apache.nifi.cluster.manager.HttpRequestReplicator;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.exception.UriConstructionException;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.logging.NiFiLog;
import org.apache.nifi.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.filter.GZIPContentEncodingFilter;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * An implementation of the <code>HttpRequestReplicator</code> interface. This implementation parallelizes the node HTTP requests using the given <code>ExecutorService</code> instance. Individual
 * requests may have connection and read timeouts set, which may be set during instance construction. Otherwise, the default is not to timeout.
 *
 * If a node protocol scheme is provided during construction, then all requests will be replicated using the given scheme. If null is provided as the scheme (the default), then the requests will be
 * replicated using the scheme of the original URI.
 *
 * Clients must call start() and stop() to initialize and shutdown the instance. The instance must be started before issuing any replication requests.
 *
 */
public class HttpRequestReplicatorImpl implements HttpRequestReplicator {

    // defaults
    private static final int DEFAULT_SHUTDOWN_REPLICATOR_SECONDS = 30;

    // logger
    private static final Logger logger = new NiFiLog(LoggerFactory.getLogger(HttpRequestReplicatorImpl.class));

    // final members
    private final Client client;            // the client to use for issuing requests
    private final int numThreads;           // number of threads to use for request replication
    private final int connectionTimeoutMs;  // connection timeout per node request
    private final int readTimeoutMs;        // read timeout per node request

    // members
    private ExecutorService executorService;
    private int shutdownReplicatorSeconds = DEFAULT_SHUTDOWN_REPLICATOR_SECONDS;

    // guarded by synchronized method access in support of multithreaded replication
    private String nodeProtocolScheme = null;

    /**
     * Creates an instance. The connection timeout and read timeout will be infinite.
     *
     * @param numThreads the number of threads to use when parallelizing requests
     * @param client a client for making requests
     */
    public HttpRequestReplicatorImpl(final int numThreads, final Client client) {
        this(numThreads, client, "0 sec", "0 sec");
    }

    /**
     * Creates an instance.
     *
     * @param numThreads the number of threads to use when parallelizing requests
     * @param client a client for making requests
     * @param connectionTimeout the connection timeout specified in milliseconds
     * @param readTimeout the read timeout specified in milliseconds
     */
    public HttpRequestReplicatorImpl(final int numThreads, final Client client, final String connectionTimeout, final String readTimeout) {

        if (numThreads <= 0) {
            throw new IllegalArgumentException("The number of threads must be greater than zero.");
        } else if (client == null) {
            throw new IllegalArgumentException("Client may not be null.");
        }

        this.numThreads = numThreads;
        this.client = client;
        this.connectionTimeoutMs = (int) FormatUtils.getTimeDuration(connectionTimeout, TimeUnit.MILLISECONDS);
        this.readTimeoutMs = (int) FormatUtils.getTimeDuration(readTimeout, TimeUnit.MILLISECONDS);

        client.getProperties().put(ClientConfig.PROPERTY_CONNECT_TIMEOUT, connectionTimeoutMs);
        client.getProperties().put(ClientConfig.PROPERTY_READ_TIMEOUT, readTimeoutMs);
        client.getProperties().put(ClientConfig.PROPERTY_FOLLOW_REDIRECTS, Boolean.TRUE);
    }

    @Override
    public void start() {
        if (isRunning()) {
            throw new IllegalStateException("Instance is already started.");
        }
        executorService = Executors.newFixedThreadPool(numThreads);
    }

    @Override
    public boolean isRunning() {
        return executorService != null && !executorService.isShutdown();
    }

    @Override
    public void stop() {

        if (!isRunning()) {
            throw new IllegalStateException("Instance is already stopped.");
        }

        // shutdown executor service
        try {
            if (getShutdownReplicatorSeconds() <= 0) {
                executorService.shutdownNow();
            } else {
                executorService.shutdown();
            }
            executorService.awaitTermination(getShutdownReplicatorSeconds(), TimeUnit.SECONDS);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
        } finally {
            if (executorService.isTerminated()) {
                logger.info("HTTP Request Replicator has been terminated successfully.");
            } else {
                logger.warn("HTTP Request Replicator has not terminated properly.  There exists an uninterruptable thread that will take an indeterminate amount of time to stop.");
            }
        }
    }

    /**
     * Sets the protocol scheme to use when issuing requests to nodes.
     *
     * @param nodeProtocolScheme the scheme. Valid values are "http", "https", or null. If null is specified, then the scheme of the originating request is used when replicating that request.
     */
    public synchronized void setNodeProtocolScheme(final String nodeProtocolScheme) {
        if (StringUtils.isNotBlank(nodeProtocolScheme)) {
            if (!"http".equalsIgnoreCase(nodeProtocolScheme) && !"https".equalsIgnoreCase(nodeProtocolScheme)) {
                throw new IllegalArgumentException("Node Protocol Scheme must be either HTTP or HTTPS");
            }
        }
        this.nodeProtocolScheme = nodeProtocolScheme;
    }

    public synchronized String getNodeProtocolScheme() {
        return nodeProtocolScheme;
    }

    private synchronized String getNodeProtocolScheme(final URI uri) {
        // if we are not configured to use a protocol scheme, then use the uri's scheme
        if (StringUtils.isBlank(nodeProtocolScheme)) {
            return uri.getScheme();
        }
        return nodeProtocolScheme;
    }

    public int getConnectionTimeoutMs() {
        return connectionTimeoutMs;
    }

    public int getReadTimeoutMs() {
        return readTimeoutMs;
    }

    public int getShutdownReplicatorSeconds() {
        return shutdownReplicatorSeconds;
    }

    public void setShutdownReplicatorSeconds(int shutdownReplicatorSeconds) {
        this.shutdownReplicatorSeconds = shutdownReplicatorSeconds;
    }

    @Override
    public Set<NodeResponse> replicate(final Set<NodeIdentifier> nodeIds, final String method,
            final URI uri, final Map<String, List<String>> parameters, final Map<String, String> headers)
            throws UriConstructionException {
        if (nodeIds == null) {
            throw new IllegalArgumentException("Node IDs may not be null.");
        } else if (method == null) {
            throw new IllegalArgumentException("HTTP method may not be null.");
        } else if (uri == null) {
            throw new IllegalArgumentException("URI may not be null.");
        } else if (parameters == null) {
            throw new IllegalArgumentException("Parameters may not be null.");
        } else if (headers == null) {
            throw new IllegalArgumentException("HTTP headers map may not be null.");
        }
        return replicateHelper(nodeIds, method, getNodeProtocolScheme(uri), uri.getPath(), parameters, /* entity */ null, headers);
    }

    @Override
    public Set<NodeResponse> replicate(final Set<NodeIdentifier> nodeIds, final String method, final URI uri,
            final Object entity, final Map<String, String> headers) throws UriConstructionException {
        if (nodeIds == null) {
            throw new IllegalArgumentException("Node IDs may not be null.");
        } else if (method == null) {
            throw new IllegalArgumentException("HTTP method may not be null.");
        } else if (method.equalsIgnoreCase(HttpMethod.DELETE) || method.equalsIgnoreCase(HttpMethod.GET) || method.equalsIgnoreCase(HttpMethod.HEAD) || method.equalsIgnoreCase(HttpMethod.OPTIONS)) {
            throw new IllegalArgumentException("HTTP (DELETE | GET | HEAD | OPTIONS) requests cannot have a body containing an entity.");
        } else if (uri == null) {
            throw new IllegalArgumentException("URI may not be null.");
        } else if (entity == null) {
            throw new IllegalArgumentException("Entity may not be null.");
        } else if (headers == null) {
            throw new IllegalArgumentException("HTTP headers map may not be null.");
        }
        return replicateHelper(nodeIds, method, getNodeProtocolScheme(uri), uri.getPath(), /* parameters */ null, entity, headers);
    }

    private Set<NodeResponse> replicateHelper(final Set<NodeIdentifier> nodeIds, final String method, final String scheme,
            final String path, final Map<String, List<String>> parameters, final Object entity, final Map<String, String> headers)
            throws UriConstructionException {

        if (nodeIds.isEmpty()) {
            return new HashSet<>(); // return quickly for trivial case
        }

        final CompletionService<NodeResponse> completionService = new ExecutorCompletionService<>(executorService);

        // keeps track of future requests so that failed requests can be tied back to the failing node
        final Collection<NodeHttpRequestFutureWrapper> futureNodeHttpRequests = new ArrayList<>();

        // construct the URIs for the nodes
        final Map<NodeIdentifier, URI> uriMap = new HashMap<>();
        try {
            for (final NodeIdentifier nodeId : nodeIds) {
                final URI nodeUri = new URI(scheme, null, nodeId.getApiAddress(), nodeId.getApiPort(), path, /* query */ null, /* fragment */ null);
                uriMap.put(nodeId, nodeUri);
            }
        } catch (final URISyntaxException use) {
            throw new UriConstructionException(use);
        }

        // submit the requests to the nodes
        final String requestId = UUID.randomUUID().toString();
        headers.put(WebClusterManager.REQUEST_ID_HEADER, requestId);
        for (final Map.Entry<NodeIdentifier, URI> entry : uriMap.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final URI nodeUri = entry.getValue();
            final NodeHttpRequestCallable callable = (entity == null)
                    ? new NodeHttpRequestCallable(nodeId, method, nodeUri, parameters, headers)
                    : new NodeHttpRequestCallable(nodeId, method, nodeUri, entity, headers);
            futureNodeHttpRequests.add(new NodeHttpRequestFutureWrapper(nodeId, method, nodeUri, completionService.submit(callable)));
        }

        // get the node responses
        final Set<NodeResponse> result = new HashSet<>();
        for (int i = 0; i < nodeIds.size(); i++) {

            // keeps track of the original request information in case we receive an exception
            NodeHttpRequestFutureWrapper futureNodeHttpRequest = null;
            try {

                // get the future resource response for the node
                final Future<NodeResponse> futureNodeResourceResponse = completionService.take();

                // find the original request by comparing the submitted future with the future returned by the completion service
                for (final NodeHttpRequestFutureWrapper futureNodeHttpRequestElem : futureNodeHttpRequests) {
                    if (futureNodeHttpRequestElem.getFuture() == futureNodeResourceResponse) {
                        futureNodeHttpRequest = futureNodeHttpRequestElem;
                    }
                }

                // try to retrieve the node response and add to result
                final NodeResponse nodeResponse = futureNodeResourceResponse.get();
                result.add(nodeResponse);

            } catch (final InterruptedException | ExecutionException ex) {

                logger.warn("Node request for " + futureNodeHttpRequest.getNodeId() + " encountered exception: " + ex, ex);

                // create node response with the thrown exception and add to result
                final NodeResponse nodeResponse = new NodeResponse(
                        futureNodeHttpRequest.getNodeId(), futureNodeHttpRequest.getHttpMethod(), futureNodeHttpRequest.getRequestUri(), ex);
                result.add(nodeResponse);

            }
        }

        if (logger.isDebugEnabled()) {
            NodeResponse min = null;
            NodeResponse max = null;
            long nanosSum = 0L;
            int nanosAdded = 0;

            for (final NodeResponse response : result) {
                final long requestNanos = response.getRequestDuration(TimeUnit.NANOSECONDS);
                final long minNanos = (min == null) ? -1 : min.getRequestDuration(TimeUnit.NANOSECONDS);
                final long maxNanos = (max == null) ? -1 : max.getRequestDuration(TimeUnit.NANOSECONDS);

                if (requestNanos < minNanos || minNanos < 0L) {
                    min = response;
                }

                if (requestNanos > maxNanos || maxNanos < 0L) {
                    max = response;
                }

                if (requestNanos >= 0L) {
                    nanosSum += requestNanos;
                    nanosAdded++;
                }
            }

            final StringBuilder sb = new StringBuilder();
            sb.append("Node Responses for ").append(method).append(" ").append(path).append(" (Request ID ").append(requestId).append("):\n");
            for (final NodeResponse response : result) {
                sb.append(response).append("\n");
            }

            final long averageNanos = (nanosAdded == 0) ? -1L : nanosSum / nanosAdded;
            final long averageMillis = (averageNanos < 0) ? averageNanos : TimeUnit.MILLISECONDS.convert(averageNanos, TimeUnit.NANOSECONDS);
            logger.debug("For {} {} (Request ID {}), minimum response time = {}, max = {}, average = {} ms",
                    method, path, requestId, min, max, averageMillis);
            logger.debug(sb.toString());
        }

        return result;
    }

    /**
     * Wraps a future node response with info from originating request. This coupling allows for futures that encountered exceptions to be linked back to the failing node and better reported.
     */
    private class NodeHttpRequestFutureWrapper {

        private final NodeIdentifier nodeId;

        private final String httpMethod;

        private final URI requestUri;

        private final Future<NodeResponse> future;

        public NodeHttpRequestFutureWrapper(final NodeIdentifier nodeId, final String httpMethod,
                final URI requestUri, final Future<NodeResponse> future) {
            if (nodeId == null) {
                throw new IllegalArgumentException("Node ID may not be null.");
            } else if (StringUtils.isBlank(httpMethod)) {
                throw new IllegalArgumentException("Http method may not be null or empty.");
            } else if (requestUri == null) {
                throw new IllegalArgumentException("Request URI may not be null.");
            } else if (future == null) {
                throw new IllegalArgumentException("Future may not be null.");
            }
            this.nodeId = nodeId;
            this.httpMethod = httpMethod;
            this.requestUri = requestUri;
            this.future = future;
        }

        public NodeIdentifier getNodeId() {
            return nodeId;
        }

        public String getHttpMethod() {
            return httpMethod;
        }

        public URI getRequestUri() {
            return requestUri;
        }

        public Future<NodeResponse> getFuture() {
            return future;
        }
    }

    /**
     * A Callable for making an HTTP request to a single node and returning its response.
     */
    private class NodeHttpRequestCallable implements Callable<NodeResponse> {

        private final NodeIdentifier nodeId;
        private final String method;
        private final URI uri;
        private final Object entity;
        private final Map<String, List<String>> parameters = new HashMap<>();
        private final Map<String, String> headers = new HashMap<>();

        private NodeHttpRequestCallable(final NodeIdentifier nodeId, final String method,
                final URI uri, final Object entity, final Map<String, String> headers) {
            this.nodeId = nodeId;
            this.method = method;
            this.uri = uri;
            this.entity = entity;
            this.headers.putAll(headers);
        }

        private NodeHttpRequestCallable(final NodeIdentifier nodeId, final String method,
                final URI uri, final Map<String, List<String>> parameters, final Map<String, String> headers) {
            this.nodeId = nodeId;
            this.method = method;
            this.uri = uri;
            this.entity = null;
            this.parameters.putAll(parameters);
            this.headers.putAll(headers);
        }

        @Override
        public NodeResponse call() {

            try {
                // create and send the request
                final WebResource.Builder resourceBuilder = getResourceBuilder();
                final String requestId = headers.get("x-nifi-request-id");

                final long startNanos = System.nanoTime();
                final ClientResponse clientResponse;
                if (HttpMethod.DELETE.equalsIgnoreCase(method)) {
                    clientResponse = resourceBuilder.delete(ClientResponse.class);
                } else if (HttpMethod.GET.equalsIgnoreCase(method)) {
                    clientResponse = resourceBuilder.get(ClientResponse.class);
                } else if (HttpMethod.HEAD.equalsIgnoreCase(method)) {
                    clientResponse = resourceBuilder.head();
                } else if (HttpMethod.OPTIONS.equalsIgnoreCase(method)) {
                    clientResponse = resourceBuilder.options(ClientResponse.class);
                } else if (HttpMethod.POST.equalsIgnoreCase(method)) {
                    clientResponse = resourceBuilder.post(ClientResponse.class);
                } else if (HttpMethod.PUT.equalsIgnoreCase(method)) {
                    clientResponse = resourceBuilder.put(ClientResponse.class);
                } else {
                    throw new IllegalArgumentException("HTTP Method '" + method + "' not supported for request replication.");
                }

                // create and return the response
                return new NodeResponse(nodeId, method, uri, clientResponse, System.nanoTime() - startNanos, requestId);

            } catch (final UniformInterfaceException | IllegalArgumentException t) {
                return new NodeResponse(nodeId, method, uri, t);
            }

        }

        private WebResource.Builder getResourceBuilder() {

            // convert parameters to a more convenient data structure
            final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
            map.putAll(parameters);

            // create the resource
            WebResource resource = client.resource(uri);

            if (new StandardHttpResponseMerger().isResponseInterpreted(uri, method)) {
                resource.addFilter(new GZIPContentEncodingFilter(false));
            }

            // set the parameters as either query parameters or as request body
            final WebResource.Builder builder;
            if (HttpMethod.DELETE.equalsIgnoreCase(method) || HttpMethod.HEAD.equalsIgnoreCase(method) || HttpMethod.GET.equalsIgnoreCase(method) || HttpMethod.OPTIONS.equalsIgnoreCase(method)) {
                resource = resource.queryParams(map);
                builder = resource.getRequestBuilder();
            } else {
                if (entity == null) {
                    builder = resource.entity(map);
                } else {
                    builder = resource.entity(entity);
                }
            }

            // set headers
            boolean foundContentType = false;
            for (final Map.Entry<String, String> entry : headers.entrySet()) {
                builder.header(entry.getKey(), entry.getValue());
                if (entry.getKey().equalsIgnoreCase("content-type")) {
                    foundContentType = true;
                }
            }

            // set default content type
            if (!foundContentType) {
                // set default content type
                builder.type(MediaType.APPLICATION_FORM_URLENCODED);
            }

            return builder;
        }

    }
}
