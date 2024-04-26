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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.nifi.cluster.coordination.http.HttpResponseMapper;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandardAsyncClusterResponse implements AsyncClusterResponse {
    private static final Logger logger = LoggerFactory.getLogger(StandardAsyncClusterResponse.class);
    private static final int DEFAULT_RESPONSE_BUFFER_SIZE = 1024 * 1024;

    public static final String VERIFICATION_PHASE = "Verification Phase";
    public static final String COMMIT_PHASE = "Execution Phase";
    public static final String ONLY_PHASE = "Only Phase";

    private final String id;
    private final Set<NodeIdentifier> nodeIds;
    private final URI uri;
    private final String method;
    private final HttpResponseMapper responseMapper;
    private final CompletionCallback completionCallback;
    private final Runnable completedResultFetchedCallback;
    private final long creationTimeNanos;
    private final boolean merge;
    private final AtomicInteger responseBufferLeft;

    private final Map<NodeIdentifier, ResponseHolder> responseMap = new HashMap<>();
    private final AtomicInteger requestsCompleted = new AtomicInteger(0);

    private NodeResponse mergedResponse; // guarded by synchronizing on this
    private RuntimeException failure; // guarded by synchronizing on this
    private volatile String phase;
    private volatile long phaseStartTime = System.nanoTime();
    private final long creationTime = System.nanoTime();

    private final Map<String, Long> timingInfo = new LinkedHashMap<>();

    public StandardAsyncClusterResponse(final String id, final URI uri, final String method, final Set<NodeIdentifier> nodeIds, final HttpResponseMapper responseMapper,
        final CompletionCallback completionCallback, final Runnable completedResultFetchedCallback, final boolean merge) {
        this(id, uri, method, nodeIds, responseMapper, completionCallback, completedResultFetchedCallback, merge, DEFAULT_RESPONSE_BUFFER_SIZE);
    }

    public StandardAsyncClusterResponse(final String id, final URI uri, final String method, final Set<NodeIdentifier> nodeIds, final HttpResponseMapper responseMapper,
        final CompletionCallback completionCallback, final Runnable completedResultFetchedCallback, final boolean merge, final int responseBufferSize) {
        this.id = id;
        this.nodeIds = Collections.unmodifiableSet(new HashSet<>(nodeIds));
        this.uri = uri;
        this.method = method;
        this.merge = merge;

        if ("POST".equalsIgnoreCase(method) || "PUT".equalsIgnoreCase(method) || "DELETE".equalsIgnoreCase(method)) {
            phase = VERIFICATION_PHASE;
        } else {
            phase = ONLY_PHASE;
        }

        creationTimeNanos = System.nanoTime();
        for (final NodeIdentifier nodeId : nodeIds) {
            responseMap.put(nodeId, new ResponseHolder(creationTimeNanos));
        }

        this.responseMapper = responseMapper;
        this.completionCallback = completionCallback;
        this.completedResultFetchedCallback = completedResultFetchedCallback;
        this.responseBufferLeft = new AtomicInteger(responseBufferSize);
    }

    public boolean requestBuffer(final int size) {
        boolean updated = false;
        while (!updated) {
            final int bytesLeft = responseBufferLeft.get();
            if (bytesLeft < size) {
                return false;
            }

            updated = responseBufferLeft.compareAndSet(bytesLeft, bytesLeft - size);
        }

        return true;
    }

    public void setPhase(final String phase) {
        this.phase = phase;
        phaseStartTime = System.nanoTime();
    }

    public synchronized void addTiming(final String description, final String node, final long nanos) {
        final StringBuilder sb = new StringBuilder(description);
        if (phase != ONLY_PHASE) {
            sb.append(" (").append(phase).append(")");
        }
        sb.append(" for ").append(node);
        timingInfo.put(sb.toString(), nanos);
    }

    private synchronized void logTimingInfo() {
        if (!logger.isDebugEnabled()) {
            return;
        }

        final StringBuilder sb = new StringBuilder();
        sb.append(String.format("For %s %s Timing Info is as follows:\n", method, uri));
        for (final Map.Entry<String, Long> entry : timingInfo.entrySet()) {
            sb.append(entry.getKey()).append(" took ").append(TimeUnit.NANOSECONDS.toMillis(entry.getValue())).append(" millis\n");
        }
        logger.debug(sb.toString());
    }


    @Override
    public String getRequestIdentifier() {
        return id;
    }

    @Override
    public Set<NodeIdentifier> getNodesInvolved() {
        return nodeIds;
    }

    @Override
    public Set<NodeIdentifier> getCompletedNodeIdentifiers() {
        return responseMap.entrySet().stream()
            .filter(entry -> entry.getValue().isComplete())
            .map(entry -> entry.getKey())
            .collect(Collectors.toSet());
    }

    @Override
    public Set<NodeResponse> getCompletedNodeResponses() {
        return responseMap.values().stream()
            .filter(responseHolder -> responseHolder.isComplete())
            .map(responseHolder -> responseHolder.getResponse())
            .collect(Collectors.toSet());
    }

    @Override
    public boolean isOlderThan(final long time, final TimeUnit timeUnit) {
        final long nanos = timeUnit.toNanos(time);
        final long threshold = System.nanoTime() - nanos;
        return creationTimeNanos < threshold;
    }

    @Override
    public synchronized boolean isComplete() {
        return failure != null || mergedResponse != null || requestsCompleted.get() >= responseMap.size();
    }

    @Override
    public String getMethod() {
        return method;
    }

    @Override
    public String getURIPath() {
        return uri.getPath();
    }

    @Override
    public NodeResponse getMergedResponse() {
        return getMergedResponse(true);
    }

    public synchronized NodeResponse getMergedResponse(final boolean triggerCallback) {
        if (failure != null) {
            if (completedResultFetchedCallback != null) {
                completedResultFetchedCallback.run();
            }

            throw failure;
        }

        if (mergedResponse != null) {
            if (triggerCallback && completedResultFetchedCallback != null) {
                completedResultFetchedCallback.run();
            }

            return mergedResponse;
        }

        if (requestsCompleted.get() < responseMap.size()) {
            return null;
        }

        final Set<NodeResponse> nodeResponses = responseMap.values().stream()
            .map(p -> p.getResponse())
            .filter(response -> response != null)
            .collect(Collectors.toSet());

        final long start = System.nanoTime();
        mergedResponse = responseMapper.mapResponses(uri, method, nodeResponses, merge);
        final long nanos = System.nanoTime() - start;
        addTiming("Map/Merge Responses", "All Nodes", nanos);

        logger.debug("Notifying all that merged response is complete for {}", id);
        this.notifyAll();

        if (triggerCallback && completedResultFetchedCallback != null) {
            completedResultFetchedCallback.run();
        }

        return mergedResponse;
    }

    @Override
    public NodeResponse awaitMergedResponse() throws InterruptedException {
        synchronized (this) {
            while (getMergedResponse(false) == null) {
                logger.debug("Waiting indefinitely for merged response to be complete for {}", id);
                this.wait();
            }
        }

        logTimingInfo();

        return getMergedResponse(true);
    }

    @Override
    public NodeResponse awaitMergedResponse(final long timeout, final TimeUnit timeUnit) throws InterruptedException {
        if (timeout < 0) {
            throw new IllegalArgumentException();
        }

        final long maxTime = System.nanoTime() + timeUnit.toNanos(timeout);

        synchronized (this) {
            while (getMergedResponse(false) == null) {
                final long nanosToWait = maxTime - System.nanoTime();
                if (nanosToWait < 0) {
                    return getMergedResponse(true);
                }

                final long millis = TimeUnit.NANOSECONDS.toMillis(nanosToWait);
                final int nanos = (int) (nanosToWait - TimeUnit.MILLISECONDS.toNanos(millis));

                logger.debug("Waiting {} millis and {} nanos for merged response to be complete for {}", millis, nanos, id);
                this.wait(millis, nanos);
            }
        }

        logTimingInfo();

        return getMergedResponse(true);
    }

    @Override
    public NodeResponse getNodeResponse(final NodeIdentifier nodeId) {
        final ResponseHolder request = responseMap.get(nodeId);
        return request == null ? null : request.getResponse();
    }

    void add(final NodeResponse nodeResponse) {
        final ResponseHolder responseHolder = responseMap.get(nodeResponse.getNodeId());
        if (responseHolder == null) {
            throw new IllegalStateException("Node " + nodeResponse.getNodeId() + " is not known for this request");
        }

        responseHolder.setResponse(nodeResponse);
        final int completedCount = requestsCompleted.incrementAndGet();

        logger.debug("Received response {} out of {} for {} from {}", completedCount, responseMap.size(), id, nodeResponse.getNodeId());

        if (completedCount == responseMap.size()) {
            logger.debug("Notifying all that merged response is ready for {}", id);
            addTiming("Phase Completed", "All Nodes", System.nanoTime() - phaseStartTime);

            final long start = System.nanoTime();

            synchronized (this) {
                this.notifyAll();
            }

            final long nanos = System.nanoTime() - start;
            timingInfo.put("Notifying All Threads that Request is Complete", nanos);
            timingInfo.put("Total Time for All Nodes", System.nanoTime() - creationTime);

            if (completionCallback != null) {
                completionCallback.onCompletion(this);
            }
        }
    }

    synchronized void setFailure(final RuntimeException failure, final NodeIdentifier nodeId) {
        this.failure = failure;

        final int completedCount = requestsCompleted.incrementAndGet();
        logger.debug("Notified of failure for {} from {}", id, nodeId);

        if (completedCount == responseMap.size()) {

            notifyAll();
            if (completionCallback != null) {
                completionCallback.onCompletion(this);
            }
        }
    }

    @Override
    public String toString() {
        return "StandardAsyncClusterResponse[id=" + id + ", uri=" + uri + ", method=" + method + ", failure=" + (failure != null)
            + ", responses=" + getCompletedNodeIdentifiers().size() + "/" + responseMap.size() + "]";
    }

    private static class ResponseHolder {
        private final long nanoStart;
        private long requestNanos;
        private NodeResponse response;

        public ResponseHolder(final long startNanos) {
            this.nanoStart = startNanos;
        }

        public synchronized void setResponse(final NodeResponse response) {
            this.response = response;
            this.requestNanos = System.nanoTime() - nanoStart;
        }

        public synchronized NodeResponse getResponse() {
            return response;
        }

        public synchronized boolean isComplete() {
            return response != null;
        }

        @SuppressWarnings("unused")
        public long getRequestDuration(final TimeUnit timeUnit) {
            return timeUnit.toNanos(requestNanos);
        }
    }
}
