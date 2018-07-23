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

package org.apache.nifi.web.api.concurrent;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class AsyncRequestManager<T> implements RequestManager<T> {
    private static final Logger logger = LoggerFactory.getLogger(AsyncRequestManager.class);

    private final long requestExpirationMillis;
    private final int maxConcurrentRequests;
    private final ConcurrentMap<String, AsynchronousWebRequest<T>> requests = new ConcurrentHashMap<>();

    private final ExecutorService threadPool;


    public AsyncRequestManager(final int maxConcurrentRequests, final long requestExpirationMillis, final String threadNamePrefix) {
        this.requestExpirationMillis = requestExpirationMillis;
        this.maxConcurrentRequests = maxConcurrentRequests;

        this.threadPool = new ThreadPoolExecutor(1, 50, 5L, TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(maxConcurrentRequests),
            new ThreadFactory() {
                private final AtomicLong counter = new AtomicLong(0L);

                @Override
                public Thread newThread(final Runnable r) {
                    final Thread thread = Executors.defaultThreadFactory().newThread(r);
                    thread.setName(threadNamePrefix + "-" + counter.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                }
            });

    }

    private String getKey(final String type, final String request) {
        return type + "/" + request;
    }

    @Override
    public void submitRequest(final String type, final String requestId, final AsynchronousWebRequest<T> request, final Consumer<AsynchronousWebRequest<T>> task) {
        Objects.requireNonNull(type);
        Objects.requireNonNull(requestId);
        Objects.requireNonNull(request);
        Objects.requireNonNull(task);

        // before adding to the request map, purge any old requests. Must do this by creating a List of ID's
        // and then removing those ID's one-at-a-time in order to avoid ConcurrentModificationException.
        final Date oneMinuteAgo = new Date(System.currentTimeMillis() - requestExpirationMillis);
        final List<String> completedRequestIds = requests.entrySet().stream()
            .filter(entry -> entry.getValue().isComplete())
            .filter(entry -> entry.getValue().getLastUpdated().before(oneMinuteAgo))
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());

        completedRequestIds.stream().forEach(id -> requests.remove(id));

        final int requestCount = requests.size();
        if (requestCount > maxConcurrentRequests) {
            throw new IllegalStateException("There are already " + requestCount + " update requests for variable registries. "
                + "Cannot issue any more requests until the older ones are deleted or expire");
        }

        final String key = getKey(type, requestId);
        final AsynchronousWebRequest<T> existing = this.requests.putIfAbsent(key, request);
        if (existing != null) {
            throw new IllegalArgumentException("A requests already exists with this ID and type");
        }

        threadPool.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    // set the user authentication token
                    final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(request.getUser()));
                    SecurityContextHolder.getContext().setAuthentication(authentication);

                    task.accept(request);
                } catch (final Exception e) {
                    logger.error("Failed to perform asynchronous task", e);
                    request.setFailureReason("Encountered unexpected error when performing asynchronous task: " + e);
                } finally {
                    // clear the authentication token
                    SecurityContextHolder.getContext().setAuthentication(null);
                }
            }
        });
    }


    @Override
    public AsynchronousWebRequest<T> removeRequest(final String type, final String id, final NiFiUser user) {
        Objects.requireNonNull(type);
        Objects.requireNonNull(id);
        Objects.requireNonNull(user);

        final String key = getKey(type, id);
        final AsynchronousWebRequest<T> request = requests.get(key);
        if (request == null) {
            throw new ResourceNotFoundException("Could not find a Request with identifier " + id);
        }

        if (!request.getUser().equals(user)) {
            throw new IllegalArgumentException("Only the user that submitted the update request can delete it.");
        }

        if (!request.isComplete()) {
            throw new IllegalStateException("Cannot remove the request because it is not yet complete");
        }

        return requests.remove(key);
    }

    @Override
    public AsynchronousWebRequest<T> getRequest(final String type, final String id, final NiFiUser user) {
        Objects.requireNonNull(type);
        Objects.requireNonNull(id);
        Objects.requireNonNull(user);

        final String key = getKey(type, id);
        final AsynchronousWebRequest<T> request = requests.get(key);
        if (request == null) {
            throw new ResourceNotFoundException("Could not find a Request with identifier " + id);
        }

        if (!request.getUser().equals(user)) {
            throw new IllegalArgumentException("Only the user that submitted the update request can delete it.");
        }

        return request;
    }

}
