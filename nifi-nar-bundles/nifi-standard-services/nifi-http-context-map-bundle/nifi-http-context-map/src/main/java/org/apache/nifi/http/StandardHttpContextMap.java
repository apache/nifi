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
package org.apache.nifi.http;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({"http", "request", "response"})
@SeeAlso(classNames = {
    "org.apache.nifi.processors.standard.HandleHttpRequest",
    "org.apache.nifi.processors.standard.HandleHttpResponse"})
@CapabilityDescription("Provides the ability to store and retrieve HTTP requests and responses external to a Processor, so that "
        + "multiple Processors can interact with the same HTTP request.")
public class StandardHttpContextMap extends AbstractControllerService implements HttpContextMap {

    public static final PropertyDescriptor MAX_OUTSTANDING_REQUESTS = new PropertyDescriptor.Builder()
            .name("Maximum Outstanding Requests")
            .description("The maximum number of HTTP requests that can be outstanding at any one time. Any attempt to register an additional HTTP Request will cause an error")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("5000")
            .build();
    public static final PropertyDescriptor REQUEST_EXPIRATION = new PropertyDescriptor.Builder()
            .name("Request Expiration")
            .description("Specifies how long an HTTP Request should be left unanswered before being evicted from the cache and being responded to with a Service Unavailable status code")
            .required(true)
            .expressionLanguageSupported(false)
            .defaultValue("1 min")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    private final ConcurrentMap<String, Wrapper> wrapperMap = new ConcurrentHashMap<>();

    private volatile int maxSize = 5000;
    private volatile long maxRequestNanos;
    private volatile ScheduledExecutorService executor;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(2);
        properties.add(MAX_OUTSTANDING_REQUESTS);
        properties.add(REQUEST_EXPIRATION);
        return properties;
    }

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) {
        maxSize = context.getProperty(MAX_OUTSTANDING_REQUESTS).asInteger();
        executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(final Runnable r) {
                final Thread thread = Executors.defaultThreadFactory().newThread(r);
                thread.setName("StandardHttpContextMap-" + getIdentifier());
                return thread;
            }
        });

        maxRequestNanos = context.getProperty(REQUEST_EXPIRATION).asTimePeriod(TimeUnit.NANOSECONDS);
        final long scheduleNanos = maxRequestNanos / 2;
        executor.scheduleWithFixedDelay(new CleanupExpiredRequests(), scheduleNanos, scheduleNanos, TimeUnit.NANOSECONDS);
    }

    @OnShutdown
    @OnDisabled
    public void cleanup() {
        if (executor != null) {
            executor.shutdown();
        }
    }

    @Override
    public boolean register(final String identifier, final HttpServletRequest request, final HttpServletResponse response, final AsyncContext context) {
        // fail if there are too many already. Maybe add a configuration property for how many
        // outstanding, with a default of say 5000
        if (wrapperMap.size() >= maxSize) {
            return false;
        }
        final Wrapper wrapper = new Wrapper(request, response, context);
        final Wrapper existing = wrapperMap.putIfAbsent(identifier, wrapper);
        if (existing != null) {
            throw new IllegalStateException("HTTP Request already registered with identifier " + identifier);
        }

        return true;
    }

    @Override
    public HttpServletResponse getResponse(final String identifier) {
        final Wrapper wrapper = wrapperMap.get(identifier);
        if (wrapper == null) {
            return null;
        }

        return wrapper.getResponse();
    }

    @Override
    public void complete(final String identifier) {
        final Wrapper wrapper = wrapperMap.remove(identifier);
        if (wrapper == null) {
            throw new IllegalStateException("No HTTP Request registered with identifier " + identifier);
        }

        wrapper.getAsync().complete();
    }

    private static class Wrapper {

        @SuppressWarnings("unused")
        private final HttpServletRequest request;
        private final HttpServletResponse response;
        private final AsyncContext async;
        private final long nanoTimeAdded = System.nanoTime();

        public Wrapper(final HttpServletRequest request, final HttpServletResponse response, final AsyncContext async) {
            this.request = request;
            this.response = response;
            this.async = async;
        }

        public HttpServletResponse getResponse() {
            return response;
        }

        public AsyncContext getAsync() {
            return async;
        }

        public long getNanoTimeAdded() {
            return nanoTimeAdded;
        }
    }

    private class CleanupExpiredRequests implements Runnable {

        @Override
        public void run() {
            final long now = System.nanoTime();
            final long threshold = now - maxRequestNanos;

            final Iterator<Map.Entry<String, Wrapper>> itr = wrapperMap.entrySet().iterator();
            while (itr.hasNext()) {
                final Map.Entry<String, Wrapper> entry = itr.next();
                if (entry.getValue().getNanoTimeAdded() < threshold) {
                    itr.remove();

                    // send SERVICE_UNAVAILABLE
                    try {
                        final AsyncContext async = entry.getValue().getAsync();
                        ((HttpServletResponse) async.getResponse()).sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
                        async.complete();
                    } catch (final Exception e) {
                        // we are trying to indicate that we are unavailable. If we have an exception and cannot respond,
                        // then so be it. Nothing to really do here.
                    }
                }
            }
        }
    }
}
