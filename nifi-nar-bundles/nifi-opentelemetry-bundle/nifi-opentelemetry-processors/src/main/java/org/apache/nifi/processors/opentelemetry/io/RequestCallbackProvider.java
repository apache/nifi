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
package org.apache.nifi.processors.opentelemetry.io;

import com.google.protobuf.Message;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import org.apache.nifi.processors.opentelemetry.protocol.TelemetryRequestType;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;

/**
 * Request Callback Provider creates Callback instances for batches of Request messages
 */
public class RequestCallbackProvider implements Iterator<RequestCallback> {
    private static final String EMPTY_ELEMENT = null;

    private final URI transitBaseUri;

    private final int batchSize;

    private final BlockingQueue<Message> messages;

    /**
     * Request Callback Provider constructor with required parameters for queued messages
     *
     * @param transitBaseUri Transit Base URI to which the provider appends the Request Type path
     * @param batchSize Maximum batch size for messages included in a single Request Callback
     * @param messages Queue of messages to be processed
     */
    public RequestCallbackProvider(final URI transitBaseUri, final int batchSize, final BlockingQueue<Message> messages) {
        this.transitBaseUri = Objects.requireNonNull(transitBaseUri, "Transit Base URI required");
        this.batchSize = batchSize;
        this.messages = Objects.requireNonNull(messages, "Messages required");
    }

    /**
     * Provider has next returns status based on queued messages
     *
     * @return Next callback available based on queued messages
     */
    @Override
    public boolean hasNext() {
        return !messages.isEmpty();
    }

    /**
     * Provider returns next Request Callback instance for handling a batch of Request messages
     *
     * @return Request Callback
     * @throws NoSuchElementException Thrown when no messages queued
     */
    @Override
    public RequestCallback next() {
        final Message head = messages.element();
        final Class<? extends Message> headMessageClass = head.getClass();
        final TelemetryRequestType requestType = getRequestType(headMessageClass);
        final String transitUri = getTransitUri(requestType);
        final List<Message> requestMessages = getRequestMessages(headMessageClass);
        return new StandardRequestCallback(requestType, headMessageClass, requestMessages, transitUri);
    }

    private TelemetryRequestType getRequestType(final Class<?> headMessageClass) {
        final TelemetryRequestType requestType;

        if (ResourceLogs.class.isAssignableFrom(headMessageClass)) {
            requestType = TelemetryRequestType.LOGS;
        } else if (ResourceMetrics.class.isAssignableFrom(headMessageClass)) {
            requestType = TelemetryRequestType.METRICS;
        } else if (ResourceSpans.class.isAssignableFrom(headMessageClass)) {
            requestType = TelemetryRequestType.TRACES;
        } else {
            throw new IllegalArgumentException(String.format("Request Class [%s] not supported", headMessageClass));
        }

        return requestType;
    }

    private String getTransitUri(final TelemetryRequestType requestType) {
        try {
            final URI uri = new URI(
                    transitBaseUri.getScheme(),
                    EMPTY_ELEMENT,
                    transitBaseUri.getHost(),
                    transitBaseUri.getPort(),
                    requestType.getPath(),
                    EMPTY_ELEMENT,
                    EMPTY_ELEMENT
            );
            return uri.toString();
        } catch (final URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private List<Message> getRequestMessages(final Class<?> headMessageClass) {
        final List<Message> requestMessages = new ArrayList<>(batchSize);

        final Iterator<Message> currentMessages = messages.iterator();
        while (currentMessages.hasNext()) {
            final Message message = currentMessages.next();
            final Class<?> messageClass = message.getClass();
            if (headMessageClass.equals(messageClass)) {
                requestMessages.add(message);
                currentMessages.remove();

                if (requestMessages.size() == batchSize) {
                    break;
                }
            }
        }

        return requestMessages;
    }
}
