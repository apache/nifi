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
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import org.apache.nifi.processors.opentelemetry.encoding.RequestMapper;
import org.apache.nifi.processors.opentelemetry.encoding.StandardRequestMapper;
import org.apache.nifi.processors.opentelemetry.protocol.TelemetryAttributeName;
import org.apache.nifi.processors.opentelemetry.protocol.TelemetryRequestType;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Standard Request Callback implementation serializes messages to JSON according to OTLP 1.0.0 formatting
 */
class StandardRequestCallback implements RequestCallback {
    private static final String APPLICATION_JSON = "application/json";

    private static final RequestMapper REQUEST_MAPPER = new StandardRequestMapper();

    private final Map<String, String> attributes = new LinkedHashMap<>();

    private final Class<? extends Message> messageClass;

    private final List<Message> messages;

    private final String transitUri;

    StandardRequestCallback(final TelemetryRequestType requestType, final Class<? extends Message> messageClass, final List<Message> messages, final String transitUri) {
        this.messageClass = Objects.requireNonNull(messageClass, "Message Class required");
        this.messages = Objects.requireNonNull(messages, "Messages required");
        this.transitUri = Objects.requireNonNull(transitUri, "Transit URI required");
        attributes.put(TelemetryAttributeName.MIME_TYPE, APPLICATION_JSON);
        attributes.put(TelemetryAttributeName.RESOURCE_TYPE, requestType.name());
        attributes.put(TelemetryAttributeName.RESOURCE_COUNT, Integer.toString(messages.size()));
    }

    /**
     * Process messages serialized as JSON wrapped in Export Service Request class
     *
     * @param outputStream FlowFile OutputStream for writing serialized JSON
     * @throws IOException Thrown on failures writing to OutputStream
     */
    @Override
    public void process(final OutputStream outputStream) throws IOException {
        if (ResourceLogs.class.isAssignableFrom(messageClass)) {
            final ExportLogsServiceRequest.Builder requestBuilder = ExportLogsServiceRequest.newBuilder();
            for (final Message message : messages) {
                final ResourceLogs resourceLogs = (ResourceLogs) message;
                requestBuilder.addResourceLogs(resourceLogs);
            }
            final ExportLogsServiceRequest request = requestBuilder.build();
            REQUEST_MAPPER.writeValue(outputStream, request);
        } else if (ResourceMetrics.class.isAssignableFrom(messageClass)) {
            final ExportMetricsServiceRequest.Builder requestBuilder = ExportMetricsServiceRequest.newBuilder();
            for (final Message message : messages) {
                final ResourceMetrics currentResourceMetrics = (ResourceMetrics) message;
                requestBuilder.addResourceMetrics(currentResourceMetrics);
            }
            final ExportMetricsServiceRequest request = requestBuilder.build();
            REQUEST_MAPPER.writeValue(outputStream, request);
        } else if (ResourceSpans.class.isAssignableFrom(messageClass)) {
            final ExportTraceServiceRequest.Builder requestBuilder = ExportTraceServiceRequest.newBuilder();
            for (final Message message : messages) {
                final ResourceSpans resourceSpans = (ResourceSpans) message;
                requestBuilder.addResourceSpans(resourceSpans);
            }
            final ExportTraceServiceRequest request = requestBuilder.build();
            REQUEST_MAPPER.writeValue(outputStream, request);
        } else {
            throw new IllegalArgumentException(String.format("Request Class [%s] not supported", messageClass));
        }
    }

    /**
     * Get Transit URI for Provenance Reporting
     *
     * @return Transit URI
     */
    @Override
    public String getTransitUri() {
        return transitUri;
    }

    /**
     * Get FlowFile attributes based on Request Type and messages queued
     *
     * @return FlowFile attributes
     */
    @Override
    public Map<String, String> getAttributes() {
        return Collections.unmodifiableMap(attributes);
    }
}
