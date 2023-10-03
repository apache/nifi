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

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.google.protobuf.Message;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.opentelemetry.encoding.JsonServiceRequestReader;
import org.apache.nifi.processors.opentelemetry.encoding.ProtobufServiceRequestReader;
import org.apache.nifi.processors.opentelemetry.encoding.ServiceRequestReader;
import org.apache.nifi.processors.opentelemetry.protocol.ServiceRequestDescription;
import org.apache.nifi.processors.opentelemetry.protocol.ServiceResponse;
import org.apache.nifi.processors.opentelemetry.protocol.ServiceResponseStatus;
import org.apache.nifi.processors.opentelemetry.protocol.TelemetryContentEncoding;
import org.apache.nifi.processors.opentelemetry.protocol.TelemetryContentType;
import org.apache.nifi.processors.opentelemetry.protocol.TelemetryRequestType;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.zip.GZIPInputStream;

/**
 * Standard implementation of OTLP Request Content Listener supporting available Request and Content Types
 */
public class StandardRequestContentListener implements RequestContentListener {
    private static final int ZERO_MESSAGES = 0;

    private static final byte COMPRESSED = 1;

    private static final String CLIENT_SOCKET_ADDRESS = "client.socket.address";

    private static final String CLIENT_SOCKET_PORT = "client.socket.port";

    private final ServiceRequestReader protobufReader = new ProtobufServiceRequestReader();

    private final ServiceRequestReader jsonReader = new JsonServiceRequestReader();

    private final ComponentLog log;

    private final BlockingQueue<Message> messages;

    public StandardRequestContentListener(final ComponentLog log, final BlockingQueue<Message> messages) {
        this.log = Objects.requireNonNull(log, "Log required");
        this.messages = Objects.requireNonNull(messages, "Messages required");
    }

    /**
     * Process Request Buffer supporting serialized Protobuf or JSON
     *
     * @param buffer Request Content Buffer to be processed
     * @param serviceRequestDescription Service Request Description describes reader attributes
     * @return Service Response with processing status
     */
    @Override
    public ServiceResponse onRequest(final ByteBuffer buffer, final ServiceRequestDescription serviceRequestDescription) {
        Objects.requireNonNull(buffer, "Buffer required");
        Objects.requireNonNull(serviceRequestDescription, "Description required");

        ServiceResponse serviceResponse;

        final InetSocketAddress remoteAddress = serviceRequestDescription.getRemoteAddress();
        final TelemetryContentType contentType = serviceRequestDescription.getContentType();
        if (TelemetryContentType.APPLICATION_GRPC == contentType) {
            try {
                final byte compression = buffer.get();
                final int messageSize = buffer.getInt();
                log.debug("Client Address [{}] Content-Type [{}] Message Size [{}] Compression [{}]", remoteAddress, contentType, messageSize, compression);

                final TelemetryContentEncoding bufferEncoding;
                if (COMPRESSED == compression) {
                    bufferEncoding = TelemetryContentEncoding.GZIP;
                } else {
                    bufferEncoding = TelemetryContentEncoding.NONE;
                }

                final InputStream decodedStream = getDecodedStream(buffer, bufferEncoding);
                serviceResponse = onSupportedRequest(decodedStream, serviceRequestDescription);
            } catch (final Exception e) {
                log.warn("Client Address [{}] Content-Type [{}] processing failed", remoteAddress, contentType, e);
                serviceResponse = new ServiceResponse(ServiceResponseStatus.INVALID, ZERO_MESSAGES);
            }
        } else if (TelemetryContentType.APPLICATION_PROTOBUF == contentType || TelemetryContentType.APPLICATION_JSON == contentType) {
            try {
                final InputStream decodedStream = getDecodedStream(buffer, serviceRequestDescription.getContentEncoding());
                serviceResponse = onSupportedRequest(decodedStream, serviceRequestDescription);
            } catch (final Exception e) {
                log.warn("Client Address [{}] Content-Type [{}] processing failed", remoteAddress, contentType, e);
                serviceResponse = new ServiceResponse(ServiceResponseStatus.INVALID, ZERO_MESSAGES);
            }
        } else {
            serviceResponse = new ServiceResponse(ServiceResponseStatus.INVALID, ZERO_MESSAGES);
        }

        return serviceResponse;
    }

    private ServiceResponse onSupportedRequest(final InputStream inputStream, final ServiceRequestDescription serviceRequestDescription) throws IOException {
        final ServiceRequestReader serviceRequestReader;
        final TelemetryContentType contentType = serviceRequestDescription.getContentType();
        if (TelemetryContentType.APPLICATION_JSON == contentType) {
            serviceRequestReader = jsonReader;
        } else {
            serviceRequestReader = protobufReader;
        }

        final TelemetryRequestType requestType = serviceRequestDescription.getRequestType();
        final List<? extends Message> resourceMessages;
        if (inputStream.available() == 0) {
            resourceMessages = Collections.emptyList();
        } else if (TelemetryRequestType.LOGS == requestType) {
            resourceMessages = readMessages(inputStream, serviceRequestDescription, ExportLogsServiceRequest.class, serviceRequestReader);
        } else if (TelemetryRequestType.METRICS == requestType) {
            resourceMessages = readMessages(inputStream, serviceRequestDescription, ExportMetricsServiceRequest.class, serviceRequestReader);
        } else if (TelemetryRequestType.TRACES == requestType) {
            resourceMessages = readMessages(inputStream, serviceRequestDescription, ExportTraceServiceRequest.class, serviceRequestReader);
        } else {
            resourceMessages = null;
        }

        return onMessages(resourceMessages);
    }

    private <T extends Message> List<Message> readMessages(
            final InputStream inputStream,
            final ServiceRequestDescription serviceRequestDescription,
            final Class<T> requestType,
            final ServiceRequestReader serviceRequestReader
    ) {
        final List<Message> messages = new ArrayList<>();

        final List<KeyValue> clientSocketAttributes = getClientSocketAttributes(serviceRequestDescription);

        final T parsed = serviceRequestReader.read(inputStream, requestType);
        if (parsed instanceof ExportLogsServiceRequest request) {
            for (final ResourceLogs resourceLogs : request.getResourceLogsList()) {
                final Resource.Builder resource = resourceLogs.getResource().toBuilder();
                resource.addAllAttributes(clientSocketAttributes);

                final Message message = resourceLogs.toBuilder().setResource(resource).build();
                messages.add(message);
            }
        } else if (parsed instanceof ExportMetricsServiceRequest request) {
            for (final ResourceMetrics resourceMetrics : request.getResourceMetricsList()) {
                final Resource.Builder resource = resourceMetrics.getResource().toBuilder();
                resource.addAllAttributes(clientSocketAttributes);

                final Message message = resourceMetrics.toBuilder().setResource(resource).build();
                messages.add(message);
            }
        } else if (parsed instanceof ExportTraceServiceRequest request) {
            for (final ResourceSpans resourceSpans : request.getResourceSpansList()) {
                final Resource.Builder resource = resourceSpans.getResource().toBuilder();
                resource.addAllAttributes(clientSocketAttributes);

                final Message message = resourceSpans.toBuilder().setResource(resource).build();
                messages.add(message);
            }
        } else {
            throw new IllegalArgumentException(String.format("Request Type [%s] not supported", requestType.getName()));
        }

        return messages;
    }

    private List<KeyValue> getClientSocketAttributes(final ServiceRequestDescription serviceRequestDescription) {
        final InetSocketAddress remoteAddress = serviceRequestDescription.getRemoteAddress();
        final InetAddress remoteSocketAddress = remoteAddress.getAddress();
        final String socketAddress = remoteSocketAddress.getHostAddress();
        final int socketPort = remoteAddress.getPort();

        final KeyValue clientSocketAddress = KeyValue.newBuilder()
                .setKey(CLIENT_SOCKET_ADDRESS)
                .setValue(AnyValue.newBuilder().setStringValue(socketAddress))
                .build();

        final KeyValue clientSocketPort = KeyValue.newBuilder()
                .setKey(CLIENT_SOCKET_PORT)
                .setValue(AnyValue.newBuilder().setIntValue(socketPort))
                .build();

        return List.of(clientSocketAddress, clientSocketPort);
    }

    private ServiceResponse onMessages(final List<? extends Message> resourceMessages) {
        final ServiceResponse serviceResponse;
        if (resourceMessages == null) {
            serviceResponse = new ServiceResponse(ServiceResponseStatus.INVALID, ZERO_MESSAGES);
        } else if (resourceMessages.isEmpty()) {
            serviceResponse = new ServiceResponse(ServiceResponseStatus.SUCCESS, ZERO_MESSAGES);
        } else {
            int accepted = 0;

            for (final Message resourceMessage : resourceMessages) {
                if (messages.offer(resourceMessage)) {
                    accepted++;
                }
            }

            final int rejected = resourceMessages.size() - accepted;
            if (ZERO_MESSAGES == rejected) {
                serviceResponse = new ServiceResponse(ServiceResponseStatus.SUCCESS, ZERO_MESSAGES);
            } else if (ZERO_MESSAGES == accepted) {
                serviceResponse = new ServiceResponse(ServiceResponseStatus.UNAVAILABLE, ZERO_MESSAGES);
            } else {
                serviceResponse = new ServiceResponse(ServiceResponseStatus.PARTIAL_SUCCESS, rejected);
            }
        }

        return serviceResponse;
    }

    private InputStream getDecodedStream(final ByteBuffer buffer, final TelemetryContentEncoding contentEncoding) throws IOException {
        final InputStream decodedStream;

        if (TelemetryContentEncoding.GZIP == contentEncoding) {
            decodedStream = new GZIPInputStream(new ByteBufferBackedInputStream(buffer));
        } else {
            decodedStream = new ByteBufferBackedInputStream(buffer);
        }

        return decodedStream;
    }
}
