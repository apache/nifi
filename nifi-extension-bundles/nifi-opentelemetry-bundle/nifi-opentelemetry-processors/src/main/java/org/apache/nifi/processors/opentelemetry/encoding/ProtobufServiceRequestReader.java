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
package org.apache.nifi.processors.opentelemetry.encoding;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;

import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Objects;

/**
 * Service Request Reader implementation based on Protobuf Parser supporting standard OTLP Request Types
 */
public class ProtobufServiceRequestReader implements ServiceRequestReader {
    private final Parser<ExportLogsServiceRequest> logsServiceRequestParser = ExportLogsServiceRequest.parser();

    private final Parser<ExportMetricsServiceRequest> metricsServiceRequestParser = ExportMetricsServiceRequest.parser();

    private final Parser<ExportTraceServiceRequest> traceServiceRequestParser = ExportTraceServiceRequest.parser();

    /**
     * Read Service Request parsed from Input Stream
     *
     * @param inputStream Input Stream to be parsed
     * @return Service Request read
     */
    @Override
    public <T extends Message> T read(final InputStream inputStream, final Class<T> requestType) {
        Objects.requireNonNull(inputStream, "Input Stream required");
        Objects.requireNonNull(requestType, "Request Type required");

        try {
            final Object serviceRequest;
            if (ExportLogsServiceRequest.class.isAssignableFrom(requestType)) {
                serviceRequest = logsServiceRequestParser.parseFrom(inputStream);
            } else if (ExportMetricsServiceRequest.class.isAssignableFrom(requestType)) {
                serviceRequest = metricsServiceRequestParser.parseFrom(inputStream);
            } else if (ExportTraceServiceRequest.class.isAssignableFrom(requestType)) {
                serviceRequest = traceServiceRequestParser.parseFrom(inputStream);
            } else {
                throw new IllegalArgumentException(String.format("Service Request Class [%s] not supported", requestType.getName()));
            }
            return requestType.cast(serviceRequest);
        } catch (final InvalidProtocolBufferException e) {
            throw new UncheckedIOException("Request parsing failed", e);
        }
    }
}
