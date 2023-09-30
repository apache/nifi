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

import com.google.protobuf.Message;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsPartialSuccess;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsPartialSuccess;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.ExportTracePartialSuccess;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import org.apache.nifi.processors.opentelemetry.protocol.ServiceRequestDescription;
import org.apache.nifi.processors.opentelemetry.protocol.ServiceResponse;
import org.apache.nifi.processors.opentelemetry.protocol.ServiceResponseStatus;
import org.apache.nifi.processors.opentelemetry.protocol.TelemetryContentType;
import org.apache.nifi.processors.opentelemetry.protocol.TelemetryRequestType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

/**
 * Standard implementation of Response Body Writer support Partial Success handling for rejected records
 */
public class StandardResponseBodyWriter implements ResponseBodyWriter {
    private static final String CAPACITY_ERROR_MESSAGE = "Queue capacity reached";

    private static final byte[] EMPTY_PROTOBUF_BODY = new byte[0];

    private static final byte[] EMPTY_JSON_OBJECT_BODY = new byte[]{123, 125};

    private static final RequestMapper REQUEST_MAPPER = new StandardRequestMapper();

    @Override
    public byte[] getResponseBody(final ServiceRequestDescription serviceRequestDescription, final ServiceResponse serviceResponse) {
        Objects.requireNonNull(serviceRequestDescription, "Request Description required");
        Objects.requireNonNull(serviceResponse, "Response required");

        final byte[] responseBody;

        final ServiceResponseStatus serviceResponseStatus = serviceResponse.getServiceResponseStatus();
        if (ServiceResponseStatus.PARTIAL_SUCCESS == serviceResponseStatus) {
            responseBody = getPartialSuccessResponseBody(serviceRequestDescription, serviceResponse);
        } else {
            final TelemetryContentType contentType = serviceRequestDescription.getContentType();
            if (TelemetryContentType.APPLICATION_JSON == contentType) {
                responseBody = EMPTY_JSON_OBJECT_BODY;
            } else {
                responseBody = EMPTY_PROTOBUF_BODY;
            }
        }

        return responseBody;
    }

    private byte[] getPartialSuccessResponseBody(final ServiceRequestDescription serviceRequestDescription, final ServiceResponse serviceResponse) {
        final Message message;

        final int rejected = serviceResponse.getRejected();
        final TelemetryRequestType requestType = serviceRequestDescription.getRequestType();
        if (TelemetryRequestType.LOGS == requestType) {
            final ExportLogsPartialSuccess partialSuccess = ExportLogsPartialSuccess.newBuilder()
                    .setRejectedLogRecords(rejected)
                    .setErrorMessage(CAPACITY_ERROR_MESSAGE)
                    .build();
            message = ExportLogsServiceResponse.newBuilder()
                    .setPartialSuccess(partialSuccess)
                    .build();
        } else if (TelemetryRequestType.METRICS == requestType) {
            final ExportMetricsPartialSuccess partialSuccess = ExportMetricsPartialSuccess.newBuilder()
                    .setRejectedDataPoints(rejected)
                    .setErrorMessage(CAPACITY_ERROR_MESSAGE)
                    .build();
            message = ExportMetricsServiceResponse.newBuilder()
                    .setPartialSuccess(partialSuccess)
                    .build();
        } else if (TelemetryRequestType.TRACES == requestType) {
            final ExportTracePartialSuccess partialSuccess = ExportTracePartialSuccess.newBuilder()
                    .setRejectedSpans(rejected)
                    .setErrorMessage(CAPACITY_ERROR_MESSAGE)
                    .build();
            message = ExportTraceServiceResponse.newBuilder()
                    .setPartialSuccess(partialSuccess)
                    .build();
        } else {
            throw new IllegalArgumentException(String.format("Service Request Type [%s] not supported", requestType));
        }

        final TelemetryContentType contentType = serviceRequestDescription.getContentType();
        final byte[] responseBody;
        if (TelemetryContentType.APPLICATION_JSON == contentType) {
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            try {
                REQUEST_MAPPER.writeValue(outputStream, message);
                responseBody = outputStream.toByteArray();
            } catch (final IOException e) {
                final String error = String.format("JSON Response Type [%s] serialization failed", message.getClass().getName());
                throw new UncheckedIOException(error, e);
            }
        } else {
            responseBody = message.toByteArray();
        }

        return responseBody;
    }
}
