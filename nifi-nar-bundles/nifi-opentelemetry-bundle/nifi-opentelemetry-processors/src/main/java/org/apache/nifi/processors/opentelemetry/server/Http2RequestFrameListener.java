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
package org.apache.nifi.processors.opentelemetry.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.netty.handler.codec.http2.Http2FrameAdapter;
import io.netty.handler.codec.http2.Http2Headers;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.opentelemetry.encoding.ResponseBodyWriter;
import org.apache.nifi.processors.opentelemetry.encoding.StandardResponseBodyWriter;
import org.apache.nifi.processors.opentelemetry.io.RequestContentListener;
import org.apache.nifi.processors.opentelemetry.protocol.ServiceResponse;
import org.apache.nifi.processors.opentelemetry.protocol.ServiceResponseStatus;
import org.apache.nifi.processors.opentelemetry.protocol.GrpcHeader;
import org.apache.nifi.processors.opentelemetry.protocol.GrpcStatusCode;
import org.apache.nifi.processors.opentelemetry.protocol.ServiceRequestDescription;
import org.apache.nifi.processors.opentelemetry.protocol.StandardServiceRequestDescription;
import org.apache.nifi.processors.opentelemetry.protocol.TelemetryContentEncoding;
import org.apache.nifi.processors.opentelemetry.protocol.TelemetryContentType;
import org.apache.nifi.processors.opentelemetry.protocol.TelemetryRequestType;
import org.apache.nifi.util.StringUtils;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * HTTP/2 Export Service Request Frame Listener handles Header and Data Frames for OTLP over HTTP
 */
public class Http2RequestFrameListener extends Http2FrameAdapter {

    private static final int ZERO_PADDING = 0;

    private static final boolean END_STREAM_DISABLED = false;

    private static final boolean END_STREAM_ENABLED = true;

    private static final String OCTET_STREAM = "application/octet-stream";

    private final ResponseBodyWriter responseBodyWriter = new StandardResponseBodyWriter();

    private final AtomicReference<Http2Headers> httpHeadersRef = new AtomicReference<>();

    private final AtomicReference<TelemetryContentEncoding> contentEncodingRef = new AtomicReference<>();

    private final AtomicReference<TelemetryContentType> contentTypeRef = new AtomicReference<>();

    private final AtomicReference<TelemetryRequestType> requestTypeRef = new AtomicReference<>();

    private final RequestContentBuffer requestContentBuffer = new RequestContentBuffer();

    private final ComponentLog log;

    private final Http2ConnectionEncoder encoder;

    private final RequestContentListener requestContentListener;

    public Http2RequestFrameListener(final ComponentLog log, final Http2ConnectionEncoder encoder, final RequestContentListener requestContentListener) {
        this.log = Objects.requireNonNull(log, "Component Log required");
        this.encoder = Objects.requireNonNull(encoder, "Encoder required");
        this.requestContentListener = Objects.requireNonNull(requestContentListener, "Request Content Listener required");
    }

    /**
     * On Headers Read handles HTTP/2 Headers and validates supported HTTP Method and Content-Type
     *
     * @param context Channel Handler Context
     * @param streamId HTTP Stream Identifier
     * @param headers HTTP/2 Headers
     * @param padding Length of frame padding
     * @param endOfStream End of Stream indicator
     */
    @Override
    public void onHeadersRead(
            final ChannelHandlerContext context,
            final int streamId,
            final Http2Headers headers,
            final int padding,
            final boolean endOfStream
    ) {
        final CharSequence method = headers.method();
        if (HttpMethod.POST.asciiName().contentEquals(method)) {
            final CharSequence requestContentType = headers.get(HttpHeaderNames.CONTENT_TYPE, OCTET_STREAM);
            final TelemetryContentType telemetryContentType = getTelemetryContentType(requestContentType);
            if (telemetryContentType == null) {
                sendCloseResponse(context, streamId, HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE);
            } else {
                final CharSequence path = headers.path();
                final TelemetryRequestType telemetryRequestType = getTelemetryType(path, telemetryContentType);
                if (telemetryRequestType == null) {
                    sendCloseResponse(context, streamId, HttpResponseStatus.NOT_FOUND);
                } else {
                    final CharSequence requestContentEncoding = headers.get(HttpHeaderNames.CONTENT_ENCODING);
                    final TelemetryContentEncoding telemetryContentEncoding = getTelemetryContentEncoding(requestContentEncoding);
                    contentEncodingRef.getAndSet(telemetryContentEncoding);

                    contentTypeRef.getAndSet(telemetryContentType);
                    requestTypeRef.getAndSet(telemetryRequestType);
                    httpHeadersRef.getAndSet(headers);
                }
            }
        } else {
            sendCloseResponse(context, streamId, HttpResponseStatus.METHOD_NOT_ALLOWED);
        }
    }

    /**
     * On Headers Read with weight and exclusive parameters delegates to common Headers handler method
     *
     * @param context Channel Handler Context
     * @param streamId HTTP Stream Identifier
     * @param headers HTTP/2 Headers
     * @param streamDependency Stream Dependency identifier
     * @param weight Weight indicator
     * @param exclusive Exclusive indicator
     * @param padding Length of frame padding
     * @param endOfStream End of Stream indicator
     */
    @Override
    public void onHeadersRead(
            final ChannelHandlerContext context,
            final int streamId,
            final Http2Headers headers,
            final int streamDependency,
            final short weight,
            final boolean exclusive,
            final int padding,
            final boolean endOfStream
    ) {
        onHeadersRead(context, streamId, headers, padding, endOfStream);
    }

    /**
     * On Data Read handles HTTP/2 Data Frames and sends responses when after receiving the end of stream status
     *
     * @param context Channel Handler Context
     * @param streamId HTTP/2 Stream Identifier
     * @param data Data frame buffer
     * @param padding Length of frame padding
     * @param endOfStream End of Stream indicator
     * @return Length of bytes processed
     */
    @Override
    public int onDataRead(final ChannelHandlerContext context, final int streamId, final ByteBuf data, final int padding, final boolean endOfStream) {
        requestContentBuffer.updateBuffer(context, data);
        final int processed = data.readableBytes() + padding;

        if (endOfStream) {
            final TelemetryContentEncoding contentEncoding = contentEncodingRef.get();
            final TelemetryContentType contentType = contentTypeRef.get();
            final TelemetryRequestType requestType = requestTypeRef.get();

            final InetSocketAddress remoteAddress = (InetSocketAddress) context.channel().remoteAddress();
            final ServiceRequestDescription serviceRequestDescription = new StandardServiceRequestDescription(contentEncoding, contentType, requestType, remoteAddress);
            final ByteBuf requestContent = requestContentBuffer.getBuffer();
            final ServiceResponse serviceResponse = requestContentListener.onRequest(requestContent.nioBuffer(), serviceRequestDescription);
            requestContentBuffer.clearBuffer();

            final ServiceResponseStatus serviceResponseStatus = serviceResponse.getServiceResponseStatus();
            final HttpResponseStatus responseStatus = HttpResponseStatus.valueOf(serviceResponseStatus.getStatusCode());
            final Http2Headers headers = new DefaultHttp2Headers().status(responseStatus.codeAsText());
            headers.set(HttpHeaderNames.CONTENT_TYPE, contentType.getContentType());

            final byte[] responseBody = responseBodyWriter.getResponseBody(serviceRequestDescription, serviceResponse);
            final ByteBuf responseBodyBuffer = Unpooled.wrappedBuffer(responseBody);
            headers.setInt(HttpHeaderNames.CONTENT_LENGTH, responseBody.length);

            final String uri;
            if (TelemetryContentType.APPLICATION_GRPC == contentType) {
                uri = requestType.getGrpcPath();

                final GrpcStatusCode grpcStatusCode = getGrpcStatusCode(serviceResponseStatus);
                headers.setInt(GrpcHeader.GRPC_STATUS.getHeader(), grpcStatusCode.getCode());
                writeResponseHeaders(context, streamId, headers);
                encoder.writeData(context, streamId, responseBodyBuffer, ZERO_PADDING, END_STREAM_ENABLED, context.newPromise());
            } else if (TelemetryContentType.APPLICATION_PROTOBUF == contentType) {
                uri = requestType.getPath();
                writeResponseHeaders(context, streamId, headers);
                encoder.writeData(context, streamId, responseBodyBuffer, ZERO_PADDING, END_STREAM_ENABLED, context.newPromise());
            } else {
                uri = requestType.getPath();
                writeResponseHeaders(context, streamId, headers);
                encoder.writeData(context, streamId, responseBodyBuffer, ZERO_PADDING, END_STREAM_ENABLED, context.newPromise());
            }

            log.debug("Client Address [{}] Method [POST] URI [{}] Version [2] Content-Type [{}] HTTP {}", remoteAddress, uri, contentType.getContentType(), responseStatus.code());
        }

        return processed;
    }

    private GrpcStatusCode getGrpcStatusCode(final ServiceResponseStatus serviceResponseStatus) {
        final GrpcStatusCode grpcStatusCode;

        if (ServiceResponseStatus.SUCCESS == serviceResponseStatus) {
            grpcStatusCode = GrpcStatusCode.OK;
        } else if (ServiceResponseStatus.INVALID == serviceResponseStatus) {
            grpcStatusCode = GrpcStatusCode.INVALID_ARGUMENT;
        } else if (ServiceResponseStatus.PARTIAL_SUCCESS == serviceResponseStatus) {
            grpcStatusCode = GrpcStatusCode.OK;
        } else if (ServiceResponseStatus.UNAVAILABLE == serviceResponseStatus) {
            grpcStatusCode = GrpcStatusCode.UNAVAILABLE;
        } else {
            grpcStatusCode = GrpcStatusCode.UNKNOWN;
        }

        return grpcStatusCode;
    }

    private void writeResponseHeaders(final ChannelHandlerContext context, final int streamId, final Http2Headers headers) {
        encoder.writeHeaders(context, streamId, headers, ZERO_PADDING, END_STREAM_DISABLED, context.newPromise());
    }

    private TelemetryContentEncoding getTelemetryContentEncoding(final CharSequence requestContentEncoding) {
        TelemetryContentEncoding telemetryContentEncoding = TelemetryContentEncoding.NONE;

        final String contentEncoding = requestContentEncoding == null ? StringUtils.EMPTY : requestContentEncoding.toString();
        for (final TelemetryContentEncoding currentEncoding : TelemetryContentEncoding.values()) {
            if (currentEncoding.getContentEncoding().contentEquals(contentEncoding)) {
                telemetryContentEncoding = currentEncoding;
                break;
            }
        }

        return telemetryContentEncoding;
    }

    private TelemetryContentType getTelemetryContentType(final CharSequence requestContentType) {
        TelemetryContentType telemetryContentType = null;

        for (final TelemetryContentType currentType : TelemetryContentType.values()) {
            if (currentType.getContentType().contentEquals(requestContentType)) {
                telemetryContentType = currentType;
                break;
            }
        }

        return telemetryContentType;
    }

    private TelemetryRequestType getTelemetryType(final CharSequence path, final TelemetryContentType telemetryContentType) {
        TelemetryRequestType telemetryRequestType = null;

        for (final TelemetryRequestType currentType :  TelemetryRequestType.values()) {
            final String requestTypePath;
            if (TelemetryContentType.APPLICATION_GRPC == telemetryContentType) {
                requestTypePath = currentType.getGrpcPath();
            } else {
                requestTypePath = currentType.getPath();
            }

            if (requestTypePath.contentEquals(path)) {
                telemetryRequestType = currentType;
                break;
            }
        }

        return telemetryRequestType;
    }

    private void sendCloseResponse(final ChannelHandlerContext context, final int streamId, final HttpResponseStatus httpResponseStatus) {
        final Http2Headers headers = new DefaultHttp2Headers().status(httpResponseStatus.codeAsText());
        headers.set(HttpHeaderNames.CONTENT_TYPE, TelemetryContentType.APPLICATION_JSON.getContentType());
        encoder.writeHeaders(context, streamId, headers, ZERO_PADDING, END_STREAM_ENABLED, context.newPromise());
    }
}
