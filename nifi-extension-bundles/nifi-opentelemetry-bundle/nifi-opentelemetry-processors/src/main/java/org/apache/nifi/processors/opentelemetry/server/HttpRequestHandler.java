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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.HttpConversionUtil;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.opentelemetry.encoding.ResponseBodyWriter;
import org.apache.nifi.processors.opentelemetry.encoding.StandardResponseBodyWriter;
import org.apache.nifi.processors.opentelemetry.io.RequestContentListener;
import org.apache.nifi.processors.opentelemetry.protocol.GrpcHeader;
import org.apache.nifi.processors.opentelemetry.protocol.GrpcStatusCode;
import org.apache.nifi.processors.opentelemetry.protocol.ServiceResponse;
import org.apache.nifi.processors.opentelemetry.protocol.ServiceResponseStatus;
import org.apache.nifi.processors.opentelemetry.protocol.ServiceRequestDescription;
import org.apache.nifi.processors.opentelemetry.protocol.StandardServiceRequestDescription;
import org.apache.nifi.processors.opentelemetry.protocol.TelemetryContentEncoding;
import org.apache.nifi.processors.opentelemetry.protocol.TelemetryContentType;
import org.apache.nifi.processors.opentelemetry.protocol.TelemetryRequestType;
import org.apache.nifi.util.StringUtils;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * HTTP Handler for OTLP Export Service Requests over gGRPC or encoded as JSON or Protobuf over HTTP
 */
public class HttpRequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    private static final TelemetryContentEncoding[] TELEMETRY_CONTENT_ENCODING_VALUES = TelemetryContentEncoding.values();
    private static final TelemetryRequestType[] TELEMETRY_REQUEST_TYPE_VALUES = TelemetryRequestType.values();
    private static final TelemetryContentType[] TELEMETRY_CONTENT_TYPE_VALUES = TelemetryContentType.values();

    private final ResponseBodyWriter responseBodyWriter = new StandardResponseBodyWriter();

    private final ComponentLog log;

    private final RequestContentListener requestContentListener;

    /**
     * HTTP Request Handler constructor with Component Log associated with Processor
     *
     * @param log Component Log
     * @param requestContentListener Service Request Listener
     */
    public HttpRequestHandler(final ComponentLog log, final RequestContentListener requestContentListener) {
        this.log = Objects.requireNonNull(log, "Component Log required");
        this.requestContentListener = Objects.requireNonNull(requestContentListener, "Listener required");
    }

    /**
     * Channel Read handles Full HTTP Request objects
     *
     * @param channelHandlerContext Channel Handler Context
     * @param httpRequest Full HTTP Request to be processed
     */
    @Override
    protected void channelRead0(final ChannelHandlerContext channelHandlerContext, final FullHttpRequest httpRequest) {
        if (HttpMethod.POST == httpRequest.method()) {
            handleHttpPostRequest(channelHandlerContext, httpRequest);
        } else {
            sendErrorResponse(channelHandlerContext, httpRequest, HttpResponseStatus.METHOD_NOT_ALLOWED);
        }
    }

    private void handleHttpPostRequest(final ChannelHandlerContext channelHandlerContext, final FullHttpRequest httpRequest) {
        final HttpHeaders headers = httpRequest.headers();
        final String requestContentType = headers.get(HttpHeaderNames.CONTENT_TYPE);
        final TelemetryContentType telemetryContentType = getTelemetryContentType(requestContentType);
        if (telemetryContentType == null) {
            sendErrorResponse(channelHandlerContext, httpRequest, HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE);
        } else {
            final String uri = httpRequest.uri();
            final TelemetryRequestType telemetryRequestType = getTelemetryRequestType(uri, telemetryContentType);
            if (telemetryRequestType == null) {
                sendErrorResponse(channelHandlerContext, httpRequest, HttpResponseStatus.NOT_FOUND);
            } else {
                handleHttpPostRequestTypeSupported(channelHandlerContext, httpRequest, telemetryRequestType, telemetryContentType);
            }
        }
    }

    private void handleHttpPostRequestTypeSupported(
            final ChannelHandlerContext channelHandlerContext,
            final FullHttpRequest httpRequest,
            final TelemetryRequestType telemetryRequestType,
            final TelemetryContentType telemetryContentType
    ) {
        final HttpHeaders headers = httpRequest.headers();
        final String requestContentEncoding = headers.get(HttpHeaderNames.CONTENT_ENCODING);
        final TelemetryContentEncoding telemetryContentEncoding = getTelemetryContentEncoding(requestContentEncoding);

        final InetSocketAddress remoteAddress = (InetSocketAddress) channelHandlerContext.channel().remoteAddress();
        final ServiceRequestDescription serviceRequestDescription = new StandardServiceRequestDescription(
                telemetryContentEncoding,
                telemetryContentType,
                telemetryRequestType,
                remoteAddress
        );

        final ByteBuf content = httpRequest.content();
        final int readableBytes = content.readableBytes();

        final TelemetryContentType contentType = serviceRequestDescription.getContentType();
        log.debug("HTTP Content Received: Client Address [{}] Content-Type [{}] Bytes [{}]", remoteAddress, contentType.getContentType(), readableBytes);

        final ByteBuffer contentBuffer = content.nioBuffer();
        final ServiceResponse serviceResponse = requestContentListener.onRequest(contentBuffer, serviceRequestDescription);

        sendResponse(channelHandlerContext, httpRequest, serviceRequestDescription, serviceResponse);
    }

    private TelemetryContentEncoding getTelemetryContentEncoding(final String requestContentEncoding) {
        TelemetryContentEncoding telemetryContentEncoding = TelemetryContentEncoding.NONE;

        final String contentEncoding = requestContentEncoding == null ? StringUtils.EMPTY : requestContentEncoding;
        for (final TelemetryContentEncoding currentEncoding : TELEMETRY_CONTENT_ENCODING_VALUES) {
            if (currentEncoding.getContentEncoding().contentEquals(contentEncoding)) {
                telemetryContentEncoding = currentEncoding;
                break;
            }
        }

        return telemetryContentEncoding;
    }

    private TelemetryRequestType getTelemetryRequestType(final String path, final TelemetryContentType telemetryContentType) {
        TelemetryRequestType telemetryRequestType = null;

        for (final TelemetryRequestType currentType : TELEMETRY_REQUEST_TYPE_VALUES) {
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

    private TelemetryContentType getTelemetryContentType(final String requestContentType) {
        TelemetryContentType telemetryContentType = null;

        for (final TelemetryContentType currentType : TELEMETRY_CONTENT_TYPE_VALUES) {
            if (currentType.getContentType().equals(requestContentType)) {
                telemetryContentType = currentType;
                break;
            }
        }

        return telemetryContentType;
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

    private void sendErrorResponse(final ChannelHandlerContext channelHandlerContext, final HttpRequest httpRequest, final HttpResponseStatus httpResponseStatus) {
        final SocketAddress remoteAddress = channelHandlerContext.channel().remoteAddress();
        final HttpMethod method = httpRequest.method();
        final String uri = httpRequest.uri();
        final HttpVersion httpVersion = httpRequest.protocolVersion();

        log.debug("HTTP Request Failed: Client Address [{}] Method [{}] URI [{}] Version [{}] HTTP {}", remoteAddress, method, uri, httpVersion, httpResponseStatus.code());

        final FullHttpResponse response = new DefaultFullHttpResponse(httpVersion, httpResponseStatus);
        setStreamId(httpRequest.headers(), response);

        channelHandlerContext.writeAndFlush(response);
    }

    private void sendResponse(
            final ChannelHandlerContext channelHandlerContext,
            final FullHttpRequest httpRequest,
            final ServiceRequestDescription serviceRequestDescription,
            final ServiceResponse serviceResponse
    ) {
        final SocketAddress remoteAddress = channelHandlerContext.channel().remoteAddress();
        final HttpMethod method = httpRequest.method();
        final String uri = httpRequest.uri();
        final HttpVersion httpVersion = httpRequest.protocolVersion();

        final ServiceResponseStatus serviceResponseStatus = serviceResponse.getServiceResponseStatus();
        final HttpResponseStatus httpResponseStatus = HttpResponseStatus.valueOf(serviceResponseStatus.getStatusCode());

        log.debug("HTTP Request Completed: Client Address [{}] Method [{}] URI [{}] Version [{}] HTTP {}", remoteAddress, method, uri, httpVersion, httpResponseStatus.code());

        final FullHttpResponse response = new DefaultFullHttpResponse(httpVersion, httpResponseStatus);
        setStreamId(httpRequest.headers(), response);

        final TelemetryContentType telemetryContentType = serviceRequestDescription.getContentType();
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, telemetryContentType.getContentType());

        if (TelemetryContentType.APPLICATION_GRPC == telemetryContentType) {
            final GrpcStatusCode grpcStatusCode = getGrpcStatusCode(serviceResponseStatus);
            response.headers().setInt(GrpcHeader.GRPC_STATUS.getHeader(), grpcStatusCode.getCode());
        }

        final byte[] responseBody = responseBodyWriter.getResponseBody(serviceRequestDescription, serviceResponse);
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, responseBody.length);
        response.content().writeBytes(responseBody);

        channelHandlerContext.writeAndFlush(response);
    }

    private void setStreamId(final HttpHeaders requestHeaders, final FullHttpResponse response) {
        final String streamId = requestHeaders.get(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text());
        if (streamId != null) {
            response.headers().set(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text(), streamId);
        }
    }
}
