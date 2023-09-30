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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.opentelemetry.encoding.ResponseBodyWriter;
import org.apache.nifi.processors.opentelemetry.encoding.StandardResponseBodyWriter;
import org.apache.nifi.processors.opentelemetry.io.RequestContentListener;
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
import java.util.concurrent.atomic.AtomicReference;

/**
 * HTTP Handler for OTLP Export Service Requests encoded as JSON or Protobuf over HTTP/1.1
 */
public class HttpRequestHandler extends SimpleChannelInboundHandler<HttpObject> {
    private final ResponseBodyWriter responseBodyWriter = new StandardResponseBodyWriter();

    private final AtomicReference<HttpRequest> httpRequestRef = new AtomicReference<>();

    private final AtomicReference<TelemetryContentEncoding> contentEncodingRef = new AtomicReference<>();

    private final AtomicReference<TelemetryContentType> contentTypeRef = new AtomicReference<>();

    private final AtomicReference<TelemetryRequestType> requestTypeRef = new AtomicReference<>();

    private final RequestContentBuffer requestContentBuffer = new RequestContentBuffer();

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
     * Channel Read handles HTTP Request and HTTP Content objects
     *
     * @param channelHandlerContext Channel Handler Context
     * @param httpObject HTTP object to be processed
     */
    @Override
    protected void channelRead0(final ChannelHandlerContext channelHandlerContext, final HttpObject httpObject) {
        if (httpObject instanceof HttpRequest) {
            final HttpRequest httpRequest;
            httpRequest = (HttpRequest) httpObject;
            handleHttpRequest(channelHandlerContext, httpRequest);
        } else if (httpObject instanceof HttpContent) {
            final HttpContent httpContent;
            httpContent = (HttpContent) httpObject;
            handleHttpContent(channelHandlerContext, httpContent);
        }
    }

    private void handleHttpRequest(final ChannelHandlerContext channelHandlerContext, final HttpRequest httpRequest) {
        httpRequestRef.getAndSet(httpRequest);

        if (HttpMethod.POST == httpRequest.method()) {
            final String uri = httpRequest.uri();
            final TelemetryRequestType telemetryRequestType = getTelemetryType(uri);
            if (telemetryRequestType == null) {
                sendCloseResponse(channelHandlerContext, httpRequest, HttpResponseStatus.NOT_FOUND);
            } else {
                requestTypeRef.getAndSet(telemetryRequestType);
                final HttpHeaders headers = httpRequest.headers();
                final String requestContentType = headers.get(HttpHeaderNames.CONTENT_TYPE);
                final TelemetryContentType telemetryContentType = getTelemetryContentType(requestContentType);
                if (telemetryContentType == null) {
                    sendCloseResponse(channelHandlerContext, httpRequest, HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE);
                } else {
                    final String requestContentEncoding = headers.get(HttpHeaderNames.CONTENT_ENCODING);
                    final TelemetryContentEncoding telemetryContentEncoding = getTelemetryContentEncoding(requestContentEncoding);
                    contentEncodingRef.getAndSet(telemetryContentEncoding);
                    contentTypeRef.getAndSet(telemetryContentType);
                }
            }
        } else {
            sendCloseResponse(channelHandlerContext, httpRequest, HttpResponseStatus.METHOD_NOT_ALLOWED);
        }
    }

    private void handleHttpContent(final ChannelHandlerContext channelHandlerContext, final HttpContent httpContent) {
        final InetSocketAddress remoteAddress = (InetSocketAddress) channelHandlerContext.channel().remoteAddress();

        final ByteBuf content = httpContent.content();
        final int readableBytes = content.readableBytes();

        final TelemetryContentType telemetryContentType = contentTypeRef.get();
        if (telemetryContentType == null) {
            log.debug("HTTP Content Unsupported: Client Address [{}] Bytes [{}]", remoteAddress, readableBytes);
        } else {
            log.debug("HTTP Content Received: Client Address [{}] Content-Type [{}] Bytes [{}]", remoteAddress, telemetryContentType.getContentType(), readableBytes);
            requestContentBuffer.updateBuffer(channelHandlerContext, content);

            if (httpContent instanceof LastHttpContent) {
                final ServiceRequestDescription serviceRequestDescription = new StandardServiceRequestDescription(
                        contentEncodingRef.get(),
                        telemetryContentType,
                        requestTypeRef.get(),
                        remoteAddress
                );
                final ByteBuffer contentBuffer = requestContentBuffer.getBuffer().nioBuffer();
                final ServiceResponse serviceResponse = requestContentListener.onRequest(contentBuffer, serviceRequestDescription);
                requestContentBuffer.clearBuffer();

                sendResponse(channelHandlerContext, serviceRequestDescription, serviceResponse);
            }
        }
    }

    private TelemetryContentEncoding getTelemetryContentEncoding(final String requestContentEncoding) {
        TelemetryContentEncoding telemetryContentEncoding = TelemetryContentEncoding.NONE;

        final String contentEncoding = requestContentEncoding == null ? StringUtils.EMPTY : requestContentEncoding;
        for (final TelemetryContentEncoding currentEncoding : TelemetryContentEncoding.values()) {
            if (currentEncoding.getContentEncoding().contentEquals(contentEncoding)) {
                telemetryContentEncoding = currentEncoding;
                break;
            }
        }

        return telemetryContentEncoding;
    }

    private TelemetryRequestType getTelemetryType(final String path) {
        TelemetryRequestType telemetryRequestType = null;

        for (final TelemetryRequestType currentType :  TelemetryRequestType.values()) {
            if (currentType.getPath().equals(path)) {
                telemetryRequestType = currentType;
                break;
            }
        }

        return telemetryRequestType;
    }

    private TelemetryContentType getTelemetryContentType(final String requestContentType) {
        TelemetryContentType telemetryContentType = null;

        for (final TelemetryContentType currentType : TelemetryContentType.values()) {
            if (currentType.getContentType().equals(requestContentType)) {
                telemetryContentType = currentType;
                break;
            }
        }

        return telemetryContentType;
    }

    private void sendCloseResponse(final ChannelHandlerContext channelHandlerContext, final HttpRequest httpRequest, final HttpResponseStatus httpResponseStatus) {
        final SocketAddress remoteAddress = channelHandlerContext.channel().remoteAddress();
        final HttpMethod method = httpRequest.method();
        final String uri = httpRequest.uri();
        final HttpVersion httpVersion = httpRequest.protocolVersion();

        log.debug("HTTP Request Closed: Client Address [{}] Method [{}] URI [{}] Version [{}] HTTP {}", remoteAddress, method, uri, httpVersion, httpResponseStatus.code());

        final FullHttpResponse response = new DefaultFullHttpResponse(httpVersion, httpResponseStatus);
        response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        final ChannelFuture future = channelHandlerContext.writeAndFlush(response);
        future.addListener(ChannelFutureListener.CLOSE);
    }

    private void sendResponse(final ChannelHandlerContext channelHandlerContext, final ServiceRequestDescription serviceRequestDescription, final ServiceResponse serviceResponse) {
        final SocketAddress remoteAddress = channelHandlerContext.channel().remoteAddress();
        final HttpRequest httpRequest = httpRequestRef.get();
        final HttpMethod method = httpRequest.method();
        final String uri = httpRequest.uri();
        final HttpVersion httpVersion = httpRequest.protocolVersion();

        final ServiceResponseStatus serviceResponseStatus = serviceResponse.getServiceResponseStatus();
        final HttpResponseStatus httpResponseStatus = HttpResponseStatus.valueOf(serviceResponseStatus.getStatusCode());

        log.debug("HTTP Request Completed: Client Address [{}] Method [{}] URI [{}] Version [{}] HTTP {}", remoteAddress, method, uri, httpVersion, httpResponseStatus.code());

        final FullHttpResponse response = new DefaultFullHttpResponse(httpVersion, httpResponseStatus);
        final TelemetryContentType telemetryContentType = serviceRequestDescription.getContentType();
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, telemetryContentType.getContentType());

        final byte[] responseBody = responseBodyWriter.getResponseBody(serviceRequestDescription, serviceResponse);
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, responseBody.length);
        response.content().writeBytes(responseBody);

        channelHandlerContext.writeAndFlush(response);
    }
}
