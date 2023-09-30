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

import io.netty.handler.codec.http2.AbstractHttp2ConnectionHandlerBuilder;
import io.netty.handler.codec.http2.Http2ConnectionDecoder;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.netty.handler.codec.http2.Http2Settings;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.opentelemetry.io.RequestContentListener;

import java.util.Objects;

/**
 * HTTP/2 Builder implementation handling connection configuration for OTLP Export Service Requests over HTTP
 */
public class Http2RequestHandlerBuilder extends AbstractHttp2ConnectionHandlerBuilder<Http2RequestHandler, Http2RequestHandlerBuilder> {
    private final ComponentLog log;

    private final RequestContentListener requestContentListener;

    public Http2RequestHandlerBuilder(final ComponentLog log, final RequestContentListener requestContentListener) {
        this.log = Objects.requireNonNull(log, "Component Log required");
        this.requestContentListener = Objects.requireNonNull(requestContentListener, "Listener required");
    }

    /**
     * Build HTTP/2 Export Request Handler
     *
     * @return HTTP/2 Export Request Handler
     */
    @Override
    public Http2RequestHandler build() {
        return super.build();
    }

    /**
     * Build HTTP/2 Export Request Handler based on current connection status
     *
     * @param http2ConnectionDecoder HTTP/2 Connection Decoder
     * @param http2ConnectionEncoder HTTP/2 Connection Encoder
     * @param http2Settings HTTP/2 Settings
     * @return HTTP/2 Export Request Handler
     */
    @Override
    protected Http2RequestHandler build(
            final Http2ConnectionDecoder http2ConnectionDecoder,
            final Http2ConnectionEncoder http2ConnectionEncoder,
            final Http2Settings http2Settings
    ) {
        final Http2RequestFrameListener frameListener = new Http2RequestFrameListener(log, http2ConnectionEncoder, requestContentListener);
        frameListener(frameListener);
        return new Http2RequestHandler(http2ConnectionDecoder, http2ConnectionEncoder, http2Settings);
    }
}
