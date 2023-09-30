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

import com.google.protobuf.Message;
import io.netty.handler.codec.http2.Http2SecurityUtil;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.netty.NettyEventServerFactory;
import org.apache.nifi.event.transport.netty.channel.LogExceptionChannelHandler;
import org.apache.nifi.logging.ComponentLog;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

/**
 * OpenTelemetry HTTP Server Factory for OTLP 1.0.0
 */
public class HttpServerFactory extends NettyEventServerFactory {

    private static final String[] APPLICATION_PROTOCOLS = {ApplicationProtocolNames.HTTP_2, ApplicationProtocolNames.HTTP_1_1};

    private static final Set<String> SUPPORTED_CIPHER_SUITES = new LinkedHashSet<>(Http2SecurityUtil.CIPHERS);

    public HttpServerFactory(final ComponentLog log, final BlockingQueue<Message> messages, final InetAddress address, final int port, final SSLContext sslContext) {
        super(address, port, TransportProtocol.TCP);

        final SSLParameters sslParameters = getApplicationSslParameters(sslContext);
        setSslParameters(sslParameters);
        setSslContext(sslContext);

        final LogExceptionChannelHandler logExceptionChannelHandler = new LogExceptionChannelHandler(log);
        setHandlerSupplier(() -> Arrays.asList(
                new HttpProtocolNegotiationHandler(log, messages),
                logExceptionChannelHandler
        ));
    }

    private SSLParameters getApplicationSslParameters(final SSLContext sslContext) {
        final SSLParameters sslParameters = sslContext.getDefaultSSLParameters();
        sslParameters.setApplicationProtocols(APPLICATION_PROTOCOLS);

        final List<String> defaultCipherSuites = Arrays.asList(sslParameters.getCipherSuites());
        final String[] cipherSuites = SupportedCipherSuiteFilter.INSTANCE.filterCipherSuites(defaultCipherSuites, defaultCipherSuites, SUPPORTED_CIPHER_SUITES);
        sslParameters.setCipherSuites(cipherSuites);

        return sslParameters;
    }
}
