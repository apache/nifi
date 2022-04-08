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
package org.apache.nifi.processors.grpc.ssl;

import io.netty.handler.codec.http2.Http2SecurityUtil;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.ssl.SSLContextService;

import javax.net.ssl.SSLContext;

/**
 * Provider for Netty SslContext from NiFi SSLContextService
 */
public class SslContextProvider {
    private static final boolean START_TLS = false;

    private static final String H2_PROTOCOL = "h2";

    public static SslContext getSslContext(final SSLContextService sslContextService, final boolean client) {
        final SSLContext sslContext = sslContextService.createContext();
        final TlsConfiguration tlsConfiguration = sslContextService.createTlsConfiguration();
        final ClientAuth clientAuth = StringUtils.isBlank(tlsConfiguration.getTruststorePath()) ? ClientAuth.NONE : ClientAuth.REQUIRE;

        final ApplicationProtocolConfig applicationProtocolConfig = new ApplicationProtocolConfig(
                ApplicationProtocolConfig.Protocol.ALPN,
                ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                H2_PROTOCOL
        );

        return new JdkSslContext(
                sslContext,
                client,
                Http2SecurityUtil.CIPHERS,
                SupportedCipherSuiteFilter.INSTANCE,
                applicationProtocolConfig,
                clientAuth,
                tlsConfiguration.getEnabledProtocols(),
                START_TLS
        );
    }
}
