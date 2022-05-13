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
package org.apache.nifi.jetty.configuration.connector.alpn;

import org.eclipse.jetty.alpn.server.ALPNServerConnection;
import org.eclipse.jetty.io.AbstractConnection;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.io.ssl.ALPNProcessor;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.NegotiatingServerConnectionFactory;

import javax.net.ssl.SSLEngine;
import java.util.List;

/**
 * ALPN Server Connection Factory with standard ALPN Processor implementation
 */
public class ALPNServerConnectionFactory extends NegotiatingServerConnectionFactory {
    private static final String ALPN_PROTOCOL = "alpn";

    private final ALPNProcessor.Server processor;

    public ALPNServerConnectionFactory() {
        super(ALPN_PROTOCOL);
        processor = new StandardALPNProcessor();
    }

    /**
     * Create new Server Connection and configure the connection using ALPN Processor
     *
     * @param connector Connector for the Connection
     * @param endPoint End Point for the Connection
     * @param sslEngine SSL Engine for the Connection
     * @param protocols Application Protocols
     * @param defaultProtocol Default Application Protocol
     * @return ALPN Server Connection
     */
    @Override
    protected AbstractConnection newServerConnection(
            final Connector connector,
            final EndPoint endPoint,
            final SSLEngine sslEngine,
            final List<String> protocols,
            final String defaultProtocol
    ) {
        final ALPNServerConnection connection = new ALPNServerConnection(connector, endPoint, sslEngine, protocols, defaultProtocol);
        processor.configure(sslEngine, connection);
        return connection;
    }
}
