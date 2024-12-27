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
package org.apache.nifi.web.server;

import org.apache.nifi.jetty.configuration.connector.ApplicationLayerProtocol;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.server.handler.HeaderWriterHandler;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.net.ssl.SSLContext;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class StandardServerProviderTest {

    private static final String RANDOM_PORT = "0";

    private static final String SSL_PROTOCOL = "ssl";

    @Test
    void testGetServer() {
        final Properties applicationProperties = new Properties();
        applicationProperties.setProperty(NiFiProperties.WEB_HTTP_PORT, RANDOM_PORT);
        final NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(null, applicationProperties);

        final StandardServerProvider provider = new StandardServerProvider(null);

        final Server server = provider.getServer(properties);

        assertStandardConfigurationFound(server);
        assertHttpConnectorFound(server);
    }

    @Test
    void testGetServerHttps() throws NoSuchAlgorithmException {
        final Properties applicationProperties = new Properties();
        applicationProperties.setProperty(NiFiProperties.WEB_HTTPS_PORT, RANDOM_PORT);
        final NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(null, applicationProperties);

        final SSLContext sslContext = SSLContext.getDefault();
        final StandardServerProvider provider = new StandardServerProvider(sslContext);

        final Server server = provider.getServer(properties);

        assertStandardConfigurationFound(server);
        assertHttpsConnectorFound(server);
    }

    @Timeout(10)
    @Test
    void testGetServerStart() throws Exception {
        final Properties applicationProperties = new Properties();
        applicationProperties.setProperty(NiFiProperties.WEB_HTTP_PORT, RANDOM_PORT);
        final NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(null, applicationProperties);

        final StandardServerProvider provider = new StandardServerProvider(null);

        final Server server = provider.getServer(properties);

        assertStandardConfigurationFound(server);
        assertHttpConnectorFound(server);

        try {
            server.start();

            assertFalse(server.isFailed());
        } finally {
            server.stop();
        }
    }

    void assertHttpConnectorFound(final Server server) {
        final Connector[] connectors = server.getConnectors();
        assertNotNull(connectors);
        final Connector connector = connectors[0];
        final List<String> protocols = connector.getProtocols();
        assertEquals(ApplicationLayerProtocol.HTTP_1_1.getProtocol(), protocols.getFirst());
    }

    void assertHttpsConnectorFound(final Server server) {
        final Connector[] connectors = server.getConnectors();
        assertNotNull(connectors);
        final Connector connector = connectors[0];
        final List<String> protocols = connector.getProtocols();
        assertEquals(SSL_PROTOCOL, protocols.getFirst());
    }

    void assertStandardConfigurationFound(final Server server) {
        assertNotNull(server);
        assertHandlersFound(server);

        final RequestLog requestLog = server.getRequestLog();
        assertNotNull(requestLog);
    }

    void assertHandlersFound(final Server server) {
        final Handler serverHandler = server.getHandler();
        assertInstanceOf(Handler.Collection.class, serverHandler);

        Handler defaultHandler = server.getDefaultHandler();
        assertInstanceOf(RewriteHandler.class, defaultHandler);

        final Handler.Collection handlerCollection = (Handler.Collection) serverHandler;
        final HeaderWriterHandler headerWriterHandler = handlerCollection.getDescendant(HeaderWriterHandler.class);
        assertNotNull(headerWriterHandler);
    }
}
