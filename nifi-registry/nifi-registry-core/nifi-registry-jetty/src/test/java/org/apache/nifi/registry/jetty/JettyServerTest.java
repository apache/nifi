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
package org.apache.nifi.registry.jetty;

import org.apache.nifi.registry.jetty.handler.HandlerProvider;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class JettyServerTest {
    private static final String RANDOM_PORT = "0";

    private static final String LOCALHOST = "127.0.0.1";

    private static final int TIMEOUT = 5000;

    @Mock
    private HandlerProvider handlerProvider;

    @Test
    void testStartStop() throws Exception {
        final Map<String, String> requiredProperties = new LinkedHashMap<>();
        requiredProperties.put(NiFiRegistryProperties.WEB_HTTP_PORT, RANDOM_PORT);
        requiredProperties.put(NiFiRegistryProperties.WEB_HTTP_HOST, LOCALHOST);
        final NiFiRegistryProperties properties = new NiFiRegistryProperties(requiredProperties);

        final JettyServer server = new JettyServer(properties, handlerProvider);

        try {
            server.start();

            final Iterator<URI> applicationUrls = server.getApplicationUrls().iterator();
            assertTrue(applicationUrls.hasNext());
            assertServerRunning(applicationUrls.next());
        } finally {
            server.stop();
        }
    }

    private void assertServerRunning(final URI applicationUrl) throws Exception {
        final URL url = applicationUrl.toURL();
        final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(TIMEOUT);
        connection.setReadTimeout(TIMEOUT);
        connection.connect();

        final int responseCode = connection.getResponseCode();
        assertEquals(HttpURLConnection.HTTP_NOT_FOUND, responseCode);
    }
}
