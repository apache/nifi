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

package org.apache.nifi.minifi.bootstrap.configuration.ingestors;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.nifi.minifi.bootstrap.configuration.ingestors.AbstractPullChangeIngestor.DEFAULT_POLLING_PERIOD_MILLISECONDS;
import static org.apache.nifi.minifi.bootstrap.configuration.ingestors.PullHttpChangeIngestor.CONNECT_TIMEOUT_KEY;
import static org.apache.nifi.minifi.bootstrap.configuration.ingestors.PullHttpChangeIngestor.DEFAULT_CONNECT_TIMEOUT_MS;
import static org.apache.nifi.minifi.bootstrap.configuration.ingestors.PullHttpChangeIngestor.DEFAULT_READ_TIMEOUT_MS;
import static org.apache.nifi.minifi.bootstrap.configuration.ingestors.PullHttpChangeIngestor.KEYSTORE_LOCATION_KEY;
import static org.apache.nifi.minifi.bootstrap.configuration.ingestors.PullHttpChangeIngestor.PULL_HTTP_BASE_KEY;
import static org.apache.nifi.minifi.bootstrap.configuration.ingestors.PullHttpChangeIngestor.PULL_HTTP_POLLING_PERIOD_KEY;
import static org.apache.nifi.minifi.bootstrap.configuration.ingestors.PullHttpChangeIngestor.QUERY_KEY;
import static org.apache.nifi.minifi.bootstrap.configuration.ingestors.PullHttpChangeIngestor.READ_TIMEOUT_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.nifi.minifi.bootstrap.ConfigurationFileHolder;
import org.apache.nifi.minifi.properties.BootstrapProperties;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.jupiter.api.BeforeAll;
import org.mockito.Mockito;

public class PullHttpChangeIngestorSSLTest extends PullHttpChangeIngestorCommonTest {

    @BeforeAll
    public static void setUp() throws Exception {
        PullHttpChangeIngestorCommonTest.init();

        SslContextFactory.Server ssl = new SslContextFactory.Server();

        ssl.setKeyStorePath("./src/test/resources/localhost-ks.jks");
        ssl.setKeyStorePassword("localtest");
        ssl.setKeyStoreType("JKS");
        ssl.setTrustStorePath("./src/test/resources/localhost-ts.jks");
        ssl.setTrustStorePassword("localtest");
        ssl.setTrustStoreType("JKS");
        ssl.setNeedClientAuth(true);

        // build the connector
        final ServerConnector https = new ServerConnector(jetty, ssl);

        // set host and port
        https.setPort(0);
        https.setHost("localhost");

        // Severely taxed environments may have significant delays when executing.
        https.setIdleTimeout(30000L);

        // add the connector
        jetty.addConnector(https);

        jetty.start();

        Thread.sleep(1000);

        if (!jetty.isStarted()) {
            throw new IllegalStateException("Jetty server not started");
        }
    }

    @Override
    public void pullHttpChangeIngestorInit(BootstrapProperties properties) throws IOException {
        when(properties.getProperty(PullHttpChangeIngestor.TRUSTSTORE_LOCATION_KEY)).thenReturn("./src/test/resources/localhost-ts.jks");
        when(properties.getProperty(PullHttpChangeIngestor.TRUSTSTORE_PASSWORD_KEY)).thenReturn("localtest");
        when(properties.getProperty(PullHttpChangeIngestor.TRUSTSTORE_TYPE_KEY)).thenReturn("JKS");
        when(properties.getProperty(KEYSTORE_LOCATION_KEY)).thenReturn("./src/test/resources/localhost-ks.jks");
        when(properties.getProperty(PullHttpChangeIngestor.KEYSTORE_PASSWORD_KEY)).thenReturn("localtest");
        when(properties.getProperty(PullHttpChangeIngestor.KEYSTORE_TYPE_KEY)).thenReturn("JKS");
        when(properties.getProperty(eq(PULL_HTTP_POLLING_PERIOD_KEY), any())).thenReturn(DEFAULT_POLLING_PERIOD_MILLISECONDS);
        when(properties.getProperty(eq(READ_TIMEOUT_KEY), any())).thenReturn(DEFAULT_READ_TIMEOUT_MS);
        when(properties.getProperty(eq(CONNECT_TIMEOUT_KEY), any())).thenReturn(DEFAULT_CONNECT_TIMEOUT_MS);
        when(properties.getProperty(eq(QUERY_KEY), any())).thenReturn(EMPTY);
        port = ((ServerConnector) jetty.getConnectors()[0]).getLocalPort();
        when(properties.getProperty(PullHttpChangeIngestor.PORT_KEY)).thenReturn(String.valueOf(port));
        when(properties.getProperty(PullHttpChangeIngestor.HOST_KEY)).thenReturn("localhost");
        when(properties.getProperty(PullHttpChangeIngestor.OVERRIDE_SECURITY)).thenReturn("true");
        when(properties.getProperty(PULL_HTTP_BASE_KEY + ".override.core")).thenReturn("true");
        when(properties.containsKey(KEYSTORE_LOCATION_KEY)).thenReturn(true);
        ConfigurationFileHolder configurationFileHolder = Mockito.mock(ConfigurationFileHolder.class);

        pullHttpChangeIngestor = new PullHttpChangeIngestor();

        pullHttpChangeIngestor.initialize(properties, configurationFileHolder, testNotifier);
        pullHttpChangeIngestor.setDifferentiator(mockDifferentiator);
    }
}
