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
import static org.apache.nifi.minifi.bootstrap.configuration.ingestors.PullHttpChangeIngestor.CONNECT_TIMEOUT_KEY;
import static org.apache.nifi.minifi.bootstrap.configuration.ingestors.PullHttpChangeIngestor.DEFAULT_CONNECT_TIMEOUT_MS;
import static org.apache.nifi.minifi.bootstrap.configuration.ingestors.PullHttpChangeIngestor.DEFAULT_READ_TIMEOUT_MS;
import static org.apache.nifi.minifi.bootstrap.configuration.ingestors.PullHttpChangeIngestor.PULL_HTTP_BASE_KEY;
import static org.apache.nifi.minifi.bootstrap.configuration.ingestors.PullHttpChangeIngestor.QUERY_KEY;
import static org.apache.nifi.minifi.bootstrap.configuration.ingestors.PullHttpChangeIngestor.READ_TIMEOUT_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.nifi.minifi.bootstrap.ConfigurationFileHolder;
import org.apache.nifi.minifi.properties.BootstrapProperties;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.jupiter.api.BeforeAll;
import org.mockito.Mockito;

public class PullHttpChangeIngestorTest extends PullHttpChangeIngestorCommonTest {

    @BeforeAll
    public static void setUp() throws Exception {
        PullHttpChangeIngestorCommonTest.init();

        final ServerConnector http = new ServerConnector(jetty);

        http.setPort(0);
        http.setHost("localhost");

        http.setIdleTimeout(3000L);
        jetty.addConnector(http);

        jetty.start();

        Thread.sleep(1000);

        if (!jetty.isStarted()) {
            throw new IllegalStateException("Jetty server not started");
        }
    }


    @Override
    public void pullHttpChangeIngestorInit(BootstrapProperties properties) throws IOException {
        port = ((ServerConnector) jetty.getConnectors()[0]).getLocalPort();
        when(properties.getProperty(PullHttpChangeIngestor.PORT_KEY)).thenReturn(String.valueOf(port));
        when(properties.getProperty(PullHttpChangeIngestor.HOST_KEY)).thenReturn("localhost");
        when(properties.getProperty(eq(PullHttpChangeIngestor.PULL_HTTP_POLLING_PERIOD_KEY), any())).thenReturn("30000");
        when(properties.getProperty(PULL_HTTP_BASE_KEY + ".override.core")).thenReturn("true");
        when(properties.getProperty(eq(READ_TIMEOUT_KEY), any())).thenReturn(DEFAULT_READ_TIMEOUT_MS);
        when(properties.getProperty(eq(CONNECT_TIMEOUT_KEY), any())).thenReturn(DEFAULT_CONNECT_TIMEOUT_MS);
        when(properties.getProperty(eq(QUERY_KEY), any())).thenReturn(EMPTY);
        ConfigurationFileHolder configurationFileHolder = Mockito.mock(ConfigurationFileHolder.class);

        pullHttpChangeIngestor = new PullHttpChangeIngestor();
        pullHttpChangeIngestor.initialize(properties, configurationFileHolder, testNotifier);
        pullHttpChangeIngestor.setDifferentiator(mockDifferentiator);
    }
}
