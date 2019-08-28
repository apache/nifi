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

import org.apache.nifi.minifi.bootstrap.ConfigurationFileHolder;
import org.apache.nifi.minifi.bootstrap.configuration.ingestors.common.PullHttpChangeIngestorCommonTest;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.BeforeClass;
import org.mockito.Mockito;

import java.util.Properties;

public class PullHttpChangeIngestorSSLTest extends PullHttpChangeIngestorCommonTest {

    @BeforeClass
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
    public void pullHttpChangeIngestorInit(Properties properties) {
        properties.setProperty(PullHttpChangeIngestor.TRUSTSTORE_LOCATION_KEY, "./src/test/resources/localhost-ts.jks");
        properties.setProperty(PullHttpChangeIngestor.TRUSTSTORE_PASSWORD_KEY, "localtest");
        properties.setProperty(PullHttpChangeIngestor.TRUSTSTORE_TYPE_KEY, "JKS");
        properties.setProperty(PullHttpChangeIngestor.KEYSTORE_LOCATION_KEY, "./src/test/resources/localhost-ks.jks");
        properties.setProperty(PullHttpChangeIngestor.KEYSTORE_PASSWORD_KEY, "localtest");
        properties.setProperty(PullHttpChangeIngestor.KEYSTORE_TYPE_KEY, "JKS");
        port = ((ServerConnector) jetty.getConnectors()[0]).getLocalPort();
        properties.put(PullHttpChangeIngestor.PORT_KEY, String.valueOf(port));
        properties.put(PullHttpChangeIngestor.HOST_KEY, "localhost");
        properties.put(PullHttpChangeIngestor.OVERRIDE_SECURITY, "true");

        pullHttpChangeIngestor = new PullHttpChangeIngestor();

        pullHttpChangeIngestor.initialize(properties, Mockito.mock(ConfigurationFileHolder.class), testNotifier);
        pullHttpChangeIngestor.setDifferentiator(mockDifferentiator);
    }
}
