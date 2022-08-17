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

import static org.apache.nifi.minifi.bootstrap.configuration.ingestors.PullHttpChangeIngestor.PULL_HTTP_BASE_KEY;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.nifi.minifi.bootstrap.ConfigurationFileHolder;
import org.apache.nifi.minifi.bootstrap.configuration.ingestors.common.PullHttpChangeIngestorCommonTest;
import org.apache.nifi.minifi.commons.schema.ConfigSchema;
import org.apache.nifi.minifi.commons.schema.exception.SchemaLoaderException;
import org.apache.nifi.minifi.commons.schema.serialization.SchemaLoader;
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
    public void pullHttpChangeIngestorInit(Properties properties) throws IOException, SchemaLoaderException {
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
        properties.put(PULL_HTTP_BASE_KEY + ".override.core", "true");
        ConfigurationFileHolder configurationFileHolder = Mockito.mock(ConfigurationFileHolder.class);

        ConfigSchema configSchema =
            SchemaLoader.loadConfigSchemaFromYaml(PullHttpChangeIngestorSSLTest.class.getClassLoader().getResourceAsStream("config.yml"));
        StringWriter writer = new StringWriter();
        SchemaLoader.toYaml(configSchema, writer);
        when(configurationFileHolder.getConfigFileReference()).thenReturn(new AtomicReference<>(ByteBuffer.wrap(writer.toString().getBytes())));

        pullHttpChangeIngestor = new PullHttpChangeIngestor();

        pullHttpChangeIngestor.initialize(properties, configurationFileHolder, testNotifier);
        pullHttpChangeIngestor.setDifferentiator(mockDifferentiator);
    }
}
