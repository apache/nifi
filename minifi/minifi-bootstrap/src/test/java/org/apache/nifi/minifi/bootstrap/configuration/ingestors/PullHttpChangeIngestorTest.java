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
    public void pullHttpChangeIngestorInit(Properties properties) throws IOException, SchemaLoaderException {
        port = ((ServerConnector) jetty.getConnectors()[0]).getLocalPort();
        properties.put(PullHttpChangeIngestor.PORT_KEY, String.valueOf(port));
        properties.put(PullHttpChangeIngestor.HOST_KEY, "localhost");
        properties.put(PullHttpChangeIngestor.PULL_HTTP_POLLING_PERIOD_KEY, "30000");
        properties.put(PULL_HTTP_BASE_KEY + ".override.core", "true");
        ConfigurationFileHolder configurationFileHolder = Mockito.mock(ConfigurationFileHolder.class);

        ConfigSchema configSchema =
            SchemaLoader.loadConfigSchemaFromYaml(PullHttpChangeIngestorTest.class.getClassLoader().getResourceAsStream("config.yml"));
        StringWriter writer = new StringWriter();
        SchemaLoader.toYaml(configSchema, writer);
        when(configurationFileHolder.getConfigFileReference()).thenReturn(new AtomicReference<>(ByteBuffer.wrap(writer.toString().getBytes())));

        pullHttpChangeIngestor = new PullHttpChangeIngestor();
        pullHttpChangeIngestor.initialize(properties, configurationFileHolder, testNotifier);
        pullHttpChangeIngestor.setDifferentiator(mockDifferentiator);
    }
}
