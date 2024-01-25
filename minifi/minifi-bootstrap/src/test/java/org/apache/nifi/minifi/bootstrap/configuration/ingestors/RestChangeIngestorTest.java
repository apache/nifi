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
import static org.apache.nifi.minifi.bootstrap.configuration.ingestors.RestChangeIngestor.HOST_KEY;
import static org.apache.nifi.minifi.bootstrap.configuration.ingestors.RestChangeIngestor.PORT_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;
import okhttp3.OkHttpClient;
import org.apache.nifi.minifi.bootstrap.ConfigurationFileHolder;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeNotifier;
import org.apache.nifi.minifi.properties.BootstrapProperties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.mockito.Mockito;

public class RestChangeIngestorTest extends RestChangeIngestorCommonTest {

    @BeforeAll
    public static void setUp() throws InterruptedException, MalformedURLException {
        BootstrapProperties properties = mock(BootstrapProperties.class);
        when(properties.getProperty(PullHttpChangeIngestor.OVERRIDE_SECURITY)).thenReturn("true");
        when(properties.getProperty(PULL_HTTP_BASE_KEY + ".override.core")).thenReturn("true");
        when(properties.getProperty(eq(PORT_KEY), any())).thenReturn("0");
        when(properties.getProperty(eq(HOST_KEY), any())).thenReturn("localhost");
        restChangeIngestor = new RestChangeIngestor();

        testNotifier = Mockito.mock(ConfigurationChangeNotifier.class);

        ConfigurationFileHolder configurationFileHolder = Mockito.mock(ConfigurationFileHolder.class);
        when(configurationFileHolder.getConfigFileReference()).thenReturn(new AtomicReference<>(ByteBuffer.wrap(new byte[0])));

        restChangeIngestor.initialize(properties, configurationFileHolder, testNotifier);
        restChangeIngestor.setDifferentiator(mockDifferentiator);
        restChangeIngestor.start();

        client = new OkHttpClient();

        url = restChangeIngestor.getURI().toURL().toString();
        Thread.sleep(1000);
    }

    @AfterAll
    public static void stop() throws Exception {
        restChangeIngestor.close();
        client = null;
    }
}
