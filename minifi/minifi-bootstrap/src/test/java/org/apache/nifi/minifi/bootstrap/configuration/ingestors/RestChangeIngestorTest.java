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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;
import okhttp3.OkHttpClient;
import org.apache.nifi.minifi.bootstrap.ConfigurationFileHolder;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeNotifier;
import org.apache.nifi.minifi.bootstrap.configuration.ingestors.common.RestChangeIngestorCommonTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.mockito.Mockito;

import java.net.MalformedURLException;
import java.util.Properties;

public class RestChangeIngestorTest extends RestChangeIngestorCommonTest {

    @BeforeAll
    public static void setUp() throws InterruptedException, MalformedURLException {
        Properties properties = new Properties();
        properties.put(PullHttpChangeIngestor.OVERRIDE_SECURITY, "true");
        properties.put(PULL_HTTP_BASE_KEY + ".override.core", "true");
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
