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
import org.junit.BeforeClass;
import org.mockito.Mockito;

import java.util.Properties;

public class PullHttpChangeIngestorTest extends PullHttpChangeIngestorCommonTest {

    @BeforeClass
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
    public void pullHttpChangeIngestorInit(Properties properties) {
        port = ((ServerConnector) jetty.getConnectors()[0]).getLocalPort();
        properties.put(PullHttpChangeIngestor.PORT_KEY, String.valueOf(port));
        properties.put(PullHttpChangeIngestor.HOST_KEY, "localhost");
        properties.put(PullHttpChangeIngestor.PULL_HTTP_POLLING_PERIOD_KEY, "30000");

        pullHttpChangeIngestor = new PullHttpChangeIngestor();
        pullHttpChangeIngestor.initialize(properties, Mockito.mock(ConfigurationFileHolder.class), testNotifier);
        pullHttpChangeIngestor.setDifferentiator(mockDifferentiator);
    }
}
