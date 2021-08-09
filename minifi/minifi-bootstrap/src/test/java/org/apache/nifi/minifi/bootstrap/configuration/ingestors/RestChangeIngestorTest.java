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


import okhttp3.OkHttpClient;
import org.apache.nifi.minifi.bootstrap.ConfigurationFileHolder;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeNotifier;
import org.apache.nifi.minifi.bootstrap.configuration.ingestors.common.RestChangeIngestorCommonTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.mockito.Mockito;

import java.net.MalformedURLException;
import java.util.Properties;


public class RestChangeIngestorTest extends RestChangeIngestorCommonTest {

    @BeforeClass
    public static void setUp() throws InterruptedException, MalformedURLException {
        Properties properties = new Properties();
        restChangeIngestor = new RestChangeIngestor();

        testNotifier = Mockito.mock(ConfigurationChangeNotifier.class);

        restChangeIngestor.initialize(properties, Mockito.mock(ConfigurationFileHolder.class), testNotifier);
        restChangeIngestor.setDifferentiator(mockDifferentiator);
        restChangeIngestor.start();

        client = new OkHttpClient();

        url = restChangeIngestor.getURI().toURL().toString();
        Thread.sleep(1000);
    }

    @AfterClass
    public static void stop() throws Exception {
        restChangeIngestor.close();
        client = null;
    }
}
