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

package org.apache.nifi.minifi.bootstrap.configuration;


import com.squareup.okhttp.OkHttpClient;
import org.apache.nifi.minifi.bootstrap.configuration.util.TestRestChangeNotifierCommon;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.net.MalformedURLException;
import java.util.Properties;


public class TestRestChangeNotifier extends TestRestChangeNotifierCommon {

    @BeforeClass
    public static void setUp() throws InterruptedException, MalformedURLException {
        Properties properties = new Properties();
        restChangeNotifier = new RestChangeNotifier();
        restChangeNotifier.initialize(properties);
        restChangeNotifier.registerListener(mockChangeListener);
        restChangeNotifier.start();

        client = new OkHttpClient();

        url = restChangeNotifier.getURI().toURL().toString();
        Thread.sleep(1000);
    }

    @AfterClass
    public static void stop() throws Exception {
        restChangeNotifier.close();
        client = null;
    }
}
