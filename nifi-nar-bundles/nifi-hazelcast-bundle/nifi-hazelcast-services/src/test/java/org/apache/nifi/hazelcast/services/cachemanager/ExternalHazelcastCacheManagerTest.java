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
package org.apache.nifi.hazelcast.services.cachemanager;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ExternalHazelcastCacheManagerTest extends AbstractHazelcastCacheManagerTest {
    private HazelcastInstance hazelcastInstance;

    private int port;

    @Before
    public void setUp() {
        port = NetworkUtils.availablePort();
        final Config config = new Config();
        config.getNetworkConfig().setPort(port);
        config.setClusterName("nifi");

        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
        hazelcastInstance.shutdown();
    }

    @Test
    public void testExecution() throws Exception {
        testSubject = new ExternalHazelcastCacheManager();
        testRunner.addControllerService("hazelcast-connection-service", testSubject);
        testRunner.setProperty(testSubject, ExternalHazelcastCacheManager.HAZELCAST_SERVER_ADDRESS, String.format("localhost:%d", port));

        givenHazelcastMapCacheClient();
        givenServicesAreEnabled();

        whenExecuting();

        thenProcessingIsSuccessful();
    }
}
