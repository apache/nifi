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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ExternalHazelcastCacheManagerTest extends AbstractHazelcastCacheManagerTest {
    private Thread hazelcastServer;

    @Before
    public void setUp() {
        hazelcastServer = new Thread(new Runnable() {
            HazelcastInstance hazelcastInstance;

            @Override
            public void run() {
                final Config config = new Config();
                config.getNetworkConfig().setPort(5704);
                config.setClusterName("nifi");
                hazelcastInstance = Hazelcast.newHazelcastInstance(config);

                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        Thread.currentThread().interrupt();
                    }
                }

                hazelcastInstance.shutdown();
                hazelcastInstance = null;
            }
        });

        hazelcastServer.start();
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
        hazelcastServer.interrupt();
    }

    @Test
    public void testExecution() throws Exception {
        // given
        testSubject = new ExternalHazelcastCacheManager();
        testRunner.addControllerService("hazelcast-connection-service", testSubject);
        testRunner.setProperty(testSubject, ExternalHazelcastCacheManager.HAZELCAST_SERVER_ADDRESS, "localhost:5704");

        givenHazelcastMapCacheClient();
        givenServicesAreEnabled();

        // when
        whenExecuting();

        // then
        thenProcessingIsSuccessful();
    }
}
