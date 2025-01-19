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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class ExternalHazelcastCacheManagerTest extends AbstractHazelcastCacheManagerTest {
    private HazelcastInstance hazelcastInstance;


    @BeforeEach
    public void setUp() {
        final Config config = new Config();
        config.getNetworkConfig().setPort(0);
        config.setClusterName("nifi");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);

        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        super.setUp();
    }

    @AfterEach
    public void tearDown() {
        super.tearDown();
        hazelcastInstance.shutdown();
    }

    @Test
    public void testExecution() throws Exception {
        testSubject = new ExternalHazelcastCacheManager();
        testRunner.addControllerService("hazelcast-connection-service", testSubject);

        final SocketAddress localAddress = hazelcastInstance.getLocalEndpoint().getSocketAddress();
        assertInstanceOf(InetSocketAddress.class, localAddress);
        final int port = ((InetSocketAddress) localAddress).getPort();
        testRunner.setProperty(testSubject, ExternalHazelcastCacheManager.HAZELCAST_SERVER_ADDRESS, "localhost:" + port);

        setupHazelcastMapCacheClient();
        enableServices();

        triggerProcessor();

        assertSuccessfulTransfer();
    }
}
