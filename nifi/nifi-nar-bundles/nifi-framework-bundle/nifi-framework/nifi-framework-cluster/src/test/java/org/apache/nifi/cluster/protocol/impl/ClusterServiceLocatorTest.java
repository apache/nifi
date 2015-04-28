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
package org.apache.nifi.cluster.protocol.impl;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import org.apache.nifi.io.socket.multicast.DiscoverableService;
import org.apache.nifi.io.socket.multicast.DiscoverableServiceImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.mockito.stubbing.OngoingStubbing;

public class ClusterServiceLocatorTest {

    private ClusterServiceDiscovery mockServiceDiscovery;

    private int fixedPort;

    private DiscoverableService fixedService;

    private ClusterServiceLocator serviceDiscoveryLocator;

    private ClusterServiceLocator serviceDiscoveryFixedPortLocator;

    private ClusterServiceLocator fixedServiceLocator;

    @Before
    public void setup() throws Exception {

        fixedPort = 1;
        mockServiceDiscovery = mock(ClusterServiceDiscovery.class);
        fixedService = new DiscoverableServiceImpl("some-service", InetSocketAddress.createUnresolved("some-host", 20));

        serviceDiscoveryLocator = new ClusterServiceLocator(mockServiceDiscovery);
        serviceDiscoveryFixedPortLocator = new ClusterServiceLocator(mockServiceDiscovery, fixedPort);
        fixedServiceLocator = new ClusterServiceLocator(fixedService);

    }

    @Test
    public void getServiceWhenServiceDiscoveryNotStarted() {
        assertNull(serviceDiscoveryLocator.getService());
    }

    @Test
    public void getServiceWhenServiceDiscoveryFixedPortNotStarted() {
        assertNull(serviceDiscoveryLocator.getService());
    }

    @Test
    public void getServiceWhenFixedServiceNotStarted() {
        assertEquals(fixedService, fixedServiceLocator.getService());
    }

    @Test
    public void getServiceNotOnFirstAttempt() {

        ClusterServiceLocator.AttemptsConfig config = new ClusterServiceLocator.AttemptsConfig();
        config.setNumAttempts(2);
        config.setTimeBetweenAttempsUnit(TimeUnit.SECONDS);
        config.setTimeBetweenAttempts(1);

        serviceDiscoveryLocator.setAttemptsConfig(config);

        OngoingStubbing<DiscoverableService> stubbing = null;
        for (int i = 0; i < config.getNumAttempts() - 1; i++) {
            if (stubbing == null) {
                stubbing = when(mockServiceDiscovery.getService()).thenReturn(null);
            } else {
                stubbing.thenReturn(null);
            }
        }
        stubbing.thenReturn(fixedService);

        assertEquals(fixedService, serviceDiscoveryLocator.getService());

    }

    @Test
    public void getServiceNotOnFirstAttemptWithFixedPort() {

        ClusterServiceLocator.AttemptsConfig config = new ClusterServiceLocator.AttemptsConfig();
        config.setNumAttempts(2);
        config.setTimeBetweenAttempsUnit(TimeUnit.SECONDS);
        config.setTimeBetweenAttempts(1);

        serviceDiscoveryFixedPortLocator.setAttemptsConfig(config);

        OngoingStubbing<DiscoverableService> stubbing = null;
        for (int i = 0; i < config.getNumAttempts() - 1; i++) {
            if (stubbing == null) {
                stubbing = when(mockServiceDiscovery.getService()).thenReturn(null);
            } else {
                stubbing.thenReturn(null);
            }
        }
        stubbing.thenReturn(fixedService);

        InetSocketAddress resultAddress = InetSocketAddress.createUnresolved(fixedService.getServiceAddress().getHostName(), fixedPort);
        DiscoverableService resultService = new DiscoverableServiceImpl(fixedService.getServiceName(), resultAddress);
        assertEquals(resultService, serviceDiscoveryFixedPortLocator.getService());
    }
}
