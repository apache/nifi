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

package org.apache.nifi.cluster.coordination.heartbeat;

import static org.junit.Assert.assertEquals;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.framework.state.ConnectionState;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.protocol.ProtocolListener;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Test;
import org.mockito.Mockito;

public class TestClusterProtocolHeartbeatMonitor {

    @Test
    public void testRepublishesAddressOnZooKeeperReconnect() throws InterruptedException {
        final ClusterCoordinator coordinator = Mockito.mock(ClusterCoordinator.class);
        final ProtocolListener protocolListener = Mockito.mock(ProtocolListener.class);
        final Properties properties = new Properties();
        properties.setProperty(NiFiProperties.CLUSTER_NODE_PROTOCOL_PORT, "0");
        properties.setProperty(NiFiProperties.ZOOKEEPER_CONNECT_STRING, "localhost:2181");

        final AtomicInteger publishCount = new AtomicInteger(0);
        final ClusterProtocolHeartbeatMonitor monitor = new ClusterProtocolHeartbeatMonitor(coordinator, protocolListener, properties) {
            @Override
            protected void publishAddress() {
                publishCount.incrementAndGet();
            }
        };

        monitor.start();

        Thread.sleep(250L);
        assertEquals(0, publishCount.get());
        monitor.stateChanged(null, ConnectionState.CONNECTED);

        Thread.sleep(250L);
        assertEquals(1, publishCount.get());

        monitor.stateChanged(null, ConnectionState.LOST);
        Thread.sleep(250L);
        assertEquals(1, publishCount.get());

        monitor.stateChanged(null, ConnectionState.CONNECTED);
        Thread.sleep(250L);
        assertEquals(2, publishCount.get());
    }
}
