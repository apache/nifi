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

package org.apache.nifi.controller.cluster;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.apache.nifi.cluster.protocol.message.HeartbeatMessage;
import org.apache.zookeeper.KeeperException.NoNodeException;

public class ZooKeeperHeartbeater implements Heartbeater {
    private static final JAXBContext jaxbContext;
    private final CuratorFramework curatorClient;
    private final String heartbeatPathPrefix;

    private final byte[] digest;
    private boolean digestSet = false;

    static {
        try {
            jaxbContext = JAXBContext.newInstance(HeartbeatMessage.class);
        } catch (JAXBException e) {
            throw new IllegalStateException("Could not create a JAXBContext for HeartbeatMessage class");
        }
    }

    public ZooKeeperHeartbeater(final Properties properties) {
        final RetryPolicy retryPolicy = new RetryForever(5000);
        final ZooKeeperClientConfig zkConfig = ZooKeeperClientConfig.createConfig(properties);

        curatorClient = CuratorFrameworkFactory.newClient(zkConfig.getConnectString(),
            zkConfig.getSessionTimeoutMillis(), zkConfig.getConnectionTimeoutMillis(), retryPolicy);
        digest = zkConfig.getDigest();

        try {
            setDigest();
        } catch (final Exception e) {
            // Ignore for now - it will get set before doing any send.
        }

        curatorClient.start();
        heartbeatPathPrefix = zkConfig.resolvePath("cluster/heartbeats/");
    }

    private void setDigest() throws IOException {
        if (digestSet) {
            return;
        }

        try {
            curatorClient.getZookeeperClient().getZooKeeper().addAuthInfo("digest", digest);
            digestSet = true;
        } catch (final Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public synchronized void send(final HeartbeatMessage heartbeatMessage) throws IOException {
        final byte[] serialized = serialize(heartbeatMessage);
        final String heartbeatPath = heartbeatPathPrefix + heartbeatMessage.getHeartbeat().getNodeIdentifier().getId();

        try {
            setDigest();
            curatorClient.setData().forPath(heartbeatPath, serialized);
        } catch (final NoNodeException nne) {
            try {
                curatorClient.create().creatingParentsIfNeeded().forPath(heartbeatPath, serialized);
                return;
            } catch (final Exception e) {
                throw new IOException("Failed to create ZNode " + heartbeatPath + " in ZooKeeper to send heartbeat", e);
            }
        } catch (Exception e) {
            throw new IOException("Failed to send heartbeat to ZooKeeper", e);
        }
    }

    @Override
    public void close() throws IOException {
        if (curatorClient != null) {
            curatorClient.close();
        }
    }

    private byte[] serialize(final HeartbeatMessage heartbeat) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            try {
                jaxbContext.createMarshaller().marshal(heartbeat, baos);
            } catch (JAXBException e) {
                throw new IOException("Failed to serialize Heartbeat message", e);
            }

            return baos.toByteArray();
        }
    }
}
