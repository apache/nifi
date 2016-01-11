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

package org.apache.nifi.controller.state.providers.zookeeper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.components.state.StateProviderInitializationContext;
import org.apache.nifi.controller.state.StandardStateMap;
import org.apache.nifi.controller.state.providers.AbstractStateProvider;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public class ZooKeeperStateProvider extends AbstractStateProvider {
    static final PropertyDescriptor CONNECTION_STRING = new PropertyDescriptor.Builder()
        .name("Connect String")
        .description("The ZooKeeper Connect String to use. This is a comma-separated list of hostnames/IP addresses, such as \"host1, host2, 127.0.0.1, host4, host5\"")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(false)
        .build();
    static final PropertyDescriptor SESSION_TIMEOUT = new PropertyDescriptor.Builder()
        .name("Session Timeout")
        .description("Specifies how long this instance of NiFi is allowed to be disconnected from ZooKeeper before creating a new ZooKeeper Session")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .defaultValue("30 sec")
        .required(true)
        .build();
    static final PropertyDescriptor ROOT_NODE = new PropertyDescriptor.Builder()
        .name("Root Node")
        .description("The Root Node to use in ZooKeeper to store state in")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("/nifi")
        .required(true)
        .build();


    private final List<ACL> acl;

    private ZooKeeper zooKeeper;
    private int timeoutMillis;
    private String rootNode;
    private String connectionString;

    private static final int ENCODING_VERSION = 1;

    public ZooKeeperStateProvider() throws Exception {
        // TODO: Provide SSL Context
        // TODO: Use more appropriate acl
        acl = Ids.OPEN_ACL_UNSAFE;
    }


    @Override
    public synchronized void init(final StateProviderInitializationContext context) {
        connectionString = context.getProperty(CONNECTION_STRING).getValue();

        rootNode = context.getProperty(ROOT_NODE).getValue();
        timeoutMillis = context.getProperty(SESSION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
    }

    @Override
    public synchronized void shutdown() {
        if (zooKeeper != null) {
            try {
                zooKeeper.close();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        zooKeeper = null;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(CONNECTION_STRING);
        properties.add(SESSION_TIMEOUT);
        properties.add(ROOT_NODE);
        return properties;
    }

    private synchronized ZooKeeper getZooKeeper() throws IOException {
        if (zooKeeper != null && !zooKeeper.getState().isAlive()) {
            invalidateClient();
        }

        if (zooKeeper == null) {
            zooKeeper = new ZooKeeper(connectionString, timeoutMillis, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                }
            });
        }

        return zooKeeper;
    }

    private synchronized void invalidateClient() {
        shutdown();
    }

    private String getComponentPath(final String componentId) {
        return rootNode + "/components/" + componentId;
    }

    private void verifyEnabled() throws IOException {
        if (!isEnabled()) {
            throw new IOException("Cannot update or retrieve cluster state becuase node is no longer connected to a cluster");
        }
    }

    @Override
    public void onComponentRemoved(final String componentId) throws IOException {
        try {
            ZKUtil.deleteRecursive(getZooKeeper(), getComponentPath(componentId));
        } catch (final KeeperException ke) {
            // Node doesn't exist so just ignore
            if (Code.NONODE == ke.code()) {
                return;
            }
            if (Code.SESSIONEXPIRED == ke.code()) {
                invalidateClient();
                onComponentRemoved(componentId);
            }

            throw new IOException("Unable to remove state for component with ID '" + componentId + "' from ZooKeeper", ke);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Failed to remove state for component with ID '" + componentId + "' from ZooKeeper due to being interrupted", e);
        }
    }

    @Override
    public void setState(final Map<String, String> state, final String componentId) throws IOException {
        setState(state, -1, componentId);
    }


    private byte[] serialize(final Map<String, String> stateValues) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeInt(ENCODING_VERSION);
            dos.writeInt(stateValues.size());
            for (final Map.Entry<String, String> entry : stateValues.entrySet()) {
                dos.writeUTF(entry.getKey());
                dos.writeUTF(entry.getValue());
            }
            return baos.toByteArray();
        }
    }

    private StateMap deserialize(final byte[] data, final int recordVersion, final String componentId) throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(data);
            final DataInputStream dis = new DataInputStream(bais)) {

            final int encodingVersion = dis.readInt();
            if (encodingVersion > ENCODING_VERSION) {
                throw new IOException("Retrieved a response from ZooKeeper when retrieving state for component with ID " + componentId
                    + ", but the response was encoded using the ZooKeeperStateProvider Encoding Version of " + encodingVersion
                    + " but this instance can only decode versions up to " + ENCODING_VERSION
                    + "; it appears that the state was encoded using a newer version of NiFi than is currently running. This information cannot be decoded.");
            }

            final int numEntries = dis.readInt();
            final Map<String, String> stateValues = new HashMap<>(numEntries);
            for (int i = 0; i < numEntries; i++) {
                final String key = dis.readUTF();
                final String value = dis.readUTF();
                stateValues.put(key, value);
            }

            return new StandardStateMap(stateValues, recordVersion);
        }
    }

    private void setState(final Map<String, String> stateValues, final int version, final String componentId) throws IOException {
        verifyEnabled();

        try {
            final String path = getComponentPath(componentId);
            final byte[] data = serialize(stateValues);

            final ZooKeeper keeper = getZooKeeper();
            try {
                keeper.setData(path, data, version);
            } catch (final KeeperException ke) {
                if (ke.code() == Code.NONODE) {
                    createNode(path, data);
                } else {
                    throw ke;
                }
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Failed to set cluster-wide state in ZooKeeper for component with ID " + componentId + " due to interruption", e);
        } catch (final KeeperException ke) {
            if (Code.SESSIONEXPIRED == ke.code()) {
                invalidateClient();
                setState(stateValues, version, componentId);
            }

            throw new IOException("Failed to set cluster-wide state in ZooKeeper for component with ID " + componentId, ke);
        } catch (final IOException ioe) {
            throw new IOException("Failed to set cluster-wide state in ZooKeeper for component with ID " + componentId, ioe);
        }
    }


    private void createNode(final String path, final byte[] data) throws IOException, KeeperException {
        try {
            getZooKeeper().create(path, data, acl, CreateMode.PERSISTENT);
        } catch (final InterruptedException ie) {
            throw new IOException("Failed to update cluster-wide state due to interruption", ie);
        } catch (final KeeperException ke) {
            if (ke.code() == Code.NONODE) {
                final String parentPath = StringUtils.substringBeforeLast(path, "/");
                createNode(parentPath, null);
                createNode(path, data);
                return;
            }
            if (Code.SESSIONEXPIRED == ke.code()) {
                invalidateClient();
                createNode(path, data);
            }

            // Node already exists. Node must have been created by "someone else". Just set the data.
            if (ke.code() == Code.NODEEXISTS) {
                try {
                    getZooKeeper().setData(path, data, -1);
                } catch (final KeeperException ke1) {
                    // Node no longer exists -- it was removed by someone else. Go recreate the node.
                    if (ke1.code() == Code.NONODE) {
                        createNode(path, data);
                    }
                } catch (final InterruptedException ie) {
                    throw new IOException("Failed to update cluster-wide state due to interruption", ie);
                }
            }


            throw ke;
        }
    }

    @Override
    public StateMap getState(final String componentId) throws IOException {
        verifyEnabled();

        try {
            final Stat stat = new Stat();
            final String path = getComponentPath(componentId);
            final byte[] data = getZooKeeper().getData(path, false, stat);

            final StateMap stateMap = deserialize(data, stat.getVersion(), componentId);
            return stateMap;
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Failed to obtain value from ZooKeeper for component with ID " + componentId + ", due to interruption", e);
        } catch (final KeeperException ke) {
            if (ke.code() == Code.NONODE) {
                return new StandardStateMap(null, -1L);
            }
            if (Code.SESSIONEXPIRED == ke.code()) {
                invalidateClient();
                return getState(componentId);
            }

            throw new IOException("Failed to obtain value from ZooKeeper for component with ID " + componentId, ke);
        } catch (final IOException ioe) {
            // provide more context in the error message
            throw new IOException("Failed to obtain value from ZooKeeper for component with ID " + componentId, ioe);
        }
    }

    @Override
    public boolean replace(final StateMap oldValue, final Map<String, String> newValue, final String componentId) throws IOException {
        verifyEnabled();

        try {
            setState(newValue, (int) oldValue.getVersion(), componentId);
            return true;
        } catch (final IOException ioe) {
            return false;
        }
    }


    @Override
    public void clear(final String componentId) throws IOException {
        verifyEnabled();
        setState(Collections.<String, String> emptyMap(), componentId);
    }
}
