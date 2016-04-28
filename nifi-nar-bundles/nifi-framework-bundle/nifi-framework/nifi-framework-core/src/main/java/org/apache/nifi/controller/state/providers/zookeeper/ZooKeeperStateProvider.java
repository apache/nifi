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
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.components.state.StateProviderInitializationContext;
import org.apache.nifi.components.state.exception.StateTooLargeException;
import org.apache.nifi.controller.state.StandardStateMap;
import org.apache.nifi.controller.state.providers.AbstractStateProvider;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ConnectStringParser;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * ZooKeeperStateProvider utilizes a ZooKeeper based store, whether provided internally via configuration and enabling of the {@link org.apache.nifi.controller.state.server.ZooKeeperStateServer}
 * or through an externally configured location.  This implementation caters to a clustered NiFi environment and accordingly only provides {@link Scope#CLUSTER} scoping to enforce
 * consistency across configuration interactions.
 */
public class ZooKeeperStateProvider extends AbstractStateProvider {
    private static final int ONE_MB = 1024 * 1024;

    static final AllowableValue OPEN_TO_WORLD = new AllowableValue("Open", "Open", "ZNodes will be open to any ZooKeeper client.");
    static final AllowableValue CREATOR_ONLY = new AllowableValue("CreatorOnly", "CreatorOnly",
        "ZNodes will be accessible only by the creator. The creator will have full access to create, read, write, delete, and administer the ZNodes.");

    static final PropertyDescriptor CONNECTION_STRING = new PropertyDescriptor.Builder()
        .name("Connect String")
        .description("The ZooKeeper Connect String to use. This is a comma-separated list of hostname/IP and port tuples, such as \"host1:2181,host2:2181,127.0.0.1:2181\". If a port is not " +
            "specified it defaults to the ZooKeeper client port default of 2181")
        .addValidator(new Validator() {
            @Override
            public ValidationResult validate(String subject, String input, ValidationContext context) {
                final String connectionString = context.getProperty(CONNECTION_STRING).getValue();
                try {
                    new ConnectStringParser(connectionString);
                } catch (Exception e) {
                    return new ValidationResult.Builder().subject(subject).input(input).explanation("Invalid Connect String: " + connectionString).valid(false).build();
                }
                return new ValidationResult.Builder().subject(subject).input(input).explanation("Valid Connect String").valid(true).build();
            }
        })
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
    static final PropertyDescriptor ACCESS_CONTROL = new PropertyDescriptor.Builder()
        .name("Access Control")
        .description("Specifies the Access Controls that will be placed on ZooKeeper ZNodes that are created by this State Provider")
        .allowableValues(OPEN_TO_WORLD, CREATOR_ONLY)
        .defaultValue(OPEN_TO_WORLD.getValue())
        .required(true)
        .build();

    private static final byte ENCODING_VERSION = 1;

    private ZooKeeper zooKeeper;

    // effectively final
    private int timeoutMillis;
    private String rootNode;
    private String connectionString;
    private byte[] auth;
    private List<ACL> acl;


    public ZooKeeperStateProvider() {
    }


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(CONNECTION_STRING);
        properties.add(SESSION_TIMEOUT);
        properties.add(ROOT_NODE);
        properties.add(ACCESS_CONTROL);
        return properties;
    }


    @Override
    public synchronized void init(final StateProviderInitializationContext context) {
        connectionString = context.getProperty(CONNECTION_STRING).getValue();
        rootNode = context.getProperty(ROOT_NODE).getValue();
        timeoutMillis = context.getProperty(SESSION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();

        if (context.getProperty(ACCESS_CONTROL).getValue().equalsIgnoreCase(CREATOR_ONLY.getValue())) {
            acl = Ids.CREATOR_ALL_ACL;
        } else {
            acl = Ids.OPEN_ACL_UNSAFE;
        }
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

    // visible for testing
    synchronized ZooKeeper getZooKeeper() throws IOException {
        if (zooKeeper != null && !zooKeeper.getState().isAlive()) {
            invalidateClient();
        }

        if (zooKeeper == null) {
            zooKeeper = new ZooKeeper(connectionString, timeoutMillis, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                }
            });

            if (auth != null) {
                zooKeeper.addAuthInfo("digest", auth);
            }
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
            throw new IOException("Cannot update or retrieve cluster state because node is no longer connected to a cluster.");
        }
    }

    @Override
    public void onComponentRemoved(final String componentId) throws IOException {
        try {
            ZKUtil.deleteRecursive(getZooKeeper(), getComponentPath(componentId));
        } catch (final KeeperException ke) {
            // Node doesn't exist so just ignore
            final Code exceptionCode = ke.code();
            if (Code.NONODE == exceptionCode) {
                return;
            }
            if (Code.SESSIONEXPIRED == exceptionCode) {
                invalidateClient();
                onComponentRemoved(componentId);
                return;
            }

            throw new IOException("Unable to remove state for component with ID '" + componentId + " with exception code " + exceptionCode, ke);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Failed to remove state for component with ID '" + componentId + "' from ZooKeeper due to being interrupted", e);
        }
    }

    @Override
    public Scope[] getSupportedScopes() {
        return new Scope[]{Scope.CLUSTER};
    }

    @Override
    public void setState(final Map<String, String> state, final String componentId) throws IOException {
        setState(state, -1, componentId);
    }


    private byte[] serialize(final Map<String, String> stateValues) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeByte(ENCODING_VERSION);
            dos.writeInt(stateValues.size());
            for (final Map.Entry<String, String> entry : stateValues.entrySet()) {
                final boolean hasKey = entry.getKey() != null;
                final boolean hasValue = entry.getValue() != null;
                dos.writeBoolean(hasKey);
                if (hasKey) {
                    dos.writeUTF(entry.getKey());
                }

                dos.writeBoolean(hasValue);
                if (hasValue) {
                    dos.writeUTF(entry.getValue());
                }
            }
            return baos.toByteArray();
        }
    }

    private StateMap deserialize(final byte[] data, final int recordVersion, final String componentId) throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(data);
             final DataInputStream dis = new DataInputStream(bais)) {

            final byte encodingVersion = dis.readByte();
            if (encodingVersion > ENCODING_VERSION) {
                throw new IOException("Retrieved a response from ZooKeeper when retrieving state for component with ID " + componentId
                    + ", but the response was encoded using the ZooKeeperStateProvider Encoding Version of " + encodingVersion
                    + " but this instance can only decode versions up to " + ENCODING_VERSION
                    + "; it appears that the state was encoded using a newer version of NiFi than is currently running. This information cannot be decoded.");
            }

            final int numEntries = dis.readInt();
            final Map<String, String> stateValues = new HashMap<>(numEntries);
            for (int i = 0; i < numEntries; i++) {
                final boolean hasKey = dis.readBoolean();
                final String key = hasKey ? dis.readUTF() : null;

                final boolean hasValue = dis.readBoolean();
                final String value = hasValue ? dis.readUTF() : null;
                stateValues.put(key, value);
            }

            return new StandardStateMap(stateValues, recordVersion);
        }
    }

    private void setState(final Map<String, String> stateValues, final int version, final String componentId) throws IOException {
        try {
            setState(stateValues, version, componentId, true);
        } catch (final NoNodeException nne) {
            // should never happen because we are passing 'true' for allowNodeCreation
            throw new IOException("Unable to create Node in ZooKeeper to set state for component with ID " + componentId, nne);
        }
    }

    /**
     * Sets the component state to the given stateValues if and only if the version is equal to the version currently
     * tracked by ZooKeeper (or if the version is -1, in which case the state will be updated regardless of the version).
     *
     * @param stateValues the new values to set
     * @param version the expected version of the ZNode
     * @param componentId the ID of the component whose state is being updated
     * @param allowNodeCreation if <code>true</code> and the corresponding ZNode does not exist in ZooKeeper, it will be created; if <code>false</code>
     *            and the corresponding node does not exist in ZooKeeper, a {@link KeeperException.NoNodeException} will be thrown
     *
     * @throws IOException if unable to communicate with ZooKeeper
     * @throws NoNodeException if the corresponding ZNode does not exist in ZooKeeper and allowNodeCreation is set to <code>false</code>
     * @throws StateTooLargeException if the state to be stored exceeds the maximum size allowed by ZooKeeper (1 MB, after serialization)
     */
    private void setState(final Map<String, String> stateValues, final int version, final String componentId, final boolean allowNodeCreation) throws IOException, NoNodeException {
        verifyEnabled();

        try {
            final String path = getComponentPath(componentId);
            final byte[] data = serialize(stateValues);
            if (data.length > ONE_MB) {
                throw new StateTooLargeException("Failed to set cluster-wide state in ZooKeeper for component with ID " + componentId
                    + " because the state had " + stateValues.size() + " values, which serialized to " + data.length
                    + " bytes, and the maximum allowed by ZooKeeper is 1 MB (" + ONE_MB + " bytes)");
            }

            final ZooKeeper keeper = getZooKeeper();
            try {
                keeper.setData(path, data, version);
            } catch (final NoNodeException nne) {
                if (allowNodeCreation) {
                    createNode(path, data, componentId, stateValues, acl);
                    return;
                } else {
                    throw nne;
                }
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Failed to set cluster-wide state in ZooKeeper for component with ID " + componentId + " due to interruption", e);
        } catch (final NoNodeException nne) {
            throw nne;
        } catch (final KeeperException ke) {
            if (Code.SESSIONEXPIRED == ke.code()) {
                invalidateClient();
                setState(stateValues, version, componentId, allowNodeCreation);
                return;
            }
            if (Code.NODEEXISTS == ke.code()) {
                setState(stateValues, version, componentId, allowNodeCreation);
                return;
            }

            throw new IOException("Failed to set cluster-wide state in ZooKeeper for component with ID " + componentId, ke);
        } catch (final StateTooLargeException stle) {
            throw stle;
        } catch (final IOException ioe) {
            throw new IOException("Failed to set cluster-wide state in ZooKeeper for component with ID " + componentId, ioe);
        }
    }


    private void createNode(final String path, final byte[] data, final String componentId, final Map<String, String> stateValues, final List<ACL> acls) throws IOException, KeeperException {
        try {
            if (data != null && data.length > ONE_MB) {
                throw new StateTooLargeException("Failed to set cluster-wide state in ZooKeeper for component with ID " + componentId
                    + " because the state had " + stateValues.size() + " values, which serialized to " + data.length
                    + " bytes, and the maximum allowed by ZooKeeper is 1 MB (" + ONE_MB + " bytes)");
            }

            getZooKeeper().create(path, data, acls, CreateMode.PERSISTENT);
        } catch (final InterruptedException ie) {
            throw new IOException("Failed to update cluster-wide state due to interruption", ie);
        } catch (final KeeperException ke) {
            final Code exceptionCode = ke.code();
            if (Code.NONODE == exceptionCode) {
                final String parentPath = StringUtils.substringBeforeLast(path, "/");
                createNode(parentPath, null, componentId, stateValues, Ids.OPEN_ACL_UNSAFE);
                createNode(path, data, componentId, stateValues, acls);
                return;
            }
            if (Code.SESSIONEXPIRED == exceptionCode) {
                invalidateClient();
                createNode(path, data, componentId, stateValues, acls);
                return;
            }

            // Node already exists. Node must have been created by "someone else". Just set the data.
            if (Code.NODEEXISTS == exceptionCode) {
                try {
                    getZooKeeper().setData(path, data, -1);
                    return;
                } catch (final KeeperException ke1) {
                    // Node no longer exists -- it was removed by someone else. Go recreate the node.
                    if (ke1.code() == Code.NONODE) {
                        createNode(path, data, componentId, stateValues, acls);
                        return;
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
            final Code exceptionCode = ke.code();
            if (Code.NONODE == exceptionCode) {
                return new StandardStateMap(null, -1L);
            }
            if (Code.SESSIONEXPIRED == exceptionCode) {
                invalidateClient();
                return getState(componentId);
            }

            throw new IOException("Failed to obtain value from ZooKeeper for component with ID " + componentId + " with exception code " + exceptionCode, ke);
        } catch (final IOException ioe) {
            // provide more context in the error message
            throw new IOException("Failed to obtain value from ZooKeeper for component with ID " + componentId, ioe);
        }
    }

    @Override
    public boolean replace(final StateMap oldValue, final Map<String, String> newValue, final String componentId) throws IOException {
        verifyEnabled();

        try {
            setState(newValue, (int) oldValue.getVersion(), componentId, false);
            return true;
        } catch (final NoNodeException nne) {
            return false;
        } catch (final IOException ioe) {
            final Throwable cause = ioe.getCause();
            if (cause != null && cause instanceof KeeperException) {
                final KeeperException ke = (KeeperException) cause;
                if (Code.BADVERSION == ke.code()) {
                    return false;
                }
            }

            throw ioe;
        }
    }


    @Override
    public void clear(final String componentId) throws IOException {
        verifyEnabled();
        setState(Collections.<String, String>emptyMap(), componentId);
    }
}
