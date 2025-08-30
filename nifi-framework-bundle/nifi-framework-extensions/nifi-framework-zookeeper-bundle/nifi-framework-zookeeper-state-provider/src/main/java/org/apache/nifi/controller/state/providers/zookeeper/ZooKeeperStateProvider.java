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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.state.annotation.StateProviderContext;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.components.state.StateProviderInitializationContext;
import org.apache.nifi.components.state.exception.StateTooLargeException;
import org.apache.nifi.framework.cluster.zookeeper.ZooKeeperClientConfig;
import org.apache.nifi.framework.cluster.zookeeper.SecureClientZooKeeperFactory;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.NiFiProperties;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.BadVersionException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ConnectStringParser;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.common.ZKConfig;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * ZooKeeperStateProvider utilizes a ZooKeeper based store.
 * This implementation caters to a clustered NiFi environment and accordingly only provides {@link Scope#CLUSTER} scoping to enforce
 * consistency across configuration interactions.
 */
public class ZooKeeperStateProvider extends AbstractStateProvider {

    private static final String COMPONENTS_RELATIVE_PATH = "/components";

    private static final String COMPONENTS_PATH_FORMAT = "%s%s/%s";

    private static final Logger logger = LoggerFactory.getLogger(ZooKeeperStateProvider.class);
    private NiFiProperties nifiProperties;

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
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .required(true)
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
    private List<ACL> acl;

    private ZooKeeperClientConfig zooKeeperClientConfig;

    @StateProviderContext
    public void setNiFiProperties(NiFiProperties properties) {
        this.nifiProperties = properties;
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

    /**
     * Combine properties from NiFiProperties and additional properties, allowing the additional properties to override settings
     * in the given NiFiProperties.
     * @param nifiProps A NiFiProperties to be combined with some additional properties
     * @param additionalProperties Additional properties that can be used to override properties in the given NiFiProperties
     * @return NiFiProperties that contains the combined properties
     */
    static NiFiProperties combineProperties(NiFiProperties nifiProps, Properties additionalProperties) {
        return new NiFiProperties() {

            @Override
            public String getProperty(String key) {
                // Get the additional properties as preference over the NiFiProperties value. Will return null if the property
                // is not available through either object.
                return additionalProperties.getProperty(key, nifiProps != null ? nifiProps.getProperty(key) : null);
            }

            @Override
            public Set<String> getPropertyKeys() {
                Set<String> prop = additionalProperties.keySet().stream().map(key -> (String) key).collect(Collectors.toSet());
                prop.addAll(nifiProps.getPropertyKeys());
                return prop;
            }
        };
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

        ZooKeeperClientConfig clientConfig = getZooKeeperConfig();

        if (zooKeeper != null && !zooKeeper.getState().isAlive()) {
            invalidateClient();
        }

        if (zooKeeper == null) {
            if (clientConfig != null && clientConfig.isClientSecure()) {
                SecureClientZooKeeperFactory factory = new SecureClientZooKeeperFactory(clientConfig);
                try {
                    zooKeeper = factory.newZooKeeper(connectionString, timeoutMillis, new NoOpWatcher(), true);
                    logger.debug("Secure ZooKeeper Client connection [{}] created", connectionString);
                } catch (final Exception e) {
                    logger.error("Secure ZooKeeper Client connection [{}] failed", connectionString, e);
                    invalidateClient();
                }
            } else {
                final ZKClientConfig zkClientConfig = new ZKClientConfig();
                if (clientConfig != null) {
                    zkClientConfig.setProperty(ZKConfig.JUTE_MAXBUFFER, Integer.toString(clientConfig.getJuteMaxbuffer()));
                }
                zooKeeper = new ZooKeeper(connectionString, timeoutMillis, new NoOpWatcher(), zkClientConfig);
                logger.debug("Standard ZooKeeper Client connection [{}] created", connectionString);
            }
        }

        return zooKeeper;
    }

    private ZooKeeperClientConfig getZooKeeperConfig() {
        if (zooKeeperClientConfig == null) {
            Properties stateProviderProperties = new Properties();
            stateProviderProperties.setProperty(NiFiProperties.ZOOKEEPER_SESSION_TIMEOUT, timeoutMillis + " millis");
            stateProviderProperties.setProperty(NiFiProperties.ZOOKEEPER_CONNECT_TIMEOUT, timeoutMillis + " millis");
            stateProviderProperties.setProperty(NiFiProperties.ZOOKEEPER_ROOT_NODE, rootNode);
            stateProviderProperties.setProperty(NiFiProperties.ZOOKEEPER_CONNECT_STRING, connectionString);
            zooKeeperClientConfig = ZooKeeperClientConfig.createConfig(combineProperties(nifiProperties, stateProviderProperties));
        }
        return zooKeeperClientConfig;
    }

    private synchronized void invalidateClient() {
        shutdown();
    }

    private String getComponentPath(final String componentId) {
        return String.format(COMPONENTS_PATH_FORMAT, rootNode, COMPONENTS_RELATIVE_PATH, componentId);
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

            final String stateVersion = String.valueOf(recordVersion);
            return new StandardStateMap(stateValues, Optional.of(stateVersion));
        }
    }

    /**
     * Sets the component state to the given stateValues unconditionally..
     *
     * @param stateValues the new values to set
     * @param componentId the ID of the component whose state is being updated
     *
     * @throws IOException if unable to communicate with ZooKeeper
     * @throws StateTooLargeException if the state to be stored exceeds the maximum size allowed by ZooKeeper (Based on jute.maxbuffer property, after serialization)
     */
    @Override
    public void setState(final Map<String, String> stateValues, final String componentId) throws IOException {
        final StateModifier stateModifier = (keeper, path, data) -> {
            try {
                keeper.setData(path, data, -1);
            } catch (final NoNodeException nne) {
                try {
                    createNode(path, data, acl);
                } catch (NodeExistsException nee) {
                    setState(stateValues, componentId);
                }
            }
            return true;
        };

        modifyState(stateValues, componentId, stateModifier);
    }

    /**
     * Sets the component state to the given stateValues if and only if oldState's version is equal to the version currently
     * tracked by ZooKeeper or if oldState's version is not present (no old state) and the Zookeeper node does not exist.
     *
     * @param oldState the old state whose version is used to compare against
     * @param stateValues the new values to set
     * @param componentId the ID of the component whose state is being updated
     *
     * @throws IOException if unable to communicate with ZooKeeper
     * @throws StateTooLargeException if the state to be stored exceeds the maximum size allowed by ZooKeeper (Based on jute.maxbuffer property, after serialization)
     */
    @Override
    public boolean replace(final StateMap oldState, final Map<String, String> stateValues, final String componentId) throws IOException {
        final Optional<Integer> version = oldState.getStateVersion().map(Integer::parseInt);

        final StateModifier stateModifier = (keeper, path, data) -> {
            if (version.isPresent()) {
                try {
                    keeper.setData(path, data, version.get());
                } catch (final BadVersionException | NoNodeException e) {
                    return false;
                }
            } else {
                try {
                    createNode(path, data, acl);
                } catch (final NodeExistsException e) {
                    return false;
                }
            }
            return true;
        };

        return modifyState(stateValues, componentId, stateModifier);
    }

    private boolean modifyState(final Map<String, String> stateValues, final String componentId, final StateModifier stateModifier) throws IOException {
        verifyEnabled();

        try {
            final ZooKeeper keeper = getZooKeeper();
            final String path = getComponentPath(componentId);
            final byte[] data = serialize(stateValues);
            validateDataSize(keeper.getClientConfig(), data, componentId, stateValues.size());

            return stateModifier.apply(keeper, path, data);
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("Failed to set cluster-wide state in ZooKeeper for component with ID " + componentId + " due to interruption", ie);
        } catch (final SessionExpiredException see) {
            invalidateClient();
            return modifyState(stateValues, componentId, stateModifier);
        } catch (final StateTooLargeException stle) {
            throw stle;
        } catch (final Exception e) {
            throw new IOException("Failed to set cluster-wide state in ZooKeeper for component with ID " + componentId, e);
        }
    }

    private void createNode(final String path, final byte[] data, final List<ACL> acls) throws IOException, KeeperException, InterruptedException {
        try {
            final ZooKeeper zooKeeper = getZooKeeper();
            zooKeeper.create(path, data, acls, CreateMode.PERSISTENT);
        } catch (final NoNodeException nne) {
            final String parentPath = StringUtils.substringBeforeLast(path, "/");
            createNode(parentPath, null, Ids.OPEN_ACL_UNSAFE);
            createNode(path, data, acls);
        } catch (final NodeExistsException nee) {
            // Fail only if it is the data/leaf node (not a parent node)
            if (data != null) {
                throw nee;
            }
        }
    }

    @Override
    public StateMap getState(final String componentId) throws IOException {
        verifyEnabled();

        try {
            final Stat stat = new Stat();
            final String path = getComponentPath(componentId);
            final byte[] data = getZooKeeper().getData(path, false, stat);

            return deserialize(data, stat.getVersion(), componentId);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Failed to obtain value from ZooKeeper for component with ID " + componentId + ", due to interruption", e);
        } catch (final KeeperException ke) {
            final Code exceptionCode = ke.code();
            if (Code.NONODE == exceptionCode) {
                return new StandardStateMap(null, Optional.empty());
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
    public void clear(final String componentId) throws IOException {
        verifyEnabled();
        setState(Collections.emptyMap(), componentId);
    }

    @Override
    public boolean isComponentEnumerationSupported() {
        return true;
    }

    @Override
    public Collection<String> getStoredComponentIds() throws IOException {
        try {
            final ZooKeeper zooKeeper = getZooKeeper();
            final String componentsPath = String.format("%s%s", rootNode, COMPONENTS_RELATIVE_PATH);
            final List<String> children = zooKeeper.getChildren(componentsPath, false);
            return Collections.unmodifiableCollection(children);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("ZooKeeper communication interrupted", e);
        } catch (final KeeperException e) {
            final Code code = e.code();
            if (Code.NONODE == code) {
                return Collections.emptyList();
            }
            throw new IOException(String.format("ZooKeeper communication failed: %s", code), e);
        }
    }

    private void validateDataSize(final ZKClientConfig clientConfig, final byte[] data, final String componentId, final int totalStateValues) throws StateTooLargeException {
        final int maximumSize = clientConfig.getInt(ZKConfig.JUTE_MAXBUFFER, NiFiProperties.DEFAULT_ZOOKEEPER_JUTE_MAXBUFFER);
        if (data != null && data.length > maximumSize) {
            final String message = String.format("Component [%s] State Values [%d] Data Size [%d B] exceeds nifi.zookeeper.jute.maxbuffer size [%d B]",
                    componentId, totalStateValues, data.length, maximumSize);
            throw new StateTooLargeException(message);
        }
    }

    private static final class NoOpWatcher implements Watcher {

        @Override
        public void process(final WatchedEvent watchedEvent) {

        }
    }

    private interface StateModifier {
        boolean apply(ZooKeeper keeper, String path, byte[] data) throws InterruptedException, KeeperException, IOException;
    }
}
