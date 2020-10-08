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

import org.apache.curator.test.InstanceSpec;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.StateProvider;
import org.apache.nifi.components.state.StateProviderInitializationContext;
import org.apache.nifi.components.state.exception.StateTooLargeException;
import org.apache.nifi.controller.state.providers.AbstractTestStateProvider;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.mock.MockComponentLogger;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testng.Assert;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.leader.election.TestSecureClientZooKeeperFactory.createAndStartServer;

public class ITZooKeeperStateProvider extends AbstractTestStateProvider {

    private static final String NETTY_SERVER_CNXN_FACTORY =
            "org.apache.zookeeper.server.NettyServerCnxnFactory";

    private volatile StateProvider provider;
    private volatile ZooKeeperServer zkServer;
    private static final Map<PropertyDescriptor, String> defaultProperties = new HashMap<>();
    private static Path tempDir;
    private static Path dataDir;
    private static int clientPort;
    private static ServerCnxnFactory serverConnectionFactory;

    private static final String CLIENT_KEYSTORE = "src/test/resources/TestSecureClientZooKeeperFactory/client.keystore.p12";
    private static final String CLIENT_TRUSTSTORE = "src/test/resources/TestSecureClientZooKeeperFactory/client.truststore.p12";
    private static final String CLIENT_KEYSTORE_TYPE = "JKS";
    private static final String CLIENT_TRUSTSTORE_TYPE = "JKS";
    private static final String SERVER_KEYSTORE = "src/test/resources/TestSecureClientZooKeeperFactory/server.keystore.p12";
    private static final String SERVER_TRUSTSTORE = "src/test/resources/TestSecureClientZooKeeperFactory/server.truststore.p12";
    private static final String TEST_PASSWORD = "testpass";

    static {
        defaultProperties.put(ZooKeeperStateProvider.SESSION_TIMEOUT, "15 secs");
        defaultProperties.put(ZooKeeperStateProvider.ROOT_NODE, "/nifi/team1/testing");
        defaultProperties.put(ZooKeeperStateProvider.ACCESS_CONTROL, ZooKeeperStateProvider.OPEN_TO_WORLD.getValue());
        defaultProperties.put(ZooKeeperStateProvider.KEYSTORE_FILEPATH, CLIENT_KEYSTORE);
        defaultProperties.put(ZooKeeperStateProvider.KEYSTORE_PASSWORD, TEST_PASSWORD);
        defaultProperties.put(ZooKeeperStateProvider.KEYSTORE_TYPE, CLIENT_KEYSTORE_TYPE);
        defaultProperties.put(ZooKeeperStateProvider.TRUSTSTORE_FILEPATH, CLIENT_TRUSTSTORE);
        defaultProperties.put(ZooKeeperStateProvider.TRUSTSTORE_PASSWORD, TEST_PASSWORD);
        defaultProperties.put(ZooKeeperStateProvider.TRUSTSTORE_TYPE, CLIENT_TRUSTSTORE_TYPE);
    }

    @Before
    public void setup() throws Exception {
        tempDir = Paths.get("target/TestSecureClientZooKeeperFactory");
        dataDir = tempDir.resolve("state");
        clientPort = InstanceSpec.getRandomPort();

        Files.createDirectory(tempDir);

        serverConnectionFactory = createAndStartServer(
                dataDir,
                tempDir,
                clientPort,
                SERVER_KEYSTORE,
                TEST_PASSWORD,
                SERVER_TRUSTSTORE,
                TEST_PASSWORD
        );

        zkServer = serverConnectionFactory.getZooKeeperServer();

        final Map<PropertyDescriptor, String> properties = new HashMap<>(defaultProperties);
        properties.put(ZooKeeperStateProvider.CONNECTION_STRING, "localhost:".concat(String.valueOf(clientPort)));
        this.provider = createProvider(properties);
    }

    private void initializeProvider(final ZooKeeperStateProvider provider, final Map<PropertyDescriptor, String> properties) throws IOException {
        provider.initialize(new StateProviderInitializationContext() {
            @Override
            public String getIdentifier() {
                return "Unit Test Provider Initialization Context";
            }

            @Override
            public Map<PropertyDescriptor, PropertyValue> getProperties() {
                final Map<PropertyDescriptor, PropertyValue> propValueMap = new HashMap<>();
                for (final Map.Entry<PropertyDescriptor, String> entry : properties.entrySet()) {
                    propValueMap.put(entry.getKey(), new StandardPropertyValue(entry.getValue(), null, ParameterLookup.EMPTY));
                }
                return propValueMap;
            }

            @Override
            public Map<String,String> getAllProperties() {
                final Map<String,String> propValueMap = new LinkedHashMap<>();
                for (final Map.Entry<PropertyDescriptor, PropertyValue> entry : getProperties().entrySet()) {
                    propValueMap.put(entry.getKey().getName(), entry.getValue().getValue());
                }
                return propValueMap;
            }

            @Override
            public PropertyValue getProperty(final PropertyDescriptor property) {
                final String prop = properties.get(property);
                return new StandardPropertyValue(prop, null, ParameterLookup.EMPTY);
            }

            // This won't be used by the ZooKeeper State Provider. I don't believe there's a way to pass an SSLContext
            // directly to ZooKeeper anyway.
            @Override
            public SSLContext getSSLContext() {
                return null;
            }

            @Override
            public ComponentLog getLogger() {
                return new MockComponentLogger();
            }
        });
    }

    private ZooKeeperStateProvider createProvider(final Map<PropertyDescriptor, String> properties) throws Exception {
        final ZooKeeperStateProvider provider = new ZooKeeperStateProvider();
        initializeProvider(provider, properties);
        provider.enable();
        return provider;
    }

    @After
    public void clear() throws IOException {



        try {
            if (provider != null) {
                provider.onComponentRemoved(componentId);
                provider.disable();
                provider.shutdown();
                clearDirectories();
            }
        } finally {
            if (zkServer != null) {
                zkServer.shutdown(true);
                clearDirectories();
            }
        }
    }

    private static void clearDirectories() {
        if (tempDir != null) {
            final List<Path> files = Arrays.asList(
                    dataDir.resolve("version-2/snapshot.0"),
                    dataDir.resolve("version-2/log.1"),
                    dataDir.resolve("version-2"),
                    dataDir.resolve("myid"),
                    dataDir,
                    tempDir
            );

            files.forEach(p -> {
                try {
                    if (p != null) Files.deleteIfExists(p);
                } catch (final IOException ioe) {}
            });
        }
    }

    @Override
    protected StateProvider getProvider() {
        return provider;
    }

    @Test(timeout = 30000)
    public void testStateTooLargeExceptionThrownOnSetState() throws InterruptedException {
        final Map<String, String> state = new HashMap<>();
        final StringBuilder sb = new StringBuilder();

        // Build a string that is a little less than 64 KB, because that's
        // the largest value available for DataOutputStream.writeUTF
        for (int i = 0; i < 6500; i++) {
            sb.append("0123456789");
        }

        for (int i = 0; i < 20; i++) {
            state.put("numbers." + i, sb.toString());
        }

        while (true) {
            try {
                getProvider().setState(state, componentId);
                Assert.fail("Expected StateTooLargeException");
            } catch (final StateTooLargeException stle) {
                // expected behavior.
                break;
            } catch (final IOException ioe) {
                // If we attempt to interact with the server too quickly, we will get a
                // ZooKeeper ConnectionLoss Exception, which the provider wraps in an IOException.
                // We will wait 1 second in this case and try again. The test will timeout if this
                // does not succeeed within 30 seconds.
                Thread.sleep(1000L);
            } catch (final Exception e) {
                e.printStackTrace();
                Assert.fail("Expected StateTooLargeException but " + e.getClass() + " was thrown", e);
            }
        }
    }

    @Test(timeout = 30000)
    public void testStateTooLargeExceptionThrownOnReplace() throws IOException, InterruptedException {
        final Map<String, String> state = new HashMap<>();
        final StringBuilder sb = new StringBuilder();

        // Build a string that is a little less than 64 KB, because that's
        // the largest value available for DataOutputStream.writeUTF
        for (int i = 0; i < 6500; i++) {
            sb.append("0123456789");
        }

        for (int i = 0; i < 20; i++) {
            state.put("numbers." + i, sb.toString());
        }

        final Map<String, String> smallState = new HashMap<>();
        smallState.put("abc", "xyz");

        while (true) {
            try {
                getProvider().setState(smallState, componentId);
                break;
            } catch (final IOException ioe) {
                // If we attempt to interact with the server too quickly, we will get a
                // ZooKeeper ConnectionLoss Exception, which the provider wraps in an IOException.
                // We will wait 1 second in this case and try again. The test will timeout if this
                // does not succeeed within 30 seconds.
                Thread.sleep(1000L);
            }
        }

        try {
            getProvider().replace(getProvider().getState(componentId), state, componentId);
            Assert.fail("Expected StateTooLargeException");
        } catch (final StateTooLargeException stle) {
            // expected behavior.
        } catch (final Exception e) {
            e.printStackTrace();
            Assert.fail("Expected StateTooLargeException", e);
        }
    }
}
