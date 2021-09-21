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

import org.apache.commons.io.FileUtils;
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
import org.apache.nifi.security.util.TemporaryKeyStoreBuilder;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.util.NiFiProperties;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.nifi.leader.election.ITSecureClientZooKeeperFactory.createAndStartServer;
import static org.apache.nifi.leader.election.ITSecureClientZooKeeperFactory.createSecureClientProperties;

public class ITZooKeeperStateProvider extends AbstractTestStateProvider {

    private static final Logger logger = LoggerFactory.getLogger(ITZooKeeperStateProvider.class);

    private volatile StateProvider provider;
    private volatile ZooKeeperServer zkServer;
    private static Map<PropertyDescriptor, String> stateProviderProperties = new HashMap<>();
    private static Path tempDir;
    private static Path dataDir;
    private static int clientPort;
    private static ServerCnxnFactory serverConnectionFactory;
    private static NiFiProperties nifiProperties;

    private static TlsConfiguration tlsConfiguration;

    @BeforeClass
    public static void setTlsConfiguration() {
        tlsConfiguration = new TemporaryKeyStoreBuilder().build();
    }

    @Before
    public void setup() throws Exception {
        tempDir = Paths.get("target/TestZooKeeperStateProvider");
        dataDir = tempDir.resolve("state");
        clientPort = InstanceSpec.getRandomPort();

        Files.createDirectory(tempDir);

        // Set up the testing server
        serverConnectionFactory = createAndStartServer(
                dataDir,
                tempDir,
                clientPort,
                Paths.get(tlsConfiguration.getKeystorePath()),
                tlsConfiguration.getKeystorePassword(),
                Paths.get(tlsConfiguration.getTruststorePath()),
                tlsConfiguration.getTruststorePassword()
        );
        zkServer = serverConnectionFactory.getZooKeeperServer();

        // Set up state provider (client) TLS properties, normally injected through StateProviderContext annotation
        nifiProperties = createSecureClientProperties(
                clientPort,
                Paths.get(tlsConfiguration.getKeystorePath()),
                tlsConfiguration.getKeystoreType().getType(),
                tlsConfiguration.getKeystorePassword(),
                Paths.get(tlsConfiguration.getTruststorePath()),
                tlsConfiguration.getTruststoreType().getType(),
                tlsConfiguration.getTruststorePassword()
                );

        // Set up state provider properties
        stateProviderProperties.put(ZooKeeperStateProvider.SESSION_TIMEOUT, "15 secs");
        stateProviderProperties.put(ZooKeeperStateProvider.ROOT_NODE, "/nifi/team1/testing");
        stateProviderProperties.put(ZooKeeperStateProvider.ACCESS_CONTROL, ZooKeeperStateProvider.OPEN_TO_WORLD.getValue());
        final Map<PropertyDescriptor, String> properties = new HashMap<>(stateProviderProperties);
        properties.put(ZooKeeperStateProvider.CONNECTION_STRING, "localhost:".concat(String.valueOf(clientPort)));
        this.provider = createProvider(properties);
    }

    private void initializeProvider(final ZooKeeperStateProvider provider, final Map<PropertyDescriptor, String> properties) throws IOException {
        provider.setNiFiProperties(nifiProperties);
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

                propValueMap.put(NiFiProperties.ZOOKEEPER_CLIENT_SECURE, Boolean.TRUE.toString());
                propValueMap.put(NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE, tlsConfiguration.getKeystorePath());
                propValueMap.put(NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_PASSWD, tlsConfiguration.getKeystorePassword());
                propValueMap.put(NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_TYPE, tlsConfiguration.getKeystoreType().getType());
                propValueMap.put(NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE, tlsConfiguration.getTruststorePath());
                propValueMap.put(NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE_PASSWD, tlsConfiguration.getTruststorePassword());
                propValueMap.put(NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE_TYPE, tlsConfiguration.getTruststoreType().getType());

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
            }
        } finally {
            if (zkServer != null) {
                zkServer.shutdown(true);
                clearDirectories();
            }
        }
    }

    private static void clearDirectories() {
        try {
            FileUtils.deleteDirectory(new File(tempDir.toString()));
        } catch (IOException e) {
            logger.error("Failed to delete: " + tempDir.toString(), e);
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
                logger.error("Something went wrong attempting to set the state in testStateTooLargeExceptionThrownOnSetState()");
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
            logger.error("Something went wrong in attempting to get the state in testStateTooLargeExceptionThrownOnReplace()");
            Assert.fail("Expected StateTooLargeException", e);
        }
    }
}
