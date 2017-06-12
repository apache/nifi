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

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.net.ssl.SSLContext;

import org.apache.curator.test.TestingServer;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.StateProvider;
import org.apache.nifi.components.state.StateProviderInitializationContext;
import org.apache.nifi.components.state.exception.StateTooLargeException;
import org.apache.nifi.controller.state.providers.AbstractTestStateProvider;
import org.apache.nifi.logging.ComponentLog;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testng.Assert;

public class TestZooKeeperStateProvider extends AbstractTestStateProvider {

    private volatile StateProvider provider;
    private volatile TestingServer zkServer;

    private static final Map<PropertyDescriptor, String> defaultProperties = new HashMap<>();

    static {
        defaultProperties.put(ZooKeeperStateProvider.SESSION_TIMEOUT, "3 secs");
        defaultProperties.put(ZooKeeperStateProvider.ROOT_NODE, "/nifi/team1/testing");
        defaultProperties.put(ZooKeeperStateProvider.ACCESS_CONTROL, ZooKeeperStateProvider.OPEN_TO_WORLD.getValue());
    }

    @Before
    public void setup() throws Exception {
        zkServer = new TestingServer(true);
        zkServer.start();

        final Map<PropertyDescriptor, String> properties = new HashMap<>(defaultProperties);
        properties.put(ZooKeeperStateProvider.CONNECTION_STRING, zkServer.getConnectString());
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
                    propValueMap.put(entry.getKey(), new StandardPropertyValue(entry.getValue(), null));
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
                return new StandardPropertyValue(prop, null);
            }

            @Override
            public SSLContext getSSLContext() {
                return null;
            }

            @Override
            public ComponentLog getLogger() {
                return null;
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
                zkServer.stop();
                zkServer.close();
            }
        }
    }

    @Override
    protected StateProvider getProvider() {
        return provider;
    }

    @Test(timeout = 20000)
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
                // does not succeeed within 20 seconds.
                Thread.sleep(1000L);
            } catch (final Exception e) {
                e.printStackTrace();
                Assert.fail("Expected StateTooLargeException but " + e.getClass() + " was thrown", e);
            }
        }
    }

    @Test(timeout = 20000)
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
                // does not succeeed within 20 seconds.
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
