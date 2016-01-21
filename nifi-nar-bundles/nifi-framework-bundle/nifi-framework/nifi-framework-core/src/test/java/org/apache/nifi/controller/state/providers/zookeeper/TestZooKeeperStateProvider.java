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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLContext;

import org.apache.curator.test.TestingServer;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.StateProvider;
import org.apache.nifi.components.state.StateProviderInitializationContext;
import org.apache.nifi.controller.state.providers.AbstractTestStateProvider;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testng.Assert;

public class TestZooKeeperStateProvider extends AbstractTestStateProvider {

    private StateProvider provider;
    private TestingServer zkServer;

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
            public PropertyValue getProperty(final PropertyDescriptor property) {
                final String prop = properties.get(property);
                return new StandardPropertyValue(prop, null);
            }

            @Override
            public SSLContext getSSLContext() {
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
            getProvider().onComponentRemoved(componentId);
            getProvider().disable();
            getProvider().shutdown();
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

    @Test
    public void testWithUsernameAndPasswordCreatorOnly() throws Exception {
        final Map<PropertyDescriptor, String> properties = new HashMap<>(defaultProperties);
        properties.put(ZooKeeperStateProvider.CONNECTION_STRING, zkServer.getConnectString());
        properties.put(ZooKeeperStateProvider.USERNAME, "nifi");
        properties.put(ZooKeeperStateProvider.PASSWORD, "nifi");
        properties.put(ZooKeeperStateProvider.ACCESS_CONTROL, ZooKeeperStateProvider.CREATOR_ONLY.getValue());

        final ZooKeeperStateProvider authorizedProvider = createProvider(properties);

        try {
            final Map<String, String> state = new HashMap<>();
            state.put("testWithUsernameAndPasswordCreatorOnly", "my value");
            authorizedProvider.setState(state, componentId);

            final List<ACL> acls = authorizedProvider.getZooKeeper().getACL(properties.get(ZooKeeperStateProvider.ROOT_NODE) + "/components/" + componentId, new Stat());
            assertNotNull(acls);
            assertEquals(1, acls.size());
            final ACL acl = acls.get(0);
            assertEquals(Perms.ALL, acl.getPerms());
            // ID is our username:<SHA1 hash>
            assertEquals("nifi:RuSeH3tpzgba3p9WrG/UpiSIsGg=", acl.getId().getId());

            final Map<String, String> stateValues = authorizedProvider.getState(componentId).toMap();
            assertEquals(state, stateValues);

            // ensure that our default provider cannot access the data, since it has not authenticated
            try {
                this.provider.getState(componentId);
                Assert.fail("Expected an IOException but it wasn't thrown");
            } catch (final IOException ioe) {
                final Throwable cause = ioe.getCause();
                assertTrue(cause instanceof KeeperException);
                final KeeperException ke = (KeeperException) cause;
                assertEquals(Code.NOAUTH, ke.code());
            }
        } finally {
            authorizedProvider.onComponentRemoved(componentId);
            authorizedProvider.disable();
            authorizedProvider.shutdown();
        }
    }

    @Test
    public void testWithUsernameAndPasswordOpen() throws Exception {
        final Map<PropertyDescriptor, String> properties = new HashMap<>(defaultProperties);
        properties.put(ZooKeeperStateProvider.CONNECTION_STRING, zkServer.getConnectString());
        properties.put(ZooKeeperStateProvider.USERNAME, "nifi");
        properties.put(ZooKeeperStateProvider.PASSWORD, "nifi");
        properties.put(ZooKeeperStateProvider.ACCESS_CONTROL, ZooKeeperStateProvider.OPEN_TO_WORLD.getValue());

        final ZooKeeperStateProvider authorizedProvider = createProvider(properties);

        try {
            final Map<String, String> state = new HashMap<>();
            state.put("testWithUsernameAndPasswordOpen", "my value");
            authorizedProvider.setState(state, componentId);

            final List<ACL> acls = authorizedProvider.getZooKeeper().getACL(properties.get(ZooKeeperStateProvider.ROOT_NODE) + "/components/" + componentId, new Stat());
            assertNotNull(acls);
            assertEquals(1, acls.size());
            final ACL acl = acls.get(0);
            assertEquals(Perms.ALL, acl.getPerms());
            assertEquals("anyone", acl.getId().getId());

            final Map<String, String> stateValues = authorizedProvider.getState(componentId).toMap();
            assertEquals(state, stateValues);

            // ensure that our default provider can also access the data, since it has not authenticated
            final Map<String, String> unauthStateValues = this.provider.getState(componentId).toMap();
            assertEquals(state, unauthStateValues);
        } finally {
            authorizedProvider.onComponentRemoved(componentId);
            authorizedProvider.disable();
            authorizedProvider.shutdown();
        }
    }
}
