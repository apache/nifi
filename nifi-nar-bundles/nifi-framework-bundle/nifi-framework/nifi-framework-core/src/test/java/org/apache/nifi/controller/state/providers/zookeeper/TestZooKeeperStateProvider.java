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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.SSLContext;

import org.apache.curator.test.TestingServer;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.StateProvider;
import org.apache.nifi.components.state.StateProviderInitializationContext;
import org.apache.nifi.controller.state.providers.AbstractTestStateProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class TestZooKeeperStateProvider extends AbstractTestStateProvider {

    private StateProvider provider;
    private static TestingServer zkServer;

    @BeforeClass
    public static void createZooKeeper() throws Exception {
        zkServer = new TestingServer(2181, true);
        zkServer.start();
    }

    @AfterClass
    public static void tearDown() throws IOException {
        if (zkServer != null) {
            zkServer.stop();
            zkServer.close();
        }
    }

    @Before
    public void setup() throws Exception {
        final Map<PropertyDescriptor, PropertyValue> properties = new HashMap<>();
        properties.put(ZooKeeperStateProvider.CONNECTION_STRING, new StandardPropertyValue(zkServer.getConnectString(), null));
        properties.put(ZooKeeperStateProvider.SESSION_TIMEOUT, new StandardPropertyValue("3 secs", null));
        properties.put(ZooKeeperStateProvider.ROOT_NODE, new StandardPropertyValue("/nifi/team1/testing", null));

        provider = new ZooKeeperStateProvider();
        provider.initialize(new StateProviderInitializationContext() {
            @Override
            public String getIdentifier() {
                return "Unit Test Provider Initialization Context";
            }

            @Override
            public Map<PropertyDescriptor, PropertyValue> getProperties() {
                return Collections.unmodifiableMap(properties);
            }

            @Override
            public PropertyValue getProperty(final PropertyDescriptor property) {
                final PropertyValue prop = properties.get(property);
                if (prop == null) {
                    return new StandardPropertyValue(null, null);
                }
                return prop;
            }

            @Override
            public SSLContext getSSLContext() {
                return null;
            }
        });

        provider.enable();
    }

    @After
    public void clear() throws IOException {
        getProvider().onComponentRemoved(componentId);
        getProvider().disable();
        getProvider().shutdown();
    }

    @Override
    protected StateProvider getProvider() {
        return provider;
    }
}
