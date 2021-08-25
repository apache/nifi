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
package org.apache.nifi.redis.state;

import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.components.state.StateProvider;
import org.apache.nifi.components.state.StateProviderInitializationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.redis.util.RedisUtils;
import org.apache.nifi.util.MockComponentLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.embedded.RedisServer;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * NOTE: These test cases should be kept in-sync with AbstractTestStateProvider which is in the framework
 * and couldn't be extended here.
 */
public class ITRedisStateProvider {

    protected final String componentId = "111111111-1111-1111-1111-111111111111";

    private RedisServer redisServer;
    private RedisStateProvider provider;

    @BeforeEach
    public void setup() throws Exception {
        final int redisPort = getAvailablePort();

        this.redisServer = new RedisServer(redisPort);
        redisServer.start();

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(RedisUtils.CONNECTION_STRING, "localhost:" + redisPort);
        this.provider = createProvider(properties);
    }

    @AfterEach
    public void teardown() throws IOException {
        if (provider != null) {
            try {
                provider.clear(componentId);
            } catch (IOException e) {
            }
            provider.disable();
            provider.shutdown();
        }

        if (redisServer != null) {
            redisServer.stop();
        }
    }

    public StateProvider getProvider() {
        return provider;
    }

    @Test
    public void testSetAndGet() throws IOException {
        getProvider().setState(Collections.singletonMap("testSetAndGet", "value"), componentId);
        Assertions.assertEquals("value", getProvider().getState(componentId).get("testSetAndGet"));
    }

    @Test
    public void testReplaceSuccessful() throws IOException {
        final String key = "testReplaceSuccessful";
        final StateProvider provider = getProvider();

        StateMap map = provider.getState(componentId);
        Assertions.assertNotNull(map);
        Assertions.assertEquals(-1, map.getVersion());

        Assertions.assertNotNull(map.toMap());
        Assertions.assertTrue(map.toMap().isEmpty());
        provider.setState(Collections.singletonMap(key, "value1"), componentId);

        map = provider.getState(componentId);
        Assertions.assertNotNull(map);
        Assertions.assertEquals(0, map.getVersion());
        Assertions.assertEquals("value1", map.get(key));
        Assertions.assertEquals("value1", map.toMap().get(key));

        final Map<String, String> newMap = new HashMap<>(map.toMap());
        newMap.put(key, "value2");
        Assertions.assertTrue(provider.replace(map, newMap, componentId));

        map = provider.getState(componentId);
        Assertions.assertEquals("value2", map.get(key));
        Assertions.assertEquals(1L, map.getVersion());
    }

    @Test
    public void testReplaceWithWrongVersion() throws IOException {
        final String key = "testReplaceWithWrongVersion";
        final StateProvider provider = getProvider();
        provider.setState(Collections.singletonMap(key, "value1"), componentId);

        StateMap stateMap = provider.getState(componentId);
        Assertions.assertNotNull(stateMap);
        Assertions.assertEquals("value1", stateMap.get(key));
        Assertions.assertEquals(0, stateMap.getVersion());

        provider.setState(Collections.singletonMap(key, "intermediate value"), componentId);

        Assertions.assertFalse(provider.replace(stateMap, Collections.singletonMap(key, "value2"), componentId));
        stateMap = provider.getState(componentId);
        Assertions.assertEquals(key, stateMap.toMap().keySet().iterator().next());
        Assertions.assertEquals(1, stateMap.toMap().size());
        Assertions.assertEquals("intermediate value", stateMap.get(key));
        Assertions.assertEquals(1, stateMap.getVersion());
    }


    @Test
    public void testToMap() throws IOException {
        final String key = "testKeySet";
        final StateProvider provider = getProvider();
        Map<String, String> map = provider.getState(componentId).toMap();
        Assertions.assertNotNull(map);
        Assertions.assertTrue(map.isEmpty());

        provider.setState(Collections.singletonMap(key, "value"), componentId);
        map = provider.getState(componentId).toMap();
        Assertions.assertNotNull(map);
        Assertions.assertEquals(1, map.size());
        Assertions.assertEquals("value", map.get(key));

        provider.setState(Collections.<String, String> emptyMap(), componentId);

        final StateMap stateMap = provider.getState(componentId);
        map = stateMap.toMap();
        Assertions.assertNotNull(map);
        Assertions.assertTrue(map.isEmpty());
        Assertions.assertEquals(1, stateMap.getVersion());
    }

    @Test
    public void testClear() throws IOException {
        final StateProvider provider = getProvider();
        StateMap stateMap = provider.getState(componentId);
        Assertions.assertNotNull(stateMap);
        Assertions.assertEquals(-1L, stateMap.getVersion());
        Assertions.assertTrue(stateMap.toMap().isEmpty());

        provider.setState(Collections.singletonMap("testClear", "value"), componentId);

        stateMap = provider.getState(componentId);
        Assertions.assertNotNull(stateMap);
        Assertions.assertEquals(0, stateMap.getVersion());
        Assertions.assertEquals("value", stateMap.get("testClear"));

        provider.clear(componentId);

        stateMap = provider.getState(componentId);
        Assertions.assertNotNull(stateMap);
        Assertions.assertEquals(1L, stateMap.getVersion());
        Assertions.assertTrue(stateMap.toMap().isEmpty());
    }

    @Test
    public void testReplaceWithNonExistingValue() throws Exception {
        final StateProvider provider = getProvider();
        StateMap stateMap = provider.getState(componentId);
        Assertions.assertNotNull(stateMap);

        final Map<String, String> newValue = new HashMap<>();
        newValue.put("value", "value");

        final boolean replaced = provider.replace(stateMap, newValue, componentId);
        Assertions.assertFalse(replaced);
    }

    @Test
    public void testReplaceWithNonExistingValueAndVersionGreaterThanNegativeOne() throws Exception {
        final StateProvider provider = getProvider();
        final StateMap stateMap = new StateMap() {
            @Override
            public long getVersion() {
                return 4;
            }

            @Override
            public String get(String key) {
                return null;
            }

            @Override
            public Map<String, String> toMap() {
                return Collections.emptyMap();
            }
        };

        final Map<String, String> newValue = new HashMap<>();
        newValue.put("value", "value");

        final boolean replaced = provider.replace(stateMap, newValue, componentId);
        Assertions.assertFalse(replaced);
    }

    @Test
    public void testOnComponentRemoved() throws IOException, InterruptedException {
        final StateProvider provider = getProvider();
        final Map<String, String> newValue = new HashMap<>();
        newValue.put("value", "value");

        provider.setState(newValue, componentId);
        final StateMap stateMap = provider.getState(componentId);
        Assertions.assertEquals(0L, stateMap.getVersion());

        provider.onComponentRemoved(componentId);

        // wait for the background process to complete
        Thread.sleep(1000L);

        final StateMap stateMapAfterRemoval = provider.getState(componentId);

        // version should be -1 because the state has been removed entirely.
        Assertions.assertEquals(-1L, stateMapAfterRemoval.getVersion());
    }


    private void initializeProvider(final RedisStateProvider provider, final Map<PropertyDescriptor, String> properties) throws IOException {
        provider.initialize(new StateProviderInitializationContext() {
            @Override
            public String getIdentifier() {
                return "Unit Test Provider Initialization Context";
            }

            @Override
            public Map<PropertyDescriptor, PropertyValue> getProperties() {
                final Map<PropertyDescriptor, PropertyValue> propValueMap = new HashMap<>();
                for (final Map.Entry<PropertyDescriptor, String> entry : properties.entrySet()) {
                    propValueMap.put(entry.getKey(), new StandardPropertyValue(entry.getValue(), null, null));
                }
                return propValueMap;
            }

            @Override
            public Map<String,String> getAllProperties() {
                final Map<String,String> propValueMap = new LinkedHashMap<>();
                for (final Map.Entry<PropertyDescriptor, String> entry : properties.entrySet()) {
                    propValueMap.put(entry.getKey().getName(), entry.getValue());
                }
                return propValueMap;
            }

            @Override
            public PropertyValue getProperty(final PropertyDescriptor property) {
                String prop = properties.get(property);

                if (prop == null) {
                    prop = property.getDefaultValue();
                }

                return new StandardPropertyValue(prop, null, null);
            }

            @Override
            public SSLContext getSSLContext() {
                return null;
            }

            @Override
            public ComponentLog getLogger() {
                return new MockComponentLog("Unit Test RedisStateProvider", provider);
            }
        });
    }

    private RedisStateProvider createProvider(final Map<PropertyDescriptor, String> properties) throws Exception {
        final RedisStateProvider provider = new RedisStateProvider();
        initializeProvider(provider, properties);
        provider.enable();
        return provider;
    }

    private int getAvailablePort() throws IOException {
        try (SocketChannel socket = SocketChannel.open()) {
            socket.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            socket.bind(new InetSocketAddress("localhost", 0));
            return socket.socket().getLocalPort();
        }
    }

}
