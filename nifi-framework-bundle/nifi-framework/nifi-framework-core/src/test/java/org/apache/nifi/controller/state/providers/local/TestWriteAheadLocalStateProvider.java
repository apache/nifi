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

package org.apache.nifi.controller.state.providers.local;

import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.StateProvider;
import org.apache.nifi.components.state.StateProviderInitializationContext;
import org.apache.nifi.controller.state.StateMapUpdate;
import org.apache.nifi.controller.state.providers.AbstractTestStateProvider;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.parameter.ParameterLookup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.wali.WriteAheadRepository;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.net.ssl.SSLContext;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestWriteAheadLocalStateProvider extends AbstractTestStateProvider {
    @TempDir
    private Path temporaryDirectory;

    private StateProvider provider;
    private WriteAheadRepository<StateMapUpdate> wal;

    @BeforeEach
    public void setup() throws IOException {
        provider = initializeProvider(temporaryDirectory.resolve("setup").toString());
    }

    @AfterEach
    public void cleanup() throws IOException {
        provider.onComponentRemoved(componentId);
        provider.shutdown();

        if (wal != null) {
            wal.shutdown();
        }
    }

    @Override
    protected StateProvider getProvider() {
        return provider;
    }

    /**
     * Verifies that recovered state reflects the values that the caller passed to
     * {@link StateProvider#setState(Map, String)} at the moment of that call, and is
     * not influenced by subsequent mutations the caller performs to the same Map
     * instance after returning. This guards against a corruption scenario where a
     * caller (such as an extension that holds onto its own ConcurrentHashMap) keeps
     * mutating its state map between calls to setState while the framework's
     * checkpoint thread is concurrently serializing a snapshot. The provider must
     * isolate its persisted state from any further mutation of the caller's map.
     */
    @Test
    public void testRecoveredStateIsolatedFromPostSetStateMapMutations() throws Exception {
        final String storageDirectory = temporaryDirectory.resolve("recovery").toString();
        final String testComponentId = "test-mutation-isolation-component";

        final WriteAheadLocalStateProvider firstProvider = initializeProvider(storageDirectory);
        final Map<String, String> liveStateValues = new HashMap<>();
        liveStateValues.put("key1", "value1");
        firstProvider.setState(liveStateValues, testComponentId);

        liveStateValues.put("key2", "value2");
        liveStateValues.put("key3", "value3");

        firstProvider.checkpoint();
        firstProvider.shutdown();

        final WriteAheadLocalStateProvider recoveredProvider = initializeProvider(storageDirectory);
        try {
            final Map<String, String> recoveredValues = recoveredProvider.getState(testComponentId).toMap();
            assertEquals(Collections.singletonMap("key1", "value1"), recoveredValues);
        } finally {
            recoveredProvider.shutdown();
        }
    }

    private WriteAheadLocalStateProvider initializeProvider(final String storageDirectory) throws IOException {
        final WriteAheadLocalStateProvider newProvider = new WriteAheadLocalStateProvider();
        final Map<PropertyDescriptor, PropertyValue> properties = new HashMap<>();
        properties.put(WriteAheadLocalStateProvider.PATH, new StandardPropertyValue(storageDirectory, null, ParameterLookup.EMPTY));
        properties.put(WriteAheadLocalStateProvider.ALWAYS_SYNC, new StandardPropertyValue("false", null, ParameterLookup.EMPTY));
        properties.put(WriteAheadLocalStateProvider.CHECKPOINT_INTERVAL, new StandardPropertyValue("2 mins", null, ParameterLookup.EMPTY));
        properties.put(WriteAheadLocalStateProvider.NUM_PARTITIONS, new StandardPropertyValue("16", null, ParameterLookup.EMPTY));

        newProvider.initialize(new StateProviderInitializationContext() {
            @Override
            public String getIdentifier() {
                return "Unit Test Provider Initialization Context";
            }

            @Override
            public Map<PropertyDescriptor, PropertyValue> getProperties() {
                return Collections.unmodifiableMap(properties);
            }

            @Override
            public Map<String, String> getAllProperties() {
                final Map<String, String> propValueMap = new LinkedHashMap<>();
                for (final Map.Entry<PropertyDescriptor, PropertyValue> entry : getProperties().entrySet()) {
                    propValueMap.put(entry.getKey().getName(), entry.getValue().getValue());
                }
                return propValueMap;
            }

            @Override
            public PropertyValue getProperty(final PropertyDescriptor property) {
                final PropertyValue prop = properties.get(property);
                if (prop == null) {
                    return new StandardPropertyValue(null, null, ParameterLookup.EMPTY);
                }
                return prop;
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

        return newProvider;
    }
}
