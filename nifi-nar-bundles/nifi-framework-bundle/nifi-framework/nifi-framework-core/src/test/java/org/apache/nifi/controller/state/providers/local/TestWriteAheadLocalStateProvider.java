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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import javax.net.ssl.SSLContext;

import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.StateProvider;
import org.apache.nifi.components.state.StateProviderInitializationContext;
import org.apache.nifi.controller.state.StateMapUpdate;
import org.apache.nifi.controller.state.providers.AbstractTestStateProvider;
import org.apache.nifi.logging.ComponentLog;
import org.junit.After;
import org.junit.Before;
import org.wali.WriteAheadRepository;

public class TestWriteAheadLocalStateProvider extends AbstractTestStateProvider {
    private StateProvider provider;
    private WriteAheadRepository<StateMapUpdate> wal;

    @Before
    public void setup() throws IOException {
        provider = new WriteAheadLocalStateProvider();
        final Map<PropertyDescriptor, PropertyValue> properties = new HashMap<>();
        properties.put(WriteAheadLocalStateProvider.PATH, new StandardPropertyValue("target/local-state-provider/" + UUID.randomUUID().toString(), null));
        properties.put(WriteAheadLocalStateProvider.ALWAYS_SYNC, new StandardPropertyValue("false", null));
        properties.put(WriteAheadLocalStateProvider.CHECKPOINT_INTERVAL, new StandardPropertyValue("2 mins", null));
        properties.put(WriteAheadLocalStateProvider.NUM_PARTITIONS, new StandardPropertyValue("16", null));

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
            public Map<String,String> getAllProperties() {
                final Map<String,String> propValueMap = new LinkedHashMap<>();
                for (final Map.Entry<PropertyDescriptor, PropertyValue> entry : getProperties().entrySet()) {
                    propValueMap.put(entry.getKey().getName(), entry.getValue().getValue());
                }
                return propValueMap;
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

            @Override
            public ComponentLog getLogger() {
                return null;
            }
        });
    }

    @After
    public void cleanup() throws IOException {
        provider.onComponentRemoved(componentId);

        if (wal != null) {
            wal.shutdown();
        }
    }

    @Override
    protected StateProvider getProvider() {
        return provider;
    }
}
