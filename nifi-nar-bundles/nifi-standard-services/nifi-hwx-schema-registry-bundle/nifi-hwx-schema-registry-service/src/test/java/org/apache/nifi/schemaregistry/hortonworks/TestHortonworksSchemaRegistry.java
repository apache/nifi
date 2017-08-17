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

package org.apache.nifi.schemaregistry.hortonworks;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockConfigurationContext;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;

public class TestHortonworksSchemaRegistry {
    private HortonworksSchemaRegistry registry;
    private SchemaRegistryClient client;

    private final Map<String, SchemaVersionInfo> schemaVersionInfoMap = new HashMap<>();
    private final Map<String, SchemaMetadataInfo> schemaMetadataInfoMap = new HashMap<>();

    @Before
    public void setup() throws SchemaNotFoundException {
        schemaVersionInfoMap.clear();
        schemaMetadataInfoMap.clear();

        client = mock(SchemaRegistryClient.class);
        doAnswer(new Answer<SchemaVersionInfo>() {
            @Override
            public SchemaVersionInfo answer(final InvocationOnMock invocation) throws Throwable {
                final String schemaName = invocation.getArgumentAt(0, String.class);
                final SchemaVersionInfo info = schemaVersionInfoMap.get(schemaName);

                if (info == null) {
                    throw new SchemaNotFoundException();
                }

                return info;
            }
        }).when(client).getLatestSchemaVersionInfo(any(String.class));

        doAnswer(new Answer<SchemaMetadataInfo>() {
            @Override
            public SchemaMetadataInfo answer(InvocationOnMock invocation) throws Throwable {
                final String schemaName = invocation.getArgumentAt(0, String.class);
                final SchemaMetadataInfo info = schemaMetadataInfoMap.get(schemaName);

                if (info == null) {
                    throw new SchemaNotFoundException();
                }

                return info;
            }
        }).when(client).getSchemaMetadataInfo(any(String.class));

        registry = new HortonworksSchemaRegistry() {
            @Override
            protected synchronized SchemaRegistryClient getClient() {
                return client;
            }
        };
    }

    @Test
    public void testCacheUsed() throws Exception {
        final String text = new String(Files.readAllBytes(Paths.get("src/test/resources/empty-schema.avsc")));
        final SchemaVersionInfo info = new SchemaVersionInfo(1L, "unit-test", 2, text, System.currentTimeMillis(), "description");
        schemaVersionInfoMap.put("unit-test", info);

        final SchemaMetadata metadata = new SchemaMetadata.Builder("unit-test")
            .compatibility(SchemaCompatibility.NONE)
            .evolve(true)
            .schemaGroup("group")
            .type("AVRO")
            .build();

        final Constructor<SchemaMetadataInfo> ctr = SchemaMetadataInfo.class.getDeclaredConstructor(SchemaMetadata.class, Long.class, Long.class);
        ctr.setAccessible(true);

        final SchemaMetadataInfo schemaMetadataInfo = ctr.newInstance(metadata, 1L, System.currentTimeMillis());

        schemaMetadataInfoMap.put("unit-test", schemaMetadataInfo);

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(HortonworksSchemaRegistry.URL, "http://localhost:44444");
        properties.put(HortonworksSchemaRegistry.CACHE_EXPIRATION, "5 mins");
        properties.put(HortonworksSchemaRegistry.CACHE_SIZE, "1000");

        final ConfigurationContext configurationContext = new MockConfigurationContext(properties, null);
        registry.enable(configurationContext);

        for (int i = 0; i < 10000; i++) {
            final RecordSchema schema = registry.retrieveSchema("unit-test");
            assertNotNull(schema);
        }

        Mockito.verify(client, Mockito.times(1)).getLatestSchemaVersionInfo(any(String.class));
    }

    @Test
    @Ignore("This can be useful for manual testing/debugging, but will keep ignored for now because we don't want automated builds to run this, since it depends on timing")
    public void testCacheExpires() throws Exception {
        final String text = new String(Files.readAllBytes(Paths.get("src/test/resources/empty-schema.avsc")));
        final SchemaVersionInfo info = new SchemaVersionInfo(1L, "unit-test", 2,  text, System.currentTimeMillis(), "description");
        schemaVersionInfoMap.put("unit-test", info);

        final SchemaMetadata metadata = new SchemaMetadata.Builder("unit-test")
            .compatibility(SchemaCompatibility.NONE)
            .evolve(true)
            .schemaGroup("group")
            .type("AVRO")
            .build();

        final Constructor<SchemaMetadataInfo> ctr = SchemaMetadataInfo.class.getDeclaredConstructor(SchemaMetadata.class, Long.class, Long.class);
        ctr.setAccessible(true);

        final SchemaMetadataInfo schemaMetadataInfo = ctr.newInstance(metadata, 1L, System.currentTimeMillis());

        schemaMetadataInfoMap.put("unit-test", schemaMetadataInfo);

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(HortonworksSchemaRegistry.URL, "http://localhost:44444");
        properties.put(HortonworksSchemaRegistry.CACHE_EXPIRATION, "1 sec");
        properties.put(HortonworksSchemaRegistry.CACHE_SIZE, "1000");

        final ConfigurationContext configurationContext = new MockConfigurationContext(properties, null);
        registry.enable(configurationContext);

        for (int i = 0; i < 2; i++) {
            final RecordSchema schema = registry.retrieveSchema("unit-test");
            assertNotNull(schema);
        }

        Mockito.verify(client, Mockito.times(1)).getLatestSchemaVersionInfo(any(String.class));

        Thread.sleep(2000L);

        for (int i = 0; i < 2; i++) {
            final RecordSchema schema = registry.retrieveSchema("unit-test");
            assertNotNull(schema);
        }

        Mockito.verify(client, Mockito.times(2)).getLatestSchemaVersionInfo(any(String.class));
    }

}
