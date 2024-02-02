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
package org.apache.nifi.apicurio.schemaregistry.client;

import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Arrays;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CachingSchemaRegistryClientTest {
    private static final String SCHEMA_NAME = "schema";

    private static final RecordSchema TEST_SCHEMA = new SimpleRecordSchema(Arrays.asList(
            new RecordField("fieldName1", RecordFieldType.INT.getDataType()),
            new RecordField("fieldName2", RecordFieldType.STRING.getDataType())
    ));

    private static final RecordSchema TEST_SCHEMA_2 = new SimpleRecordSchema(Arrays.asList(
            new RecordField("fieldName3", RecordFieldType.INT.getDataType()),
            new RecordField("fieldName4", RecordFieldType.STRING.getDataType())
    ));

    @Mock
    private SchemaRegistryClient mockClient;
    private CachingSchemaRegistryClient cachingClient;

    @BeforeEach
    public void setUp() {
        cachingClient = new CachingSchemaRegistryClient(mockClient, 100, TimeUnit.SECONDS.toNanos(1));
    }

    @Test
    void testGetSchemaWithNameInvokesClientAndCacheResult() throws IOException, SchemaNotFoundException {
        final OptionalInt version = OptionalInt.empty();

        when(mockClient.getSchema(SCHEMA_NAME, version)).thenReturn(TEST_SCHEMA);

        RecordSchema actualSchema1 = cachingClient.getSchema(SCHEMA_NAME, version);
        RecordSchema actualSchema2 = cachingClient.getSchema(SCHEMA_NAME, version);

        assertEquals(TEST_SCHEMA, actualSchema1);
        assertEquals(TEST_SCHEMA, actualSchema2);
        verify(mockClient).getSchema(SCHEMA_NAME, version);
    }

    @Test
    void testGetSchemaWithNameAndVersionInvokesClientAndCacheResult() throws IOException, SchemaNotFoundException {
        final OptionalInt version = OptionalInt.of(1);

        when(mockClient.getSchema(SCHEMA_NAME, version)).thenReturn(TEST_SCHEMA);

        RecordSchema actualSchema1 = cachingClient.getSchema(SCHEMA_NAME, version);
        RecordSchema actualSchema2 = cachingClient.getSchema(SCHEMA_NAME, version);

        assertEquals(TEST_SCHEMA, actualSchema1);
        assertEquals(TEST_SCHEMA, actualSchema2);
        verify(mockClient).getSchema(SCHEMA_NAME, version);
    }

    @Test
    void testGetSchemaWithNameAndVersionDoesNotCacheDifferentVersions() throws IOException, SchemaNotFoundException {
        final OptionalInt version1 = OptionalInt.of(1);
        final OptionalInt version2 = OptionalInt.of(2);
        RecordSchema expectedSchema1 = TEST_SCHEMA;
        RecordSchema expectedSchema2 = TEST_SCHEMA_2;

        when(mockClient.getSchema(SCHEMA_NAME, version1)).thenReturn(expectedSchema1);
        when(mockClient.getSchema(SCHEMA_NAME, version2)).thenReturn(expectedSchema2);

        RecordSchema actualSchema1 = cachingClient.getSchema(SCHEMA_NAME, version1);
        RecordSchema actualSchema2 = cachingClient.getSchema(SCHEMA_NAME, version2);

        assertEquals(expectedSchema1, actualSchema1);
        assertEquals(expectedSchema2, actualSchema2);
        verify(mockClient).getSchema(SCHEMA_NAME, version1);
        verify(mockClient).getSchema(SCHEMA_NAME, version2);
    }
}
