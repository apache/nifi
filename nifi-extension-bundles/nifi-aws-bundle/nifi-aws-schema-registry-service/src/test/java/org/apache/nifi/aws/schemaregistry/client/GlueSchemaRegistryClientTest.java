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
package org.apache.nifi.aws.schemaregistry.client;

import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GlueSchemaRegistryClientTest {
    private static final String SCHEMA_NAME = "schema";
    private static final String REGISTRY_NAME = "registry";
    private static final String SCHEMA_DEFINITION = "{\"namespace\":\"com.example\",\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}";
    private static final GetSchemaVersionResponse MOCK_RESPONSE = GetSchemaVersionResponse.builder()
            .schemaDefinition(SCHEMA_DEFINITION)
            .versionNumber(1L)
            .build();
    private static final String EXPECTED_SCHEMA_NAMESPACE = "com.example";
    private static final String EXPECTED_SCHEMA_NAME = "User";
    @Mock
    private GlueClient mockClient;
    private GlueSchemaRegistryClient schemaRegistryClient;

    @Test
    void testGetSchemaWithNameInvokesClientAndReturnsRecordSchema() throws IOException, SchemaNotFoundException {
        schemaRegistryClient = new GlueSchemaRegistryClient(mockClient, REGISTRY_NAME);

        when(mockClient.getSchemaVersion(any(GetSchemaVersionRequest.class))).thenReturn(MOCK_RESPONSE);

        final RecordSchema actualSchema = schemaRegistryClient.getSchema(SCHEMA_NAME);

        assertNotNull(actualSchema);
        assertEquals(EXPECTED_SCHEMA_NAMESPACE, actualSchema.getSchemaNamespace().orElseThrow(() -> new RuntimeException("Schema namespace not found")));
        assertEquals(EXPECTED_SCHEMA_NAME, actualSchema.getSchemaName().orElseThrow(() -> new RuntimeException("Schema name not found")));
        verify(mockClient).getSchemaVersion(any(GetSchemaVersionRequest.class));
    }

    @Test
    void getSchemaWithNameAndVersionInvokesClientAndReturnsRecordSchema() throws IOException, SchemaNotFoundException {
        int version = 1;

        final GetSchemaVersionResponse mockResponse = GetSchemaVersionResponse.builder()
                .schemaDefinition(SCHEMA_DEFINITION)
                .versionNumber(1L)
                .build();

        schemaRegistryClient = new GlueSchemaRegistryClient(mockClient, REGISTRY_NAME);

        when(mockClient.getSchemaVersion(any(GetSchemaVersionRequest.class))).thenReturn(mockResponse);

        final RecordSchema actualSchema = schemaRegistryClient.getSchema(SCHEMA_NAME, version);

        assertNotNull(actualSchema);
        assertEquals(EXPECTED_SCHEMA_NAMESPACE, actualSchema.getSchemaNamespace().orElseThrow(() -> new RuntimeException("Schema namespace not found")));
        assertEquals(EXPECTED_SCHEMA_NAME, actualSchema.getSchemaName().orElseThrow(() -> new RuntimeException("Schema name not found")));
        verify(mockClient).getSchemaVersion(any(GetSchemaVersionRequest.class));
    }
}
