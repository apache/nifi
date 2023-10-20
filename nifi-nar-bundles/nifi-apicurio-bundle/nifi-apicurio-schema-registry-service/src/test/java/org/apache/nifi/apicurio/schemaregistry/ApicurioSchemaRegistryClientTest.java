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
package org.apache.nifi.apicurio.schemaregistry;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.apicurio.schemaregistry.client.ApicurioSchemaRegistryClient;
import org.apache.nifi.apicurio.schemaregistry.client.SchemaRegistryApiClient;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ApicurioSchemaRegistryClientTest {

    private static final String TEST_URL = "http://test.apicurio-schema-registry.com:8888";
    private static final String SCHEMA_NAME = "schema1";
    private static final String SEARCH_URL = TEST_URL + "/search";
    private static final String METADATA_URL = TEST_URL + "/meta";
    private static final String SCHEMA_URL = TEST_URL + "/schema";
    private static final String GROUP_ID = "groupId1";
    private static final String ARTIFACT_ID = "artifactId1";
    @Mock
    private SchemaRegistryApiClient apiClient;
    private ApicurioSchemaRegistryClient schemaRegistryClient;

    @BeforeEach
    void setup() {
        doReturn(URI.create(SEARCH_URL)).when(apiClient).buildSearchUri(SCHEMA_NAME);
        doReturn(URI.create(SCHEMA_URL)).when(apiClient).buildSchemaUri(GROUP_ID, ARTIFACT_ID);
        doReturn(getResource("search_response.json")).when(apiClient).retrieveResponse(URI.create(SEARCH_URL));
        doReturn(getResource("schema_response.json")).when(apiClient).retrieveResponse(URI.create(SCHEMA_URL));
    }

    @Test
    void testGetSchemaWithNameInvokesApiClientAndReturnsRecordSchema() throws IOException, SchemaNotFoundException {
        doReturn(URI.create(METADATA_URL)).when(apiClient).buildMetaDataUri(GROUP_ID, ARTIFACT_ID);
        doReturn(getResource("metadata_response.json")).when(apiClient).retrieveResponse(URI.create(METADATA_URL));

        schemaRegistryClient = new ApicurioSchemaRegistryClient(apiClient);

        final RecordSchema schema = schemaRegistryClient.getSchema(SCHEMA_NAME);

        verify(apiClient).buildSearchUri(SCHEMA_NAME);
        verify(apiClient).buildMetaDataUri(GROUP_ID, ARTIFACT_ID);
        verify(apiClient).buildSchemaUri(GROUP_ID, ARTIFACT_ID);

        final String expectedSchemaText = IOUtils.toString(getResource("schema_response.json"))
                .replace("\n", "")
                .replaceAll(" +", "");
        assertEquals(expectedSchemaText, schema.getSchemaText().get());
        assertEquals(expectedSchemaText, schema.getSchemaText().get());
    }

    @Test
    void testGetSchemaWithNameAndVersionInvokesApiClientAndReturnsRecordSchema() throws IOException, SchemaNotFoundException {
        schemaRegistryClient = new ApicurioSchemaRegistryClient(apiClient);

        final RecordSchema schema = schemaRegistryClient.getSchema(SCHEMA_NAME, 3);

        verify(apiClient).buildSearchUri(SCHEMA_NAME);
        verify(apiClient, never()).buildMetaDataUri(GROUP_ID, ARTIFACT_ID);
        verify(apiClient).buildSchemaUri(GROUP_ID, ARTIFACT_ID);

        final String expectedSchemaText = IOUtils.toString(getResource("schema_response.json"))
                .replace("\n", "")
                .replaceAll(" +", "");
        assertEquals(expectedSchemaText, schema.getSchemaText().get());
        assertEquals(expectedSchemaText, schema.getSchemaText().get());
    }


    private InputStream getResource(final String resourceName) {
        return this.getClass().getClassLoader().getResourceAsStream(resourceName);
    }
}
