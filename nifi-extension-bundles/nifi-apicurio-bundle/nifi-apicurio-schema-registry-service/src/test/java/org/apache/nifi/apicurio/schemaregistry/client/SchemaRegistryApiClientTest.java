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

import org.apache.nifi.web.client.StandardHttpUriBuilder;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;

@ExtendWith(MockitoExtension.class)
class SchemaRegistryApiClientTest {

    private static final String BASE_URL = "http://test.apicurio-schema-registry.com:8888";
    private static final String API_PATH = "/apis/registry/v2";
    private static final String GROUP_ID = "groupId1";
    private static final String ARTIFACT_ID = "artifactId1";
    private static final int VERSION = 3;
    private static final String GROUP_PATH = String.format("/groups/%s", GROUP_ID);
    private static final String ARTIFACT_PATH = String.format("/artifacts/%s", ARTIFACT_ID);
    private static final String VERSION_PATH = String.format("/versions/%d", VERSION);

    @Mock
    private WebClientServiceProvider webClientServiceProvider;
    private SchemaRegistryApiClient client;

    @BeforeEach
    void setup() {
        doReturn(new StandardHttpUriBuilder()).when(webClientServiceProvider).getHttpUriBuilder();
    }

    @Test
    void testBuildBaseUrl() {
        client = new SchemaRegistryApiClient(webClientServiceProvider, BASE_URL, GROUP_ID);

        final HttpUriBuilder httpUriBuilder = client.buildBaseUri();

        assertEquals(BASE_URL + API_PATH, httpUriBuilder.build().toString());
    }

    @Test
    void testBuildSchemaArtifactUri() {
        client = new SchemaRegistryApiClient(webClientServiceProvider, BASE_URL, GROUP_ID);

        final URI uri = client.buildSchemaArtifactUri(ARTIFACT_ID);

        assertEquals(BASE_URL + API_PATH  + GROUP_PATH + ARTIFACT_PATH, uri.toString());
    }

    @Test
    void testBuildSchemaVersionUri() {
        client = new SchemaRegistryApiClient(webClientServiceProvider, BASE_URL, GROUP_ID);

        final URI uri = client.buildSchemaVersionUri(ARTIFACT_ID, VERSION);

        assertEquals(BASE_URL + API_PATH + GROUP_PATH + ARTIFACT_PATH + VERSION_PATH, uri.toString());
    }
}
