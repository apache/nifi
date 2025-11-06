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
package org.apache.nifi.registry.extension;

import com.fasterxml.jackson.databind.ObjectMapper;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.RecordedRequest;
import okhttp3.Headers;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.StandardNiFiUser;
import org.apache.nifi.registry.extension.bundle.BundleVersionMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class NiFiRegistryExtensionRegistryTest {
    private static final int REQUEST_TIMEOUT = 2;

    private static final String IDENTIFIER = NiFiRegistryExtensionRegistryTest.class.getSimpleName();

    private static final String API_PATH = "/nifi-registry-api";

    private static final String VERSIONS_PATH = "/nifi-registry-api/bundles/versions";

    private static final String IDENTITY = "username";

    private static final NiFiUser USER = new StandardNiFiUser.Builder().identity(IDENTITY).build();

    private static final String PROXIED_ENTITIES_HEADER = "X-ProxiedEntitiesChain";

    private static final String EXPECTED_PROXIED_ENTITIES = "<username>";

    private static final String BUNDLE_CONTENT_LOCATION = "org.apache.nifi::nifi-extension-nar::2.0.0::nifi-extension";

    private static final String CONTENT_PATH = "/nifi-registry-api/bundles/nifi-extension/versions/2.0.0/content";

    private static final String CONTENT_TYPE_HEADER = "Content-Type";

    private static final String APPLICATION_JSON = "application/json";

    private static final String EMPTY_ARRAY = "[]";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private MockWebServer mockWebServer;

    private NiFiRegistryExtensionRegistry registry;

    @BeforeEach
    void startServer() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();

        final String url = mockWebServer.url(API_PATH).toString();

        registry = new NiFiRegistryExtensionRegistry(IDENTIFIER, url, IDENTIFIER, null);
    }

    @AfterEach
    void shutdownServer() {
        mockWebServer.close();
    }

    @Test
    void testGetExtensionBundleMetadata() throws Exception {
        final MockResponse response = new MockResponse.Builder()
                .code(HttpURLConnection.HTTP_OK)
                .body(EMPTY_ARRAY)
                .build();
        mockWebServer.enqueue(response);

        final Set<NiFiRegistryExtensionBundleMetadata> metadata = registry.getExtensionBundleMetadata(null);

        assertNotNull(metadata);
        assertRequestRecorded(VERSIONS_PATH);
    }

    @Test
    void testGetExtensionBundleMetadataProxyUser() throws Exception {
        final NiFiRegistryExtensionBundleMetadata metadataExpected = NiFiRegistryExtensionBundleMetadata.fromLocationString(BUNDLE_CONTENT_LOCATION)
                .registryIdentifier(IDENTIFIER)
                .build();

        final BundleVersionMetadata versionMetadata = new BundleVersionMetadata();
        versionMetadata.setGroupId(metadataExpected.getGroup());
        versionMetadata.setVersion(metadataExpected.getVersion());
        versionMetadata.setArtifactId(metadataExpected.getArtifact());
        versionMetadata.setBundleId(metadataExpected.getBundleIdentifier());
        final String bundleMetadataBody = objectMapper.writeValueAsString(List.of(versionMetadata));

        final MockResponse response = new MockResponse.Builder()
                .code(HttpURLConnection.HTTP_OK)
                .setHeader(CONTENT_TYPE_HEADER, APPLICATION_JSON)
                .body(bundleMetadataBody)
                .build();
        mockWebServer.enqueue(response);

        final Set<NiFiRegistryExtensionBundleMetadata> metadataFound = registry.getExtensionBundleMetadata(USER);

        assertNotNull(metadataFound);
        final NiFiRegistryExtensionBundleMetadata bundleMetadataFound = metadataFound.iterator().next();
        assertEquals(metadataExpected, bundleMetadataFound);

        final RecordedRequest request = assertRequestRecorded(VERSIONS_PATH);
        assertProxiedEntitiesMatched(request);
    }

    @Test
    void testGetExtensionBundleContent() throws Exception {
        final MockResponse response = new MockResponse.Builder()
                .code(HttpURLConnection.HTTP_OK)
                .build();
        mockWebServer.enqueue(response);

        final NiFiRegistryExtensionBundleMetadata metadata = NiFiRegistryExtensionBundleMetadata.fromLocationString(BUNDLE_CONTENT_LOCATION)
                .registryIdentifier(IDENTIFIER)
                .build();
        final InputStream bundleContent = registry.getExtensionBundleContent(USER, metadata);

        assertNotNull(bundleContent);
        final RecordedRequest request = assertRequestRecorded(CONTENT_PATH);
        assertProxiedEntitiesMatched(request);
    }

    private void assertProxiedEntitiesMatched(final RecordedRequest request) {
        final Headers headers = request.getHeaders();
        final String proxiedEntities = headers.get(PROXIED_ENTITIES_HEADER);
        assertEquals(EXPECTED_PROXIED_ENTITIES, proxiedEntities);
    }

    private RecordedRequest assertRequestRecorded(final String encodedPathExpected) throws InterruptedException {
        final RecordedRequest request = mockWebServer.takeRequest(REQUEST_TIMEOUT, TimeUnit.SECONDS);
        assertNotNull(request);
        final String encodedPath = request.getUrl().encodedPath();
        assertEquals(encodedPathExpected, encodedPath);
        return request;
    }
}
