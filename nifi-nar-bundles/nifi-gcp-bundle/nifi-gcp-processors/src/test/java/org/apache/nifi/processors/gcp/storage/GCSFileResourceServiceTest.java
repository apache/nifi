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
package org.apache.nifi.processors.gcp.storage;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.testing.RemoteStorageHelper;
import org.apache.nifi.fileresource.service.api.FileResource;
import org.apache.nifi.gcp.credentials.service.GCPCredentialsService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.gcp.credentials.service.GCPCredentialsControllerService;
import org.apache.nifi.processors.gcp.util.MockReadChannel;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.processors.gcp.util.GoogleUtils.GCP_CREDENTIALS_PROVIDER_SERVICE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class GCSFileResourceServiceTest {

    private static final String TEST_NAME = GCSFileResourceServiceTest.class.getSimpleName();
    private static final String CONTROLLER_SERVICE = "GCPCredentialsService";
    private static final String BUCKET = RemoteStorageHelper.generateBucketName();
    private static final String KEY = "test-file";
    private static final BlobId BLOB_ID = BlobId.of(BUCKET, KEY);
    private static final String TEST_DATA = "test-data";

    @Mock
    Storage storage;

    private TestRunner runner;
    private TestGCSFileResourceService service;

    @BeforeEach
    void setup() throws InitializationException {
        service = new TestGCSFileResourceService(storage);
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.addControllerService(TEST_NAME, service);
    }

    @Test
    void testValidBlob(@Mock Blob blob) throws InitializationException, IOException {
        when(blob.getBlobId()).thenReturn(BLOB_ID);
        when(blob.getSize()).thenReturn((long) TEST_DATA.length());
        when(storage.get(BLOB_ID)).thenReturn(blob);
        when(storage.reader(BLOB_ID)).thenReturn(new MockReadChannel(TEST_DATA));

        setUpService(KEY, BUCKET);

        final FileResource fileResource = service.getFileResource(Collections.emptyMap());

        assertFileResource(fileResource);
    }

    @Test
    void testValidBlobUsingEL(@Mock Blob blob) throws IOException, InitializationException {
        when(blob.getBlobId()).thenReturn(BLOB_ID);
        when(blob.getSize()).thenReturn((long) TEST_DATA.length());
        when(storage.get(BLOB_ID)).thenReturn(blob);
        when(storage.reader(BLOB_ID)).thenReturn(new MockReadChannel(TEST_DATA));

        final Map<String, String> attributes = setUpServiceWithEL(KEY, BUCKET);

        final FileResource fileResource = service.getFileResource(attributes);

        assertFileResource(fileResource);
    }

    @Test
    void testValidBlobUsingELButMissingAttribute() throws InitializationException {
        runner.setValidateExpressionUsage(false);

        setUpServiceWithEL(KEY, BUCKET);

        assertThrows(IllegalArgumentException.class, () -> service.getFileResource(Collections.emptyMap()));
    }

    @Test
    void testNonExistingBlob() throws InitializationException {
        final Map<String, String> attributes = setUpServiceWithEL("invalid-key", "invalid-bucket");

        assertThrows(ProcessException.class, () -> service.getFileResource(attributes));
    }

    private void setUpService(String key, String bucket) throws InitializationException {
        final GCPCredentialsService credentialsService = new GCPCredentialsControllerService();

        runner.addControllerService(CONTROLLER_SERVICE, credentialsService);
        runner.enableControllerService(credentialsService);

        runner.setProperty(service, GCP_CREDENTIALS_PROVIDER_SERVICE, CONTROLLER_SERVICE);
        runner.setProperty(service, GCSFileResourceService.KEY, key);
        runner.setProperty(service, GCSFileResourceService.BUCKET, bucket);

        runner.enableControllerService(service);
    }

    private Map<String, String> setUpServiceWithEL(String key, String bucket) throws InitializationException {
        final String keyAttribute = "file.key";
        final String bucketAttribute = "file.bucket";

        Map<String, String> attributes = new HashMap<>();
        attributes.put(keyAttribute, key);
        attributes.put(bucketAttribute, bucket);

        setUpService(String.format("${%s}", keyAttribute), String.format("${%s}", bucketAttribute));

        return attributes;
    }

    private void assertFileResource(FileResource fileResource) throws  IOException {
        assertNotNull(fileResource);
        assertEquals(TEST_DATA.length(), fileResource.getSize());
        try (final InputStream inputStream = fileResource.getInputStream()) {
            assertArrayEquals(TEST_DATA.getBytes(), inputStream.readAllBytes());
        }
    }

    private static class TestGCSFileResourceService extends GCSFileResourceService {

        private final Storage storage;

        public TestGCSFileResourceService(Storage storage) {
            this.storage = storage;
        }

        @Override
        protected Storage getCloudService(GoogleCredentials credentials) {
            return storage;
        }
    }
}
