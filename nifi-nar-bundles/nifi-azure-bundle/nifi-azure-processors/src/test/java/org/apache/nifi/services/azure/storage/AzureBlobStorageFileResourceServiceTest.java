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

package org.apache.nifi.services.azure.storage;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.specialized.BlobInputStream;
import org.apache.nifi.fileresource.service.api.FileResource;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.services.azure.AzureCredentialsService;
import org.apache.nifi.services.azure.StandardAzureCredentialsControllerService;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.BLOB_STORAGE_CREDENTIALS_SERVICE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AzureBlobStorageFileResourceServiceTest {
    private static final String CONTROLLER_SERVICE = "AzureCredentialsService";
    private static final String CONTAINER = "container-name";
    private static final String BLOB_NAME = "test-file";
    private static final long CONTENT_LENGTH = 10L;

    @Mock
    private BlobServiceClient client;

    @Mock
    private BlobContainerClient containerClient;

    @Mock
    private BlobClient blobClient;

    @Mock
    private BlobProperties blobProperties;

    @Mock
    private BlobInputStream blobInputStream;

    @InjectMocks
    private TestAzureBlobStorageFileResourceService service;

    private TestRunner runner;

    @BeforeEach
    void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.addControllerService("AzureBlobStorageFileResourceService", service);
    }

    @Test
    void testValidBlob() throws InitializationException {
        setupService();
        setupMocking(CONTAINER, BLOB_NAME);

        final FileResource fileResource = service.getFileResource(Map.of());

        assertFileResource(fileResource);
        verifyMockInvocations(CONTAINER, BLOB_NAME);
    }

    @Test
    void testValidBlobWithEL() throws InitializationException {
        String customContainer = "custom-container";
        String customBlobName = "custom-blob-name";
        String blobKey = "blob.name";
        String containerKey = "container.name";
        setupService(String.format("${%s}", blobKey), String.format("${%s}", containerKey));
        setupMocking(customContainer, customBlobName);
        runner.setValidateExpressionUsage(false);

        final FileResource fileResource = service.getFileResource(Map.of(
                blobKey, customBlobName,
                containerKey, customContainer));

        assertFileResource(fileResource);
        verifyMockInvocations(customContainer, customBlobName);
    }

    @Test
    void testNonExistingBlob() throws InitializationException {
        setupService();
        when(client.getBlobContainerClient(CONTAINER)).thenReturn(containerClient);
        when(containerClient.getBlobClient(BLOB_NAME)).thenReturn(blobClient);
        when(blobClient.exists()).thenReturn(false);

        assertThrows(ProcessException.class,
                () -> service.getFileResource(Map.of()),
                "Failed to fetch blob from Azure Blob Storage");
    }

    @Test
    void testELWithMissingAttribute() throws InitializationException {
        runner.setValidateExpressionUsage(false);

        setupService(String.format("${%s}", BLOB_NAME), String.format("${%s}", CONTAINER));

        assertThrows(ProcessException.class,
                () -> service.getFileResource(Map.of()),
                "Container name and blob name cannot be empty");
    }

    private void setupService() throws InitializationException {
        setupService(BLOB_NAME, CONTAINER);
    }

    private void setupService(String blobName, String container) throws InitializationException {
        final AzureCredentialsService credentialsService = new StandardAzureCredentialsControllerService();

        runner.addControllerService(CONTROLLER_SERVICE, credentialsService);
        runner.enableControllerService(credentialsService);

        runner.setProperty(service, BLOB_STORAGE_CREDENTIALS_SERVICE, CONTROLLER_SERVICE);
        runner.setProperty(service, AzureBlobStorageFileResourceService.BLOB_NAME, blobName);
        runner.setProperty(service, AzureBlobStorageFileResourceService.CONTAINER, container);

        runner.enableControllerService(service);
    }

    private void setupMocking(String container, String blobName) {
        when(client.getBlobContainerClient(container)).thenReturn(containerClient);
        when(containerClient.getBlobClient(blobName)).thenReturn(blobClient);
        when(blobClient.exists()).thenReturn(true);
        when(blobClient.getProperties()).thenReturn(blobProperties);
        when(blobProperties.getBlobSize()).thenReturn(CONTENT_LENGTH);
        when(blobClient.openInputStream()).thenReturn(blobInputStream);
    }

    private void assertFileResource(FileResource fileResource) {
        assertNotNull(fileResource);
        assertEquals(fileResource.getInputStream(), blobInputStream);
        assertEquals(fileResource.getSize(), CONTENT_LENGTH);
    }

    private void verifyMockInvocations(String customContainer, String customBlobName) {
        verify(client).getBlobContainerClient(customContainer);
        verify(containerClient).getBlobClient(customBlobName);
        verify(blobClient).exists();
        verify(blobClient).getProperties();
        verify(blobProperties).getBlobSize();
        verify(blobClient).openInputStream();
        verifyNoMoreInteractions(containerClient, blobClient, blobProperties);
    }

    private static class TestAzureBlobStorageFileResourceService extends AzureBlobStorageFileResourceService {

        private final BlobServiceClient client;

        public TestAzureBlobStorageFileResourceService(BlobServiceClient client) {
            this.client = client;
        }

        @Override
        protected BlobServiceClient getStorageClient(Map<String, String> attributes) {
            return client;
        }
    }

}
