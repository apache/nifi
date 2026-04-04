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
package org.apache.nifi.processors.azure.storage;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.BLOB_STORAGE_CREDENTIALS_SERVICE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestGetAzureBlobStorageMetadata_v12 {

    private static final String CONTAINER_NAME = "test-container";
    private static final String BLOB_NAME = "test-blob";

    private TestRunner runner;
    private BlobServiceClient storageClient;
    private BlobClient blobClient;
    private BlobProperties blobProperties;

    @BeforeEach
    void setUp() {
        storageClient = mock(BlobServiceClient.class);
        final BlobContainerClient containerClient = mock(BlobContainerClient.class);
        blobClient = mock(BlobClient.class);
        blobProperties = mock(BlobProperties.class);

        when(storageClient.getBlobContainerClient(CONTAINER_NAME)).thenReturn(containerClient);
        when(containerClient.getBlobClient(BLOB_NAME)).thenReturn(blobClient);
        when(blobClient.getProperties()).thenReturn(blobProperties);

        final GetAzureBlobStorageMetadata_v12 processor = new GetAzureBlobStorageMetadata_v12() {
            @Override
            protected BlobServiceClient getStorageClient(PropertyContext context, FlowFile flowFile) {
                return storageClient;
            }

            @Override
            protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
                return super.getSupportedPropertyDescriptors().stream()
                        .filter(pd -> !pd.equals(BLOB_STORAGE_CREDENTIALS_SERVICE))
                        .toList();
            }
        };

        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(AbstractGetAzureBlobStoragePropertiesProcessor_v12.CONTAINER, CONTAINER_NAME);
        runner.setProperty(AbstractGetAzureBlobStoragePropertiesProcessor_v12.BLOB_NAME, BLOB_NAME);
    }

    @Test
    void testSuccessfulMetadataRetrieval() {
        final Map<String, String> metadata = Map.of(
                "author", "jane-doe",
                "source-system", "erp",
                "processing-date", "2024-01-15"
        );
        when(blobProperties.getMetadata()).thenReturn(metadata);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(
                AbstractGetAzureBlobStoragePropertiesProcessor_v12.REL_FOUND, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(
                AbstractGetAzureBlobStoragePropertiesProcessor_v12.REL_FOUND).getFirst();

        assertEquals("jane-doe", flowFile.getAttribute("azure.user.metadata.author"));
        assertEquals("erp", flowFile.getAttribute("azure.user.metadata.source-system"));
        assertEquals("2024-01-15", flowFile.getAttribute("azure.user.metadata.processing-date"));
    }

    @Test
    void testEmptyMetadataRetrieval() {
        when(blobProperties.getMetadata()).thenReturn(Map.of());

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(
                AbstractGetAzureBlobStoragePropertiesProcessor_v12.REL_FOUND, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(
                AbstractGetAzureBlobStoragePropertiesProcessor_v12.REL_FOUND).getFirst();

        flowFile.getAttributes().forEach((key, value) ->
            assertFalse(key.startsWith("azure.user.metadata."),
                    "No metadata attributes should exist when blob has none, found: " + key)
        );
    }

    @Test
    void testBlobNotFound() {
        BlobStorageException exception = mockBlobStorageException(BlobErrorCode.BLOB_NOT_FOUND);
        when(blobClient.getProperties()).thenThrow(exception);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(
                AbstractGetAzureBlobStoragePropertiesProcessor_v12.REL_NOT_FOUND, 1);
    }

    @Test
    void testOtherBlobStorageExceptionRoutesToFailure() {
        BlobStorageException exception = mockBlobStorageException(BlobErrorCode.AUTHORIZATION_FAILURE);
        when(blobClient.getProperties()).thenThrow(exception);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(
                AbstractGetAzureBlobStoragePropertiesProcessor_v12.REL_FAILURE, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(
                AbstractGetAzureBlobStoragePropertiesProcessor_v12.REL_FAILURE).getFirst();
        assertTrue(flowFile.isPenalized(), "FlowFile should be penalized on failure");
    }

    @Test
    void testContainerAndBlobNameFromFlowFileAttributes() {
        runner.setProperty(AbstractGetAzureBlobStoragePropertiesProcessor_v12.CONTAINER,
                "${azure.container}");
        runner.setProperty(AbstractGetAzureBlobStoragePropertiesProcessor_v12.BLOB_NAME,
                "${azure.blobname}");

        final String dynamicContainer = "other-container";
        final String dynamicBlob = "other-blob";

        final BlobContainerClient otherContainerClient = mock(BlobContainerClient.class);
        final BlobClient otherBlobClient = mock(BlobClient.class);
        final BlobProperties otherBlobProperties = mock(BlobProperties.class);

        when(storageClient.getBlobContainerClient(dynamicContainer))
                .thenReturn(otherContainerClient);
        when(otherContainerClient.getBlobClient(dynamicBlob))
                .thenReturn(otherBlobClient);
        when(otherBlobClient.getProperties()).thenReturn(otherBlobProperties);
        when(otherBlobProperties.getMetadata()).thenReturn(Map.of("origin", "external"));

        runner.enqueue("", Map.of(
                "azure.container", dynamicContainer,
                "azure.blobname", dynamicBlob
        ));
        runner.run();

        runner.assertAllFlowFilesTransferred(
                AbstractGetAzureBlobStoragePropertiesProcessor_v12.REL_FOUND, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(
                AbstractGetAzureBlobStoragePropertiesProcessor_v12.REL_FOUND).getFirst();
        assertEquals("external", flowFile.getAttribute("azure.user.metadata.origin"));
    }

    @Test
    void testProvenanceEventOnFound() {
        when(blobProperties.getMetadata()).thenReturn(Map.of("key", "value"));

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(
                AbstractGetAzureBlobStoragePropertiesProcessor_v12.REL_FOUND, 1);

        final ProvenanceEventRecord modifyEvent = runner.getProvenanceEvents().stream()
                .filter(e -> e.getEventType() == ProvenanceEventType.ATTRIBUTES_MODIFIED)
                .findFirst()
                .orElse(null);
        assertNotNull(modifyEvent, "Should have an ATTRIBUTES_MODIFIED provenance event");
    }

    @Test
    void testMetadataAttributePrefix() {
        when(blobProperties.getMetadata()).thenReturn(Map.of("customKey", "customValue"));

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(
                AbstractGetAzureBlobStoragePropertiesProcessor_v12.REL_FOUND, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(
                AbstractGetAzureBlobStoragePropertiesProcessor_v12.REL_FOUND).getFirst();
        assertEquals("customValue", flowFile.getAttribute("azure.user.metadata.customKey"));
        assertNull(flowFile.getAttribute("customKey"),
                "Raw key should not appear without prefix");
    }

    @Test
    void testMultipleFlowFiles() {
        when(blobProperties.getMetadata())
                .thenReturn(Map.of("seq", "1"))
                .thenReturn(Map.of("seq", "2"));

        runner.enqueue("");
        runner.enqueue("");
        runner.run(2);

        assertEquals(2, runner.getFlowFilesForRelationship(
                AbstractGetAzureBlobStoragePropertiesProcessor_v12.REL_FOUND).size());
    }

    @Test
    void testMetadataWithManyEntries() {
        final Map<String, String> largeMetadata = new HashMap<>();
        for (int i = 0; i < 20; i++) {
            largeMetadata.put("key" + i, "value" + i);
        }
        when(blobProperties.getMetadata()).thenReturn(largeMetadata);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(
                AbstractGetAzureBlobStoragePropertiesProcessor_v12.REL_FOUND, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(
                AbstractGetAzureBlobStoragePropertiesProcessor_v12.REL_FOUND).getFirst();

        for (int i = 0; i < 20; i++) {
            assertEquals("value" + i,
                    flowFile.getAttribute("azure.user.metadata.key" + i));
        }
    }

    private static BlobStorageException mockBlobStorageException(BlobErrorCode errorCode) {
        final BlobStorageException exception = mock(BlobStorageException.class);
        when(exception.getErrorCode()).thenReturn(errorCode);
        when(exception.getMessage()).thenReturn("Mocked: " + errorCode);
        return exception;
    }
}
