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

import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobType;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.migration.ProxyServiceMigration;
import org.apache.nifi.processors.azure.AbstractAzureBlobProcessor_v12;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.BLOB_STORAGE_CREDENTIALS_SERVICE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_USER_METADATA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestPutAzureBlobStorage_v12 {

    private static final String CONTAINER_NAME = "test-container";
    private static final String BLOB_NAME = "test-blob";
    private static final String TEST_CONTENT = "test content";
    private static final String ETAG = "test-etag";
    private static final String BLOB_URL = "https://account.blob.core.windows.net/test-container/test-blob";

    private TestRunner runner;
    private BlobClient blobClient;
    private ArgumentCaptor<BlobParallelUploadOptions> uploadOptionsCaptor;

    @Test
    void testMigration() {
        TestRunner runner = TestRunners.newTestRunner(PutAzureBlobStorage_v12.class);
        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        final Map<String, String> expectedRenamed =
                Map.ofEntries(Map.entry(AbstractAzureBlobProcessor_v12.OLD_BLOB_NAME_PROPERTY_DESCRIPTOR_NAME, AbstractAzureBlobProcessor_v12.BLOB_NAME.getName()),
                        Map.entry(AzureStorageUtils.OLD_CONFLICT_RESOLUTION_DESCRIPTOR_NAME, AzureStorageUtils.CONFLICT_RESOLUTION.getName()),
                        Map.entry(AzureStorageUtils.OLD_CREATE_CONTAINER_DESCRIPTOR_NAME, AzureStorageUtils.CREATE_CONTAINER.getName()),
                        Map.entry(AzureStorageUtils.OLD_CONTAINER_DESCRIPTOR_NAME, AzureStorageUtils.CONTAINER.getName()),
                        Map.entry(AzureStorageUtils.OLD_BLOB_STORAGE_CREDENTIALS_SERVICE_DESCRIPTOR_NAME, AzureStorageUtils.BLOB_STORAGE_CREDENTIALS_SERVICE.getName()),
                        Map.entry(ProxyServiceMigration.OBSOLETE_PROXY_CONFIGURATION_SERVICE, ProxyServiceMigration.PROXY_CONFIGURATION_SERVICE));

        assertEquals(expectedRenamed, propertyMigrationResult.getPropertiesRenamed());
    }

    @BeforeEach
    void setUp() {
        final BlobServiceClient storageClient = mock(BlobServiceClient.class);
        final BlobContainerClient containerClient = mock(BlobContainerClient.class);
        blobClient = mock(BlobClient.class);
        uploadOptionsCaptor = ArgumentCaptor.forClass(BlobParallelUploadOptions.class);

        when(storageClient.getBlobContainerClient(CONTAINER_NAME)).thenReturn(containerClient);
        when(containerClient.getBlobClient(BLOB_NAME)).thenReturn(blobClient);
        when(containerClient.exists()).thenReturn(true);
        when(blobClient.getBlobUrl()).thenReturn(BLOB_URL);
        when(blobClient.getContainerName()).thenReturn(CONTAINER_NAME);
        when(blobClient.getBlobName()).thenReturn(BLOB_NAME);

        final BlockBlobItem blockBlobItem = mock(BlockBlobItem.class);
        when(blockBlobItem.getETag()).thenReturn(ETAG);
        when(blockBlobItem.getLastModified()).thenReturn(OffsetDateTime.now());

        final Response<BlockBlobItem> response = mock(Response.class);
        when(response.getValue()).thenReturn(blockBlobItem);
        when(blobClient.uploadWithResponse(
                any(BlobParallelUploadOptions.class), isNull(), any(Context.class)))
                .thenReturn(response);

        final BlobProperties blobProperties = mock(BlobProperties.class);
        when(blobProperties.getBlobType()).thenReturn(BlobType.BLOCK_BLOB);
        when(blobProperties.getETag()).thenReturn(ETAG);
        when(blobProperties.getContentType()).thenReturn("application/octet-stream");
        when(blobProperties.getLastModified()).thenReturn(OffsetDateTime.now());
        when(blobProperties.getBlobSize()).thenReturn((long) TEST_CONTENT.length());
        when(blobProperties.getContentLanguage()).thenReturn(null);
        when(blobClient.getProperties()).thenReturn(blobProperties);

        final PutAzureBlobStorage_v12 processor = new PutAzureBlobStorage_v12() {
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
        runner.setProperty(AbstractAzureBlobProcessor_v12.BLOB_NAME, BLOB_NAME);
        runner.setProperty("Container Name", CONTAINER_NAME);
    }

    @Test
    void testTagsFromFlowFileAttributesWithPrefix() {
        runner.setProperty(PutAzureBlobStorage_v12.BLOB_TAG_PREFIX, "azure.tag.");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("azure.tag.environment", "production");
        attributes.put("azure.tag.team", "engineering");
        attributes.put("other.attribute", "should-be-ignored");

        runner.enqueue(TEST_CONTENT, attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractAzureBlobProcessor_v12.REL_SUCCESS, 1);

        verify(blobClient).uploadWithResponse(
                uploadOptionsCaptor.capture(), isNull(), any(Context.class));
        final Map<String, String> tags = uploadOptionsCaptor.getValue().getTags();

        assertNotNull(tags, "Tags should not be null when tag prefix is set");
        assertEquals(2, tags.size());
        assertEquals("production", tags.get("azure.tag.environment"));
        assertEquals("engineering", tags.get("azure.tag.team"));
    }

    @Test
    void testTagsFromFlowFileAttributesWithPrefixRemoval() {
        runner.setProperty(PutAzureBlobStorage_v12.BLOB_TAG_PREFIX, "azure.tag.");
        runner.setProperty(PutAzureBlobStorage_v12.REMOVE_TAG_PREFIX, "true");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("azure.tag.environment", "staging");
        attributes.put("azure.tag.department", "finance");
        attributes.put("unrelated.key", "unrelated-value");

        runner.enqueue(TEST_CONTENT, attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractAzureBlobProcessor_v12.REL_SUCCESS, 1);

        verify(blobClient).uploadWithResponse(
                uploadOptionsCaptor.capture(), isNull(), any(Context.class));
        final Map<String, String> tags = uploadOptionsCaptor.getValue().getTags();

        assertNotNull(tags);
        assertEquals(2, tags.size());
        assertEquals("staging", tags.get("environment"));
        assertEquals("finance", tags.get("department"));
    }

    @Test
    void testNoTagsWhenPrefixNotSet() {
        runner.enqueue(TEST_CONTENT, Map.of("azure.tag.something", "value"));
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractAzureBlobProcessor_v12.REL_SUCCESS, 1);

        verify(blobClient).uploadWithResponse(
                uploadOptionsCaptor.capture(), isNull(), any(Context.class));
        assertNull(uploadOptionsCaptor.getValue().getTags(),
                "Tags should be null when tag prefix property is not set");
    }

    @Test
    void testNoMatchingTagAttributes() {
        runner.setProperty(PutAzureBlobStorage_v12.BLOB_TAG_PREFIX, "azure.tag.");

        runner.enqueue(TEST_CONTENT, Map.of("other.key", "other-value"));
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractAzureBlobProcessor_v12.REL_SUCCESS, 1);

        verify(blobClient).uploadWithResponse(
                uploadOptionsCaptor.capture(), isNull(), any(Context.class));
        final Map<String, String> tags = uploadOptionsCaptor.getValue().getTags();

        assertNotNull(tags);
        assertTrue(tags.isEmpty(),
                "Tags should be empty when no attributes match the prefix");
    }

    @Test
    void testTagPrefixWithExpressionLanguage() {
        runner.setProperty(PutAzureBlobStorage_v12.BLOB_TAG_PREFIX, "${tag.prefix}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("tag.prefix", "custom.");
        attributes.put("custom.region", "us-east-1");
        attributes.put("custom.tier", "standard");

        runner.enqueue(TEST_CONTENT, attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractAzureBlobProcessor_v12.REL_SUCCESS, 1);

        verify(blobClient).uploadWithResponse(
                uploadOptionsCaptor.capture(), isNull(), any(Context.class));
        final Map<String, String> tags = uploadOptionsCaptor.getValue().getTags();

        assertNotNull(tags);
        assertEquals(2, tags.size());
        assertEquals("us-east-1", tags.get("custom.region"));
        assertEquals("standard", tags.get("custom.tier"));
    }

    @Test
    void testRemoveTagPrefixDefaultsToFalse() {
        runner.setProperty(PutAzureBlobStorage_v12.BLOB_TAG_PREFIX, "pfx.");

        runner.enqueue(TEST_CONTENT, Map.of("pfx.key", "val"));
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractAzureBlobProcessor_v12.REL_SUCCESS, 1);

        verify(blobClient).uploadWithResponse(
                uploadOptionsCaptor.capture(), isNull(), any(Context.class));
        final Map<String, String> tags = uploadOptionsCaptor.getValue().getTags();

        assertNotNull(tags);
        assertTrue(tags.containsKey("pfx.key"),
                "Full attribute name should be the tag key when prefix removal is off");
        assertFalse(tags.containsKey("key"),
                "Stripped key should not appear when prefix removal is off");
    }

    @Test
    void testUserMetadataFromDynamicProperties() {
        runner.setProperty("x-custom-header", "header-value");
        runner.setProperty("department", "engineering");

        runner.enqueue(TEST_CONTENT);
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractAzureBlobProcessor_v12.REL_SUCCESS, 1);

        verify(blobClient).uploadWithResponse(
                uploadOptionsCaptor.capture(), isNull(), any(Context.class));
        final Map<String, String> metadata =
                uploadOptionsCaptor.getValue().getMetadata();

        assertNotNull(metadata);
        assertEquals(2, metadata.size());
        assertEquals("header-value", metadata.get("x-custom-header"));
        assertEquals("engineering", metadata.get("department"));
    }

    @Test
    void testUserMetadataWithExpressionLanguage() {
        runner.setProperty("source-system", "${system.name}");

        runner.enqueue(TEST_CONTENT, Map.of("system.name", "crm-export"));
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractAzureBlobProcessor_v12.REL_SUCCESS, 1);

        verify(blobClient).uploadWithResponse(
                uploadOptionsCaptor.capture(), isNull(), any(Context.class));
        final Map<String, String> metadata =
                uploadOptionsCaptor.getValue().getMetadata();

        assertNotNull(metadata);
        assertEquals("crm-export", metadata.get("source-system"));
    }

    @Test
    void testNoUserMetadataWhenNoDynamicProperties() {
        runner.enqueue(TEST_CONTENT);
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractAzureBlobProcessor_v12.REL_SUCCESS, 1);

        verify(blobClient).uploadWithResponse(
                uploadOptionsCaptor.capture(), isNull(), any(Context.class));
        assertNull(uploadOptionsCaptor.getValue().getMetadata(),
                "Metadata should not be set when no dynamic properties exist");
    }

    @Test
    void testUserMetadataAttributeOnSuccessFlowFile() {
        runner.setProperty("project", "alpha");
        runner.setProperty("owner", "team-a");

        runner.enqueue(TEST_CONTENT);
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractAzureBlobProcessor_v12.REL_SUCCESS, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(
                AbstractAzureBlobProcessor_v12.REL_SUCCESS).getFirst();
        final String attr = flowFile.getAttribute(ATTR_NAME_USER_METADATA);

        assertNotNull(attr, "User metadata attribute should be present");
        assertTrue(attr.contains("project=alpha"));
        assertTrue(attr.contains("owner=team-a"));
    }

    @Test
    void testNoUserMetadataAttributeWhenNoDynamicProperties() {
        runner.enqueue(TEST_CONTENT);
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractAzureBlobProcessor_v12.REL_SUCCESS, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(
                AbstractAzureBlobProcessor_v12.REL_SUCCESS).getFirst();
        assertNull(flowFile.getAttribute(ATTR_NAME_USER_METADATA),
                "User metadata attribute should be absent when no dynamic properties exist");
    }

    @Test
    void testBothTagsAndMetadataSet() {
        runner.setProperty(PutAzureBlobStorage_v12.BLOB_TAG_PREFIX, "tag.");
        runner.setProperty(PutAzureBlobStorage_v12.REMOVE_TAG_PREFIX, "true");
        runner.setProperty("meta-key", "meta-value");

        runner.enqueue(TEST_CONTENT, Map.of("tag.env", "prod"));
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractAzureBlobProcessor_v12.REL_SUCCESS, 1);

        verify(blobClient).uploadWithResponse(
                uploadOptionsCaptor.capture(), isNull(), any(Context.class));
        final BlobParallelUploadOptions options = uploadOptionsCaptor.getValue();

        assertEquals("prod", options.getTags().get("env"));
        assertEquals("meta-value", options.getMetadata().get("meta-key"));

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(
                AbstractAzureBlobProcessor_v12.REL_SUCCESS).getFirst();
        assertNotNull(flowFile.getAttribute(ATTR_NAME_USER_METADATA));
    }

    @Test
    void testProvenanceEventOnSuccess() {
        runner.enqueue(TEST_CONTENT);
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractAzureBlobProcessor_v12.REL_SUCCESS, 1);

        final ProvenanceEventRecord sendEvent = runner.getProvenanceEvents().stream()
                .filter(e -> e.getEventType() == ProvenanceEventType.SEND)
                .findFirst()
                .orElse(null);
        assertNotNull(sendEvent, "Should have a SEND provenance event");
        assertTrue(sendEvent.getTransitUri().contains(BLOB_NAME));
    }
}
