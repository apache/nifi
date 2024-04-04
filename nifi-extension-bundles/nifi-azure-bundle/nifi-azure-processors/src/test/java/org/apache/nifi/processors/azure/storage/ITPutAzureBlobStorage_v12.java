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
import com.azure.storage.blob.models.BlobErrorCode;
import org.apache.nifi.fileresource.service.StandardFileResourceService;
import org.apache.nifi.fileresource.service.api.FileResourceService;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.azure.ClientSideEncryptionSupport;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.processors.azure.storage.utils.ClientSideEncryptionMethod;
import org.apache.nifi.processors.transfer.ResourceTransferProperties;
import org.apache.nifi.processors.transfer.ResourceTransferSource;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.services.azure.storage.AzureStorageConflictResolutionStrategy;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_ERROR_CODE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_IGNORED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ITPutAzureBlobStorage_v12 extends AbstractAzureBlobStorage_v12IT {

    public static class ITProcessor extends PutAzureBlobStorage_v12 {
        public boolean blobMetadataApplied = false;

        @Override
        protected void applyBlobMetadata(Map<String, String> attributes, BlobClient blobClient) {
            super.applyBlobMetadata(attributes, blobClient);
            blobMetadataApplied = true;
        }
    }

    @Override
    protected Class<? extends Processor> getProcessorClass() {
        return ITProcessor.class;
    }

    @BeforeEach
    public void setUp() {
        runner.setProperty(PutAzureBlobStorage_v12.BLOB_NAME, BLOB_NAME);
    }

    @Test
    public void testPutBlobWithSimpleName() throws Exception {
        runProcessor(BLOB_DATA);

        assertSuccess(getContainerName(), BLOB_NAME, BLOB_DATA);
    }

    @Test
    public void testPutBlobApplyBlobMetadata() throws Exception {
        runProcessor(BLOB_DATA);

        assertSuccess(getContainerName(), BLOB_NAME, BLOB_DATA);
        assertTrue(((ITProcessor) runner.getProcessor()).blobMetadataApplied);
    }

    @Test
    public void testPutBlobWithSimpleNameUsingProxyConfigurationService() throws Exception {
        configureProxyService();

        runProcessor(BLOB_DATA);

        assertSuccess(getContainerName(), BLOB_NAME, BLOB_DATA);
    }

    @Test
    public void testPutBlobWithCompoundName() throws Exception {
        String blobName = "dir1/dir2/blob1";
        runner.setProperty(PutAzureBlobStorage_v12.BLOB_NAME, blobName);

        runProcessor(BLOB_DATA);

        assertSuccess(getContainerName(), blobName, BLOB_DATA);
    }

    @Test
    public void testPutEmptyBlob() throws Exception {
        byte[] blobData = new byte[0];

        runProcessor(blobData);

        assertSuccess(getContainerName(), BLOB_NAME, blobData);
    }

    @Test
    public void testPutBigBlob() throws Exception {
        Random random = new Random();
        byte[] blobData = new byte[120_000_000];
        random.nextBytes(blobData);

        runProcessor(blobData);

        assertSuccess(getContainerName(), BLOB_NAME, blobData);
    }

    @Test
    public void testPutBlobWithNonExistingContainerAndCreateContainerFalse() throws Exception {
        String containerName = generateContainerName();
        runner.setProperty(AzureStorageUtils.CONTAINER, containerName);
        runner.setProperty(AzureStorageUtils.CREATE_CONTAINER, "false");

        runProcessor(BLOB_DATA);

        assertFailure(BLOB_DATA, BlobErrorCode.CONTAINER_NOT_FOUND);
    }

    @Test
    public void testPutBlobWithNonExistingContainerAndCreateContainerTrue() throws Exception {
        String containerName = generateContainerName();
        runner.setProperty(AzureStorageUtils.CONTAINER, containerName);
        runner.setProperty(AzureStorageUtils.CREATE_CONTAINER, "true");

        try {
            runProcessor(BLOB_DATA);

            assertSuccess(containerName, BLOB_NAME, BLOB_DATA);
        } finally {
            BlobContainerClient containerClient = getStorageClient().getBlobContainerClient(containerName);
            containerClient.delete();
        }
    }

    @Test
    public void testPutBlobWithSpacesInBlobName() throws Exception {
        String blobName = "dir 1/blob 1";
        runner.setProperty(PutAzureBlobStorage_v12.BLOB_NAME, blobName);

        runProcessor(BLOB_DATA);

        assertSuccess(getContainerName(), blobName, BLOB_DATA);
    }

    @Test
    public void testPutBlobToExistingBlob() throws Exception {
        uploadBlob(BLOB_NAME, BLOB_DATA);

        runProcessor(BLOB_DATA);

        MockFlowFile flowFile = assertFailure(BLOB_DATA, BlobErrorCode.BLOB_ALREADY_EXISTS);
        assertNull(flowFile.getAttribute(ATTR_NAME_IGNORED));
    }

    @Test
    public void testPutBlobToExistingBlobConflictStrategyIgnore() throws Exception {
        uploadBlob(BLOB_NAME, BLOB_DATA);
        runner.setProperty(AzureStorageUtils.CONFLICT_RESOLUTION, AzureStorageConflictResolutionStrategy.IGNORE_RESOLUTION);

        runProcessor(BLOB_DATA);

        MockFlowFile flowFile = assertIgnored(getContainerName(), BLOB_NAME);
        assertEquals(flowFile.getAttribute(ATTR_NAME_IGNORED), "true");
    }

    @Test
    public void testPutBlobToExistingBlobConflictStrategyReplace() throws Exception {
        uploadBlob(BLOB_NAME, BLOB_DATA);
        runner.setProperty(AzureStorageUtils.CONFLICT_RESOLUTION, AzureStorageConflictResolutionStrategy.REPLACE_RESOLUTION);

        runProcessor(BLOB_DATA);

        assertSuccess(getContainerName(), BLOB_NAME, BLOB_DATA);
    }

    @Test
    public void testPutBlobUsingExpressionLanguage() throws Exception {
        Map<String, String> attributes = initCommonExpressionLanguageAttributes();

        runProcessor(BLOB_DATA, attributes);

        assertSuccess(getContainerName(), BLOB_NAME, BLOB_DATA);
    }

    @Test
    public void testPutBlob64BLocalCSE() {
        runner.setProperty(ClientSideEncryptionSupport.CSE_KEY_TYPE, ClientSideEncryptionMethod.LOCAL);
        runner.setProperty(ClientSideEncryptionSupport.CSE_KEY_ID, KEY_ID_VALUE);
        runner.setProperty(ClientSideEncryptionSupport.CSE_LOCAL_KEY, KEY_64B_VALUE);
        runner.assertNotValid();
    }

    @Test
    public void testPutBlob128BLocalCSE() throws Exception {
        runner.setProperty(ClientSideEncryptionSupport.CSE_KEY_TYPE, ClientSideEncryptionMethod.LOCAL);
        runner.setProperty(ClientSideEncryptionSupport.CSE_KEY_ID, KEY_ID_VALUE);
        runner.setProperty(ClientSideEncryptionSupport.CSE_LOCAL_KEY, KEY_128B_VALUE);
        runProcessor(BLOB_DATA);
        assertSuccessForCSE(getContainerName(), BLOB_NAME, BLOB_DATA);
    }

    @Test
    public void testPutBlob192BLocalCSE() throws Exception {
        runner.setProperty(ClientSideEncryptionSupport.CSE_KEY_TYPE, ClientSideEncryptionMethod.LOCAL);
        runner.setProperty(ClientSideEncryptionSupport.CSE_KEY_ID, KEY_ID_VALUE);
        runner.setProperty(ClientSideEncryptionSupport.CSE_LOCAL_KEY, KEY_192B_VALUE);
        runProcessor(BLOB_DATA);
        assertSuccessForCSE(getContainerName(), BLOB_NAME, BLOB_DATA);
    }

    @Test
    public void testPutBlob256BLocalCSE() throws Exception {
        runner.setProperty(ClientSideEncryptionSupport.CSE_KEY_TYPE, ClientSideEncryptionMethod.LOCAL);
        runner.setProperty(ClientSideEncryptionSupport.CSE_KEY_ID, KEY_ID_VALUE);
        runner.setProperty(ClientSideEncryptionSupport.CSE_LOCAL_KEY, KEY_256B_VALUE);
        runProcessor(BLOB_DATA);
        assertSuccessForCSE(getContainerName(), BLOB_NAME, BLOB_DATA);
    }

    @Test
    public void testPutBlob384BLocalCSE() throws Exception {
        runner.setProperty(ClientSideEncryptionSupport.CSE_KEY_TYPE, ClientSideEncryptionMethod.LOCAL);
        runner.setProperty(ClientSideEncryptionSupport.CSE_KEY_ID, KEY_ID_VALUE);
        runner.setProperty(ClientSideEncryptionSupport.CSE_LOCAL_KEY, KEY_384B_VALUE);
        runProcessor(BLOB_DATA);
        assertSuccessForCSE(getContainerName(), BLOB_NAME, BLOB_DATA);
    }

    @Test
    public void testPutBlob512BLocalCSE() throws Exception {
        runner.setProperty(ClientSideEncryptionSupport.CSE_KEY_TYPE, ClientSideEncryptionMethod.LOCAL);
        runner.setProperty(ClientSideEncryptionSupport.CSE_KEY_ID, KEY_ID_VALUE);
        runner.setProperty(ClientSideEncryptionSupport.CSE_LOCAL_KEY, KEY_512B_VALUE);
        runProcessor(BLOB_DATA);
        assertSuccessForCSE(getContainerName(), BLOB_NAME, BLOB_DATA);
    }

    @Test
    public void testPutBlobFromLocalFile() throws Exception {
        String attributeName = "file.path";

        String serviceId = FileResourceService.class.getSimpleName();
        FileResourceService service = new StandardFileResourceService();
        runner.addControllerService(serviceId, service);
        runner.setProperty(service, StandardFileResourceService.FILE_PATH, String.format("${%s}", attributeName));
        runner.enableControllerService(service);

        runner.setProperty(ResourceTransferProperties.RESOURCE_TRANSFER_SOURCE, ResourceTransferSource.FILE_RESOURCE_SERVICE);
        runner.setProperty(ResourceTransferProperties.FILE_RESOURCE_SERVICE, serviceId);

        Path tempFilePath = Files.createTempFile("ITPutAzureBlobStorage_v12_testPutBlobFromLocalFile_", "");
        Files.write(tempFilePath, BLOB_DATA);

        Map<String, String> attributes = new HashMap<>();
        attributes.put(attributeName, tempFilePath.toString());

        runProcessor(EMPTY_CONTENT, attributes);

        runner.assertAllFlowFilesTransferred(PutAzureBlobStorage_v12.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutAzureBlobStorage_v12.REL_SUCCESS).getFirst();
        assertFlowFileCommonBlobAttributes(flowFile, getContainerName(), BLOB_NAME);
        assertFlowFileResultBlobAttributes(flowFile, BLOB_DATA.length);

        assertAzureBlob(getContainerName(), BLOB_NAME, BLOB_DATA);
        assertProvenanceEvents();
    }

    @Test
    public void testPutBlobFromNonExistentLocalFile() throws Exception {
        String attributeName = "file.path";

        String serviceId = FileResourceService.class.getSimpleName();
        FileResourceService service = new StandardFileResourceService();
        runner.addControllerService(serviceId, service);
        runner.setProperty(service, StandardFileResourceService.FILE_PATH, String.format("${%s}", attributeName));
        runner.enableControllerService(service);

        runner.setProperty(ResourceTransferProperties.RESOURCE_TRANSFER_SOURCE, ResourceTransferSource.FILE_RESOURCE_SERVICE);
        runner.setProperty(ResourceTransferProperties.FILE_RESOURCE_SERVICE, serviceId);

        String filePath = "nonexistent.txt";

        Map<String, String> attributes = new HashMap<>();
        attributes.put(attributeName, filePath);

        runProcessor(EMPTY_CONTENT, attributes);

        runner.assertAllFlowFilesTransferred(PutAzureBlobStorage_v12.REL_FAILURE, 1);

        assertProvenanceEvents();
    }


    private void runProcessor(byte[] data) {
        runProcessor(data, Collections.emptyMap());
    }

    private void runProcessor(byte[] data, Map<String, String> attributes) {
        runner.assertValid();
        runner.enqueue(data, attributes);
        runner.run();
    }

    private void assertSuccess(String containerName, String blobName, byte[] blobData) throws Exception {
        assertFlowFile(containerName, blobName, blobData);
        assertAzureBlob(containerName, blobName, blobData);
        assertProvenanceEvents();
    }

    private void assertSuccessForCSE(String containerName, String blobName, byte[] blobData) throws Exception {
        assertFlowFile(containerName, blobName, blobData);
        assertAzureBlobExists(containerName, blobName);
        assertProvenanceEvents();
    }

    private MockFlowFile assertIgnored(String containerName, String blobName) throws Exception {
        MockFlowFile flowFile = assertFlowFile(containerName, blobName, null);
        assertProvenanceEvents();
        return flowFile;
    }

    private MockFlowFile assertFlowFile(String containerName, String blobName, byte[] blobData) throws Exception {
        runner.assertAllFlowFilesTransferred(PutAzureBlobStorage_v12.REL_SUCCESS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutAzureBlobStorage_v12.REL_SUCCESS).getFirst();

        assertFlowFileCommonBlobAttributes(flowFile, containerName, blobName);
        if (blobData != null) {
            assertFlowFileResultBlobAttributes(flowFile, blobData.length);
            flowFile.assertContentEquals(blobData);
            flowFile.assertAttributeEquals("azure.length", String.valueOf(blobData.length));
        }
        return flowFile;
    }

    private void assertAzureBlob(String containerName, String blobName, byte[] blobData) {
        BlobClient blobClient = assertAzureBlobExists(containerName, blobName);
        assertEquals(blobData.length, blobClient.getProperties().getBlobSize());
    }

    private BlobClient assertAzureBlobExists(String containerName, String blobName) {
        BlobContainerClient containerClient = getStorageClient().getBlobContainerClient(containerName);
        BlobClient blobClient = containerClient.getBlobClient(blobName);
        assertTrue(blobClient.exists());
        return blobClient;
    }

    private void assertProvenanceEvents() {
        Set<ProvenanceEventType> expectedEventTypes = Collections.singleton(ProvenanceEventType.SEND);

        Set<ProvenanceEventType> actualEventTypes = runner.getProvenanceEvents().stream()
                .map(ProvenanceEventRecord::getEventType)
                .collect(Collectors.toSet());
        assertEquals(expectedEventTypes, actualEventTypes);
    }

    private MockFlowFile assertFailure(byte[] blobData, BlobErrorCode errorCode) throws Exception {
        runner.assertAllFlowFilesTransferred(PutAzureBlobStorage_v12.REL_FAILURE, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(DeleteAzureBlobStorage_v12.REL_FAILURE).getFirst();
        flowFile.assertContentEquals(blobData);
        flowFile.assertAttributeEquals(ATTR_NAME_ERROR_CODE, errorCode.toString());
        return flowFile;
    }
}
