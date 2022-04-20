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
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ITPutAzureBlobStorage_v12 extends AbstractAzureBlobStorage_v12IT {

    @Override
    protected Class<? extends Processor> getProcessorClass() {
        return PutAzureBlobStorage_v12.class;
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
        runner.setProperty(PutAzureBlobStorage_v12.CREATE_CONTAINER, "false");

        runProcessor(BLOB_DATA);

        assertFailure(BLOB_DATA);
    }

    @Test
    public void testPutBlobWithNonExistingContainerAndCreateContainerTrue() throws Exception {
        String containerName = generateContainerName();
        runner.setProperty(AzureStorageUtils.CONTAINER, containerName);
        runner.setProperty(PutAzureBlobStorage_v12.CREATE_CONTAINER, "true");

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

        assertFailure(BLOB_DATA);
    }

    @Test
    public void testPutBlobUsingExpressionLanguage() throws Exception {
        Map<String, String> attributes = initCommonExpressionLanguageAttributes();

        runProcessor(BLOB_DATA, attributes);

        assertSuccess(getContainerName(), BLOB_NAME, BLOB_DATA);
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

    private void assertFlowFile(String containerName, String blobName, byte[] blobData) throws Exception {
        runner.assertAllFlowFilesTransferred(PutAzureDataLakeStorage.REL_SUCCESS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutAzureDataLakeStorage.REL_SUCCESS).get(0);

        assertFlowFileBlobAttributes(flowFile, containerName, blobName, blobData.length);

        flowFile.assertContentEquals(blobData);
    }

    private void assertAzureBlob(String containerName, String blobName, byte[] blobData) {
        BlobContainerClient containerClient = getStorageClient().getBlobContainerClient(containerName);
        BlobClient blobClient = containerClient.getBlobClient(blobName);

        assertTrue(blobClient.exists());
        assertEquals(blobData.length, blobClient.getProperties().getBlobSize());
    }

    private void assertProvenanceEvents() {
        Set<ProvenanceEventType> expectedEventTypes = Collections.singleton(ProvenanceEventType.SEND);

        Set<ProvenanceEventType> actualEventTypes = runner.getProvenanceEvents().stream()
                .map(ProvenanceEventRecord::getEventType)
                .collect(Collectors.toSet());
        assertEquals(expectedEventTypes, actualEventTypes);
    }

    private void assertFailure(byte[] blobData) throws Exception {
        runner.assertAllFlowFilesTransferred(PutAzureBlobStorage_v12.REL_FAILURE, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(DeleteAzureBlobStorage_v12.REL_FAILURE).get(0);
        flowFile.assertContentEquals(blobData);
    }
}
