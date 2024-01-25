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
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.nifi.processors.azure.storage.CopyAzureBlobStorage_v12.SOURCE_STORAGE_CREDENTIALS_SERVICE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ITCopyAzureBlobStorage_v12 extends AbstractAzureBlobStorage_v12IT {
    public static class ITProcessor extends CopyAzureBlobStorage_v12 {
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
        runner.setProperty(CopyAzureBlobStorage_v12.SOURCE_CONTAINER_NAME, getContainerName());
        runner.setProperty(CopyAzureBlobStorage_v12.SOURCE_BLOB_NAME, BLOB_NAME);
    }

    @ValueSource(booleans={true, false})
    @ParameterizedTest
    public void testPutBlobFromUrl(boolean proxied) throws Exception {
        if (proxied) {
            configureProxyService();
        }

        uploadBlob(BLOB_NAME, BLOB_DATA);
        final String destinationBlobName = BLOB_NAME + "-target";
        runner.setProperty(CopyAzureBlobStorage_v12.DESTINATION_BLOB_NAME, destinationBlobName);
        runner.setProperty(SOURCE_STORAGE_CREDENTIALS_SERVICE, SERVICE_ID);
        runner.setProperty(CopyAzureBlobStorage_v12.SOURCE_BLOB_NAME, BLOB_NAME);

        runProcessor(BLOB_DATA);

        assertSuccess(getContainerName(), destinationBlobName, BLOB_DATA);
        assertTrue(((ITProcessor) runner.getProcessor()).blobMetadataApplied);
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
        runner.assertAllFlowFilesTransferred(CopyAzureBlobStorage_v12.REL_SUCCESS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CopyAzureBlobStorage_v12.REL_SUCCESS).getFirst();

        assertFlowFileCommonBlobAttributes(flowFile, containerName, blobName);
        if (blobData != null) {
            assertFlowFileResultBlobAttributes(flowFile, blobData.length);
            flowFile.assertContentEquals(blobData);
        }
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
}
