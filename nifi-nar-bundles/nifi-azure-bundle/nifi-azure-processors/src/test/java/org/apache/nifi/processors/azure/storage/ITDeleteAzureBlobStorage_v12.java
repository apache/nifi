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
import com.azure.storage.blob.specialized.BlobClientBase;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ITDeleteAzureBlobStorage_v12 extends AbstractAzureBlobStorage_v12IT {

    @Override
    protected Class<? extends Processor> getProcessorClass() {
        return DeleteAzureBlobStorage_v12.class;
    }

    @BeforeEach
    public void setUp() {
        runner.setProperty(DeleteAzureBlobStorage_v12.BLOB_NAME, BLOB_NAME);
    }

    @Test
    public void testDeleteBlobWithSimpleName() throws Exception {
        uploadBlob(BLOB_NAME, BLOB_DATA);

        runProcessor();

        assertSuccess(BLOB_NAME);
    }

    @Test
    public void testDeleteBlobWithSimpleNameUsingProxyConfigurationService() throws Exception {
        uploadBlob(BLOB_NAME, BLOB_DATA);

        configureProxyService();

        runProcessor();

        assertSuccess(BLOB_NAME);
    }

    @Test
    public void testDeleteBlobWithCompoundName() throws Exception {
        String blobName = "dir1/dir2/blob1";
        runner.setProperty(DeleteAzureBlobStorage_v12.BLOB_NAME, blobName);
        uploadBlob(blobName, BLOB_DATA);

        runProcessor();

        assertSuccess(blobName);
    }

    @Test
    public void testDeleteNonExistingBlob() throws Exception {
        runProcessor();

        assertSuccess(BLOB_NAME);
    }

    @Test
    public void testDeleteBlobWithSpacesInBlobName() throws Exception {
        String blobName = "dir 1/blob 1";
        runner.setProperty(DeleteAzureBlobStorage_v12.BLOB_NAME, blobName);
        uploadBlob(blobName, BLOB_DATA);

        runProcessor();

        assertSuccess(blobName);
    }

    @Test
    public void testDeleteBlobUsingExpressionLanguage() throws Exception {
        Map<String, String> attributes = initCommonExpressionLanguageAttributes();

        uploadBlob(BLOB_NAME, BLOB_DATA);

        runProcessor(attributes);

        assertSuccess(BLOB_NAME);
    }

    @Test
    public void testDeleteBlobWithSnapshotAndDeleteSnapshotsOptionNone() throws Exception {
        runner.setProperty(DeleteAzureBlobStorage_v12.DELETE_SNAPSHOTS_OPTION, DeleteAzureBlobStorage_v12.DELETE_SNAPSHOTS_NONE);

        BlobClient blobClient = uploadBlob(BLOB_NAME, BLOB_DATA);
        BlobClientBase snapshotClient = blobClient.createSnapshot();

        runProcessor();

        assertFailure(BLOB_NAME);
        assertTrue(snapshotClient.exists());
    }

    @Test
    public void testDeleteBlobWithSnapshotAndDeleteSnapshotsOptionInclude() throws Exception {
        runner.setProperty(DeleteAzureBlobStorage_v12.DELETE_SNAPSHOTS_OPTION, DeleteAzureBlobStorage_v12.DELETE_SNAPSHOTS_ALSO);

        BlobClient blobClient = uploadBlob(BLOB_NAME, BLOB_DATA);
        BlobClientBase snapshotClient = blobClient.createSnapshot();

        runProcessor();

        assertSuccess(BLOB_NAME);
        assertFalse(snapshotClient.exists());
    }

    @Test
    public void testDeleteBlobWithSnapshotAndDeleteSnapshotsOptionOnly() throws Exception {
        runner.setProperty(DeleteAzureBlobStorage_v12.DELETE_SNAPSHOTS_OPTION, DeleteAzureBlobStorage_v12.DELETE_SNAPSHOTS_ONLY);

        BlobClient blobClient = uploadBlob(BLOB_NAME, BLOB_DATA);
        BlobClientBase snapshotClient = blobClient.createSnapshot();

        runProcessor();

        assertFlowFile(DeleteAzureBlobStorage_v12.REL_SUCCESS);
        assertTrue(blobClient.exists());
        assertFalse(snapshotClient.exists());
        assertProvenanceEvents();
    }

    private void runProcessor() {
        runProcessor(Collections.emptyMap());
    }

    private void runProcessor(Map<String, String> attributes) {
        runner.assertValid();
        runner.enqueue(EMPTY_CONTENT, attributes);
        runner.run();
    }

    private void assertSuccess(String blobName) throws Exception {
        assertFlowFile(DeleteAzureBlobStorage_v12.REL_SUCCESS);
        assertAzureBlob(blobName, false);
        assertProvenanceEvents();
    }

    private void assertFailure(String blobName) throws Exception {
        assertFlowFile(DeleteAzureBlobStorage_v12.REL_FAILURE);
        assertAzureBlob(blobName, true);
    }

    private void assertFlowFile(Relationship relationship) throws Exception {
        runner.assertAllFlowFilesTransferred(relationship, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(relationship).get(0);

        flowFile.assertContentEquals(EMPTY_CONTENT);
    }

    private void assertAzureBlob(String blobName, boolean expectedExists) {
        BlobClient blobClient = getContainerClient().getBlobClient(blobName);

        assertEquals(expectedExists, blobClient.exists());
    }

    private void assertProvenanceEvents() {
        Set<ProvenanceEventType> expectedEventTypes = Collections.singleton(ProvenanceEventType.REMOTE_INVOCATION);

        Set<ProvenanceEventType> actualEventTypes = runner.getProvenanceEvents().stream()
                .map(ProvenanceEventRecord::getEventType)
                .collect(Collectors.toSet());
        assertEquals(expectedEventTypes, actualEventTypes);
    }
}
