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

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.processors.azure.storage.utils.BlobAttributes;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ITListAzureBlobStorage_v12 extends AbstractAzureBlobStorage_v12IT {

    protected static final String BLOB_NAME_1 = "blob1";
    protected static final String BLOB_NAME_2 = "dir1/blob2";
    protected static final String BLOB_NAME_3 = "dir1/dir2/blob3";
    protected static final String BLOB_NAME_4 = "dir2/blob4";

    protected static final String EL_BLOB_NAME_PREFIX = "az.blobnameprefix";

    @Override
    protected Class<? extends Processor> getProcessorClass() {
        return ListAzureBlobStorage_v12.class;
    }

    @Test
    public void testListBlobs() throws Exception {
        uploadBlobs();

        runProcessor();

        assertSuccess(BLOB_NAME_1, BLOB_NAME_2, BLOB_NAME_3, BLOB_NAME_4);
    }

    @Test
    public void testListBlobsUsingProxyConfigurationService() throws Exception {
        uploadBlobs();

        configureProxyService();

        runProcessor();

        assertSuccess(BLOB_NAME_1, BLOB_NAME_2, BLOB_NAME_3, BLOB_NAME_4);
    }

    @Test
    public void testListBlobsWithPrefix_1() throws Exception {
        uploadBlobs();
        runner.setProperty(ListAzureBlobStorage_v12.BLOB_NAME_PREFIX, "blob");

        runProcessor();

        assertSuccess(BLOB_NAME_1);
    }

    @Test
    public void testListBlobsWithPrefix_2() throws Exception {
        uploadBlobs();
        runner.setProperty(ListAzureBlobStorage_v12.BLOB_NAME_PREFIX, "dir1/");

        runProcessor();

        assertSuccess(BLOB_NAME_2, BLOB_NAME_3);
    }

    @Test
    public void testListBlobsUsingExpressionLanguage() throws Exception {
        uploadBlobs();

        runner.setProperty(ListAzureBlobStorage_v12.CONTAINER, String.format("${%s}", EL_CONTAINER_NAME));
        runner.setProperty(ListAzureBlobStorage_v12.BLOB_NAME_PREFIX, String.format("${%s}", EL_BLOB_NAME_PREFIX));

        runner.setEnvironmentVariableValue(EL_CONTAINER_NAME, getContainerName());
        runner.setEnvironmentVariableValue(EL_BLOB_NAME_PREFIX, "dir1/dir");

        runProcessor();

        assertSuccess(BLOB_NAME_3);
    }

    @Test
    public void testListEmptyContainer() throws Exception {
        runProcessor();

        assertSuccess();
    }

    @Test
    public void testListWithNonExistingContainer() {
        runner.setProperty(AzureStorageUtils.CONTAINER, "dummy");

        runProcessor();

        assertFailure();
    }

    @Test
    public void testListWithRecords() throws Exception {
        uploadBlobs();

        MockRecordWriter recordWriter = new MockRecordWriter(null, false);
        runner.addControllerService("record-writer", recordWriter);
        runner.enableControllerService(recordWriter);
        runner.setProperty(ListAzureBlobStorage_v12.RECORD_WRITER, "record-writer");

        runner.run();

        runner.assertAllFlowFilesTransferred(ListAzureBlobStorage_v12.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ListAzureBlobStorage_v12.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals("record.count", "4");
    }

    @Test
    public void testListWithMinAge() throws Exception {
        uploadBlobs();
        runner.setProperty(ListAzureBlobStorage_v12.MIN_AGE, "1 hour");

        runProcessor();

        runner.assertTransferCount(ListAzureBlobStorage_v12.REL_SUCCESS, 0);
    }

    @Test
    public void testListWithMaxAge() throws Exception {
        uploadBlobs();
        runner.setProperty(ListAzureBlobStorage_v12.MAX_AGE, "1 hour");

        runProcessor();

        assertSuccess(BLOB_NAME_1, BLOB_NAME_2, BLOB_NAME_3, BLOB_NAME_4);
    }

    @Test
    public void testListWithMinSize() throws Exception {
        uploadBlobs();
        runner.setProperty(ListAzureBlobStorage_v12.MIN_SIZE, "5 B");
        uploadBlob("blob5", "Test".getBytes(StandardCharsets.UTF_8));

        runProcessor();

        assertSuccess(BLOB_NAME_1, BLOB_NAME_2, BLOB_NAME_3, BLOB_NAME_4);
    }

    @Test
    public void testListWithMaxSize() throws Exception {
        uploadBlobs();
        runner.setProperty(ListAzureBlobStorage_v12.MAX_SIZE, "5 B");
        uploadBlob("blob5", "Test".getBytes(StandardCharsets.UTF_8));

        runProcessor();

        runner.assertAllFlowFilesTransferred(ListAzureBlobStorage_v12.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ListAzureBlobStorage_v12.REL_SUCCESS).getFirst();
        assertFlowFileCommonBlobAttributes(flowFile, getContainerName(), "blob5");
        assertFlowFileResultBlobAttributes(flowFile, "Test".length());
    }

    private void uploadBlobs() throws Exception {
        uploadBlob(BLOB_NAME_1, BLOB_DATA);
        uploadBlob(BLOB_NAME_2, BLOB_DATA);
        uploadBlob(BLOB_NAME_3, BLOB_DATA);
        uploadBlob(BLOB_NAME_4, BLOB_DATA);
    }

    private void runProcessor() {
        runner.assertValid();
        runner.run();
    }

    private void assertSuccess(String... blobNames) throws Exception {
        runner.assertTransferCount(ListAzureBlobStorage_v12.REL_SUCCESS, blobNames.length);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListAzureBlobStorage_v12.REL_SUCCESS);

        Set<String> expectedBlobNames = new HashSet<>(Arrays.asList(blobNames));

        for (MockFlowFile flowFile : flowFiles) {
            String blobName = flowFile.getAttribute(BlobAttributes.ATTR_NAME_BLOBNAME);
            assertTrue(expectedBlobNames.remove(blobName), "Blob should not be listed: " + blobName);

            assertFlowFile(flowFile, blobName);
        }

        assertTrue(expectedBlobNames.isEmpty(), "Blobs should be listed: " + expectedBlobNames);
    }

    private void assertFlowFile(MockFlowFile flowFile, String blobName) throws Exception {
        assertFlowFileCommonBlobAttributes(flowFile, getContainerName(), blobName);
        assertFlowFileResultBlobAttributes(flowFile, BLOB_DATA.length);
        flowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), blobName.substring(blobName.lastIndexOf('/') + 1));

        flowFile.assertContentEquals(EMPTY_CONTENT);
    }

    private void assertFailure() {
        assertFalse(runner.getLogger().getErrorMessages().isEmpty());
        runner.assertTransferCount(ListAzureBlobStorage_v12.REL_SUCCESS, 0);
    }
}
