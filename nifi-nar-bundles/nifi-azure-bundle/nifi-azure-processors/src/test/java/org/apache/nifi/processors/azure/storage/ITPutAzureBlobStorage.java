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

import com.microsoft.azure.storage.blob.ListBlobItem;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.util.MockFlowFile;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class ITPutAzureBlobStorage extends AbstractAzureBlobStorageIT {

    @Override
    protected Class<? extends Processor> getProcessorClass() {
        return PutAzureBlobStorage.class;
    }

    @Before
    public void setUp() {
        runner.setProperty(PutAzureBlobStorage.BLOB, TEST_BLOB_NAME);
    }

    @Test
    public void testPutBlob() throws Exception {
        runner.assertValid();
        runner.enqueue("0123456789".getBytes());
        runner.run();

        assertResult();
    }

    @Test
    public void testPutBlobUsingCredentialsService() throws Exception {
        configureCredentialsService();

        runner.assertValid();
        runner.enqueue("0123456789".getBytes());
        runner.run();

        assertResult();
    }

    @Test
    public void testInvalidCredentialsRoutesToFailure() {
        runner.setProperty(AzureStorageUtils.ACCOUNT_NAME, "invalid");
        runner.setProperty(AzureStorageUtils.ACCOUNT_KEY, "aW52YWxpZGludmFsaWQ=");
        runner.assertValid();
        runner.enqueue("test".getBytes());
        runner.run();

        runner.assertTransferCount(PutAzureBlobStorage.REL_FAILURE, 1);
    }

    private void assertResult() throws Exception {
        runner.assertAllFlowFilesTransferred(PutAzureBlobStorage.REL_SUCCESS, 1);
        List<MockFlowFile> flowFilesForRelationship = runner.getFlowFilesForRelationship(PutAzureBlobStorage.REL_SUCCESS);
        for (MockFlowFile flowFile : flowFilesForRelationship) {
            flowFile.assertContentEquals("0123456789".getBytes());
            flowFile.assertAttributeEquals("azure.length", "10");
        }

        Iterable<ListBlobItem> blobs = container.listBlobs(TEST_BLOB_NAME);
        assertTrue(blobs.iterator().hasNext());
    }
}
