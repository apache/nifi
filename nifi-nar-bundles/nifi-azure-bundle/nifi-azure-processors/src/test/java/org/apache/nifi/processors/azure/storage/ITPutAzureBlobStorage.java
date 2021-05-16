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
import org.apache.nifi.processors.azure.storage.utils.AzureBlobClientSideEncryptionMethod;
import org.apache.nifi.processors.azure.storage.utils.AzureBlobClientSideEncryptionUtils;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.util.MockFlowFile;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class ITPutAzureBlobStorage extends AbstractAzureBlobStorageIT {

    public static final String TEST_FILE_CONTENT = "0123456789";
    private static final String KEY_ID_VALUE = "key:id";
    private static final String KEY_64B_VALUE = "1234567890ABCDEF";
    private static final String KEY_128B_VALUE = KEY_64B_VALUE + KEY_64B_VALUE;
    private static final String KEY_192B_VALUE = KEY_128B_VALUE + KEY_64B_VALUE;
    private static final String KEY_256B_VALUE = KEY_128B_VALUE + KEY_128B_VALUE;
    private static final String KEY_384B_VALUE = KEY_256B_VALUE + KEY_128B_VALUE;
    private static final String KEY_512B_VALUE = KEY_256B_VALUE + KEY_256B_VALUE;


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
        runner.enqueue(TEST_FILE_CONTENT.getBytes());
        runner.run();

        assertResult();
    }

    @Test
    public void testPutBlob64BSymmetricCSE() {
        runner.setProperty(AzureBlobClientSideEncryptionUtils.CSE_KEY_TYPE, AzureBlobClientSideEncryptionMethod.SYMMETRIC.name());
        runner.setProperty(AzureBlobClientSideEncryptionUtils.CSE_KEY_ID, KEY_ID_VALUE);
        runner.setProperty(AzureBlobClientSideEncryptionUtils.CSE_SYMMETRIC_KEY_HEX, KEY_64B_VALUE);
        runner.assertNotValid();
    }

    @Test
    public void testPutBlob128BSymmetricCSE() throws Exception {
        runner.setProperty(AzureBlobClientSideEncryptionUtils.CSE_KEY_TYPE, AzureBlobClientSideEncryptionMethod.SYMMETRIC.name());
        runner.setProperty(AzureBlobClientSideEncryptionUtils.CSE_KEY_ID, KEY_ID_VALUE);
        runner.setProperty(AzureBlobClientSideEncryptionUtils.CSE_SYMMETRIC_KEY_HEX, KEY_128B_VALUE);
        runner.assertValid();
        runner.enqueue(TEST_FILE_CONTENT.getBytes());
        runner.run();

        assertResult();
    }

    @Test
    public void testPutBlob192BSymmetricCSE() throws Exception {
        runner.setProperty(AzureBlobClientSideEncryptionUtils.CSE_KEY_TYPE, AzureBlobClientSideEncryptionMethod.SYMMETRIC.name());
        runner.setProperty(AzureBlobClientSideEncryptionUtils.CSE_KEY_ID, KEY_ID_VALUE);
        runner.setProperty(AzureBlobClientSideEncryptionUtils.CSE_SYMMETRIC_KEY_HEX, KEY_192B_VALUE);
        runner.assertValid();
        runner.enqueue(TEST_FILE_CONTENT.getBytes());
        runner.run();

        assertResult();
    }

    @Test
    public void testPutBlob256BSymmetricCSE() throws Exception {
        runner.setProperty(AzureBlobClientSideEncryptionUtils.CSE_KEY_TYPE, AzureBlobClientSideEncryptionMethod.SYMMETRIC.name());
        runner.setProperty(AzureBlobClientSideEncryptionUtils.CSE_KEY_ID, KEY_ID_VALUE);
        runner.setProperty(AzureBlobClientSideEncryptionUtils.CSE_SYMMETRIC_KEY_HEX, KEY_256B_VALUE);
        runner.assertValid();
        runner.enqueue(TEST_FILE_CONTENT.getBytes());
        runner.run();

        assertResult();
    }

    @Test
    public void testPutBlob384BSymmetricCSE() throws Exception {
        runner.setProperty(AzureBlobClientSideEncryptionUtils.CSE_KEY_TYPE, AzureBlobClientSideEncryptionMethod.SYMMETRIC.name());
        runner.setProperty(AzureBlobClientSideEncryptionUtils.CSE_KEY_ID, KEY_ID_VALUE);
        runner.setProperty(AzureBlobClientSideEncryptionUtils.CSE_SYMMETRIC_KEY_HEX, KEY_384B_VALUE);
        runner.assertValid();
        runner.enqueue(TEST_FILE_CONTENT.getBytes());
        runner.run();

        assertResult();
    }

    @Test
    public void testPutBlob512BSymmetricCSE() throws Exception {
        runner.setProperty(AzureBlobClientSideEncryptionUtils.CSE_KEY_TYPE, AzureBlobClientSideEncryptionMethod.SYMMETRIC.name());
        runner.setProperty(AzureBlobClientSideEncryptionUtils.CSE_KEY_ID, KEY_ID_VALUE);
        runner.setProperty(AzureBlobClientSideEncryptionUtils.CSE_SYMMETRIC_KEY_HEX, KEY_512B_VALUE);
        runner.assertValid();
        runner.enqueue(TEST_FILE_CONTENT.getBytes());
        runner.run();

        assertResult();
    }

    @Test
    public void testPutBlobUsingCredentialsService() throws Exception {
        configureCredentialsService();

        runner.assertValid();
        runner.enqueue(TEST_FILE_CONTENT.getBytes());
        runner.run();

        assertResult();
    }

    @Test
    public void testInvalidCredentialsRoutesToFailure() {
        runner.setProperty(AzureStorageUtils.ACCOUNT_NAME, "invalid");
        runner.setProperty(AzureStorageUtils.ACCOUNT_KEY, "aW52YWxpZGludmFsaWQ=");
        runner.assertValid();
        runner.enqueue(TEST_FILE_CONTENT.getBytes());
        runner.run();

        runner.assertTransferCount(PutAzureBlobStorage.REL_FAILURE, 1);
    }

    private void assertResult() throws Exception {
        runner.assertAllFlowFilesTransferred(PutAzureBlobStorage.REL_SUCCESS, 1);
        List<MockFlowFile> flowFilesForRelationship = runner.getFlowFilesForRelationship(PutAzureBlobStorage.REL_SUCCESS);
        for (MockFlowFile flowFile : flowFilesForRelationship) {
            flowFile.assertContentEquals(TEST_FILE_CONTENT.getBytes());
            flowFile.assertAttributeEquals("azure.length", "10");
        }

        Iterable<ListBlobItem> blobs = container.listBlobs(TEST_BLOB_NAME);
        assertTrue(blobs.iterator().hasNext());
    }
}
