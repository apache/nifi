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

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;

import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.UUID;

public abstract class AbstractAzureBlobStorageIT extends AbstractAzureStorageIT {

    protected static final String TEST_CONTAINER_NAME_PREFIX = "nifi-test-container";
    protected static final String TEST_BLOB_NAME = "nifi-test-blob";
    protected static final String TEST_FILE_NAME = "nifi-test-file";

    protected BlobContainerClient container;

    protected BlobServiceClient getBlobServiceClient() throws Exception {
        String endpoint = String.format("https://%s.blob.core.windows.net", getAccountName());
        StorageSharedKeyCredential storageSharedKeyCredential = new StorageSharedKeyCredential(getAccountName(), getAccountKey());

        return new BlobServiceClientBuilder()
            .endpoint(endpoint)
            .credential(storageSharedKeyCredential)
            .buildClient();
    }

    @Before
    public void setUpAzureBlobStorageIT() throws Exception {
        String containerName = String.format("%s-%s", TEST_CONTAINER_NAME_PREFIX, UUID.randomUUID());
        container            = getBlobServiceClient().getBlobContainerClient(containerName);

        if (!container.exists()) {
            container.create();
        }

        runner.setProperty(AzureStorageUtils.CONTAINER, containerName);
    }

    @After
    public void tearDownAzureBlobStorageIT() throws Exception {
        if (container.exists()) {
            container.delete();
        }
    }

    protected void uploadTestBlob() throws Exception {
        BlockBlobClient blob = container.getBlobClient(TEST_BLOB_NAME).getBlockBlobClient();
        byte[] buf = "0123456789".getBytes();
        InputStream in = new ByteArrayInputStream(buf);
        blob.upload(in, 10);
    }
}
