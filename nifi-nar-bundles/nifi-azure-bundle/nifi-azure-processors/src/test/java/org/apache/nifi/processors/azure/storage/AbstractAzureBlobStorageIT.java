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

import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
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
    protected static final String TEST_FILE_CONTENT = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

    protected CloudBlobContainer container;

    @Before
    public void setUpAzureBlobStorageIT() throws Exception {
        String containerName = String.format("%s-%s", TEST_CONTAINER_NAME_PREFIX, UUID.randomUUID());
        CloudBlobClient blobClient = getStorageAccount().createCloudBlobClient();
        container = blobClient.getContainerReference(containerName);
        container.createIfNotExists();

        runner.setProperty(AzureStorageUtils.CONTAINER, containerName);
    }

    @After
    public void tearDownAzureBlobStorageIT() throws Exception {
        container.deleteIfExists();
    }

    protected void uploadTestBlob() throws Exception {
        CloudBlob blob = container.getBlockBlobReference(TEST_BLOB_NAME);
        byte[] buf = TEST_FILE_CONTENT.getBytes();
        InputStream in = new ByteArrayInputStream(buf);
        blob.upload(in, buf.length);
    }
}
