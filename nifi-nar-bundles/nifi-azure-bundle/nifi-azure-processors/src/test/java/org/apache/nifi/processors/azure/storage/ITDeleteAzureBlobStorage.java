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

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.UUID;

import org.apache.nifi.processors.azure.AbstractAzureBlobStorageIT;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

public class ITDeleteAzureBlobStorage extends AbstractAzureBlobStorageIT{

    @Test
    public void testDeleteBlob() throws StorageException, URISyntaxException, InvalidKeyException, IOException {
        String containerName = String.format("%s-%s", AzureTestUtil.TEST_CONTAINER_NAME_PREFIX, UUID.randomUUID());
        CloudBlobContainer container = AzureTestUtil.getContainer(containerName);
        container.createIfNotExists();

        uploadBlob(containerName, getFileFromResource(SAMPLE_FILE_NAME));

        final TestRunner runner = TestRunners.newTestRunner(DeleteAzureBlobStorage.class);

        try {
            runner.setProperty(AzureStorageUtils.ACCOUNT_NAME, AzureTestUtil.getAccountName());
            runner.setProperty(AzureStorageUtils.ACCOUNT_KEY, AzureTestUtil.getAccountKey());
            runner.setProperty(AzureStorageUtils.CONTAINER, containerName);
            runner.setProperty(DeleteAzureBlobStorage.BLOB, AzureTestUtil.TEST_BLOB_NAME);

            runner.enqueue(new byte[0]);
            runner.run(1);

            runner.assertAllFlowFilesTransferred(DeleteAzureBlobStorage.REL_SUCCESS);

        } finally {
            container.deleteIfExists();
        }
    }

}
