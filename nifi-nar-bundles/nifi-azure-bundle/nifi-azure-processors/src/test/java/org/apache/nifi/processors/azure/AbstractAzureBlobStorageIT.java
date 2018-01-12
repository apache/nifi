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
package org.apache.nifi.processors.azure;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.apache.nifi.processors.azure.storage.AzureTestUtil;
import org.junit.Assert;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

public class AbstractAzureBlobStorageIT {

    protected final static String SAMPLE_FILE_NAME = "/hello.txt";
    protected final static String SAMPLE_BLOB_NAME = "testing";

    protected void uploadBlob(String containerName, String filePath) throws URISyntaxException, StorageException, InvalidKeyException, IOException {
        CloudBlobContainer container = AzureTestUtil.getContainer(containerName);
        CloudBlob blob = container.getBlockBlobReference(SAMPLE_BLOB_NAME);
        blob.uploadFromFile(filePath);
    }

    protected String getFileFromResource(String fileName) {
        URI uri = null;
        try {
            uri = this.getClass().getResource(fileName).toURI();
        } catch (URISyntaxException e) {
            Assert.fail("Cannot proceed without File : " + fileName);
        }

        return uri.toString();
    }

}

