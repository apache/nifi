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

import static org.junit.Assert.fail;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Properties;

import org.apache.nifi.processors.azure.AzureConstants;
import org.apache.nifi.util.file.FileUtils;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

class AzureTestUtil {
    private static final String CREDENTIALS_FILE = System.getProperty("user.home") + "/azure-credentials.PROPERTIES";
    static final String TEST_CONTAINER_NAME_PREFIX = "nifitest";

    private static final Properties CONFIG;
    static final String TEST_BLOB_NAME = "testing";

    static {
        final FileInputStream fis;
        CONFIG = new Properties();
        try {
            fis = new FileInputStream(CREDENTIALS_FILE);
            try {
                CONFIG.load(fis);
            } catch (IOException e) {
                fail("Could not open credentials file " + CREDENTIALS_FILE + ": " + e.getLocalizedMessage());
            } finally {
                FileUtils.closeQuietly(fis);
            }
        } catch (FileNotFoundException e) {
            fail("Could not open credentials file " + CREDENTIALS_FILE + ": " + e.getLocalizedMessage());
        }

    }

    static String getAccountName() {
        return CONFIG.getProperty("accountName");
    }

    static String getAccountKey() {
        return CONFIG.getProperty("accountKey");
    }

    static CloudBlobContainer getContainer(String containerName) throws InvalidKeyException, URISyntaxException, StorageException {
        String storageConnectionString = String.format(AzureConstants.FORMAT_DEFAULT_CONNECTION_STRING, getAccountName(), getAccountKey());
        CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
        CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
        return blobClient.getContainerReference(containerName);
    }

}