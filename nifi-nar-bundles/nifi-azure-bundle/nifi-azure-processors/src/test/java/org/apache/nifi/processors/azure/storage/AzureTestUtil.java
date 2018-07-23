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
import java.util.Iterator;
import java.util.Properties;

import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueClient;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import org.apache.nifi.util.file.FileUtils;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

public class AzureTestUtil {

    private static final Properties CONFIG;

    private static final String CREDENTIALS_FILE = System.getProperty("user.home") + "/azure-credentials.PROPERTIES";
    private static final String FORMAT_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s";

    public static final String TEST_BLOB_NAME = "testing";
    public static final String TEST_STORAGE_QUEUE = "testqueue";
    public static final String TEST_CONTAINER_NAME_PREFIX = "nifitest";

    public static CloudQueue cloudQueue;

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

    public static String getAccountName() {
        return CONFIG.getProperty("accountName");
    }

    public static String getAccountKey() {
        return CONFIG.getProperty("accountKey");
    }

    public static CloudBlobContainer getContainer(String containerName) throws InvalidKeyException, URISyntaxException, StorageException {
        CloudBlobClient blobClient = getStorageAccount().createCloudBlobClient();
        return blobClient.getContainerReference(containerName);
    }

    public static CloudQueue getQueue(String queueName) throws URISyntaxException, InvalidKeyException, StorageException {
        CloudQueueClient cloudQueueClient = getStorageAccount().createCloudQueueClient();
        cloudQueue = cloudQueueClient.getQueueReference(queueName);
        return cloudQueue;
    }

    private static CloudStorageAccount getStorageAccount() throws URISyntaxException, InvalidKeyException {
        String storageConnectionString = String.format(FORMAT_CONNECTION_STRING, getAccountName(), getAccountKey());
        return CloudStorageAccount.parse(storageConnectionString);
    }

    public static int getQueueCount() throws StorageException {
        Iterator<CloudQueueMessage> retrievedMessages = cloudQueue.retrieveMessages(10, 1, null, null).iterator();
        int count = 0;

        while (retrievedMessages.hasNext()) {
            retrievedMessages.next();
            count++;
        }

        return count;
    }
}