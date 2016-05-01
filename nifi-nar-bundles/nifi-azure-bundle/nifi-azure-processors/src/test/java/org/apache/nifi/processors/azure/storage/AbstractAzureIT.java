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

import org.apache.nifi.util.file.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.DeleteSnapshotsOption;
import com.microsoft.azure.storage.blob.ListBlobItem;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;

public abstract class AbstractAzureIT {
    protected static final String CREDENTIALS_FILE = System.getProperty("user.home") + "/azure-credentials.PROPERTIES";
    public static final String TEST_CONTAINER_NAME = "nifitest";

    private static final Properties CONFIG;
    protected static final String TEST_BLOB_NAME = "testing";
    protected static final String TEST_TABLE_NAME = "testing";

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

    @BeforeClass
    public static void oneTimeSetup() throws StorageException, InvalidKeyException, URISyntaxException {
        CloudBlobContainer container = getContainer();
        container.createIfNotExists();
    }

    @AfterClass
    public static void tearDown() throws InvalidKeyException, URISyntaxException, StorageException {
        CloudBlobContainer container = getContainer();
        for (ListBlobItem blob : container.listBlobs()) {
            if (blob instanceof CloudBlob) {
                ((CloudBlob) blob).delete(DeleteSnapshotsOption.INCLUDE_SNAPSHOTS, null, null, null);
            }
        }
    }

    public static String getAccountName() {
        return CONFIG.getProperty("accountName");
    }

    public static String getAccountKey() {
        return CONFIG.getProperty("accountKey");
    }

    protected static CloudBlobContainer getContainer() throws InvalidKeyException, URISyntaxException, StorageException {
        String storageConnectionString = String.format("DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s", getAccountName(), getAccountKey());
        CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
        CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
        return blobClient.getContainerReference(TEST_CONTAINER_NAME);
    }

    protected static CloudTable getTable() throws InvalidKeyException, URISyntaxException, StorageException {
        String storageConnectionString = String.format("DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s", getAccountName(), getAccountKey());
        CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
        CloudTableClient tableClient = storageAccount.createCloudTableClient();
        return tableClient.getTableReference(TEST_TABLE_NAME);
    }

}