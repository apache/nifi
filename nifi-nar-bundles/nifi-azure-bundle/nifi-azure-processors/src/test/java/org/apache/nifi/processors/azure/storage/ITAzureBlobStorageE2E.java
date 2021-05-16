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

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.apache.nifi.processors.azure.AbstractAzureBlobProcessor;
import org.apache.nifi.processors.azure.storage.utils.AzureBlobClientSideEncryptionMethod;
import org.apache.nifi.processors.azure.storage.utils.AzureBlobClientSideEncryptionUtils;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.file.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ComparisonFailure;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;

public class ITAzureBlobStorageE2E  {

    private static final Properties CONFIG;

    private static final String CREDENTIALS_FILE = System.getProperty("user.home") + "/azure-credentials.PROPERTIES";

    static {
        CONFIG = new Properties();
        try {
            final FileInputStream fis = new FileInputStream(CREDENTIALS_FILE);
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

    protected static String getAccountName() {
        return CONFIG.getProperty("accountName");
    }

    protected static String getAccountKey() {
        return CONFIG.getProperty("accountKey");
    }

    protected static final String TEST_CONTAINER_NAME_PREFIX = "nifi-test-container";
    protected static final String TEST_BLOB_NAME = "nifi-test-blob";
    protected static final String TEST_FILE_CONTENT = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

    private static final String KEY_ID_VALUE = "key:id";
    private static final String KEY_64B_VALUE = "1234567890ABCDEF";
    private static final String KEY_128B_VALUE = KEY_64B_VALUE + KEY_64B_VALUE;
    private static final String KEY_192B_VALUE = KEY_128B_VALUE + KEY_64B_VALUE;
    private static final String KEY_256B_VALUE = KEY_128B_VALUE + KEY_128B_VALUE;
    private static final String KEY_384B_VALUE = KEY_256B_VALUE + KEY_128B_VALUE;
    private static final String KEY_512B_VALUE = KEY_256B_VALUE + KEY_256B_VALUE;

    protected TestRunner putRunner;
    protected TestRunner listRunner;
    protected TestRunner fetchRunner;

    protected CloudBlobContainer container;

    @Before
    public void setupRunners() throws Exception {
        putRunner = TestRunners.newTestRunner(new PutAzureBlobStorage());
        listRunner = TestRunners.newTestRunner(new ListAzureBlobStorage());
        fetchRunner = TestRunners.newTestRunner(new FetchAzureBlobStorage());

        String containerName = String.format("%s-%s", TEST_CONTAINER_NAME_PREFIX, UUID.randomUUID());

        StorageCredentials storageCredentials = new StorageCredentialsAccountAndKey(getAccountName(), getAccountKey());
        CloudStorageAccount storageAccount = new CloudStorageAccount(storageCredentials, true);

        CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
        container = blobClient.getContainerReference(containerName);
        container.createIfNotExists();

        setRunnerProperties(putRunner, containerName);
        setRunnerProperties(listRunner, containerName);
        setRunnerProperties(fetchRunner, containerName);
    }

    private void setRunnerProperties(TestRunner runner, String containerName) {
        runner.setProperty(AzureStorageUtils.ACCOUNT_NAME, getAccountName());
        runner.setProperty(AzureStorageUtils.ACCOUNT_KEY, getAccountKey());
        runner.setProperty(AzureStorageUtils.CONTAINER, containerName);
    }

    @After
    public void tearDownAzureContainer() throws Exception {
        container.deleteIfExists();
    }

    @Test
    public void AzureBlobStorageE2ENoCSE() throws Exception {
        testE2E(AzureBlobClientSideEncryptionMethod.NONE.name(),
                null,
                null,
                AzureBlobClientSideEncryptionMethod.NONE.name(),
                null,
                null
        );
    }

    @Test
    public void AzureBlobStorageE2E128BCSE() throws Exception {
        testE2E(AzureBlobClientSideEncryptionMethod.SYMMETRIC.name(),
                KEY_ID_VALUE,
                KEY_128B_VALUE,
                AzureBlobClientSideEncryptionMethod.SYMMETRIC.name(),
                KEY_ID_VALUE,
                KEY_128B_VALUE
        );
    }

    @Test
    public void AzureBlobStorageE2E192BCSE() throws Exception {
        testE2E(AzureBlobClientSideEncryptionMethod.SYMMETRIC.name(),
                KEY_ID_VALUE,
                KEY_192B_VALUE,
                AzureBlobClientSideEncryptionMethod.SYMMETRIC.name(),
                KEY_ID_VALUE,
                KEY_192B_VALUE
        );
    }

    @Test
    public void AzureBlobStorageE2E256BCSE() throws Exception {
        testE2E(AzureBlobClientSideEncryptionMethod.SYMMETRIC.name(),
                KEY_ID_VALUE,
                KEY_256B_VALUE,
                AzureBlobClientSideEncryptionMethod.SYMMETRIC.name(),
                KEY_ID_VALUE,
                KEY_256B_VALUE
        );
    }

    @Test
    public void AzureBlobStorageE2E384BCSE() throws Exception {
        testE2E(AzureBlobClientSideEncryptionMethod.SYMMETRIC.name(),
                KEY_ID_VALUE,
                KEY_384B_VALUE,
                AzureBlobClientSideEncryptionMethod.SYMMETRIC.name(),
                KEY_ID_VALUE,
                KEY_384B_VALUE
        );
    }

    @Test
    public void AzureBlobStorageE2E512BCSE() throws Exception {
        testE2E(AzureBlobClientSideEncryptionMethod.SYMMETRIC.name(),
                KEY_ID_VALUE,
                KEY_512B_VALUE,
                AzureBlobClientSideEncryptionMethod.SYMMETRIC.name(),
                KEY_ID_VALUE,
                KEY_512B_VALUE
        );
    }

    @Test(expected = ComparisonFailure.class)
    public void AzureBlobStorageE2E128BCSENoDecryption() throws Exception {
        testE2E(AzureBlobClientSideEncryptionMethod.SYMMETRIC.name(),
                KEY_ID_VALUE,
                KEY_128B_VALUE,
                AzureBlobClientSideEncryptionMethod.NONE.name(),
                KEY_ID_VALUE,
                KEY_128B_VALUE
        );
    }

    private void testE2E(String encryptionKeyType, String encryptionKeyId, String encryptionKeyHex, String decryptionKeyType, String decryptionKeyId, String decryptionKeyHex) throws Exception {
        putRunner.setProperty(PutAzureBlobStorage.BLOB, TEST_BLOB_NAME);
        putRunner.setProperty(AzureBlobClientSideEncryptionUtils.CSE_KEY_TYPE, encryptionKeyType);
        if (encryptionKeyId == null || encryptionKeyId.isEmpty() || encryptionKeyId.trim().isEmpty()) {
            putRunner.removeProperty(AzureBlobClientSideEncryptionUtils.CSE_KEY_ID);
        } else {
            putRunner.setProperty(AzureBlobClientSideEncryptionUtils.CSE_KEY_ID, encryptionKeyId);
        }
        if (encryptionKeyHex == null || encryptionKeyHex.isEmpty() || encryptionKeyHex.trim().isEmpty()) {
            putRunner.removeProperty(AzureBlobClientSideEncryptionUtils.CSE_SYMMETRIC_KEY_HEX);
        } else {
            putRunner.setProperty(AzureBlobClientSideEncryptionUtils.CSE_SYMMETRIC_KEY_HEX, encryptionKeyHex);
        }

        putRunner.assertValid();
        putRunner.enqueue(TEST_FILE_CONTENT.getBytes());
        putRunner.run();
        putRunner.assertAllFlowFilesTransferred(PutAzureBlobStorage.REL_SUCCESS, 1);

        Thread.sleep(ListAzureBlobStorage.LISTING_LAG_MILLIS.get(TimeUnit.SECONDS) * 2);

        listRunner.assertValid();
        listRunner.run();
        listRunner.assertAllFlowFilesTransferred(PutAzureBlobStorage.REL_SUCCESS, 1);

        MockFlowFile entry = listRunner.getFlowFilesForRelationship(ListAzureBlobStorage.REL_SUCCESS).get(0);
        entry.assertAttributeEquals("mime.type", "application/octet-stream");

        fetchRunner.setProperty(AzureBlobClientSideEncryptionUtils.CSE_KEY_TYPE, decryptionKeyType);
        if (decryptionKeyId == null || decryptionKeyId.isEmpty() || decryptionKeyId.trim().isEmpty()) {
            fetchRunner.removeProperty(AzureBlobClientSideEncryptionUtils.CSE_KEY_ID);
        } else {
            fetchRunner.setProperty(AzureBlobClientSideEncryptionUtils.CSE_KEY_ID, decryptionKeyId);
        }
        if (decryptionKeyHex == null || decryptionKeyHex.isEmpty() || decryptionKeyHex.trim().isEmpty()) {
            fetchRunner.removeProperty(AzureBlobClientSideEncryptionUtils.CSE_SYMMETRIC_KEY_HEX);
        } else {
            fetchRunner.setProperty(AzureBlobClientSideEncryptionUtils.CSE_SYMMETRIC_KEY_HEX, decryptionKeyHex);
        }
        fetchRunner.assertValid();
        fetchRunner.enqueue(entry);
        fetchRunner.run();
        fetchRunner.assertAllFlowFilesTransferred(AbstractAzureBlobProcessor.REL_SUCCESS, 1);
        MockFlowFile fetchedEntry = fetchRunner.getFlowFilesForRelationship(ListAzureBlobStorage.REL_SUCCESS).get(0);
        fetchedEntry.assertContentEquals(TEST_FILE_CONTENT);
    }

}
