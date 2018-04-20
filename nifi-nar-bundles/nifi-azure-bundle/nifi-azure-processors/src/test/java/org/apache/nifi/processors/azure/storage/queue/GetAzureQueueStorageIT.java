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
package org.apache.nifi.processors.azure.storage.queue;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import org.apache.nifi.processors.azure.storage.AzureTestUtil;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.List;

public class GetAzureQueueStorageIT {

    private final TestRunner runner = TestRunners.newTestRunner(GetAzureQueueStorage.class);
    private static CloudQueue cloudQueue;

    @BeforeClass
    public static void setup() throws InvalidKeyException, StorageException, URISyntaxException {
        cloudQueue = AzureTestUtil.getQueue(AzureTestUtil.TEST_STORAGE_QUEUE);
        cloudQueue.createIfNotExists();
    }

    @Test
    public void testGetWithAutoDeleteFalse() throws StorageException, InterruptedException {
        cloudQueue.clear();
        insertDummyMessages();

        runner.setProperty(AzureStorageUtils.ACCOUNT_NAME, AzureTestUtil.getAccountName());
        runner.setProperty(AzureStorageUtils.ACCOUNT_KEY, AzureTestUtil.getAccountKey());
        runner.setProperty(GetAzureQueueStorage.QUEUE, AzureTestUtil.TEST_STORAGE_QUEUE);
        runner.setProperty(GetAzureQueueStorage.BATCH_SIZE, "10");
        runner.setProperty(GetAzureQueueStorage.AUTO_DELETE, "false");
        runner.setProperty(GetAzureQueueStorage.VISIBILITY_TIMEOUT, "1 secs");

        runner.run(1);

        final List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(GetAzureQueueStorage.REL_SUCCESS);
        Assert.assertFalse(mockFlowFiles.isEmpty());

        Thread.sleep(1500);
        cloudQueue.downloadAttributes();
        Assert.assertEquals(3, cloudQueue.getApproximateMessageCount());
    }

    @Test
    public void testGetWithELAndAutoDeleteTrue() throws StorageException, InterruptedException {
        cloudQueue.clear();
        insertDummyMessages();

        runner.setValidateExpressionUsage(true);

        runner.setVariable("account.name", AzureTestUtil.getAccountName());
        runner.setVariable("account.key", AzureTestUtil.getAccountKey());
        runner.setVariable("queue.name", AzureTestUtil.TEST_STORAGE_QUEUE);

        runner.setProperty(AzureStorageUtils.ACCOUNT_NAME, "${account.name}");
        runner.setProperty(AzureStorageUtils.ACCOUNT_KEY, "${account.key}");
        runner.setProperty(GetAzureQueueStorage.QUEUE, "${queue.name}");
        runner.setProperty(GetAzureQueueStorage.BATCH_SIZE, "10");
        runner.setProperty(GetAzureQueueStorage.AUTO_DELETE, "true");
        runner.setProperty(GetAzureQueueStorage.VISIBILITY_TIMEOUT, "1 secs");

        runner.run(1);

        final List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(GetAzureQueueStorage.REL_SUCCESS);
        Assert.assertFalse(mockFlowFiles.isEmpty());

        Thread.sleep(1500);
        cloudQueue.downloadAttributes();
        Assert.assertEquals(0, cloudQueue.getApproximateMessageCount());
    }

    @Test
    public void testGetWithVisibilityTimeout() throws StorageException, InterruptedException {
        cloudQueue.clear();
        insertDummyMessages();

        runner.setProperty(AzureStorageUtils.ACCOUNT_NAME, AzureTestUtil.getAccountName());
        runner.setProperty(AzureStorageUtils.ACCOUNT_KEY, AzureTestUtil.getAccountKey());
        runner.setProperty(GetAzureQueueStorage.QUEUE, AzureTestUtil.TEST_STORAGE_QUEUE);
        runner.setProperty(GetAzureQueueStorage.BATCH_SIZE, "10");
        runner.setProperty(GetAzureQueueStorage.AUTO_DELETE, "false");
        runner.setProperty(GetAzureQueueStorage.VISIBILITY_TIMEOUT, "1 secs");

        runner.run(1);

        final List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(GetAzureQueueStorage.REL_SUCCESS);
        Assert.assertFalse(mockFlowFiles.isEmpty());
        Assert.assertEquals(0, AzureTestUtil.getQueueCount());

        Thread.sleep(1500);
        Assert.assertEquals(3, AzureTestUtil.getQueueCount());
    }

    @Test
    public void testGetWithBatchSize() throws StorageException {
        cloudQueue.clear();
        insertDummyMessages();

        runner.setProperty(AzureStorageUtils.ACCOUNT_NAME, AzureTestUtil.getAccountName());
        runner.setProperty(AzureStorageUtils.ACCOUNT_KEY, AzureTestUtil.getAccountKey());
        runner.setProperty(GetAzureQueueStorage.QUEUE, AzureTestUtil.TEST_STORAGE_QUEUE);
        runner.setProperty(GetAzureQueueStorage.BATCH_SIZE, "2");
        runner.setProperty(GetAzureQueueStorage.AUTO_DELETE, "true");
        runner.setProperty(GetAzureQueueStorage.VISIBILITY_TIMEOUT, "1 secs");

        runner.run(1);
        runner.assertAllFlowFilesTransferred(GetAzureQueueStorage.REL_SUCCESS, 2);

        runner.run(1);
        runner.assertAllFlowFilesTransferred(GetAzureQueueStorage.REL_SUCCESS, 3);

    }

    private static void insertDummyMessages() throws StorageException {
        cloudQueue.addMessage(new CloudQueueMessage("Dummy Message 1"), 604800, 0, null, null);
        cloudQueue.addMessage(new CloudQueueMessage("Dummy Message 2"), 604800, 0, null, null);
        cloudQueue.addMessage(new CloudQueueMessage("Dummy Message 3"), 604800, 0, null, null);
    }

    @AfterClass
    public static void cleanup() throws StorageException {
        cloudQueue.deleteIfExists();
    }
}
