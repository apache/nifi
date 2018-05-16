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
import org.apache.nifi.processors.azure.storage.AzureTestUtil;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

public class PutAzureQueueStorageIT {

    private final TestRunner runner = TestRunners.newTestRunner(PutAzureQueueStorage.class);
    private static CloudQueue cloudQueue;

    @BeforeClass
    public static void setup() throws InvalidKeyException, StorageException, URISyntaxException {
        cloudQueue = AzureTestUtil.getQueue(AzureTestUtil.TEST_STORAGE_QUEUE);
        cloudQueue.createIfNotExists();
    }

    @Test
    public void testSimplePut() throws InvalidKeyException, StorageException, URISyntaxException {
        runner.setProperty(AzureStorageUtils.ACCOUNT_NAME, AzureTestUtil.getAccountName());
        runner.setProperty(AzureStorageUtils.ACCOUNT_KEY, AzureTestUtil.getAccountKey());
        runner.setProperty(PutAzureQueueStorage.QUEUE, AzureTestUtil.TEST_STORAGE_QUEUE);

        runner.enqueue("Dummy message");
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutAzureQueueStorage.REL_SUCCESS, 1);
    }

    @Test
    public void testSimplePutWithEL() throws StorageException, URISyntaxException, InvalidKeyException {
        runner.setValidateExpressionUsage(true);

        runner.setVariable("account.name", AzureTestUtil.getAccountName());
        runner.setVariable("account.key", AzureTestUtil.getAccountKey());
        runner.setVariable("queue.name", AzureTestUtil.TEST_STORAGE_QUEUE);

        runner.setProperty(AzureStorageUtils.ACCOUNT_NAME, "${account.name}");
        runner.setProperty(AzureStorageUtils.ACCOUNT_KEY, "${account.key}");
        runner.setProperty(PutAzureQueueStorage.QUEUE, "${queue.name}");

        runner.enqueue("Dummy message");
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutAzureQueueStorage.REL_SUCCESS, 1);
    }

    @Test
    public void testPutWithTTL() throws StorageException, InterruptedException {
        cloudQueue.clear();

        runner.setProperty(AzureStorageUtils.ACCOUNT_NAME, AzureTestUtil.getAccountName());
        runner.setProperty(AzureStorageUtils.ACCOUNT_KEY, AzureTestUtil.getAccountKey());
        runner.setProperty(PutAzureQueueStorage.QUEUE, AzureTestUtil.TEST_STORAGE_QUEUE);
        runner.setProperty(PutAzureQueueStorage.TTL, "2 secs");

        runner.enqueue("Dummy message");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutAzureQueueStorage.REL_SUCCESS, 1);
        Assert.assertEquals(1, AzureTestUtil.getQueueCount());

        Thread.sleep(2400);
        Assert.assertEquals(0, AzureTestUtil.getQueueCount());
    }

    @Test
    public void testPutWithVisibilityDelay() throws StorageException, InterruptedException {
        cloudQueue.clear();

        cloudQueue.clear();

        runner.setProperty(AzureStorageUtils.ACCOUNT_NAME, AzureTestUtil.getAccountName());
        runner.setProperty(AzureStorageUtils.ACCOUNT_KEY, AzureTestUtil.getAccountKey());
        runner.setProperty(PutAzureQueueStorage.QUEUE, AzureTestUtil.TEST_STORAGE_QUEUE);
        runner.setProperty(PutAzureQueueStorage.VISIBILITY_DELAY, "2 secs");

        runner.enqueue("Dummy message");
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutAzureQueueStorage.REL_SUCCESS, 1);
        Assert.assertEquals(0, AzureTestUtil.getQueueCount());

        Thread.sleep(2400);
        Assert.assertEquals(1, AzureTestUtil.getQueueCount());
    }

    @AfterClass
    public static void cleanup() throws StorageException {
        cloudQueue.deleteIfExists();
    }
}
