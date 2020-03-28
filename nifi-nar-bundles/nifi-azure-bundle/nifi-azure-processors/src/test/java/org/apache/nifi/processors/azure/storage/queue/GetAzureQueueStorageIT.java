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

import java.util.List;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueueMessage;

import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.util.MockFlowFile;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GetAzureQueueStorageIT extends AbstractAzureQueueStorageIT {

    @Override
    protected Class<? extends Processor> getProcessorClass() {
        return GetAzureQueueStorage.class;
    }

    @Before
    public void setUp() throws StorageException {
        cloudQueue.addMessage(new CloudQueueMessage("Dummy Message 1"), 604800, 0, null, null);
        cloudQueue.addMessage(new CloudQueueMessage("Dummy Message 2"), 604800, 0, null, null);
        cloudQueue.addMessage(new CloudQueueMessage("Dummy Message 3"), 604800, 0, null, null);
    }

    @Test
    public void testSimpleGet() throws Exception {
        runner.assertValid();
        runner.run(1);

        assertResult(0);
    }

    @Test
    public void testSimpleGetWithCredentialsService() throws Exception {
        configureCredentialsService();

        runner.assertValid();
        runner.run(1);

        assertResult(0);
    }

    @Test
    public void testNotValidWithCredentialsServiceAndEndpointSuffix() throws Exception {
        configureCredentialsService();
        runner.setProperty(AzureStorageUtils.ENDPOINT_SUFFIX, "core.windows.net");

        runner.assertNotValid();
    }

    @Test
    public void testSimpleGetWithEL() throws Exception {
        runner.setValidateExpressionUsage(true);

        runner.setVariable("account.name", getAccountName());
        runner.setVariable("account.key", getAccountKey());
        runner.setVariable("queue.name", cloudQueue.getName());

        runner.setProperty(AzureStorageUtils.ACCOUNT_NAME, "${account.name}");
        runner.setProperty(AzureStorageUtils.ACCOUNT_KEY, "${account.key}");
        runner.setProperty(GetAzureQueueStorage.QUEUE, "${queue.name}");

        runner.assertValid();
        runner.run(1);

        assertResult(0);
    }

    @Test
    public void testGetWithAutoDeleteFalse() throws Exception {
        runner.setProperty(GetAzureQueueStorage.AUTO_DELETE, "false");

        runner.assertValid();
        runner.run(1);

        assertResult(3);
    }

    @Test
    public void testGetWithVisibilityTimeout() throws Exception {
        runner.setProperty(GetAzureQueueStorage.AUTO_DELETE, "false");
        runner.setProperty(GetAzureQueueStorage.VISIBILITY_TIMEOUT, "1 secs");

        runner.assertValid();
        runner.run(1);

        runner.assertAllFlowFilesTransferred(GetAzureQueueStorage.REL_SUCCESS, 3);
        Assert.assertEquals(0, getMessageCount());

        Thread.sleep(1500);
        Assert.assertEquals(3, getMessageCount());
    }

    @Test
    public void testGetWithBatchSize() throws Exception {
        runner.setProperty(GetAzureQueueStorage.BATCH_SIZE, "2");

        runner.assertValid();
        runner.run(1);

        runner.assertAllFlowFilesTransferred(GetAzureQueueStorage.REL_SUCCESS, 2);
        cloudQueue.downloadAttributes();
        Assert.assertEquals(1, cloudQueue.getApproximateMessageCount());

        runner.run(1);

        runner.assertAllFlowFilesTransferred(GetAzureQueueStorage.REL_SUCCESS, 3);
        cloudQueue.downloadAttributes();
        Assert.assertEquals(0, cloudQueue.getApproximateMessageCount());
    }

    private void assertResult(int expectedMessageCountInQueue) throws Exception {
        runner.assertAllFlowFilesTransferred(GetAzureQueueStorage.REL_SUCCESS, 3);

        List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(GetAzureQueueStorage.REL_SUCCESS);
        int i = 1;
        for (MockFlowFile mockFlowFile : mockFlowFiles) {
            mockFlowFile.assertContentEquals("Dummy Message " + i++);
        }

        cloudQueue.downloadAttributes();
        Assert.assertEquals(expectedMessageCountInQueue, cloudQueue.getApproximateMessageCount());
    }
}
