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

import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.junit.Assert;
import org.junit.Test;

public class PutAzureQueueStorageIT extends AbstractAzureQueueStorageIT {

    @Override
    protected Class<? extends Processor> getProcessorClass() {
        return PutAzureQueueStorage.class;
    }

    @Test
    public void testSimplePut() {
        runner.assertValid();
        runner.enqueue("Dummy message");
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutAzureQueueStorage.REL_SUCCESS, 1);
    }

    @Test
    public void testSimplePutWithCredentialsService() throws Exception {
        configureCredentialsService();

        runner.assertValid();
        runner.enqueue("Dummy message");
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutAzureQueueStorage.REL_SUCCESS, 1);
    }

    @Test
    public void testSimplePutWithEL() {
        runner.setValidateExpressionUsage(true);

        runner.setVariable("account.name", getAccountName());
        runner.setVariable("account.key", getAccountKey());
        runner.setVariable("queue.name", cloudQueue.getName());

        runner.setProperty(AzureStorageUtils.ACCOUNT_NAME, "${account.name}");
        runner.setProperty(AzureStorageUtils.ACCOUNT_KEY, "${account.key}");
        runner.setProperty(PutAzureQueueStorage.QUEUE, "${queue.name}");

        runner.assertValid();
        runner.enqueue("Dummy message");
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutAzureQueueStorage.REL_SUCCESS, 1);
    }

    @Test
    public void testPutWithTTL() throws Exception {
        runner.setProperty(PutAzureQueueStorage.TTL, "2 secs");

        runner.assertValid();
        runner.enqueue("Dummy message");
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutAzureQueueStorage.REL_SUCCESS, 1);
        Assert.assertEquals(1, getMessageCount());

        Thread.sleep(2400);
        Assert.assertEquals(0, getMessageCount());
    }

    @Test
    public void testPutWithVisibilityDelay() throws Exception {
        runner.setProperty(PutAzureQueueStorage.VISIBILITY_DELAY, "2 secs");

        runner.assertValid();
        runner.enqueue("Dummy message");
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutAzureQueueStorage.REL_SUCCESS, 1);
        Assert.assertEquals(0, getMessageCount());

        Thread.sleep(2400);
        Assert.assertEquals(1, getMessageCount());
    }
}
