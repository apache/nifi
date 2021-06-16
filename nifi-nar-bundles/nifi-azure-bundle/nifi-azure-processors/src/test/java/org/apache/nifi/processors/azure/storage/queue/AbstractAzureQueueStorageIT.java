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

import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueClient;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import org.apache.nifi.processors.azure.storage.AbstractAzureStorageIT;
import org.junit.After;
import org.junit.Before;

import java.util.Iterator;
import java.util.UUID;

public abstract class AbstractAzureQueueStorageIT extends AbstractAzureStorageIT {

    protected static final String TEST_QUEUE_NAME_PREFIX = "nifi-test-queue";

    protected CloudQueue cloudQueue;

    @Before
    public void setUpAzureQueueStorageIT() throws Exception {
        String queueName = String.format("%s-%s", TEST_QUEUE_NAME_PREFIX, UUID.randomUUID());
        CloudQueueClient cloudQueueClient = getStorageAccount().createCloudQueueClient();
        cloudQueue = cloudQueueClient.getQueueReference(queueName);
        cloudQueue.createIfNotExists();

        runner.setProperty(AbstractAzureQueueStorage.QUEUE, queueName);
    }

    @After
    public void tearDownAzureQueueStorageIT() throws Exception {
        cloudQueue.deleteIfExists();
    }

    protected int getMessageCount() throws Exception {
        Iterator<CloudQueueMessage> retrievedMessages = cloudQueue.retrieveMessages(10, 1, null, null).iterator();
        int count = 0;

        while (retrievedMessages.hasNext()) {
            retrievedMessages.next();
            count++;
        }

        return count;
    }
}
