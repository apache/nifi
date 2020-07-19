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


import java.time.Duration;
import java.util.Iterator;
import java.util.UUID;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueServiceClient;
import com.azure.storage.queue.QueueServiceClientBuilder;
import com.azure.storage.queue.models.QueueMessageItem;
import com.azure.storage.queue.models.QueueStorageException;

import org.apache.nifi.processors.azure.storage.AbstractAzureStorageIT;
import org.junit.After;
import org.junit.Before;

public abstract class AbstractAzureQueueStorageIT extends AbstractAzureStorageIT {

    protected static final String TEST_QUEUE_NAME_PREFIX = "nifi-test-queue";

    protected QueueClient queueClient;

    protected QueueServiceClient getQueueServiceClient() throws Exception {
        String endpoint = String.format("https://%s.queue.core.windows.net", getAccountName());
        StorageSharedKeyCredential storageSharedKeyCredential = new StorageSharedKeyCredential(getAccountName(), getAccountKey());

        return new QueueServiceClientBuilder()
            .endpoint(endpoint)
            .credential(storageSharedKeyCredential)
            .buildClient();
    }

    @Before
    public void setUpAzureQueueStorageIT() throws Exception {
        String queueName = String.format("%s-%s", TEST_QUEUE_NAME_PREFIX, UUID.randomUUID());
        queueClient = getQueueServiceClient().getQueueClient(queueName);

        try {
            queueClient.create();
        } catch (QueueStorageException e) {
            if(e.getStatusCode() == 409) {
                System.out.println("This queue already exists, continuing.");
            } else {
                throw e;
            }
        } catch (RuntimeException e) {
            throw e;
        }

        runner.setProperty(AbstractAzureQueueStorage.QUEUE, queueName);
    }

    @After
    public void tearDownAzureQueueStorageIT() throws Exception {
        try {
            queueClient.delete();
        } catch (QueueStorageException e) {
            if(e.getStatusCode() == 404) {
                System.out.println("This queue does not exist, not deleted.");
            } else {
                throw e;
            }
        } catch (RuntimeException e) {
            throw e;
        }

    }

    protected int getMessageCount() throws Exception {
        final PagedIterable<QueueMessageItem> retrievedMessages = queueClient.receiveMessages(10, Duration.ofSeconds(1), null, null);
        final Iterator<QueueMessageItem> retrievedMessagesIterator = retrievedMessages.iterator();

        int count = 0;

        while (retrievedMessagesIterator.hasNext()) {
            retrievedMessagesIterator.next();
            count++;
        }

        return count;
    }
}
