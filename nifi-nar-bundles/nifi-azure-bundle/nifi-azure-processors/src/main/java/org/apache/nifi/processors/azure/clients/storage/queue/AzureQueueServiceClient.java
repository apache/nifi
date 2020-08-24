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
package org.apache.nifi.processors.azure.clients.storage.queue;

import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueServiceClient;
import com.azure.storage.queue.QueueServiceClientBuilder;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processors.azure.clients.AbstractAzureServiceClient;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsDetails;

public class AzureQueueServiceClient extends AbstractAzureServiceClient<QueueServiceClient> {

    public AzureQueueServiceClient(PropertyContext context, FlowFile flowFile) {
        super(context, flowFile);
    }

    @Override
    public void setServiceClient(PropertyContext context, FlowFile flowFile) {
        final AzureStorageCredentialsDetails storageCredentialsDetails = AzureStorageUtils.getStorageCredentialsDetails(context, flowFile);
        final String accountName = storageCredentialsDetails.getStorageAccountName();
        final String queueURL = String.format("https://%s.queue.core.windows.net", accountName);

        // use HttpClient object to allow proxy setting
        final QueueServiceClientBuilder queueServiceClientBuilder = new QueueServiceClientBuilder()
                .endpoint(queueURL);

        switch (storageCredentialsDetails.getCredentialType()) {
            case SAS_TOKEN:
                this.client = queueServiceClientBuilder.sasToken(storageCredentialsDetails.getSasToken())
                        .buildClient();
                break;
            case STORAGE_ACCOUNT_KEY:
                this.client = queueServiceClientBuilder.credential(storageCredentialsDetails.getStorageSharedKeyCredential())
                        .buildClient();
                break;
            default:
                throw new IllegalArgumentException(String.format("Invalid credential type '%s'!", storageCredentialsDetails.getCredentialType().toString()));
        }
    }

    public QueueClient getQueueClient(final String queueName) {
        return this.client.getQueueClient(queueName);
    }
}
