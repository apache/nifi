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

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsSharedAccessSignature;
import com.microsoft.azure.storage.queue.CloudQueueClient;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public abstract class AbstractAzureQueueStorage extends AbstractProcessor {

    public static final PropertyDescriptor QUEUE = new PropertyDescriptor.Builder()
            .name("storage-queue-name")
            .displayName("Queue Name")
            .description("Name of the Azure Storage Queue")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All successfully processed FlowFiles are routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Unsuccessful operations will be transferred to the failure relationship.")
            .build();

    private static final String FORMAT_QUEUE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s";
    private static final String FORMAT_QUEUE_BASE_URI = "https://%s.queue.core.windows.net";

    private static final Set<Relationship> relationships = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    protected final CloudQueueClient createCloudQueueClient(final ProcessContext context, final FlowFile flowFile) {
        final String storageAccountName;
        final String storageAccountKey;
        final String sasToken;
        final String connectionString;

        if (flowFile == null) {
            storageAccountName = context.getProperty(AzureStorageUtils.ACCOUNT_NAME).evaluateAttributeExpressions().getValue();
            storageAccountKey = context.getProperty(AzureStorageUtils.ACCOUNT_KEY).evaluateAttributeExpressions().getValue();
            sasToken = context.getProperty(AzureStorageUtils.PROP_SAS_TOKEN).evaluateAttributeExpressions().getValue();
        } else {
            storageAccountName = context.getProperty(AzureStorageUtils.ACCOUNT_NAME).evaluateAttributeExpressions(flowFile).getValue();
            storageAccountKey = context.getProperty(AzureStorageUtils.ACCOUNT_KEY).evaluateAttributeExpressions(flowFile).getValue();
            sasToken = context.getProperty(AzureStorageUtils.PROP_SAS_TOKEN).evaluateAttributeExpressions(flowFile).getValue();
        }

        CloudQueueClient cloudQueueClient;
        try {
            if (StringUtils.isNoneBlank(sasToken)) {
                connectionString = String.format(FORMAT_QUEUE_BASE_URI, storageAccountName);
                StorageCredentials storageCredentials = new StorageCredentialsSharedAccessSignature(sasToken);
                cloudQueueClient = new CloudQueueClient(new URI(connectionString), storageCredentials);
            } else {
                connectionString = String.format(FORMAT_QUEUE_CONNECTION_STRING, storageAccountName, storageAccountKey);
                CloudStorageAccount storageAccount = CloudStorageAccount.parse(connectionString);
                cloudQueueClient = storageAccount.createCloudQueueClient();
            }
        } catch (IllegalArgumentException | URISyntaxException e) {
            getLogger().error("Invalid connection string URI for '{}'", new Object[]{context.getName()}, e);
            throw new IllegalArgumentException(e);
        } catch (InvalidKeyException e) {
            getLogger().error("Invalid connection credentials for '{}'", new Object[]{context.getName()}, e);
            throw new IllegalArgumentException(e);
        }
        return cloudQueueClient;
    }

}
