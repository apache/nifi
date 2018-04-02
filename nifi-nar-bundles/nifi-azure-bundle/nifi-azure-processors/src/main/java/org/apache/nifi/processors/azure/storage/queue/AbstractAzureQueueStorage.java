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
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueClient;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class AbstractAzureQueueStorage extends AbstractProcessor {

    public static final PropertyDescriptor QUEUE = new PropertyDescriptor.Builder()
            .name("storage-cloudQueue-name")
            .displayName("Queue Name")
            .description("Name of the Azure Storage Queue")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
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

    protected CloudQueue cloudQueue;

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> problems = new ArrayList<>(AzureStorageUtils.validateCredentialProperties(validationContext));

        final String queueName = validationContext.getProperty(QUEUE).evaluateAttributeExpressions().getValue();
        if(!StringUtils.isAllLowerCase(queueName)) {
            problems.add(new ValidationResult.Builder()
                                             .subject(QUEUE.getDisplayName())
                                             .valid(false)
                                             .explanation("the name of the Azure Storage Queue should be in lowercase")
                                             .build());
        }

        return problems;
    }

    @OnScheduled
    public final void createCloudQueueClient(final ProcessContext context) {
        final String storageAccountName = context.getProperty(AzureStorageUtils.ACCOUNT_NAME).evaluateAttributeExpressions().getValue();
        final String storageAccountKey = context.getProperty(AzureStorageUtils.ACCOUNT_KEY).evaluateAttributeExpressions().getValue();
        final String sasToken = context.getProperty(AzureStorageUtils.PROP_SAS_TOKEN).evaluateAttributeExpressions().getValue();
        final String storageConnectionString;

        try {
            CloudQueueClient cloudQueueClient;
            if (StringUtils.isNoneBlank(sasToken)) {
                storageConnectionString = String.format(FORMAT_QUEUE_BASE_URI, storageAccountName);
                StorageCredentials storageCredentials = new StorageCredentialsSharedAccessSignature(sasToken);
                cloudQueueClient = new CloudQueueClient(new URI(storageConnectionString), storageCredentials);
            } else {
                storageConnectionString = String.format(FORMAT_QUEUE_CONNECTION_STRING, storageAccountName, storageAccountKey);
                CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
                cloudQueueClient = storageAccount.createCloudQueueClient();
            }

            String queueName = context.getProperty(QUEUE).evaluateAttributeExpressions().getValue();
            cloudQueue = cloudQueueClient.getQueueReference(queueName);

        } catch (URISyntaxException e) {
            getLogger().error("Invalid connection string URI for '{}'", new Object[]{context.getName()}, e);
        } catch (InvalidKeyException e) {
            getLogger().error("Invalid connection credentials for '{}'", new Object[]{context.getName()}, e);
        } catch (StorageException e) {
            getLogger().error("Failed to establish the connection to Azure Queue Storage for '{}'", new Object[]{context.getName()}, e);
        }

    }

}
