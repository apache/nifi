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
package org.apache.nifi.processors.azure;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;

import com.microsoft.azure.storage.CloudStorageAccount;

public abstract class AbstractAzureProcessor extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("All successfully processed FlowFiles are routed to this relationship").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("Unsuccessful operations will be transferred to the failure relationship.").build();
    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    protected CloudStorageAccount createStorageConnection(ProcessContext context) {
        final String accountName = context.getProperty(AzureConstants.ACCOUNT_NAME).evaluateAttributeExpressions().getValue();
        final String accountKey = context.getProperty(AzureConstants.ACCOUNT_KEY).evaluateAttributeExpressions().getValue();
        return createStorageConnectionFromNameAndKey(accountName, accountKey);
    }

    protected CloudStorageAccount createStorageConnection(ProcessContext context, FlowFile flowFile) {
        final String accountName = context.getProperty(AzureConstants.ACCOUNT_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String accountKey = context.getProperty(AzureConstants.ACCOUNT_KEY).evaluateAttributeExpressions(flowFile).getValue();
        return createStorageConnectionFromNameAndKey(accountName, accountKey);
    }

    private CloudStorageAccount createStorageConnectionFromNameAndKey(String accountName, String accountKey) {
        final String storageConnectionString = String.format(AzureConstants.FORMAT_DEFAULT_CONNECTION_STRING, accountName, accountKey);
        try {
            return createStorageAccountFromConnectionString(storageConnectionString);
        } catch (InvalidKeyException | IllegalArgumentException | URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Validates the connection string and returns the storage account. The connection string must be in the Azure connection string format.
     *
     * @param storageConnectionString
     *            Connection string for the storage service or the emulator
     * @return The newly created CloudStorageAccount object
     *
     */
    private static CloudStorageAccount createStorageAccountFromConnectionString(String storageConnectionString) throws IllegalArgumentException, URISyntaxException, InvalidKeyException {
        CloudStorageAccount storageAccount;
        storageAccount = CloudStorageAccount.parse(storageConnectionString);
        return storageAccount;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

}
