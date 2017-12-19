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
package org.apache.nifi.processors.azure.storage.utils;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsSharedAccessSignature;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class AzureStorageUtils {
    public static final String BLOCK = "Block";
    public static final String PAGE = "Page";

    public static final PropertyDescriptor ACCOUNT_KEY = new PropertyDescriptor.Builder().name("storage-account-key").displayName("Storage Account Key")
            .description("The storage account key. This is an admin-like password providing access to every container in this account. It is recommended " +
                    "one uses Shared Access Signature (SAS) token instead for fine-grained control with policies. " +
                    "There are certain risks in allowing the account key to be stored as a flowfile" +
                    "attribute. While it does provide for a more flexible flow by allowing the account key to " +
                    "be fetched dynamically from a flow file attribute, care must be taken to restrict access to " +
                    "the event provenance data (e.g. by strictly controlling the policies governing provenance for this Processor). " +
                    "In addition, the provenance repositories may be put on encrypted disk partitions.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true).required(false).sensitive(true).build();

    public static final PropertyDescriptor ACCOUNT_NAME = new PropertyDescriptor.Builder().name("storage-account-name").displayName("Storage Account Name")
            .description("The storage account name.  There are certain risks in allowing the account name to be stored as a flowfile " +
                    "attribute. While it does provide for a more flexible flow by allowing the account name to " +
                    "be fetched dynamically from a flowfile attribute, care must be taken to restrict access to " +
                    "the event provenance data (e.g. by strictly controlling the policies governing provenance for this Processor). " +
                    "In addition, the provenance repositories may be put on encrypted disk partitions.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true).required(true).sensitive(true).build();

    public static final PropertyDescriptor CONTAINER = new PropertyDescriptor.Builder().name("container-name").displayName("Container Name")
            .description("Name of the Azure storage container").addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true).required(true).build();

    public static final PropertyDescriptor PROP_SAS_TOKEN = new PropertyDescriptor.Builder()
            .name("storage-sas-token")
            .displayName("SAS Token")
            .description("Shared Access Signature token, including the leading '?'. Specify either SAS Token (recommended) or Account Key")
            .required(false)
            .expressionLanguageSupported(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    // use HTTPS by default as per MSFT recommendation
    public static final String FORMAT_BLOB_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s";
    public static final String FORMAT_BASE_URI = "https://%s.blob.core.windows.net";

    private AzureStorageUtils() {
        // do not instantiate
    }

    public static CloudBlobClient createCloudBlobClient(ProcessContext context, ComponentLog logger) {
        final String accountName = context.getProperty(AzureStorageUtils.ACCOUNT_NAME).evaluateAttributeExpressions().getValue();
        final String accountKey = context.getProperty(AzureStorageUtils.ACCOUNT_KEY).evaluateAttributeExpressions().getValue();
        final String sasToken = context.getProperty(AzureStorageUtils.PROP_SAS_TOKEN).evaluateAttributeExpressions().getValue();

        CloudBlobClient cloudBlobClient;

        try {
            // sas token and acct name/key have different ways of creating a secure connection (e.g. new StorageCredentialsAccountAndKey didn't work)
            if (StringUtils.isNotBlank(sasToken)) {
                String storageConnectionString = String.format(AzureStorageUtils.FORMAT_BASE_URI, accountName);
                StorageCredentials creds = new StorageCredentialsSharedAccessSignature(sasToken);
                cloudBlobClient = new CloudBlobClient(new URI(storageConnectionString), creds);
            } else {
                String blobConnString = String.format(AzureStorageUtils.FORMAT_BLOB_CONNECTION_STRING, accountName, accountKey);
                CloudStorageAccount storageAccount = CloudStorageAccount.parse(blobConnString);
                cloudBlobClient = storageAccount.createCloudBlobClient();
            }
        } catch (IllegalArgumentException | URISyntaxException e) {
            logger.error("Invalid connection string URI for '{}'", new Object[]{context.getName()}, e);
            throw new IllegalArgumentException(e);
        } catch (InvalidKeyException e) {
            logger.error("Invalid connection credentials for '{}'", new Object[]{context.getName()}, e);
            throw new IllegalArgumentException(e);
        }

        return cloudBlobClient;
    }

    public static Collection<ValidationResult> validateCredentialProperties(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        String sasToken = validationContext.getProperty(PROP_SAS_TOKEN).getValue();
        String acctName = validationContext.getProperty(ACCOUNT_KEY).getValue();
        if ((StringUtils.isBlank(sasToken) && StringUtils.isBlank(acctName))
                || (StringUtils.isNotBlank(sasToken) && StringUtils.isNotBlank(acctName))) {
            results.add(new ValidationResult.Builder().subject("AzureStorageUtils Credentials")
                        .valid(false)
                        .explanation("either Azure Account Key or Shared Access Signature required, but not both")
                        .build());
        }

        return results;
    }
}
