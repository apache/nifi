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
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.StorageCredentialsSharedAccessSignature;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxySpec;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsDetails;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsService;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public final class AzureStorageUtils {
    public static final String BLOCK = "Block";
    public static final String PAGE = "Page";

    public static final PropertyDescriptor ACCOUNT_KEY = new PropertyDescriptor.Builder()
            .name("storage-account-key")
            .displayName("Storage Account Key")
            .description("The storage account key. This is an admin-like password providing access to every container in this account. It is recommended " +
                    "one uses Shared Access Signature (SAS) token instead for fine-grained control with policies. " +
                    "There are certain risks in allowing the account key to be stored as a flowfile " +
                    "attribute. While it does provide for a more flexible flow by allowing the account key to " +
                    "be fetched dynamically from a flow file attribute, care must be taken to restrict access to " +
                    "the event provenance data (e.g. by strictly controlling the policies governing provenance for this Processor). " +
                    "In addition, the provenance repositories may be put on encrypted disk partitions.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .sensitive(true)
            .build();

    public static final String ACCOUNT_NAME_BASE_DESCRIPTION =
            "The storage account name.  There are certain risks in allowing the account name to be stored as a flowfile " +
            "attribute. While it does provide for a more flexible flow by allowing the account name to " +
            "be fetched dynamically from a flowfile attribute, care must be taken to restrict access to " +
            "the event provenance data (e.g. by strictly controlling the policies governing provenance for this Processor). " +
            "In addition, the provenance repositories may be put on encrypted disk partitions.";

    public static final PropertyDescriptor ACCOUNT_NAME = new PropertyDescriptor.Builder()
            .name("storage-account-name")
            .displayName("Storage Account Name")
            .description(ACCOUNT_NAME_BASE_DESCRIPTION +
                    " Instead of defining the Storage Account Name, Storage Account Key and SAS Token properties directly on the processor, " +
                    "the preferred way is to configure them through a controller service specified in the Storage Credentials property. " +
                    "The controller service can provide a common/shared configuration for multiple/all Azure processors. Furthermore, the credentials " +
                    "can also be looked up dynamically with the 'Lookup' version of the service.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor CONTAINER = new PropertyDescriptor.Builder()
            .name("container-name")
            .displayName("Container Name")
            .description("Name of the Azure storage container")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final PropertyDescriptor PROP_SAS_TOKEN = new PropertyDescriptor.Builder()
            .name("storage-sas-token")
            .displayName("SAS Token")
            .description("Shared Access Signature token, including the leading '?'. Specify either SAS Token (recommended) or Account Key. " +
                    "There are certain risks in allowing the SAS token to be stored as a flowfile " +
                    "attribute. While it does provide for a more flexible flow by allowing the account name to " +
                    "be fetched dynamically from a flowfile attribute, care must be taken to restrict access to " +
                    "the event provenance data (e.g. by strictly controlling the policies governing provenance for this Processor). " +
                    "In addition, the provenance repositories may be put on encrypted disk partitions.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor STORAGE_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
            .name("storage-credentials-service")
            .displayName("Storage Credentials")
            .description("The Controller Service used to obtain Azure Storage Credentials. Instead of the processor level properties, " +
                    "the credentials can be configured here through a common/shared controller service, which is the preferred way. " +
                    "The 'Lookup' version of the service can also be used to select the credentials dynamically at runtime " +
                    "based on a FlowFile attribute (if the processor has FlowFile input).")
            .identifiesControllerService(AzureStorageCredentialsService.class)
            .required(false)
            .build();

    private AzureStorageUtils() {
        // do not instantiate
    }

    /**
     * Create CloudBlobClient instance.
     * @param flowFile An incoming FlowFile can be used for NiFi Expression Language evaluation to derive
     *                 Account Name, Account Key or SAS Token. This can be null if not available.
     */
    public static CloudBlobClient createCloudBlobClient(ProcessContext context, ComponentLog logger, FlowFile flowFile) throws URISyntaxException {
        final AzureStorageCredentialsDetails storageCredentialsDetails = getStorageCredentialsDetails(context, flowFile);
        final CloudStorageAccount cloudStorageAccount = new CloudStorageAccount(storageCredentialsDetails.getStorageCredentials(), true, null, storageCredentialsDetails.getStorageAccountName());
        final CloudBlobClient cloudBlobClient = cloudStorageAccount.createCloudBlobClient();

        return cloudBlobClient;
    }

    public static AzureStorageCredentialsDetails getStorageCredentialsDetails(PropertyContext context, FlowFile flowFile) {
        final Map<String, String> attributes = flowFile != null ? flowFile.getAttributes() : Collections.emptyMap();

        final AzureStorageCredentialsService storageCredentialsService = context.getProperty(STORAGE_CREDENTIALS_SERVICE).asControllerService(AzureStorageCredentialsService.class);

        if (storageCredentialsService != null) {
            return storageCredentialsService.getStorageCredentialsDetails(attributes);
        } else {
            return createStorageCredentialsDetails(context, attributes);
        }
    }

    public static AzureStorageCredentialsDetails createStorageCredentialsDetails(PropertyContext context, Map<String, String> attributes) {
        final String accountName = context.getProperty(ACCOUNT_NAME).evaluateAttributeExpressions(attributes).getValue();
        final String accountKey = context.getProperty(ACCOUNT_KEY).evaluateAttributeExpressions(attributes).getValue();
        final String sasToken = context.getProperty(PROP_SAS_TOKEN).evaluateAttributeExpressions(attributes).getValue();

        if (StringUtils.isBlank(accountName)) {
            throw new IllegalArgumentException(String.format("'%s' must not be empty.", ACCOUNT_NAME.getDisplayName()));
        }

        StorageCredentials storageCredentials;

        if (StringUtils.isNotBlank(accountKey)) {
            storageCredentials = new StorageCredentialsAccountAndKey(accountName, accountKey);
        } else if (StringUtils.isNotBlank(sasToken)) {
            storageCredentials = new StorageCredentialsSharedAccessSignature(sasToken);
        } else {
            throw new IllegalArgumentException(String.format("Either '%s' or '%s' must be defined.", ACCOUNT_KEY.getDisplayName(), PROP_SAS_TOKEN.getDisplayName()));
        }

        return new AzureStorageCredentialsDetails(accountName, storageCredentials);
    }

    public static Collection<ValidationResult> validateCredentialProperties(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final String storageCredentials = validationContext.getProperty(STORAGE_CREDENTIALS_SERVICE).getValue();
        final String accountName = validationContext.getProperty(ACCOUNT_NAME).getValue();
        final String accountKey = validationContext.getProperty(ACCOUNT_KEY).getValue();
        final String sasToken = validationContext.getProperty(PROP_SAS_TOKEN).getValue();

        if (!((StringUtils.isNotBlank(storageCredentials) && StringUtils.isBlank(accountName) && StringUtils.isBlank(accountKey) && StringUtils.isBlank(sasToken))
                || (StringUtils.isBlank(storageCredentials) && StringUtils.isNotBlank(accountName) && StringUtils.isNotBlank(accountKey) && StringUtils.isBlank(sasToken))
                || (StringUtils.isBlank(storageCredentials) && StringUtils.isNotBlank(accountName) && StringUtils.isBlank(accountKey) && StringUtils.isNotBlank(sasToken)))) {
            results.add(new ValidationResult.Builder().subject("AzureStorageUtils Credentials")
                    .valid(false)
                    .explanation("either " + STORAGE_CREDENTIALS_SERVICE.getDisplayName()
                            + ", or " + ACCOUNT_NAME.getDisplayName() + " with " + ACCOUNT_KEY.getDisplayName()
                            + " or " + ACCOUNT_NAME.getDisplayName() + " with " + PROP_SAS_TOKEN.getDisplayName() + " must be specified")
                    .build());
        }

        return results;
    }

    private static final ProxySpec[] PROXY_SPECS = {ProxySpec.HTTP, ProxySpec.SOCKS};
    public static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE
            = ProxyConfiguration.createProxyConfigPropertyDescriptor(false, PROXY_SPECS);

    public static void validateProxySpec(ValidationContext context, Collection<ValidationResult> results) {
        ProxyConfiguration.validateProxySpec(context, results, PROXY_SPECS);
    }

    public static void setProxy(final OperationContext operationContext, final ProcessContext processContext) {
        final ProxyConfiguration proxyConfig = ProxyConfiguration.getConfiguration(processContext);
        operationContext.setProxy(proxyConfig.createProxy());
    }
}
