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
package org.apache.nifi.services.azure.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.StorageCredentialsSharedAccessSignature;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.services.azure.keyvault.AzureKeyVaultConnectionService;

/**
 * Implementation of AbstractControllerService interface
 *
 * @see AbstractControllerService
 */
@Tags({ "azure", "microsoft", "cloud", "storage", "blob", "queue", "credentials" })
@CapabilityDescription("Defines credentials for Azure Storage processors. " +
        "Uses Account Name with Account Key or Account Name with SAS Token.")
public class AzureStorageSecureCredentialsControllerService
        extends AbstractControllerService
        implements AzureStorageCredentialsService {

    private static final List<PropertyDescriptor> PROPERTIES = Collections
            .unmodifiableList(Arrays.asList(
                    AzureStorageUtils.KEYVAULT_CONNECTION_SERVICE,
                    AzureStorageUtils.ACCOUNT_NAME_SECRET,
                    AzureStorageUtils.ACCOUNT_KEY_SECRET,
                    AzureStorageUtils.ACCOUNT_SAS_TOKEN_SECRET,
                    AzureStorageUtils.ENDPOINT_SUFFIX));

    private ConfigurationContext context;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final String accountKey = validationContext.getProperty(
                AzureStorageUtils.ACCOUNT_KEY_SECRET).getValue();
        final String accountName = validationContext.getProperty(
                AzureStorageUtils.ACCOUNT_NAME_SECRET).getValue();
        final String sasToken = validationContext.getProperty(
                AzureStorageUtils.ACCOUNT_SAS_TOKEN_SECRET).getValue();

        if (StringUtils.isBlank(accountName)) {
            results.add(new ValidationResult.Builder().subject(this.getClass().getSimpleName())
                    .valid(false)
                    .explanation(AzureStorageUtils.ACCOUNT_NAME_SECRET.getDisplayName()
                            + " is required")
                    .build());
        }

        if (StringUtils.isBlank(accountKey) && StringUtils.isBlank(sasToken)) {
            results.add(new ValidationResult.Builder().subject(this.getClass().getSimpleName())
                    .valid(false)
                    .explanation("either "
                            + AzureStorageUtils.ACCOUNT_KEY_SECRET.getDisplayName()
                            + " or "
                            + AzureStorageUtils.ACCOUNT_SAS_TOKEN_SECRET.getDisplayName()
                            + " must be defined")
                    .build());
        }
        return results;
    }

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        this.context = context;
    }

    @Override
    public AzureStorageCredentialsDetails getStorageCredentialsDetails(Map<String, String> attributes) {
        final String accountNameSecret = context.getProperty(
                AzureStorageUtils.ACCOUNT_NAME_SECRET
        ).evaluateAttributeExpressions(attributes).getValue();
        final String accountKeySecret = context.getProperty(
                AzureStorageUtils.ACCOUNT_KEY_SECRET).evaluateAttributeExpressions(attributes).getValue();
        final String sasTokenSecret = context.getProperty(
                AzureStorageUtils.ACCOUNT_SAS_TOKEN_SECRET
        ).evaluateAttributeExpressions(attributes).getValue();
        final String storageSuffix = context.getProperty(
                AzureStorageUtils.ENDPOINT_SUFFIX
        ).evaluateAttributeExpressions(attributes).getValue();;
        final AzureKeyVaultConnectionService keyVaultClientService = context.getProperty(
                AzureStorageUtils.KEYVAULT_CONNECTION_SERVICE
        ).asControllerService(AzureKeyVaultConnectionService.class);

        if (keyVaultClientService == null) {
            throw new IllegalArgumentException(String.format(
                    "Cannot get '%s'.", AzureStorageUtils.KEYVAULT_CONNECTION_SERVICE.getDisplayName()));
        }

        if (StringUtils.isBlank(accountNameSecret)) {
            throw new IllegalArgumentException(String.format(
                    "'%s' must not be empty.", AzureStorageUtils.ACCOUNT_NAME_SECRET.getDisplayName()));
        }

        String accountNameValue = keyVaultClientService.getSecret(accountNameSecret);

        StorageCredentials storageCredentials;

        if (StringUtils.isNotBlank(accountKeySecret)) {
            String accountKeyValue = keyVaultClientService.getSecret(accountKeySecret);
            storageCredentials = new StorageCredentialsAccountAndKey(accountNameValue, accountKeyValue);
        } else if (StringUtils.isNotBlank(sasTokenSecret)) {
            String sasTokenValue = keyVaultClientService.getSecret(sasTokenSecret);
            storageCredentials = new StorageCredentialsSharedAccessSignature(sasTokenValue);
        } else {
            throw new IllegalArgumentException(String.format(
                    "Either '%s' or '%s' must be defined.",
                    AzureStorageUtils.ACCOUNT_KEY_SECRET.getDisplayName(),
                    AzureStorageUtils.ACCOUNT_SAS_TOKEN_SECRET.getDisplayName()));
        }
        return new AzureStorageCredentialsDetails(accountNameValue, storageSuffix, storageCredentials);
    }
}

