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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.services.azure.keyvault.AzureKeyVaultConnectionService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Provides secure credentials details for ADLS
 *
 * @see AbstractControllerService
 */
@Tags({"azure", "microsoft", "cloud", "storage", "adls", "credentials", "secure"})
@CapabilityDescription("Defines credentials for ADLS processors.")
public class ADLSKeyVaultCredentialsControllerService extends AbstractControllerService implements ADLSCredentialsService {

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            AzureStorageUtils.KEYVAULT_CONNECTION_SERVICE,
            AzureStorageUtils.ACCOUNT_NAME_SECRET,
            AzureStorageUtils.ADLS_ENDPOINT_SUFFIX,
            AzureStorageUtils.ACCOUNT_KEY_SECRET,
            AzureStorageUtils.ACCOUNT_SAS_TOKEN_SECRET
    ));

    private ConfigurationContext context;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        boolean accountKeySet = StringUtils.isNotBlank(validationContext.getProperty(
                AzureStorageUtils.ACCOUNT_KEY_SECRET).getValue());
        boolean sasTokenSet = StringUtils.isNotBlank(validationContext.getProperty(
                AzureStorageUtils.ACCOUNT_SAS_TOKEN_SECRET).getValue());

        if (!onlyOneSet(accountKeySet, sasTokenSet)) {
            StringJoiner options = new StringJoiner(", ")
                    .add(AzureStorageUtils.ACCOUNT_KEY_SECRET.getDisplayName())
                    .add(AzureStorageUtils.ACCOUNT_SAS_TOKEN_SECRET.getDisplayName());

            results.add(new ValidationResult.Builder().subject(this.getClass().getSimpleName())
                    .valid(false)
                    .explanation("one and only one of [" + options + "] should be set")
                    .build());
        }

        return results;
    }

    private boolean onlyOneSet(Boolean... checks) {
        long nrOfSet = Arrays.stream(checks)
                .filter(check -> check)
                .count();

        return nrOfSet == 1;
    }

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        this.context = context;
    }

    @Override
    public ADLSCredentialsDetails getCredentialsDetails(Map<String, String> attributes) {
        ADLSCredentialsDetails.Builder credentialsBuilder = ADLSCredentialsDetails.Builder.newBuilder();

        AzureStorageUtils.setValue(context, credentialsBuilder, AzureStorageUtils.ADLS_ENDPOINT_SUFFIX, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setEndpointSuffix, attributes);

        final AzureKeyVaultConnectionService keyVaultClientService = context.getProperty(AzureStorageUtils.KEYVAULT_CONNECTION_SERVICE).asControllerService(AzureKeyVaultConnectionService.class);
        String accountNameSecret = context.getProperty(AzureStorageUtils.ACCOUNT_NAME_SECRET).getValue();
        String accountKeySecret = context.getProperty(AzureStorageUtils.ACCOUNT_KEY_SECRET).getValue();
        String sasTokenSecret = context.getProperty(AzureStorageUtils.ACCOUNT_SAS_TOKEN_SECRET).getValue();

        if(StringUtils.isNotBlank(accountNameSecret)) {
            credentialsBuilder.setAccountName(keyVaultClientService.getSecret(accountNameSecret));
        }
        if(StringUtils.isNotBlank(accountKeySecret)) {
            credentialsBuilder.setAccountKey(keyVaultClientService.getSecret(accountKeySecret));
        } else if(StringUtils.isNotBlank(sasTokenSecret)) {
            credentialsBuilder.setSasToken(keyVaultClientService.getSecret(sasTokenSecret));
        } else {
            throw new IllegalArgumentException(String.format(
                    "Either '%s' or '%s' must be defined.",
                    AzureStorageUtils.ACCOUNT_KEY_SECRET.getDisplayName(),
                    AzureStorageUtils.ACCOUNT_SAS_TOKEN_SECRET.getDisplayName()));
        }

        return credentialsBuilder.build();
    }

//     private <T> void setValue(
//             ADLSCredentialsDetails.Builder credentialsBuilder,
//             PropertyDescriptor propertyDescriptor, Function<PropertyValue, T> getPropertyValue,
//             BiConsumer<ADLSCredentialsDetails.Builder, T> setBuilderValue
//     ) {
//         PropertyValue property = context.getProperty(propertyDescriptor);

//         if (property.isSet()) {
//             T value = getPropertyValue.apply(property);
//             setBuilderValue.accept(credentialsBuilder, value);
//         }
//     }
}

