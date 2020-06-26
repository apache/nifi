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
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Provides credentials details for ADLS
 *
 * @see AbstractControllerService
 */
@Tags({"azure", "microsoft", "cloud", "storage", "adls", "credentials"})
@CapabilityDescription("Defines credentials for ADLS processors.")
public class ADLSCredentialsControllerService extends AbstractControllerService implements ADLSCredentialsService {

    public static final PropertyDescriptor ACCOUNT_NAME = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(AzureStorageUtils.ACCOUNT_NAME)
        .description(AzureStorageUtils.ACCOUNT_NAME_BASE_DESCRIPTION)
        .required(true)
        .build();

    public static final PropertyDescriptor ENDPOINT_SUFFIX = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(AzureStorageUtils.ENDPOINT_SUFFIX)
        .displayName("Endpoint Suffix")
        .description(
            "Storage accounts in public Azure always use a common FQDN suffix. " +
                "Override this endpoint suffix with a different suffix in certain circumstances (like Azure Stack or non-public Azure regions).")
        .required(true)
        .defaultValue("dfs.core.windows.net")
        .build();

    public static final PropertyDescriptor USE_MANAGED_IDENTITY = new PropertyDescriptor.Builder()
        .name("storage-use-managed-identity")
        .displayName("Use Azure Managed Identity")
        .description("Choose whether or not to use the managed identity of Azure VM/VMSS ")
        .required(false)
        .defaultValue("false")
        .allowableValues("true", "false")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
        ACCOUNT_NAME,
        ENDPOINT_SUFFIX,
        AzureStorageUtils.ACCOUNT_KEY,
        AzureStorageUtils.PROP_SAS_TOKEN,
        USE_MANAGED_IDENTITY
    ));

    private ConfigurationContext context;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        boolean accountKeySet = StringUtils.isNotBlank(validationContext.getProperty(AzureStorageUtils.ACCOUNT_KEY).getValue());
        boolean sasTokenSet = StringUtils.isNotBlank(validationContext.getProperty(AzureStorageUtils.PROP_SAS_TOKEN).getValue());
        boolean useManagedIdentitySet = validationContext.getProperty(USE_MANAGED_IDENTITY).asBoolean();

        if (!onlyOneSet(accountKeySet, sasTokenSet, useManagedIdentitySet)) {
            StringJoiner options = new StringJoiner(", ")
                .add(AzureStorageUtils.ACCOUNT_KEY.getDisplayName())
                .add(AzureStorageUtils.PROP_SAS_TOKEN.getDisplayName())
                .add(USE_MANAGED_IDENTITY.getDisplayName());

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

        setValue(credentialsBuilder, ACCOUNT_NAME, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setAccountName);
        setValue(credentialsBuilder, AzureStorageUtils.ACCOUNT_KEY, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setAccountKey);
        setValue(credentialsBuilder, AzureStorageUtils.PROP_SAS_TOKEN, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setSasToken);
        setValue(credentialsBuilder, ENDPOINT_SUFFIX, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setEndpointSuffix);
        setValue(credentialsBuilder, USE_MANAGED_IDENTITY, PropertyValue::asBoolean, ADLSCredentialsDetails.Builder::setUseManagedIdentity);

        return credentialsBuilder.build();
    }

    private <T> void setValue(
        ADLSCredentialsDetails.Builder credentialsBuilder,
        PropertyDescriptor propertyDescriptor, Function<PropertyValue, T> getPropertyValue,
        BiConsumer<ADLSCredentialsDetails.Builder, T> setBuilderValue
    ) {
        PropertyValue property = context.getProperty(propertyDescriptor);

        if (property.isSet()) {
            T value = getPropertyValue.apply(property);
            setBuilderValue.accept(credentialsBuilder, value);
        }
    }
}
