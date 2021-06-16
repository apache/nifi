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
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor ENDPOINT_SUFFIX = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AzureStorageUtils.ENDPOINT_SUFFIX)
            .displayName("Endpoint Suffix")
            .description("Storage accounts in public Azure always use a common FQDN suffix. " +
                    "Override this endpoint suffix with a different suffix in certain circumstances (like Azure Stack or non-public Azure regions).")
            .required(true)
            .defaultValue("dfs.core.windows.net")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor ACCOUNT_KEY = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AzureStorageUtils.ACCOUNT_KEY)
            .description(AzureStorageUtils.ACCOUNT_KEY_BASE_DESCRIPTION)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor SAS_TOKEN = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AzureStorageUtils.PROP_SAS_TOKEN)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
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

    public static final PropertyDescriptor SERVICE_PRINCIPAL_TENANT_ID = new PropertyDescriptor.Builder()
            .name("service-principal-tenant-id")
            .displayName("Service Principal Tenant ID")
            .description("Tenant ID of the Azure Active Directory hosting the Service Principal. The property is required when Service Principal authentication is used.")
            .sensitive(true)
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor SERVICE_PRINCIPAL_CLIENT_ID = new PropertyDescriptor.Builder()
            .name("service-principal-client-id")
            .displayName("Service Principal Client ID")
            .description("Client ID (or Application ID) of the Client/Application having the Service Principal. The property is required when Service Principal authentication is used.")
            .sensitive(true)
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor SERVICE_PRINCIPAL_CLIENT_SECRET = new PropertyDescriptor.Builder()
            .name("service-principal-client-secret")
            .displayName("Service Principal Client Secret")
            .description("Password of the Client/Application. The property is required when Service Principal authentication is used.")
            .sensitive(true)
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            ACCOUNT_NAME,
            ENDPOINT_SUFFIX,
            ACCOUNT_KEY,
            SAS_TOKEN,
            USE_MANAGED_IDENTITY,
            SERVICE_PRINCIPAL_TENANT_ID,
            SERVICE_PRINCIPAL_CLIENT_ID,
            SERVICE_PRINCIPAL_CLIENT_SECRET
    ));

    private ConfigurationContext context;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        boolean accountKeySet = StringUtils.isNotBlank(validationContext.getProperty(ACCOUNT_KEY).getValue());
        boolean sasTokenSet = StringUtils.isNotBlank(validationContext.getProperty(SAS_TOKEN).getValue());
        boolean useManagedIdentitySet = validationContext.getProperty(USE_MANAGED_IDENTITY).asBoolean();

        boolean servicePrincipalTenantIdSet = StringUtils.isNotBlank(validationContext.getProperty(SERVICE_PRINCIPAL_TENANT_ID).getValue());
        boolean servicePrincipalClientIdSet = StringUtils.isNotBlank(validationContext.getProperty(SERVICE_PRINCIPAL_CLIENT_ID).getValue());
        boolean servicePrincipalClientSecretSet = StringUtils.isNotBlank(validationContext.getProperty(SERVICE_PRINCIPAL_CLIENT_SECRET).getValue());

        boolean servicePrincipalSet = servicePrincipalTenantIdSet || servicePrincipalClientIdSet || servicePrincipalClientSecretSet;

        if (!onlyOneSet(accountKeySet, sasTokenSet, useManagedIdentitySet, servicePrincipalSet)) {
            results.add(new ValidationResult.Builder().subject(this.getClass().getSimpleName())
                .valid(false)
                .explanation("one and only one authentication method of [Account Key, SAS Token, Managed Identity, Service Principal] should be used")
                .build());
        } else if (servicePrincipalSet) {
            String template = "'%s' must be set when Service Principal authentication is being configured";
            if (!servicePrincipalTenantIdSet) {
                results.add(new ValidationResult.Builder().subject(this.getClass().getSimpleName())
                        .valid(false)
                        .explanation(String.format(template, SERVICE_PRINCIPAL_TENANT_ID.getDisplayName()))
                        .build());
            }
            if (!servicePrincipalClientIdSet) {
                results.add(new ValidationResult.Builder().subject(this.getClass().getSimpleName())
                        .valid(false)
                        .explanation(String.format(template, SERVICE_PRINCIPAL_CLIENT_ID.getDisplayName()))
                        .build());
            }
            if (!servicePrincipalClientSecretSet) {
                results.add(new ValidationResult.Builder().subject(this.getClass().getSimpleName())
                        .valid(false)
                        .explanation(String.format(template, SERVICE_PRINCIPAL_CLIENT_SECRET.getDisplayName()))
                        .build());
            }
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

        setValue(credentialsBuilder, ACCOUNT_NAME, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setAccountName, attributes);
        setValue(credentialsBuilder, ACCOUNT_KEY, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setAccountKey, attributes);
        setValue(credentialsBuilder, SAS_TOKEN, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setSasToken, attributes);
        setValue(credentialsBuilder, ENDPOINT_SUFFIX, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setEndpointSuffix, attributes);
        setValue(credentialsBuilder, USE_MANAGED_IDENTITY, PropertyValue::asBoolean, ADLSCredentialsDetails.Builder::setUseManagedIdentity, attributes);
        setValue(credentialsBuilder, SERVICE_PRINCIPAL_TENANT_ID, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setServicePrincipalTenantId, attributes);
        setValue(credentialsBuilder, SERVICE_PRINCIPAL_CLIENT_ID, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setServicePrincipalClientId, attributes);
        setValue(credentialsBuilder, SERVICE_PRINCIPAL_CLIENT_SECRET, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setServicePrincipalClientSecret, attributes);

        return credentialsBuilder.build();
    }

    private <T> void setValue(
            ADLSCredentialsDetails.Builder credentialsBuilder,
            PropertyDescriptor propertyDescriptor, Function<PropertyValue, T> getPropertyValue,
            BiConsumer<ADLSCredentialsDetails.Builder, T> setBuilderValue, Map<String, String> attributes
    ) {
        PropertyValue property = context.getProperty(propertyDescriptor);

        if (property.isSet()) {
            if (propertyDescriptor.isExpressionLanguageSupported()) {
                if (propertyDescriptor.getExpressionLanguageScope() == ExpressionLanguageScope.FLOWFILE_ATTRIBUTES) {
                    property = property.evaluateAttributeExpressions(attributes);
                } else {
                    property = property.evaluateAttributeExpressions();
                }
            }
            T value = getPropertyValue.apply(property);
            setBuilderValue.accept(credentialsBuilder, value);
        }
    }
}
