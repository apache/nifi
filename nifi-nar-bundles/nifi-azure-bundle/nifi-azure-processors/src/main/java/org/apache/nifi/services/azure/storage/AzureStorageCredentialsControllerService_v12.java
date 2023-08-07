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

import com.azure.core.http.ProxyOptions;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processors.azure.AzureServiceEndpoints;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Provides credentials details for Azure Storage processors
 *
 * @see AbstractControllerService
 */
@Tags({"azure", "microsoft", "cloud", "storage", "blob", "credentials", "queue"})
@CapabilityDescription("Provides credentials for Azure Storage processors using Azure Storage client library v12.")
public class AzureStorageCredentialsControllerService_v12 extends AbstractControllerService implements AzureStorageCredentialsService_v12 {

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
            .defaultValue(AzureServiceEndpoints.DEFAULT_BLOB_ENDPOINT_SUFFIX)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor CREDENTIALS_TYPE = new PropertyDescriptor.Builder()
            .name("credentials-type")
            .displayName("Credentials Type")
            .description("Credentials type to be used for authenticating to Azure")
            .required(true)
            .allowableValues(AzureStorageCredentialsType.ACCOUNT_KEY.getAllowableValue(),
                    AzureStorageCredentialsType.SAS_TOKEN.getAllowableValue(),
                    AzureStorageCredentialsType.MANAGED_IDENTITY.getAllowableValue(),
                    AzureStorageCredentialsType.SERVICE_PRINCIPAL.getAllowableValue())
            .defaultValue(AzureStorageCredentialsType.SAS_TOKEN.name())
            .build();

    public static final PropertyDescriptor ACCOUNT_KEY = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AzureStorageUtils.ACCOUNT_KEY)
            .displayName("Account Key")
            .description(AzureStorageUtils.ACCOUNT_KEY_BASE_DESCRIPTION)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .dependsOn(CREDENTIALS_TYPE, AzureStorageCredentialsType.ACCOUNT_KEY.getAllowableValue())
            .build();

    public static final PropertyDescriptor SAS_TOKEN = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AzureStorageUtils.PROP_SAS_TOKEN)
            .description(AzureStorageUtils.SAS_TOKEN_BASE_DESCRIPTION)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .dependsOn(CREDENTIALS_TYPE, AzureStorageCredentialsType.SAS_TOKEN.getAllowableValue())
            .build();

    public static final PropertyDescriptor MANAGED_IDENTITY_CLIENT_ID = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AzureStorageUtils.MANAGED_IDENTITY_CLIENT_ID)
            .dependsOn(CREDENTIALS_TYPE, AzureStorageCredentialsType.MANAGED_IDENTITY.getAllowableValue())
            .build();

    public static final PropertyDescriptor SERVICE_PRINCIPAL_TENANT_ID = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AzureStorageUtils.SERVICE_PRINCIPAL_TENANT_ID)
            .required(true)
            .dependsOn(CREDENTIALS_TYPE, AzureStorageCredentialsType.SERVICE_PRINCIPAL.getAllowableValue())
            .build();

    public static final PropertyDescriptor SERVICE_PRINCIPAL_CLIENT_ID = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AzureStorageUtils.SERVICE_PRINCIPAL_CLIENT_ID)
            .required(true)
            .dependsOn(CREDENTIALS_TYPE, AzureStorageCredentialsType.SERVICE_PRINCIPAL.getAllowableValue())
            .build();

    public static final PropertyDescriptor SERVICE_PRINCIPAL_CLIENT_SECRET = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AzureStorageUtils.SERVICE_PRINCIPAL_CLIENT_SECRET)
            .required(true)
            .dependsOn(CREDENTIALS_TYPE, AzureStorageCredentialsType.SERVICE_PRINCIPAL.getAllowableValue())
            .build();

    public static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AzureStorageUtils.PROXY_CONFIGURATION_SERVICE)
            .dependsOn(CREDENTIALS_TYPE, AzureStorageCredentialsType.SERVICE_PRINCIPAL.getAllowableValue(), AzureStorageCredentialsType.MANAGED_IDENTITY.getAllowableValue())
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            ACCOUNT_NAME,
            ENDPOINT_SUFFIX,
            CREDENTIALS_TYPE,
            ACCOUNT_KEY,
            SAS_TOKEN,
            MANAGED_IDENTITY_CLIENT_ID,
            SERVICE_PRINCIPAL_TENANT_ID,
            SERVICE_PRINCIPAL_CLIENT_ID,
            SERVICE_PRINCIPAL_CLIENT_SECRET,
            PROXY_CONFIGURATION_SERVICE
    ));

    private ConfigurationContext context;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        this.context = context;
    }

    @Override
    public AzureStorageCredentialsDetails_v12 getCredentialsDetails(Map<String, String> attributes) {
        String accountName = context.getProperty(ACCOUNT_NAME).getValue();
        String endpointSuffix = context.getProperty(ENDPOINT_SUFFIX).getValue();
        AzureStorageCredentialsType credentialsType = AzureStorageCredentialsType.valueOf(context.getProperty(CREDENTIALS_TYPE).getValue());
        ProxyOptions proxyOptions = AzureStorageUtils.getProxyOptions(context);

        switch (credentialsType) {
            case ACCOUNT_KEY:
                String accountKey = context.getProperty(ACCOUNT_KEY).getValue();
                return AzureStorageCredentialsDetails_v12.createWithAccountKey(accountName, endpointSuffix, accountKey);
            case SAS_TOKEN:
                String sasToken = context.getProperty(SAS_TOKEN).getValue();
                return AzureStorageCredentialsDetails_v12.createWithSasToken(accountName, endpointSuffix, sasToken);
            case MANAGED_IDENTITY:
                String managedIdentityClientId = context.getProperty(MANAGED_IDENTITY_CLIENT_ID).getValue();
                return AzureStorageCredentialsDetails_v12.createWithManagedIdentity(accountName, endpointSuffix, managedIdentityClientId, proxyOptions);
            case SERVICE_PRINCIPAL:
                String servicePrincipalTenantId = context.getProperty(SERVICE_PRINCIPAL_TENANT_ID).getValue();
                String servicePrincipalClientId = context.getProperty(SERVICE_PRINCIPAL_CLIENT_ID).getValue();
                String servicePrincipalClientSecret = context.getProperty(SERVICE_PRINCIPAL_CLIENT_SECRET).getValue();
                return AzureStorageCredentialsDetails_v12.createWithServicePrincipal(accountName, endpointSuffix,
                        servicePrincipalTenantId, servicePrincipalClientId, servicePrincipalClientSecret, proxyOptions);
            default:
                throw new IllegalArgumentException("Unhandled credentials type: " + credentialsType);
        }
    }
}
