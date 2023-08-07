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

import com.azure.core.http.ProxyOptions;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.Collection;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxySpec;
import org.apache.nifi.proxy.SocksVersion;
import org.apache.nifi.services.azure.storage.AzureStorageConflictResolutionStrategy;
import reactor.netty.http.client.HttpClient;

public final class AzureStorageUtils {
    public static final String STORAGE_ACCOUNT_NAME_PROPERTY_DESCRIPTOR_NAME = "storage-account-name";
    public static final String STORAGE_ACCOUNT_KEY_PROPERTY_DESCRIPTOR_NAME = "storage-account-key";
    public static final String STORAGE_SAS_TOKEN_PROPERTY_DESCRIPTOR_NAME = "storage-sas-token";
    public static final String STORAGE_ENDPOINT_SUFFIX_PROPERTY_DESCRIPTOR_NAME = "storage-endpoint-suffix";

    public static final String ACCOUNT_KEY_BASE_DESCRIPTION =
            "The storage account key. This is an admin-like password providing access to every container in this account. It is recommended " +
            "one uses Shared Access Signature (SAS) token instead for fine-grained control with policies.";

    public static final String ACCOUNT_KEY_SECURITY_DESCRIPTION =
            " There are certain risks in allowing the account key to be stored as a flowfile " +
            "attribute. While it does provide for a more flexible flow by allowing the account key to " +
            "be fetched dynamically from a flowfile attribute, care must be taken to restrict access to " +
            "the event provenance data (e.g., by strictly controlling the policies governing provenance for this processor). " +
            "In addition, the provenance repositories may be put on encrypted disk partitions.";

    public static final PropertyDescriptor ACCOUNT_KEY = new PropertyDescriptor.Builder()
            .name(STORAGE_ACCOUNT_KEY_PROPERTY_DESCRIPTOR_NAME)
            .displayName("Storage Account Key")
            .description(ACCOUNT_KEY_BASE_DESCRIPTION + ACCOUNT_KEY_SECURITY_DESCRIPTION)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .sensitive(true)
            .build();

    public static final String ACCOUNT_NAME_BASE_DESCRIPTION = "The storage account name.";

    public static final String ACCOUNT_NAME_SECURITY_DESCRIPTION =
            " There are certain risks in allowing the account name to be stored as a flowfile " +
            "attribute. While it does provide for a more flexible flow by allowing the account name to " +
            "be fetched dynamically from a flowfile attribute, care must be taken to restrict access to " +
            "the event provenance data (e.g., by strictly controlling the policies governing provenance for this processor). " +
            "In addition, the provenance repositories may be put on encrypted disk partitions.";

    public static final String ACCOUNT_NAME_CREDENTIAL_SERVICE_DESCRIPTION =
            " Instead of defining the Storage Account Name, Storage Account Key and SAS Token properties directly on the processor, " +
            "the preferred way is to configure them through a controller service specified in the Storage Credentials property. " +
            "The controller service can provide a common/shared configuration for multiple/all Azure processors. Furthermore, the credentials " +
            "can also be looked up dynamically with the 'Lookup' version of the service.";

    public static final PropertyDescriptor ACCOUNT_NAME = new PropertyDescriptor.Builder()
            .name(STORAGE_ACCOUNT_NAME_PROPERTY_DESCRIPTOR_NAME)
            .displayName("Storage Account Name")
            .description(ACCOUNT_NAME_BASE_DESCRIPTION + ACCOUNT_NAME_SECURITY_DESCRIPTION + ACCOUNT_NAME_CREDENTIAL_SERVICE_DESCRIPTION)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor ENDPOINT_SUFFIX = new PropertyDescriptor.Builder()
            .name(STORAGE_ENDPOINT_SUFFIX_PROPERTY_DESCRIPTOR_NAME)
            .displayName("Common Storage Account Endpoint Suffix")
            .description(
                    "Storage accounts in public Azure always use a common FQDN suffix. " +
                    "Override this endpoint suffix with a different suffix in certain circumstances (like Azure Stack or non-public Azure regions). " +
                    "The preferred way is to configure them through a controller service specified in the Storage Credentials property. " +
                    "The controller service can provide a common/shared configuration for multiple/all Azure processors. Furthermore, the credentials " +
                    "can also be looked up dynamically with the 'Lookup' version of the service.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor CONTAINER = new PropertyDescriptor.Builder()
            .name("container-name")
            .displayName("Container Name")
            .description("Name of the Azure storage container. In case of PutAzureBlobStorage processor, container can be created if it does not exist.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final PropertyDescriptor CREATE_CONTAINER = new PropertyDescriptor.Builder()
            .name("create-container")
            .displayName("Create Container")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .description("Specifies whether to check if the container exists and to automatically create it if it does not. " +
                    "Permission to list containers is required. If false, this check is not made, but the Put operation " +
                    "will fail if the container does not exist.")
            .build();

    public static final PropertyDescriptor CONFLICT_RESOLUTION = new PropertyDescriptor.Builder()
            .name("conflict-resolution-strategy")
            .displayName("Conflict Resolution Strategy")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .allowableValues(AzureStorageConflictResolutionStrategy.class)
            .defaultValue(AzureStorageConflictResolutionStrategy.FAIL_RESOLUTION.getValue())
            .description("Specifies whether an existing blob will have its contents replaced upon conflict.")
            .build();

    public static final String SAS_TOKEN_BASE_DESCRIPTION = "Shared Access Signature token, including the leading '?'. Specify either SAS token (recommended) or Account Key.";

    public static final String SAS_TOKEN_SECURITY_DESCRIPTION =
            " There are certain risks in allowing the SAS token to be stored as a flowfile " +
            "attribute. While it does provide for a more flexible flow by allowing the SAS token to " +
            "be fetched dynamically from a flowfile attribute, care must be taken to restrict access to " +
            "the event provenance data (e.g., by strictly controlling the policies governing provenance for this processor). " +
            "In addition, the provenance repositories may be put on encrypted disk partitions.";

    public static final PropertyDescriptor PROP_SAS_TOKEN = new PropertyDescriptor.Builder()
            .name(STORAGE_SAS_TOKEN_PROPERTY_DESCRIPTOR_NAME)
            .displayName("SAS Token")
            .description(SAS_TOKEN_BASE_DESCRIPTION + SAS_TOKEN_SECURITY_DESCRIPTION)
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MANAGED_IDENTITY_CLIENT_ID = new PropertyDescriptor.Builder()
            .name("managed-identity-client-id")
            .displayName("Managed Identity Client ID")
            .description("Client ID of the managed identity. The property is required when User Assigned Managed Identity is used for authentication. " +
                    "It must be empty in case of System Assigned Managed Identity.")
            .sensitive(true)
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
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

    private AzureStorageUtils() {
        // do not instantiate
    }

    private static final ProxySpec[] PROXY_SPECS = {ProxySpec.HTTP, ProxySpec.SOCKS};
    public static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE
            = ProxyConfiguration.createProxyConfigPropertyDescriptor(false, PROXY_SPECS);

    public static void validateProxySpec(ValidationContext context, Collection<ValidationResult> results) {
        ProxyConfiguration.validateProxySpec(context, results, PROXY_SPECS);
    }

    /**
     *
     * Creates the {@link ProxyOptions proxy options} that {@link HttpClient} will use.
     *
     * @param propertyContext to supply Proxy configurations
     * @return {@link ProxyOptions proxy options}, null if Proxy is not set
     */
    public static ProxyOptions getProxyOptions(final PropertyContext propertyContext) {
        final ProxyConfiguration proxyConfiguration = ProxyConfiguration.getConfiguration(propertyContext);

        if (proxyConfiguration != ProxyConfiguration.DIRECT_CONFIGURATION) {

            final ProxyOptions proxyOptions = new ProxyOptions(
                    getProxyType(proxyConfiguration),
                    new InetSocketAddress(proxyConfiguration.getProxyServerHost(), proxyConfiguration.getProxyServerPort()));

            final String proxyUserName = proxyConfiguration.getProxyUserName();
            final String proxyUserPassword = proxyConfiguration.getProxyUserPassword();
            if (proxyUserName != null && proxyUserPassword != null) {
                proxyOptions.setCredentials(proxyUserName, proxyUserPassword);
            }

            return proxyOptions;
        }

        return null;
    }

    private static ProxyOptions.Type getProxyType(ProxyConfiguration proxyConfiguration) {
        if (proxyConfiguration.getProxyType() == Proxy.Type.HTTP) {
            return ProxyOptions.Type.HTTP;
        } else if (proxyConfiguration.getProxyType() == Proxy.Type.SOCKS) {
            final SocksVersion socksVersion = proxyConfiguration.getSocksVersion();
            return ProxyOptions.Type.valueOf(socksVersion.name());
        } else {
            throw new IllegalArgumentException("Unsupported proxy type: " + proxyConfiguration.getProxyType());
        }
    }
}
