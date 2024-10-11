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
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxySpec;
import org.apache.nifi.proxy.SocksVersion;
import org.apache.nifi.services.azure.storage.ADLSCredentialsService;
import org.apache.nifi.services.azure.storage.AzureStorageConflictResolutionStrategy;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsService_v12;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsType;
import reactor.netty.http.client.HttpClient;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;

import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_FILENAME;

public final class AzureStorageUtils {
    public static final String STORAGE_ACCOUNT_NAME_PROPERTY_DESCRIPTOR_NAME = "storage-account-name";
    public static final String STORAGE_ACCOUNT_KEY_PROPERTY_DESCRIPTOR_NAME = "storage-account-key";
    public static final String STORAGE_SAS_TOKEN_PROPERTY_DESCRIPTOR_NAME = "storage-sas-token";
    public static final String STORAGE_ENDPOINT_SUFFIX_PROPERTY_DESCRIPTOR_NAME = "storage-endpoint-suffix";

    public static final PropertyDescriptor ADLS_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
            .name("adls-credentials-service")
            .displayName("ADLS Credentials")
            .description("Controller Service used to obtain Azure Credentials.")
            .identifiesControllerService(ADLSCredentialsService.class)
            .required(true)
            .build();

    public static final PropertyDescriptor BLOB_STORAGE_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
            .name("storage-credentials-service")
            .displayName("Storage Credentials")
            .description("Controller Service used to obtain Azure Blob Storage Credentials.")
            .identifiesControllerService(AzureStorageCredentialsService_v12.class)
            .required(true)
            .build();

    public static final PropertyDescriptor CREDENTIALS_TYPE = new PropertyDescriptor.Builder()
            .name("credentials-type")
            .displayName("Credentials Type")
            .description("Credentials type to be used for authenticating to Azure")
            .required(true)
            .allowableValues(EnumSet.of(
                    AzureStorageCredentialsType.ACCOUNT_KEY,
                    AzureStorageCredentialsType.SAS_TOKEN,
                    AzureStorageCredentialsType.MANAGED_IDENTITY,
                    AzureStorageCredentialsType.SERVICE_PRINCIPAL))
            .defaultValue(AzureStorageCredentialsType.SAS_TOKEN)
            .build();

    public static final PropertyDescriptor FILESYSTEM = new PropertyDescriptor.Builder()
            .name("filesystem-name").displayName("Filesystem Name")
            .description("Name of the Azure Storage File System (also called Container). It is assumed to be already existing.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("directory-name")
            .displayName("Directory Name")
            .description("Name of the Azure Storage Directory. The Directory Name cannot contain a leading '/'. The root directory can be designated by the empty string value. " +
                    "In case of the PutAzureDataLakeStorage processor, the directory will be created if not already existing.")
            .addValidator(new DirectoryValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final PropertyDescriptor FILE = new PropertyDescriptor.Builder()
            .name("file-name").displayName("File Name")
            .description("The filename")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .defaultValue(String.format("${%s}", ATTR_NAME_FILENAME))
            .build();

    public static final String ACCOUNT_KEY_BASE_DESCRIPTION =
            "The storage account key. This is an admin-like password providing access to every container in this account. It is recommended " +
            "one uses Shared Access Signature (SAS) token, Managed Identity or Service Principal instead for fine-grained control with policies.";

    public static final String ACCOUNT_KEY_SECURITY_DESCRIPTION =
            " There are certain risks in allowing the account key to be stored as a FlowFile " +
            "attribute. While it does provide for a more flexible flow by allowing the account key to " +
            "be fetched dynamically from a FlowFile attribute, care must be taken to restrict access to " +
            "the event provenance data (e.g., by strictly controlling the policies governing provenance for this processor). " +
            "In addition, the provenance repositories may be put on encrypted disk partitions.";

    public static final PropertyDescriptor ACCOUNT_KEY = new PropertyDescriptor.Builder()
            .name(STORAGE_ACCOUNT_KEY_PROPERTY_DESCRIPTOR_NAME)
            .displayName("Account Key")
            .description(ACCOUNT_KEY_BASE_DESCRIPTION)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .sensitive(true)
            .dependsOn(CREDENTIALS_TYPE, AzureStorageCredentialsType.ACCOUNT_KEY)
            .build();

    public static final String ACCOUNT_NAME_BASE_DESCRIPTION = "The storage account name.";

    public static final String ACCOUNT_NAME_SECURITY_DESCRIPTION =
            " There are certain risks in allowing the account name to be stored as a FlowFile " +
            "attribute. While it does provide for a more flexible flow by allowing the account name to " +
            "be fetched dynamically from a FlowFile attribute, care must be taken to restrict access to " +
            "the event provenance data (e.g., by strictly controlling the policies governing provenance for this processor). " +
            "In addition, the provenance repositories may be put on encrypted disk partitions.";

    public static final PropertyDescriptor ACCOUNT_NAME = new PropertyDescriptor.Builder()
            .name(STORAGE_ACCOUNT_NAME_PROPERTY_DESCRIPTOR_NAME)
            .displayName("Storage Account Name")
            .description(ACCOUNT_NAME_BASE_DESCRIPTION)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor ENDPOINT_SUFFIX = new PropertyDescriptor.Builder()
            .name(STORAGE_ENDPOINT_SUFFIX_PROPERTY_DESCRIPTOR_NAME)
            .displayName("Endpoint Suffix")
            .description("Storage accounts in public Azure always use a common FQDN suffix. " +
                    "Override this endpoint suffix with a different suffix in certain circumstances (like Azure Stack or non-public Azure regions).")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
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
            .defaultValue(AzureStorageConflictResolutionStrategy.FAIL_RESOLUTION)
            .description("Specifies whether an existing blob will have its contents replaced upon conflict.")
            .build();

    public static final String SAS_TOKEN_BASE_DESCRIPTION = "Shared Access Signature token (the leading '?' may be included)";

    public static final String SAS_TOKEN_SECURITY_DESCRIPTION =
            " There are certain risks in allowing the SAS token to be stored as a FlowFile " +
            "attribute. While it does provide for a more flexible flow by allowing the SAS token to " +
            "be fetched dynamically from a FlowFile attribute, care must be taken to restrict access to " +
            "the event provenance data (e.g., by strictly controlling the policies governing provenance for this processor). " +
            "In addition, the provenance repositories may be put on encrypted disk partitions.";

    public static final PropertyDescriptor SAS_TOKEN = new PropertyDescriptor.Builder()
            .name(STORAGE_SAS_TOKEN_PROPERTY_DESCRIPTOR_NAME)
            .displayName("SAS Token")
            .description(SAS_TOKEN_BASE_DESCRIPTION)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(CREDENTIALS_TYPE, AzureStorageCredentialsType.SAS_TOKEN)
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
            .dependsOn(CREDENTIALS_TYPE, AzureStorageCredentialsType.MANAGED_IDENTITY)
            .build();

    public static final PropertyDescriptor SERVICE_PRINCIPAL_TENANT_ID = new PropertyDescriptor.Builder()
            .name("service-principal-tenant-id")
            .displayName("Service Principal Tenant ID")
            .description("Tenant ID of the Azure Active Directory hosting the Service Principal.")
            .sensitive(true)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .dependsOn(CREDENTIALS_TYPE, AzureStorageCredentialsType.SERVICE_PRINCIPAL)
            .build();

    public static final PropertyDescriptor SERVICE_PRINCIPAL_CLIENT_ID = new PropertyDescriptor.Builder()
            .name("service-principal-client-id")
            .displayName("Service Principal Client ID")
            .description("Client ID (or Application ID) of the Client/Application having the Service Principal.")
            .sensitive(true)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .dependsOn(CREDENTIALS_TYPE, AzureStorageCredentialsType.SERVICE_PRINCIPAL)
            .build();

    public static final PropertyDescriptor SERVICE_PRINCIPAL_CLIENT_SECRET = new PropertyDescriptor.Builder()
            .name("service-principal-client-secret")
            .displayName("Service Principal Client Secret")
            .description("Password of the Client/Application.")
            .sensitive(true)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .dependsOn(CREDENTIALS_TYPE, AzureStorageCredentialsType.SERVICE_PRINCIPAL)
            .build();

    private AzureStorageUtils() {
        // do not instantiate
    }

    private static final ProxySpec[] PROXY_SPECS = {ProxySpec.HTTP, ProxySpec.SOCKS};
    public static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(ProxyConfiguration.createProxyConfigPropertyDescriptor(PROXY_SPECS))
            .build();

    public static void validateProxySpec(ValidationContext context, Collection<ValidationResult> results) {
        ProxyConfiguration.validateProxySpec(context, results, PROXY_SPECS);
    }

    public static String evaluateFileSystemProperty(PropertyDescriptor property, PropertyContext context) {
        return evaluateFileSystemProperty(property, context, (Map<String, String>) null);
    }

    public static String evaluateFileSystemProperty(PropertyDescriptor property, PropertyContext context, FlowFile flowFile) {
        return evaluateFileSystemProperty(property, context, flowFile.getAttributes());
    }

    public static String evaluateFileSystemProperty(PropertyDescriptor property, PropertyContext context, Map<String, String> attributes) {
        final String fileSystem = evaluateProperty(property, context, attributes);
        if (StringUtils.isBlank(fileSystem)) {
            throw new ProcessException(String.format("'%1$s' property evaluated to blank string. '%s' must be specified as a non-blank string.",
                    property.getDisplayName()));
        }
        return fileSystem;
    }

    public static String evaluateDirectoryProperty(PropertyDescriptor property, PropertyContext context) {
        return evaluateDirectoryProperty(property, context, (Map<String, String>) null);
    }

    public static String evaluateDirectoryProperty(PropertyDescriptor property, PropertyContext context, FlowFile flowFile) {
        return evaluateDirectoryProperty(property, context, flowFile.getAttributes());
    }

    public static String evaluateDirectoryProperty(PropertyDescriptor property, PropertyContext context, Map<String, String> attributes) {
        final String directory = evaluateProperty(property, context, attributes);
        if (directory.startsWith("/")) {
            throw new ProcessException(String.format("'%1$s' starts with '/'. '%s' cannot contain a leading '/'.", property.getDisplayName()));
        } else if (StringUtils.isNotEmpty(directory) && StringUtils.isWhitespace(directory)) {
            throw new ProcessException(String.format("'%1$s' contains whitespace characters only.", property.getDisplayName()));
        }
        return directory;
    }

    public static String evaluateFileProperty(PropertyContext context, FlowFile flowFile) {
        return evaluateFileProperty(context, flowFile.getAttributes());
    }

    public static String evaluateFileProperty(PropertyContext context, Map<String, String> attributes) {
        final String fileName = evaluateProperty(FILE, context, attributes);
        if (StringUtils.isBlank(fileName)) {
            throw new ProcessException(String.format("'%1$s' property evaluated to blank string. '%s' must be specified as a non-blank string.", FILE.getDisplayName()));
        }
        return fileName;
    }

    private static String evaluateProperty(PropertyDescriptor propertyDescriptor, PropertyContext context, Map<String, String> attributes) {
        return context.getProperty(propertyDescriptor).evaluateAttributeExpressions(attributes).getValue();
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

    public static class DirectoryValidator implements Validator {
        private String displayName;

        public DirectoryValidator() {
            this.displayName = null;
        }

        public DirectoryValidator(String displayName) {
            this.displayName = displayName;
        }

        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            displayName = displayName == null ? DIRECTORY.getDisplayName() : displayName;
            ValidationResult.Builder builder = new ValidationResult.Builder()
                    .subject(displayName)
                    .input(input);

            if (context.isExpressionLanguagePresent(input)) {
                builder.valid(true).explanation("Expression Language Present");
            } else if (input.startsWith("/")) {
                builder.valid(false).explanation(String.format("'%s' cannot contain a leading '/'", displayName));
            } else if (StringUtils.isNotEmpty(input) && StringUtils.isWhitespace(input)) {
                builder.valid(false).explanation(String.format("'%s' cannot contain whitespace characters only", displayName));
            } else {
                builder.valid(true);
            }

            return builder.build();
        }
    }
}
