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

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.http.HttpClient;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.ManagedIdentityCredential;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.services.azure.storage.ADLSCredentialsDetails;
import org.apache.nifi.services.azure.storage.ADLSCredentialsService;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_FILENAME;

public abstract class AbstractAzureDataLakeStorageProcessor extends AbstractProcessor {

    public static final PropertyDescriptor ADLS_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
            .name("adls-credentials-service")
            .displayName("ADLS Credentials")
            .description("Controller Service used to obtain Azure Credentials.")
            .identifiesControllerService(ADLSCredentialsService.class)
            .required(true)
            .build();

    public static final PropertyDescriptor FILESYSTEM = new PropertyDescriptor.Builder()
            .name("filesystem-name").displayName("Filesystem Name")
            .description("Name of the Azure Storage File System. It is assumed to be already existing.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("directory-name").displayName("Directory Name")
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

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description(
            "Files that have been successfully written to Azure storage are transferred to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description(
            "Files that could not be written to Azure storage for some reason are transferred to this relationship")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            ADLS_CREDENTIALS_SERVICE,
            FILESYSTEM,
            DIRECTORY,
            FILE,
            AzureStorageUtils.PROXY_CONFIGURATION_SERVICE
    ));

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_SUCCESS,
            REL_FAILURE
    )));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    public static DataLakeServiceClient getStorageClient(PropertyContext context, FlowFile flowFile) {
        final Map<String, String> attributes = flowFile != null ? flowFile.getAttributes() : Collections.emptyMap();

        final ADLSCredentialsService credentialsService = context.getProperty(ADLS_CREDENTIALS_SERVICE).asControllerService(ADLSCredentialsService.class);

        final ADLSCredentialsDetails credentialsDetails = credentialsService.getCredentialsDetails(attributes);

        final String accountName = credentialsDetails.getAccountName();
        final String accountKey = credentialsDetails.getAccountKey();
        final String sasToken = credentialsDetails.getSasToken();
        final AccessToken accessToken = credentialsDetails.getAccessToken();
        final String endpointSuffix = credentialsDetails.getEndpointSuffix();
        final boolean useManagedIdentity = credentialsDetails.getUseManagedIdentity();
        final String managedIdentityClientId = credentialsDetails.getManagedIdentityClientId();
        final String servicePrincipalTenantId = credentialsDetails.getServicePrincipalTenantId();
        final String servicePrincipalClientId = credentialsDetails.getServicePrincipalClientId();
        final String servicePrincipalClientSecret = credentialsDetails.getServicePrincipalClientSecret();

        final String endpoint = String.format("https://%s.%s", accountName, endpointSuffix);

        final DataLakeServiceClientBuilder dataLakeServiceClientBuilder = new DataLakeServiceClientBuilder();
        dataLakeServiceClientBuilder.endpoint(endpoint);

        if (StringUtils.isNotBlank(accountKey)) {
            final StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
            dataLakeServiceClientBuilder.credential(credential);
        } else if (StringUtils.isNotBlank(sasToken)) {
            dataLakeServiceClientBuilder.sasToken(sasToken);
        } else if (accessToken != null) {
            final TokenCredential credential = tokenRequestContext -> Mono.just(accessToken);
            dataLakeServiceClientBuilder.credential(credential);
        } else if (useManagedIdentity) {
            final ManagedIdentityCredential misCredential = new ManagedIdentityCredentialBuilder()
                    .clientId(managedIdentityClientId)
                    .build();
            dataLakeServiceClientBuilder.credential(misCredential);
        } else if (StringUtils.isNoneBlank(servicePrincipalTenantId, servicePrincipalClientId, servicePrincipalClientSecret)) {
            final ClientSecretCredential credential = new ClientSecretCredentialBuilder()
                    .tenantId(servicePrincipalTenantId)
                    .clientId(servicePrincipalClientId)
                    .clientSecret(servicePrincipalClientSecret)
                    .build();
            dataLakeServiceClientBuilder.credential(credential);
        } else {
            throw new IllegalArgumentException("No valid credentials were provided");
        }

        final NettyAsyncHttpClientBuilder nettyClientBuilder = new NettyAsyncHttpClientBuilder();
        AzureStorageUtils.configureProxy(nettyClientBuilder, context);

        final HttpClient nettyClient = nettyClientBuilder.build();
        dataLakeServiceClientBuilder.httpClient(nettyClient);

        final DataLakeServiceClient storageClient = dataLakeServiceClientBuilder.buildClient();

        return storageClient;
    }

    public static String evaluateFileSystemProperty(ProcessContext context, FlowFile flowFile) {
        return evaluateFileSystemProperty(context, flowFile, FILESYSTEM);
    }

    public static String evaluateFileSystemProperty(ProcessContext context, FlowFile flowFile, PropertyDescriptor property) {
        String fileSystem = context.getProperty(property).evaluateAttributeExpressions(flowFile).getValue();
        if (StringUtils.isBlank(fileSystem)) {
            throw new ProcessException(String.format("'%1$s' property evaluated to blank string. '%s' must be specified as a non-blank string.", property.getDisplayName()));
        }
        return fileSystem;
    }

    public static String evaluateDirectoryProperty(ProcessContext context, FlowFile flowFile) {
        return evaluateDirectoryProperty(context, flowFile, DIRECTORY);
    }

    public static String evaluateDirectoryProperty(ProcessContext context, FlowFile flowFile, PropertyDescriptor property) {
        String directory = context.getProperty(property).evaluateAttributeExpressions(flowFile).getValue();
        if (directory.startsWith("/")) {
            throw new ProcessException(String.format("'%1$s' starts with '/'. '%s' cannot contain a leading '/'.", property.getDisplayName()));
        } else if (StringUtils.isNotEmpty(directory) && StringUtils.isWhitespace(directory)) {
            throw new ProcessException(String.format("'%1$s' contains whitespace characters only.", property.getDisplayName()));
        }
        return directory;
    }

    public static String evaluateFileNameProperty(ProcessContext context, FlowFile flowFile) {
        String fileName = context.getProperty(FILE).evaluateAttributeExpressions(flowFile).getValue();
        if (StringUtils.isBlank(fileName)) {
            throw new ProcessException(String.format("'%1$s' property evaluated to blank string. '%s' must be specified as a non-blank string.", FILE.getDisplayName()));
        }
        return fileName;
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
