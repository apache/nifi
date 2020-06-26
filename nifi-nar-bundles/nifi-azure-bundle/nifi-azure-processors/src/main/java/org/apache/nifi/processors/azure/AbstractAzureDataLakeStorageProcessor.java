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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.identity.ManagedIdentityCredential;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.services.azure.storage.ADLSCredentialsDetails;
import org.apache.nifi.services.azure.storage.ADLSCredentialsService;
import reactor.core.publisher.Mono;

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
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("directory-name").displayName("Directory Name")
            .description("Name of the Azure Storage Directory. In case of the PutAzureDatalakeStorage processor, it will be created if not already existing.")
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final PropertyDescriptor FILE = new PropertyDescriptor.Builder()
            .name("file-name").displayName("File Name")
            .description("The filename")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .defaultValue("${azure.filename}")
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
            FILE
    ));

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_SUCCESS,
            REL_FAILURE
    )));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    public static DataLakeServiceClient getStorageClient(PropertyContext context, FlowFile flowFile) {
        final Map<String, String> attributes = flowFile != null ? flowFile.getAttributes() : Collections.emptyMap();

        final ADLSCredentialsService credentialsService = context.getProperty(ADLS_CREDENTIALS_SERVICE).asControllerService(ADLSCredentialsService.class);

        ADLSCredentialsDetails credentialsDetails = credentialsService.getCredentialsDetails(attributes);

        final String accountName = credentialsDetails.getAccountName();
        final String accountKey = credentialsDetails.getAccountKey();
        final String sasToken = credentialsDetails.getSasToken();
        final AccessToken accessToken = credentialsDetails.getAccessToken();
        final String endpointSuffix = credentialsDetails.getEndpointSuffix();
        final boolean useManagedIdentity = credentialsDetails.getUseManagedIdentity();

        final String endpoint = String.format("https://%s.%s", accountName,endpointSuffix);
        DataLakeServiceClient storageClient;
        if (StringUtils.isNotBlank(accountKey)) {
            final StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName,
                    accountKey);
            storageClient = new DataLakeServiceClientBuilder().endpoint(endpoint).credential(credential)
                    .buildClient();
        } else if (StringUtils.isNotBlank(sasToken)) {
            storageClient = new DataLakeServiceClientBuilder().endpoint(endpoint).sasToken(sasToken)
                    .buildClient();
        } else if (accessToken != null) {
            TokenCredential credential = tokenRequestContext -> Mono.just(accessToken);

            storageClient = new DataLakeServiceClientBuilder().endpoint(endpoint).credential(credential)
                .buildClient();
        } else if(useManagedIdentity){
            final ManagedIdentityCredential misCrendential = new ManagedIdentityCredentialBuilder()
                                                                .build();
            storageClient = new  DataLakeServiceClientBuilder()
                                    .endpoint(endpoint)
                                    .credential(misCrendential)
                                    .buildClient();
        } else {
            throw new IllegalArgumentException("No valid credentials were provided");
        }

        return storageClient;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }
}
