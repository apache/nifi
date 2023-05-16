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
package org.apache.nifi.processors.azure.storage.queue;

import com.azure.core.credential.AzureSasCredential;
import com.azure.core.credential.TokenCredential;
import com.azure.core.http.HttpClient;
import com.azure.core.http.ProxyOptions;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueClientBuilder;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsDetails_v12;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsService_v12;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public abstract class AbstractAzureQueueStorage_v12 extends AbstractProcessor {
    public static final PropertyDescriptor QUEUE = new PropertyDescriptor.Builder()
            .name("storage-queue-name")
            .displayName("Queue Name")
            .description("Name of the Azure Storage Queue")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor STORAGE_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
            .name("storage-credentials-service")
            .displayName("Storage Credentials")
            .description("Controller Service used to obtain Azure Storage Credentials.")
            .identifiesControllerService(AzureStorageCredentialsService_v12.class)
            .required(true)
            .build();

    public static final PropertyDescriptor REQUEST_TIMEOUT = new PropertyDescriptor.Builder()
            .name("request-timeout")
            .displayName("Request Timeout")
            .description("The timeout for read or write requests to Azure Queue Storage. " +
                    "Defaults to 1 second.")
            .required(true)
            .defaultValue("1 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All successfully processed FlowFiles are routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Unsuccessful operations will be transferred to the failure relationship.")
            .build();
    private static final Set<Relationship> relationships = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    protected final QueueClient createQueueClient(final ProcessContext context, final FlowFile flowFile) {
        final QueueClientBuilder clientBuilder = new QueueClientBuilder();

        final AzureStorageCredentialsService_v12 storageCredentialsService = context.getProperty(STORAGE_CREDENTIALS_SERVICE).asControllerService(AzureStorageCredentialsService_v12.class);
        final AzureStorageCredentialsDetails_v12 storageCredentialsDetails = storageCredentialsService.getCredentialsDetails();
        clientBuilder.endpoint(String.format("https://%s.%s", storageCredentialsDetails.getAccountName(), storageCredentialsDetails.getEndpointSuffix()));
        processCredentials(clientBuilder, storageCredentialsDetails);
        processProxyOptions(clientBuilder, context);

        final String queueName = context.getProperty(QUEUE).evaluateAttributeExpressions(flowFile).getValue();
        clientBuilder.queueName(queueName);
        return clientBuilder.buildClient();
    }

    private void processCredentials(final QueueClientBuilder clientBuilder, final AzureStorageCredentialsDetails_v12 storageCredentialsDetails) {
        switch (storageCredentialsDetails.getCredentialsType()) {
            case ACCOUNT_KEY:
                clientBuilder.credential(new StorageSharedKeyCredential(storageCredentialsDetails.getAccountName(), storageCredentialsDetails.getAccountKey()));
                break;
            case SAS_TOKEN:
                clientBuilder.credential(new AzureSasCredential(storageCredentialsDetails.getSasToken()));
                break;
            case MANAGED_IDENTITY:
                clientBuilder.credential(new ManagedIdentityCredentialBuilder()
                        .clientId(storageCredentialsDetails.getManagedIdentityClientId())
                        .build());
                break;
            case SERVICE_PRINCIPAL:
                clientBuilder.credential(new ClientSecretCredentialBuilder()
                        .tenantId(storageCredentialsDetails.getServicePrincipalTenantId())
                        .clientId(storageCredentialsDetails.getServicePrincipalClientId())
                        .clientSecret(storageCredentialsDetails.getServicePrincipalClientSecret())
                        .build());
                break;
            case ACCESS_TOKEN:
                TokenCredential credential = tokenRequestContext -> Mono.just(storageCredentialsDetails.getAccessToken());
                clientBuilder.credential(credential);
                break;
            default:
                throw new IllegalArgumentException("Unhandled credentials type: " + storageCredentialsDetails.getCredentialsType());
        }
    }

    private void processProxyOptions(final QueueClientBuilder clientBuilder,
                                     final PropertyContext propertyContext) {
        final ProxyOptions proxyOptions = AzureStorageUtils.getProxyOptions(propertyContext);
        final NettyAsyncHttpClientBuilder nettyClientBuilder = new NettyAsyncHttpClientBuilder();
        nettyClientBuilder.proxy(proxyOptions);

        final HttpClient nettyClient = nettyClientBuilder.build();
        clientBuilder.httpClient(nettyClient);
    }
}
