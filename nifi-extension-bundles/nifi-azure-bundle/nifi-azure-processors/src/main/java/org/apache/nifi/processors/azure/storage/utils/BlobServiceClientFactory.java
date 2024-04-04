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

import com.azure.core.credential.AzureSasCredential;
import com.azure.core.credential.TokenCredential;
import com.azure.core.http.ProxyOptions;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.core.util.ClientOptions;
import com.azure.core.util.HttpClientOptions;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsDetails_v12;
import reactor.core.publisher.Mono;

public class BlobServiceClientFactory extends AbstractStorageClientFactory<AzureStorageCredentialsDetails_v12, BlobServiceClient> {

    public BlobServiceClientFactory(final ComponentLog logger, final ProxyOptions proxyOptions) {
        super(logger, proxyOptions);
    }

    protected BlobServiceClient createStorageClient(final AzureStorageCredentialsDetails_v12 credentialsDetails, final ProxyOptions proxyOptions) {
        final BlobServiceClientBuilder clientBuilder = new BlobServiceClientBuilder();
        clientBuilder.endpoint(String.format("https://%s.%s", credentialsDetails.getAccountName(), credentialsDetails.getEndpointSuffix()));

        final ClientOptions clientOptions = new HttpClientOptions().setProxyOptions(proxyOptions);
        clientBuilder.clientOptions(clientOptions);

        configureCredential(clientBuilder, credentialsDetails);

        return clientBuilder.buildClient();
    }

    private void configureCredential(final BlobServiceClientBuilder clientBuilder, final AzureStorageCredentialsDetails_v12 credentialsDetails) {
        switch (credentialsDetails.getCredentialsType()) {
            case ACCOUNT_KEY:
                clientBuilder.credential(new StorageSharedKeyCredential(credentialsDetails.getAccountName(), credentialsDetails.getAccountKey()));
                break;
            case SAS_TOKEN:
                clientBuilder.credential(new AzureSasCredential(credentialsDetails.getSasToken()));
                break;
            case MANAGED_IDENTITY:
                clientBuilder.credential(new ManagedIdentityCredentialBuilder()
                        .clientId(credentialsDetails.getManagedIdentityClientId())
                        .httpClient(new NettyAsyncHttpClientBuilder()
                                .proxy(credentialsDetails.getProxyOptions())
                                .build())
                        .build());
                break;
            case SERVICE_PRINCIPAL:
                clientBuilder.credential(new ClientSecretCredentialBuilder()
                        .tenantId(credentialsDetails.getServicePrincipalTenantId())
                        .clientId(credentialsDetails.getServicePrincipalClientId())
                        .clientSecret(credentialsDetails.getServicePrincipalClientSecret())
                        .httpClient(new NettyAsyncHttpClientBuilder()
                                .proxy(credentialsDetails.getProxyOptions())
                                .build())
                        .build());
                break;
            case ACCESS_TOKEN:
                TokenCredential credential = tokenRequestContext -> Mono.just(credentialsDetails.getAccessToken());
                clientBuilder.credential(credential);
                break;
            default:
                throw new IllegalArgumentException("Unhandled credentials type: " + credentialsDetails.getCredentialsType());
        }
    }
}
