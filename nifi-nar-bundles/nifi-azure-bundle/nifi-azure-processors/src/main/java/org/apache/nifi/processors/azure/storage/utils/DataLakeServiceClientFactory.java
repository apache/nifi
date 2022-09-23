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

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.http.HttpClient;
import com.azure.core.http.ProxyOptions;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.ManagedIdentityCredential;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.services.azure.storage.ADLSCredentialsDetails;
import reactor.core.publisher.Mono;

public class DataLakeServiceClientFactory {

    private static final long STORAGE_CLIENT_CACHE_SIZE = 10;

    private final ComponentLog logger;
    private final ProxyOptions proxyOptions;

    private final Cache<ADLSCredentialsDetails, DataLakeServiceClient> clientCache;

    public DataLakeServiceClientFactory(ComponentLog logger, ProxyOptions proxyOptions) {
        this.logger = logger;
        this.proxyOptions = proxyOptions;
        this.clientCache = createCache();
    }

    private Cache<ADLSCredentialsDetails, DataLakeServiceClient> createCache() {
        // Beware! By default, Caffeine does not perform cleanup and evict values
        // "automatically" or instantly after a value expires. Because of that it
        // can happen that there are more elements in the cache than the maximum size.
        // See: https://github.com/ben-manes/caffeine/wiki/Cleanup
        return Caffeine.newBuilder()
                .maximumSize(STORAGE_CLIENT_CACHE_SIZE)
                .build();
    }

    /**
     * Retrieves a {@link DataLakeServiceClient}
     *
     * @param credentialsDetails used for caching because it can contain properties that are results of an expression
     * @return DataLakeServiceClient
     */
    public DataLakeServiceClient getStorageClient(ADLSCredentialsDetails credentialsDetails) {
        return clientCache.get(credentialsDetails, __ -> {
            logger.debug("DataLakeServiceClient is not found in the cache with the given credentials. Creating it.");
            return createStorageClient(credentialsDetails, proxyOptions);
        });
    }

    private static DataLakeServiceClient createStorageClient(ADLSCredentialsDetails credentialsDetails, ProxyOptions proxyOptions) {
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
        nettyClientBuilder.proxy(proxyOptions);

        final HttpClient nettyClient = nettyClientBuilder.build();
        dataLakeServiceClientBuilder.httpClient(nettyClient);

        return dataLakeServiceClientBuilder.buildClient();
    }
}
