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
package org.apache.nifi.services.azure.util;

import com.azure.core.credential.TokenCredential;
import com.azure.core.http.HttpClient;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.identity.ClientAssertionCredentialBuilder;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Utility class for building Azure {@link TokenCredential} objects for workload identity federation.
 * <p>
 * This utility simplifies the creation of {@link com.azure.identity.ClientAssertionCredential} by providing
 * factory methods that accept common NiFi components like {@link OAuth2AccessTokenProvider}.
 */
public final class AzureWorkloadIdentityCredentialUtils {

    private AzureWorkloadIdentityCredentialUtils() {
        // Utility class - prevent instantiation
    }

    /**
     * Creates a {@link TokenCredential} for Azure workload identity federation using the provided
     * OAuth2 access token provider as the source of client assertions.
     *
     * @param tenantId                the Microsoft Entra tenant ID
     * @param clientId                the application (client) ID of the Microsoft Entra application
     * @param clientAssertionProvider the OAuth2 access token provider that supplies the client assertion token
     * @return a TokenCredential configured for workload identity federation
     * @throws NullPointerException if any parameter is null
     */
    public static TokenCredential createCredential(
            final String tenantId,
            final String clientId,
            final OAuth2AccessTokenProvider clientAssertionProvider) {
        Objects.requireNonNull(tenantId, "Tenant ID is required");
        Objects.requireNonNull(clientId, "Client ID is required");
        Objects.requireNonNull(clientAssertionProvider, "Client Assertion Provider is required");

        return createCredential(tenantId, clientId, () -> getClientAssertion(clientAssertionProvider));
    }

    /**
     * Creates a {@link TokenCredential} for Azure workload identity federation using a custom
     * client assertion supplier.
     *
     * @param tenantId               the Microsoft Entra tenant ID
     * @param clientId               the application (client) ID of the Microsoft Entra application
     * @param clientAssertionSupplier a supplier that provides the client assertion token string
     * @return a TokenCredential configured for workload identity federation
     * @throws NullPointerException if any parameter is null
     */
    public static TokenCredential createCredential(
            final String tenantId,
            final String clientId,
            final Supplier<String> clientAssertionSupplier) {
        Objects.requireNonNull(tenantId, "Tenant ID is required");
        Objects.requireNonNull(clientId, "Client ID is required");
        Objects.requireNonNull(clientAssertionSupplier, "Client Assertion Supplier is required");

        return createCredential(tenantId, clientId, clientAssertionSupplier, new NettyAsyncHttpClientBuilder().build());
    }

    /**
     * Creates a {@link TokenCredential} for Azure workload identity federation using a custom
     * client assertion supplier and HTTP client.
     *
     * @param tenantId               the Microsoft Entra tenant ID
     * @param clientId               the application (client) ID of the Microsoft Entra application
     * @param clientAssertionSupplier a supplier that provides the client assertion token string
     * @param httpClient             the HTTP client to use for token requests
     * @return a TokenCredential configured for workload identity federation
     * @throws NullPointerException if any parameter is null
     */
    public static TokenCredential createCredential(
            final String tenantId,
            final String clientId,
            final Supplier<String> clientAssertionSupplier,
            final HttpClient httpClient) {
        Objects.requireNonNull(tenantId, "Tenant ID is required");
        Objects.requireNonNull(clientId, "Client ID is required");
        Objects.requireNonNull(clientAssertionSupplier, "Client Assertion Supplier is required");
        Objects.requireNonNull(httpClient, "HTTP Client is required");

        return new ClientAssertionCredentialBuilder()
                .tenantId(tenantId)
                .clientId(clientId)
                .clientAssertion(clientAssertionSupplier)
                .httpClient(httpClient)
                .build();
    }

    /**
     * Extracts the client assertion token from an OAuth2 access token provider.
     *
     * @param provider the OAuth2 access token provider
     * @return the client assertion token string
     * @throws IllegalStateException if the provider returns null or an empty token
     */
    private static String getClientAssertion(final OAuth2AccessTokenProvider provider) {
        final AccessToken accessToken = provider.getAccessDetails();
        if (accessToken == null) {
            throw new IllegalStateException("Client assertion provider returned null");
        }
        final String assertion = accessToken.getAccessToken();
        if (assertion == null || assertion.isBlank()) {
            throw new IllegalStateException("Client assertion provider returned empty token");
        }
        return assertion;
    }
}
