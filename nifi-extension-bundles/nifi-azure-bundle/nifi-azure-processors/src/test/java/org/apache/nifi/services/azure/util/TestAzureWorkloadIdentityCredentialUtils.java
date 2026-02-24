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
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link AzureWorkloadIdentityCredentialUtils}.
 */
public class TestAzureWorkloadIdentityCredentialUtils {

    private static final String TENANT_ID = "test-tenant-id";
    private static final String CLIENT_ID = "test-client-id";
    private static final String ASSERTION_TOKEN = "test-assertion-token";

    @Test
    public void testCreateCredentialWithOAuth2Provider() {
        final OAuth2AccessTokenProvider provider = new MockOAuth2Provider(ASSERTION_TOKEN);

        final TokenCredential credential = AzureWorkloadIdentityCredentialUtils.createCredential(
                TENANT_ID, CLIENT_ID, provider);

        assertNotNull(credential);
    }

    @Test
    public void testCreateCredentialWithSupplier() {
        final TokenCredential credential = AzureWorkloadIdentityCredentialUtils.createCredential(
                TENANT_ID, CLIENT_ID, () -> ASSERTION_TOKEN);

        assertNotNull(credential);
    }

    @Test
    public void testCreateCredentialWithNullTenantId() {
        assertThrows(NullPointerException.class, () ->
                AzureWorkloadIdentityCredentialUtils.createCredential(null, CLIENT_ID, () -> ASSERTION_TOKEN));
    }

    @Test
    public void testCreateCredentialWithNullClientId() {
        assertThrows(NullPointerException.class, () ->
                AzureWorkloadIdentityCredentialUtils.createCredential(TENANT_ID, null, () -> ASSERTION_TOKEN));
    }

    @Test
    public void testCreateCredentialWithNullSupplier() {
        assertThrows(NullPointerException.class, () ->
                AzureWorkloadIdentityCredentialUtils.createCredential(TENANT_ID, CLIENT_ID, (java.util.function.Supplier<String>) null));
    }

    @Test
    public void testCreateCredentialWithNullProvider() {
        assertThrows(NullPointerException.class, () ->
                AzureWorkloadIdentityCredentialUtils.createCredential(TENANT_ID, CLIENT_ID, (OAuth2AccessTokenProvider) null));
    }

    @Test
    public void testProviderReturnsNullAccessToken() {
        final OAuth2AccessTokenProvider provider = new MockOAuth2Provider(null, true);

        final TokenCredential credential = AzureWorkloadIdentityCredentialUtils.createCredential(
                TENANT_ID, CLIENT_ID, provider);

        // Credential is created, but calling getToken() would fail when the supplier is invoked
        assertNotNull(credential);
    }

    @Test
    public void testProviderReturnsEmptyToken() {
        final OAuth2AccessTokenProvider provider = new MockOAuth2Provider("");

        final TokenCredential credential = AzureWorkloadIdentityCredentialUtils.createCredential(
                TENANT_ID, CLIENT_ID, provider);

        // Credential is created, but calling getToken() would fail when the supplier is invoked
        assertNotNull(credential);
    }

    private static class MockOAuth2Provider extends AbstractControllerService implements OAuth2AccessTokenProvider {
        private final String tokenValue;
        private final boolean returnNull;

        MockOAuth2Provider(final String tokenValue) {
            this(tokenValue, false);
        }

        MockOAuth2Provider(final String tokenValue, final boolean returnNull) {
            this.tokenValue = tokenValue;
            this.returnNull = returnNull;
        }

        @Override
        public AccessToken getAccessDetails() {
            if (returnNull) {
                return null;
            }
            final AccessToken accessToken = new AccessToken();
            accessToken.setAccessToken(tokenValue);
            accessToken.setExpiresIn(600L);
            return accessToken;
        }
    }
}
