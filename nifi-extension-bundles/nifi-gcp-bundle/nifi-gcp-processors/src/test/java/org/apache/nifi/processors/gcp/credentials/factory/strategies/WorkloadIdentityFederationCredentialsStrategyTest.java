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
package org.apache.nifi.processors.gcp.credentials.factory.strategies;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdentityPoolCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class WorkloadIdentityFederationCredentialsStrategyTest {
    private static final String AUDIENCE = "projects/123456789/locations/global/workloadIdentityPools/pool/providers/provider";
    private static final String SCOPE = "https://www.googleapis.com/auth/cloud-platform";
    private static final String TOKEN_ENDPOINT = "https://sts.googleapis.com/v1/token";
    private static final String SUBJECT_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:jwt";
    private static final String TARGET_SERVICE_ACCOUNT = "target-account@test-project.iam.gserviceaccount.com";
    private static final String SQLSERVICE_LOGIN_SCOPE = "https://www.googleapis.com/auth/sqlservice.login";

    private final WorkloadIdentityFederationCredentialsStrategy strategy = new WorkloadIdentityFederationCredentialsStrategy();

    @Test
    void testReturnsIdentityPoolCredentialsWhenTargetServiceAccountNotConfigured() throws IOException {
        final ConfigurationContext context = mockConfigurationContext(null);

        final GoogleCredentials credentials = strategy.getGoogleCredentials(context, transportFactory());

        assertInstanceOf(IdentityPoolCredentials.class, credentials);
    }

    @Test
    void testReturnsImpersonatedCredentialsWithEmptyScopesAndPreservedTransportFactory() throws IOException {
        final HttpTransportFactory transportFactory = transportFactory();
        final ConfigurationContext context = mockConfigurationContext(TARGET_SERVICE_ACCOUNT);

        final GoogleCredentials credentials = strategy.getGoogleCredentials(context, transportFactory);

        final ImpersonatedCredentials impersonatedCredentials = assertInstanceOf(ImpersonatedCredentials.class, credentials);
        assertEquals(TARGET_SERVICE_ACCOUNT, impersonatedCredentials.getAccount());
        assertInstanceOf(IdentityPoolCredentials.class, impersonatedCredentials.getSourceCredentials());
        assertTrue(impersonatedCredentials.toBuilder().getScopes().isEmpty());
        assertTrue(impersonatedCredentials.createScopedRequired());
        assertSame(transportFactory, impersonatedCredentials.toBuilder().getHttpTransportFactory());

        final GoogleCredentials scopedCredentials = credentials.createScoped(List.of(SQLSERVICE_LOGIN_SCOPE));

        final ImpersonatedCredentials scopedImpersonatedCredentials = assertInstanceOf(ImpersonatedCredentials.class, scopedCredentials);
        assertEquals(TARGET_SERVICE_ACCOUNT, scopedImpersonatedCredentials.getAccount());
        assertSame(impersonatedCredentials.getSourceCredentials(), scopedImpersonatedCredentials.getSourceCredentials());
        assertEquals(List.of(SQLSERVICE_LOGIN_SCOPE), scopedImpersonatedCredentials.toBuilder().getScopes());
        assertFalse(scopedImpersonatedCredentials.createScopedRequired());
        assertSame(transportFactory, scopedImpersonatedCredentials.toBuilder().getHttpTransportFactory());
    }

    private ConfigurationContext mockConfigurationContext(final String targetServiceAccount) {
        final ConfigurationContext context = mock(ConfigurationContext.class);
        final PropertyValue audiencePropertyValue = stringPropertyValue(AUDIENCE);
        final PropertyValue scopePropertyValue = stringPropertyValue(SCOPE);
        final PropertyValue tokenEndpointPropertyValue = stringPropertyValue(TOKEN_ENDPOINT);
        final PropertyValue subjectTokenTypePropertyValue = stringPropertyValue(SUBJECT_TOKEN_TYPE);
        final PropertyValue targetServiceAccountPropertyValue = stringPropertyValue(targetServiceAccount);

        when(context.getProperty(CredentialPropertyDescriptors.WORKLOAD_IDENTITY_AUDIENCE)).thenReturn(audiencePropertyValue);
        when(context.getProperty(CredentialPropertyDescriptors.WORKLOAD_IDENTITY_SCOPE)).thenReturn(scopePropertyValue);
        when(context.getProperty(CredentialPropertyDescriptors.WORKLOAD_IDENTITY_TOKEN_ENDPOINT)).thenReturn(tokenEndpointPropertyValue);
        when(context.getProperty(CredentialPropertyDescriptors.WORKLOAD_IDENTITY_SUBJECT_TOKEN_TYPE)).thenReturn(subjectTokenTypePropertyValue);
        when(context.getProperty(CredentialPropertyDescriptors.TARGET_SERVICE_ACCOUNT)).thenReturn(targetServiceAccountPropertyValue);

        final PropertyValue subjectTokenProviderProperty = mock(PropertyValue.class);
        when(subjectTokenProviderProperty.asControllerService(OAuth2AccessTokenProvider.class)).thenReturn(new MockOAuth2AccessTokenProvider());
        when(context.getProperty(CredentialPropertyDescriptors.WORKLOAD_IDENTITY_SUBJECT_TOKEN_PROVIDER)).thenReturn(subjectTokenProviderProperty);
        return context;
    }

    private PropertyValue stringPropertyValue(final String value) {
        final PropertyValue propertyValue = mock(PropertyValue.class);
        when(propertyValue.getValue()).thenReturn(value);
        return propertyValue;
    }

    private HttpTransportFactory transportFactory() {
        final HttpTransport transport = new NetHttpTransport();
        return () -> transport;
    }

    private static final class MockOAuth2AccessTokenProvider extends AbstractControllerService implements OAuth2AccessTokenProvider {
        @Override
        public AccessToken getAccessDetails() {
            final AccessToken accessToken = new AccessToken();
            accessToken.setAccessToken("subject-token");
            accessToken.setExpiresIn(3600L);
            return accessToken;
        }

        @Override
        public void refreshAccessDetails() {
        }
    }
}
