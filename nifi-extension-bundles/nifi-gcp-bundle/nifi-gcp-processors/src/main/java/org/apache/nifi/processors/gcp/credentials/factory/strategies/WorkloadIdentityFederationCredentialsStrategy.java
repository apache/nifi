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

import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdentityPoolCredentials;
import com.google.auth.oauth2.IdentityPoolSubjectTokenSupplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Credentials strategy that configures Workload Identity Federation using Google Identity Pool credentials.
 */
public class WorkloadIdentityFederationCredentialsStrategy extends AbstractCredentialsStrategy {

    private static final String ERROR_NO_SUBJECT_TOKEN = "Subject token provider returned no usable token";
    private static final String SUBJECT_TOKEN_PARAMETER_ID_TOKEN = "id_token";
    private static final String SUBJECT_TOKEN_PARAMETER_ACCESS_TOKEN = "access_token";

    public WorkloadIdentityFederationCredentialsStrategy() {
        super("Workload Identity Federation");
    }

    @Override
    public GoogleCredentials getGoogleCredentials(final Map<PropertyDescriptor, String> properties, final HttpTransportFactory transportFactory) {
        throw new UnsupportedOperationException("Workload Identity Federation requires the controller service configuration context");
    }

    @Override
    public GoogleCredentials getGoogleCredentials(final ConfigurationContext context, final HttpTransportFactory transportFactory) throws IOException {
        final OAuth2AccessTokenProvider subjectTokenProvider = context.getProperty(CredentialPropertyDescriptors.WORKLOAD_IDENTITY_SUBJECT_TOKEN_PROVIDER)
                .asControllerService(OAuth2AccessTokenProvider.class);
        final String audience = context.getProperty(CredentialPropertyDescriptors.WORKLOAD_IDENTITY_AUDIENCE).getValue();
        final String scopeValue = context.getProperty(CredentialPropertyDescriptors.WORKLOAD_IDENTITY_SCOPE).getValue();
        final String tokenEndpoint = context.getProperty(CredentialPropertyDescriptors.WORKLOAD_IDENTITY_TOKEN_ENDPOINT).getValue();
        final String subjectTokenType = context.getProperty(CredentialPropertyDescriptors.WORKLOAD_IDENTITY_SUBJECT_TOKEN_TYPE).getValue();

        final List<String> scopes = parseScopes(scopeValue);
        final IdentityPoolSubjectTokenSupplier tokenSupplier = createSubjectTokenSupplier(subjectTokenProvider);
        final IdentityPoolCredentials.Builder builder = IdentityPoolCredentials.newBuilder()
                .setAudience(audience)
                .setTokenUrl(tokenEndpoint)
                .setSubjectTokenType(subjectTokenType)
                .setSubjectTokenSupplier(tokenSupplier);

        if (!scopes.isEmpty()) {
            builder.setScopes(scopes);
        }

        if (transportFactory != null) {
            builder.setHttpTransportFactory(transportFactory);
        }

        return builder.build();
    }

    private IdentityPoolSubjectTokenSupplier createSubjectTokenSupplier(final OAuth2AccessTokenProvider tokenProvider) {
        return context -> getSubjectToken(tokenProvider);
    }

    private String getSubjectToken(final OAuth2AccessTokenProvider tokenProvider) throws IOException {
        final AccessToken subjectToken = tokenProvider.getAccessDetails();

        final String subjectTokenValue = extractTokenValue(subjectToken);
        if (StringUtils.isBlank(subjectTokenValue)) {
            throw new IOException(ERROR_NO_SUBJECT_TOKEN);
        }

        return subjectTokenValue;
    }

    private String extractTokenValue(final AccessToken subjectToken) {
        if (subjectToken == null) {
            return null;
        }

        final Map<String, Object> additionalParameters = subjectToken.getAdditionalParameters();
        if (additionalParameters != null) {
            final Object idToken = additionalParameters.get(SUBJECT_TOKEN_PARAMETER_ID_TOKEN);
            if (idToken instanceof String && StringUtils.isNotBlank((String) idToken)) {
                return (String) idToken;
            }

            final Object accessToken = additionalParameters.get(SUBJECT_TOKEN_PARAMETER_ACCESS_TOKEN);
            if (accessToken instanceof String && StringUtils.isNotBlank((String) accessToken)) {
                return (String) accessToken;
            }
        }

        final String accessTokenValue = subjectToken.getAccessToken();
        if (StringUtils.isNotBlank(accessTokenValue)) {
            return accessTokenValue;
        }

        return null;
    }

    private static List<String> parseScopes(final String scopeValue) {
        if (StringUtils.isBlank(scopeValue)) {
            return Collections.emptyList();
        }

        return Arrays.stream(scopeValue.split("[\\s,]+"))
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList());
    }
}
