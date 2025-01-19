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
package org.apache.nifi.web.security.oidc.registration;

import static com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod.CLIENT_SECRET_BASIC;
import static com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod.CLIENT_SECRET_POST;
import static com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod.NONE;

import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.openid.connect.sdk.op.OIDCProviderMetadata;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.oidc.OidcConfigurationException;
import org.apache.nifi.web.security.oidc.OidcUrlPath;
import org.apache.nifi.web.security.oidc.client.web.OidcRegistrationProperty;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.security.oauth2.core.ClientAuthenticationMethod;
import org.springframework.security.oauth2.core.oidc.IdTokenClaimNames;
import org.springframework.security.oauth2.core.oidc.OidcScopes;
import org.springframework.web.client.RestClient;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Standard implementation of Client Registration Provider using Application Properties
 */
public class StandardClientRegistrationProvider implements ClientRegistrationProvider {

    private static final String REGISTRATION_REDIRECT_URI = String.format("{baseUrl}%s", OidcUrlPath.CALLBACK.getPath());

    private static final Set<String> STANDARD_SCOPES = Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList(OidcScopes.OPENID, OidcScopes.EMAIL)));

    private static final String FILE_URI_SCHEME = "file";

    private final NiFiProperties properties;

    private final RestClient restClient;

    public StandardClientRegistrationProvider(
            final NiFiProperties properties,
            final RestClient restClient
    ) {
        this.properties = Objects.requireNonNull(properties, "Properties required");
        this.restClient = Objects.requireNonNull(restClient, "REST Client required");
    }

    /**
     * Get Client Registration using OpenID Connect Discovery URL
     *
     * @return Client Registration
     */
    @Override
    public ClientRegistration getClientRegistration() {
        final String clientId = properties.getOidcClientId();
        final String clientSecret = properties.getOidcClientSecret();

        final OIDCProviderMetadata providerMetadata = getProviderMetadata();
        final ClientAuthenticationMethod clientAuthenticationMethod = getClientAuthenticationMethod(providerMetadata.getTokenEndpointAuthMethods());
        final String issuerUri = providerMetadata.getIssuer().getValue();
        final String tokenUri = providerMetadata.getTokenEndpointURI().toASCIIString();
        final Map<String, Object> configurationMetadata = new LinkedHashMap<>(providerMetadata.toJSONObject());
        final String authorizationUri = providerMetadata.getAuthorizationEndpointURI().toASCIIString();
        final String jwkSetUri = providerMetadata.getJWKSetURI().toASCIIString();
        final String userInfoUri = providerMetadata.getUserInfoEndpointURI().toASCIIString();

        final Set<String> scope = new LinkedHashSet<>(STANDARD_SCOPES);
        final List<String> additionalScopes = properties.getOidcAdditionalScopes();
        scope.addAll(additionalScopes);

        return ClientRegistration.withRegistrationId(OidcRegistrationProperty.REGISTRATION_ID.getProperty())
                .clientId(clientId)
                .clientSecret(clientSecret)
                .clientName(issuerUri)
                .issuerUri(issuerUri)
                .tokenUri(tokenUri)
                .authorizationUri(authorizationUri)
                .jwkSetUri(jwkSetUri)
                .userInfoUri(userInfoUri)
                .providerConfigurationMetadata(configurationMetadata)
                .redirectUri(REGISTRATION_REDIRECT_URI)
                .scope(scope)
                // OpenID Connect 1.0 requires the sub claim and other components handle application username mapping
                .userNameAttributeName(IdTokenClaimNames.SUB)
                .clientAuthenticationMethod(clientAuthenticationMethod)
                .authorizationGrantType(AuthorizationGrantType.AUTHORIZATION_CODE)
                .build();
    }

    private OIDCProviderMetadata getProviderMetadata() {
        final String discoveryUrl = properties.getOidcDiscoveryUrl();
        final URI discoveryUri = URI.create(discoveryUrl);
        final String discoveryUriScheme = discoveryUri.getScheme();

        final String metadataObject;

        if (FILE_URI_SCHEME.equals(discoveryUriScheme)) {
            try {
                final Path discoveryPath = Paths.get(discoveryUri);
                metadataObject = Files.readString(discoveryPath);
            } catch (final Exception e) {
                final String message = String.format("OpenID Connect Metadata File URI [%s] read failed", discoveryUrl);
                throw new OidcConfigurationException(message, e);
            }
        } else {
            try {
                metadataObject = restClient.get().uri(discoveryUrl).retrieve().body(String.class);
            } catch (final RuntimeException e) {
                final String message = String.format("OpenID Connect Metadata URL [%s] retrieval failed", discoveryUrl);
                throw new OidcConfigurationException(message, e);
            }
        }

        try {
            return OIDCProviderMetadata.parse(metadataObject);
        } catch (final ParseException e) {
            throw new OidcConfigurationException("OpenID Connect Metadata parsing failed", e);
        }
    }

    private ClientAuthenticationMethod getClientAuthenticationMethod(final List<com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod> metadataAuthMethods) {
        final ClientAuthenticationMethod clientAuthenticationMethod;

        if (metadataAuthMethods == null || metadataAuthMethods.contains(CLIENT_SECRET_BASIC)) {
            clientAuthenticationMethod = ClientAuthenticationMethod.CLIENT_SECRET_BASIC;
        } else if (metadataAuthMethods.contains(CLIENT_SECRET_POST)) {
            clientAuthenticationMethod = ClientAuthenticationMethod.CLIENT_SECRET_POST;
        } else if (metadataAuthMethods.contains(NONE)) {
            clientAuthenticationMethod = ClientAuthenticationMethod.NONE;
        } else {
            clientAuthenticationMethod = ClientAuthenticationMethod.CLIENT_SECRET_BASIC;
        }

        return clientAuthenticationMethod;
    }
}
