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
package org.apache.nifi.processors.aws.credentials.provider.factory.strategies;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processors.aws.credentials.provider.factory.CredentialsStrategy;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.ssl.SSLContextProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.net.Proxy;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import javax.net.ssl.SSLContext;

import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ASSUME_ROLE_ARN;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ASSUME_ROLE_NAME;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ASSUME_ROLE_PROXY_CONFIGURATION_SERVICE;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ASSUME_ROLE_SSL_CONTEXT_SERVICE;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ASSUME_ROLE_STS_ENDPOINT;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ASSUME_ROLE_STS_REGION;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.MAX_SESSION_TIME;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.OAUTH2_ACCESS_TOKEN_PROVIDER;

/**
 * Supports AWS SDK v2 credentials using STS AssumeRoleWithWebIdentity and an OAuth2/OIDC token
 * provided by an {@link OAuth2AccessTokenProvider}. This is a primary strategy for SDK v2 only.
 */
public class WebIdentityCredentialsStrategy extends AbstractCredentialsStrategy implements CredentialsStrategy {

    public WebIdentityCredentialsStrategy() {
        super("Web Identity", new PropertyDescriptor[]{
                OAUTH2_ACCESS_TOKEN_PROVIDER,
                ASSUME_ROLE_ARN,
                ASSUME_ROLE_NAME
        });
    }

    @Override
    public Collection<ValidationResult> validate(final ValidationContext validationContext, final CredentialsStrategy primaryStrategy) {
        // Avoid cross-strategy validation conflicts: Web Identity reuses Assume Role properties.
        // Controller-level validation enforces required combinations when OAuth2 is configured.
        return Collections.emptyList();
    }

    @Override
    public AwsCredentialsProvider getAwsCredentialsProvider(final PropertyContext propertyContext) {
        final OAuth2AccessTokenProvider tokenProvider = propertyContext.getProperty(OAUTH2_ACCESS_TOKEN_PROVIDER).asControllerService(OAuth2AccessTokenProvider.class);
        final String roleArn = propertyContext.getProperty(ASSUME_ROLE_ARN).getValue();
        final String roleSessionName = propertyContext.getProperty(ASSUME_ROLE_NAME).getValue();
        final Integer sessionSeconds = propertyContext.getProperty(MAX_SESSION_TIME).asInteger();
        final String stsRegionId = propertyContext.getProperty(ASSUME_ROLE_STS_REGION).getValue();
        final String stsEndpoint = propertyContext.getProperty(ASSUME_ROLE_STS_ENDPOINT).getValue();
        final SSLContextProvider sslContextProvider = propertyContext.getProperty(ASSUME_ROLE_SSL_CONTEXT_SERVICE).asControllerService(SSLContextProvider.class);
        final ProxyConfigurationService proxyConfigurationService = propertyContext.getProperty(ASSUME_ROLE_PROXY_CONFIGURATION_SERVICE).asControllerService(ProxyConfigurationService.class);

        final ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder();

        if (sslContextProvider != null) {
            final SSLContext sslContext = sslContextProvider.createContext();
            httpClientBuilder.socketFactory(new SSLConnectionSocketFactory(sslContext));
        }

        if (proxyConfigurationService != null) {
            final ProxyConfiguration proxyConfiguration = proxyConfigurationService.getConfiguration();
            if (proxyConfiguration.getProxyType() == Proxy.Type.HTTP) {
                final software.amazon.awssdk.http.apache.ProxyConfiguration.Builder proxyConfigBuilder = software.amazon.awssdk.http.apache.ProxyConfiguration.builder()
                        .endpoint(URI.create(String.format("http://%s:%s", proxyConfiguration.getProxyServerHost(), proxyConfiguration.getProxyServerPort())));

                if (proxyConfiguration.hasCredential()) {
                    proxyConfigBuilder.username(proxyConfiguration.getProxyUserName());
                    proxyConfigBuilder.password(proxyConfiguration.getProxyUserPassword());
                }

                httpClientBuilder.proxyConfiguration(proxyConfigBuilder.build());
            }
        }

        final StsClientBuilder stsClientBuilder = StsClient.builder().httpClient(httpClientBuilder.build());

        if (stsRegionId != null) {
            stsClientBuilder.region(Region.of(stsRegionId));
        }

        if (stsEndpoint != null && !stsEndpoint.isEmpty()) {
            stsClientBuilder.endpointOverride(URI.create(stsEndpoint));
        }

        final StsClient stsClient = stsClientBuilder.build();

        return new WebIdentityRefreshingCredentialsProvider(stsClient, tokenProvider, roleArn, roleSessionName, sessionSeconds);
    }

    private static final class WebIdentityRefreshingCredentialsProvider implements AwsCredentialsProvider {
        private static final Duration SKEW = Duration.ofSeconds(60);

        private final StsClient stsClient;
        private final OAuth2AccessTokenProvider oauth2AccessTokenProvider;
        private final String roleArn;
        private final String roleSessionName;
        private final Integer sessionSeconds;

        private volatile AwsSessionCredentials cached;
        private volatile Instant expiration;

        private WebIdentityRefreshingCredentialsProvider(final StsClient stsClient,
                                                         final OAuth2AccessTokenProvider oauth2AccessTokenProvider,
                                                         final String roleArn,
                                                         final String roleSessionName,
                                                         final Integer sessionSeconds) {
            this.stsClient = Objects.requireNonNull(stsClient, "stsClient required");
            this.oauth2AccessTokenProvider = Objects.requireNonNull(oauth2AccessTokenProvider, "OAuth2AccessTokenProvider required");
            this.roleArn = Objects.requireNonNull(roleArn, "roleArn required");
            this.roleSessionName = Objects.requireNonNull(roleSessionName, "roleSessionName required");
            this.sessionSeconds = sessionSeconds;
        }

        @Override
        public AwsCredentials resolveCredentials() {
            final Instant now = Instant.now();
            final AwsSessionCredentials current = cached;
            final Instant currentExpiration = expiration;
            if (current != null && currentExpiration != null && now.isBefore(currentExpiration.minus(SKEW))) {
                return current;
            }

            synchronized (this) {
                if (cached != null && expiration != null && Instant.now().isBefore(expiration.minus(SKEW))) {
                    return cached;
                }

                final String webIdentityToken = getWebIdentityToken();

                final AssumeRoleWithWebIdentityRequest.Builder reqBuilder = AssumeRoleWithWebIdentityRequest.builder()
                        .roleArn(roleArn)
                        .roleSessionName(roleSessionName)
                        .webIdentityToken(webIdentityToken);

                if (sessionSeconds != null) {
                    reqBuilder.durationSeconds(sessionSeconds);
                }

                final AssumeRoleWithWebIdentityResponse resp = stsClient.assumeRoleWithWebIdentity(reqBuilder.build());
                final Credentials creds = resp.credentials();
                final AwsSessionCredentials sessionCreds = AwsSessionCredentials.create(
                        creds.accessKeyId(), creds.secretAccessKey(), creds.sessionToken());

                this.cached = sessionCreds;
                this.expiration = creds.expiration();
                return sessionCreds;
            }
        }

        private String getWebIdentityToken() {
            final AccessToken accessToken = oauth2AccessTokenProvider.getAccessDetails();
            if (accessToken == null) {
                throw new IllegalStateException("OAuth2AccessTokenProvider returned null AccessToken");
            }

            // Prefer id_token when available
            final Map<String, Object> additional = accessToken.getAdditionalParameters();
            if (additional != null) {
                final String idToken = (String) additional.get("id_token");
                if (idToken != null) {
                    if (StringUtils.isBlank(idToken)) {
                        throw new IllegalStateException("OAuth2AccessTokenProvider returned an empty id_token");
                    } else {
                        return idToken;
                    }
                }
            }

            final String accessTokenValue = accessToken.getAccessToken();
            if (StringUtils.isBlank(accessTokenValue)) {
                throw new IllegalStateException("No usable token found in AccessToken (id_token or access_token)");
            } else {
                return accessTokenValue;
            }
        }
    }
}
