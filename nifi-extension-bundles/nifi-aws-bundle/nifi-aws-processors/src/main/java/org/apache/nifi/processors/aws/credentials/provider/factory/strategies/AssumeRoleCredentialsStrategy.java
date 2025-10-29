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

import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.processors.aws.credentials.provider.factory.CredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.ssl.SSLContextProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import javax.net.ssl.SSLContext;
import java.net.Proxy;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;

import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ASSUME_ROLE_ARN;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ASSUME_ROLE_EXTERNAL_ID;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ASSUME_ROLE_NAME;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ASSUME_ROLE_PROXY_CONFIGURATION_SERVICE;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ASSUME_ROLE_SSL_CONTEXT_SERVICE;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ASSUME_ROLE_STS_ENDPOINT;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ASSUME_ROLE_STS_REGION;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.MAX_SESSION_TIME;

/**
 * Supports AWS credentials via Assume Role.  Assume Role is a derived credential strategy, requiring a primary
 * credential to retrieve and periodically refresh temporary credentials.
 *
 * @see <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/STSAssumeRoleSessionCredentialsProvider.html">
 *     STSAssumeRoleCredentialsProvider</a>
 */
public class AssumeRoleCredentialsStrategy extends AbstractCredentialsStrategy {

    public AssumeRoleCredentialsStrategy() {
        super("Assume Role", new PropertyDescriptor[] {
                ASSUME_ROLE_ARN,
                ASSUME_ROLE_NAME,
                MAX_SESSION_TIME
        });
    }

    @Override
    public boolean canCreatePrimaryCredential(final PropertyContext propertyContext) {
        return false;
    }

    @Override
    public boolean canCreateDerivedCredential(final PropertyContext propertyContext) {
        if (propertyContext.getProperty(AWSCredentialsProviderControllerService.OAUTH2_ACCESS_TOKEN_PROVIDER).isSet()) {
            return false;
        }

        final String assumeRoleArn = propertyContext.getProperty(ASSUME_ROLE_ARN).getValue();
        final String assumeRoleName = propertyContext.getProperty(ASSUME_ROLE_NAME).getValue();
        if (assumeRoleArn != null && !assumeRoleArn.isEmpty()
                && assumeRoleName != null && !assumeRoleName.isEmpty()) {
            return true;
        }
        return false;
    }

    @Override
    public Collection<ValidationResult> validate(final ValidationContext validationContext,
                                                 final CredentialsStrategy primaryStrategy) {
        // Assume Role participates as a derived strategy or reused property group.
        // Do not produce cross-strategy validation failures here; required/missing
        // fields are enforced by PropertyDescriptor requirements and selected
        // strategies, and derived selection is handled separately.
        return Collections.emptyList();
    }

    @Override
    public AwsCredentialsProvider getAwsCredentialsProvider(final PropertyContext propertyContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AwsCredentialsProvider getDerivedAwsCredentialsProvider(final PropertyContext propertyContext,
                                                                   final AwsCredentialsProvider primaryCredentialsProvider) {
        final String assumeRoleArn = propertyContext.getProperty(ASSUME_ROLE_ARN).getValue();
        final String assumeRoleName = propertyContext.getProperty(ASSUME_ROLE_NAME).getValue();
        final int maxSessionTime = propertyContext.getProperty(MAX_SESSION_TIME).asInteger();
        final String assumeRoleExternalId = propertyContext.getProperty(ASSUME_ROLE_EXTERNAL_ID).getValue();
        final String assumeRoleSTSEndpoint = propertyContext.getProperty(ASSUME_ROLE_STS_ENDPOINT).getValue();
        final String stsRegion = propertyContext.getProperty(ASSUME_ROLE_STS_REGION).getValue();
        final SSLContextProvider sslContextProvider = propertyContext.getProperty(ASSUME_ROLE_SSL_CONTEXT_SERVICE).asControllerService(SSLContextProvider.class);
        final ProxyConfigurationService proxyConfigurationService = propertyContext.getProperty(ASSUME_ROLE_PROXY_CONFIGURATION_SERVICE).asControllerService(ProxyConfigurationService.class);

        final StsAssumeRoleCredentialsProvider.Builder builder = StsAssumeRoleCredentialsProvider.builder();

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

        if (stsRegion == null) {
            throw new IllegalStateException("Assume Role Region is required to interact with STS");
        }

        final StsClientBuilder stsClientBuilder = StsClient.builder()
                .credentialsProvider(primaryCredentialsProvider)
                .region(Region.of(stsRegion))
                .httpClient(httpClientBuilder.build());
        if (assumeRoleSTSEndpoint != null && !assumeRoleSTSEndpoint.isEmpty()) {
            stsClientBuilder.endpointOverride(URI.create(assumeRoleSTSEndpoint));
        }
        final StsClient stsClient = stsClientBuilder.build();

        final AssumeRoleRequest.Builder roleRequestBuilder = AssumeRoleRequest.builder()
                .roleArn(assumeRoleArn)
                .roleSessionName(assumeRoleName)
                .durationSeconds(maxSessionTime);

        if (assumeRoleExternalId != null && !assumeRoleExternalId.isEmpty()) {
            roleRequestBuilder.externalId(assumeRoleExternalId);
        }

        builder.refreshRequest(roleRequestBuilder.build())
                .stsClient(stsClient);

        return builder.build();
    }
}
