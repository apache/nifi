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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processors.aws.credentials.provider.factory.CredentialsStrategy;
import org.apache.nifi.processors.aws.signer.AwsCustomSignerUtil;
import org.apache.nifi.processors.aws.signer.AwsSignerType;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ASSUME_ROLE_ARN;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ASSUME_ROLE_EXTERNAL_ID;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ASSUME_ROLE_NAME;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ASSUME_ROLE_PROXY_HOST;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ASSUME_ROLE_PROXY_PORT;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ASSUME_ROLE_STS_CUSTOM_SIGNER_CLASS_NAME;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ASSUME_ROLE_STS_ENDPOINT;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ASSUME_ROLE_STS_REGION;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ASSUME_ROLE_STS_SIGNER_OVERRIDE;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.MAX_SESSION_TIME;
import static org.apache.nifi.processors.aws.signer.AwsSignerType.CUSTOM_SIGNER;
import static org.apache.nifi.processors.aws.signer.AwsSignerType.DEFAULT_SIGNER;


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
    public boolean canCreatePrimaryCredential(final Map<PropertyDescriptor, String> properties) {
        return false;
    }

    @Override
    public boolean canCreateDerivedCredential(final Map<PropertyDescriptor, String> properties) {
        final String assumeRoleArn = properties.get(ASSUME_ROLE_ARN);
        final String assumeRoleName = properties.get(ASSUME_ROLE_NAME);
        if (assumeRoleArn != null && !assumeRoleArn.isEmpty()
                && assumeRoleName != null && !assumeRoleName.isEmpty()) {
            return true;
        }
        return false;
    }

    public boolean proxyVariablesValidForAssumeRole(final Map<PropertyDescriptor, String> properties){
        final String assumeRoleProxyHost = properties.get(ASSUME_ROLE_PROXY_HOST);
        final String assumeRoleProxyPort = properties.get(ASSUME_ROLE_PROXY_PORT);
        if (assumeRoleProxyHost != null && !assumeRoleProxyHost.isEmpty()
                && assumeRoleProxyPort != null && !assumeRoleProxyPort.isEmpty()) {
            return true;
        }
        return false;
    }

    @Override
    public Collection<ValidationResult> validate(final ValidationContext validationContext,
                                                 final CredentialsStrategy primaryStrategy) {
        final Collection<ValidationResult> validationFailureResults  = new ArrayList<>();

        final boolean assumeRoleArnIsSet = validationContext.getProperty(ASSUME_ROLE_ARN).isSet();

        if (assumeRoleArnIsSet) {
            final Integer maxSessionTime = validationContext.getProperty(MAX_SESSION_TIME).asInteger();

            // Session time only b/w 900 to 3600 sec (see com.amazonaws.services.securitytoken.model.AssumeRoleRequest#withDurationSeconds)
            if (maxSessionTime < 900 || maxSessionTime > 3600) {
                validationFailureResults.add(new ValidationResult.Builder().valid(false).input(maxSessionTime + "")
                        .explanation(MAX_SESSION_TIME.getDisplayName() +
                                " must be between 900 and 3600 seconds").build());
            }

            final boolean assumeRoleProxyHostIsSet = validationContext.getProperty(ASSUME_ROLE_PROXY_HOST).isSet();
            final boolean assumeRoleProxyPortIsSet = validationContext.getProperty(ASSUME_ROLE_PROXY_PORT).isSet();

            // Both proxy host and proxy port are required if present
            if (assumeRoleProxyHostIsSet ^ assumeRoleProxyPortIsSet) {
                validationFailureResults.add(new ValidationResult.Builder().input("Assume Role Proxy Host and Port")
                        .valid(false)
                        .explanation("Assume role with proxy requires both host and port for the proxy to be set")
                        .build());
            }
        }

        return validationFailureResults;
    }

    @Override
    public AWSCredentialsProvider getCredentialsProvider(final Map<PropertyDescriptor, String> properties) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AWSCredentialsProvider getDerivedCredentialsProvider(final Map<PropertyDescriptor, String> properties,
                                                                final AWSCredentialsProvider primaryCredentialsProvider) {
        final String assumeRoleArn = properties.get(ASSUME_ROLE_ARN);
        final String assumeRoleName = properties.get(ASSUME_ROLE_NAME);
        String rawMaxSessionTime = properties.get(MAX_SESSION_TIME);
        rawMaxSessionTime = rawMaxSessionTime == null ? MAX_SESSION_TIME.getDefaultValue() : rawMaxSessionTime;
        final Integer maxSessionTime = Integer.parseInt(rawMaxSessionTime.trim());
        final String assumeRoleExternalId = properties.get(ASSUME_ROLE_EXTERNAL_ID);
        final String assumeRoleSTSRegion = properties.get(ASSUME_ROLE_STS_REGION);
        final String assumeRoleSTSEndpoint = properties.get(ASSUME_ROLE_STS_ENDPOINT);
        final String assumeRoleSTSSigner = properties.get(ASSUME_ROLE_STS_SIGNER_OVERRIDE);
        STSAssumeRoleSessionCredentialsProvider.Builder builder;
        ClientConfiguration config = new ClientConfiguration();

        // If proxy variables are set, then create Client Configuration with those values
        if (proxyVariablesValidForAssumeRole(properties)) {
            final String assumeRoleProxyHost = properties.get(ASSUME_ROLE_PROXY_HOST);
            final Integer assumeRoleProxyPort = Integer.parseInt(properties.get(ASSUME_ROLE_PROXY_PORT));
            config.withProxyHost(assumeRoleProxyHost);
            config.withProxyPort(assumeRoleProxyPort);
        }

        final AwsSignerType assumeRoleSTSSignerType = AwsSignerType.forValue(assumeRoleSTSSigner);
        if (assumeRoleSTSSignerType == CUSTOM_SIGNER) {
            final String signerClassName = properties.get(ASSUME_ROLE_STS_CUSTOM_SIGNER_CLASS_NAME);

            config.withSignerOverride(AwsCustomSignerUtil.registerCustomSigner(signerClassName));
        } else if (assumeRoleSTSSignerType != DEFAULT_SIGNER) {
            config.withSignerOverride(assumeRoleSTSSigner);
        }

        AWSSecurityTokenServiceClient securityTokenService = new AWSSecurityTokenServiceClient(primaryCredentialsProvider, config);
        if (assumeRoleSTSEndpoint != null && !assumeRoleSTSEndpoint.isEmpty()) {
            if (assumeRoleSTSSignerType == CUSTOM_SIGNER) {
                securityTokenService.setEndpoint(assumeRoleSTSEndpoint, securityTokenService.getServiceName(), assumeRoleSTSRegion);
            } else {
                securityTokenService.setEndpoint(assumeRoleSTSEndpoint);
            }
        }

        builder = new STSAssumeRoleSessionCredentialsProvider
                .Builder(assumeRoleArn, assumeRoleName)
                .withStsClient(securityTokenService)
                .withRoleSessionDurationSeconds(maxSessionTime);

        if (assumeRoleExternalId != null && !assumeRoleExternalId.isEmpty()) {
            builder = builder.withExternalId(assumeRoleExternalId);
        }

        final AWSCredentialsProvider credsProvider = builder.build();

        return credsProvider;
    }

    @Override
    public AwsCredentialsProvider getAwsCredentialsProvider(final Map<PropertyDescriptor, String> properties) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AwsCredentialsProvider getDerivedAwsCredentialsProvider(final Map<PropertyDescriptor, String> properties,
                                                                   AwsCredentialsProvider primaryCredentialsProvider) {
        final String assumeRoleArn = properties.get(ASSUME_ROLE_ARN);
        final String assumeRoleName = properties.get(ASSUME_ROLE_NAME);
        String rawMaxSessionTime = properties.get(MAX_SESSION_TIME);
        rawMaxSessionTime = rawMaxSessionTime == null ? MAX_SESSION_TIME.getDefaultValue() : rawMaxSessionTime;
        final Integer maxSessionTime = Integer.parseInt(rawMaxSessionTime.trim());
        final String assumeRoleExternalId = properties.get(ASSUME_ROLE_EXTERNAL_ID);
        final String assumeRoleSTSEndpoint = properties.get(ASSUME_ROLE_STS_ENDPOINT);
        final String stsRegion = properties.get(ASSUME_ROLE_STS_REGION);

        final StsAssumeRoleCredentialsProvider.Builder builder = StsAssumeRoleCredentialsProvider.builder();

        // If proxy variables are set, then create Client Configuration with those values
        final ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder();
        if (proxyVariablesValidForAssumeRole(properties)) {
            final String assumeRoleProxyHost = properties.get(ASSUME_ROLE_PROXY_HOST);
            final Integer assumeRoleProxyPort = Integer.parseInt(properties.get(ASSUME_ROLE_PROXY_PORT));
            final software.amazon.awssdk.http.apache.ProxyConfiguration proxyConfig = software.amazon.awssdk.http.apache.ProxyConfiguration.builder()
                    .endpoint(URI.create(String.format("%s:%s", assumeRoleProxyHost, assumeRoleProxyPort)))
                    .build();
            httpClientBuilder.proxyConfiguration(proxyConfig);
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
                .roleSessionName(assumeRoleName);

        if (assumeRoleExternalId != null && !assumeRoleExternalId.isEmpty()) {
            roleRequestBuilder.externalId(assumeRoleExternalId);
        }

        builder.refreshRequest(roleRequestBuilder.build())
                .stsClient(stsClient)
                .staleTime(Duration.ofSeconds(maxSessionTime));

        return builder.build();
    }
}
