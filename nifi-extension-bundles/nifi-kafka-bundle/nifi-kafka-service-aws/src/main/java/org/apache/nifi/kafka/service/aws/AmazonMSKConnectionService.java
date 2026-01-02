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
package org.apache.nifi.kafka.service.aws;

import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.kafka.service.Kafka3ConnectionService;
import org.apache.nifi.kafka.shared.aws.AmazonMSKProperty;
import org.apache.nifi.kafka.shared.component.KafkaClientComponent;
import org.apache.nifi.kafka.shared.property.AwsRoleSource;
import org.apache.nifi.kafka.shared.property.SaslMechanism;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextProvider;
import org.apache.nifi.util.StringUtils;
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

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;

@Tags({"AWS", "MSK", "streaming", "kafka"})
@CapabilityDescription("Provides and manages connections to AWS MSK Kafka Brokers for producer or consumer operations.")
public class AmazonMSKConnectionService extends Kafka3ConnectionService {

    public static final PropertyDescriptor AWS_SASL_MECHANISM = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SASL_MECHANISM)
            .allowableValues(
                    SaslMechanism.AWS_MSK_IAM,
                    SaslMechanism.SCRAM_SHA_512
            )
            .defaultValue(SaslMechanism.AWS_MSK_IAM)
            .build();

    public static final PropertyDescriptor AWS_WEB_IDENTITY_TOKEN_PROVIDER = KafkaClientComponent.AWS_WEB_IDENTITY_TOKEN_PROVIDER;

    private static final int MIN_SESSION_DURATION_SECONDS = 900;
    private static final int MAX_SESSION_DURATION_SECONDS = 3600;

    public static final PropertyDescriptor AWS_WEB_IDENTITY_SESSION_TIME = new PropertyDescriptor.Builder()
            .name("AWS Web Identity Session Time")
            .description("Session time for AWS STS AssumeRoleWithWebIdentity (between 900 seconds and 3600 seconds).")
            .dependsOn(
                    KafkaClientComponent.AWS_ROLE_SOURCE,
                    AwsRoleSource.WEB_IDENTITY_TOKEN
            )
            .required(true)
            .defaultValue("%d sec".formatted(MAX_SESSION_DURATION_SECONDS))
            .addValidator(StandardValidators.createTimePeriodValidator(
                    MIN_SESSION_DURATION_SECONDS, TimeUnit.SECONDS, MAX_SESSION_DURATION_SECONDS, TimeUnit.SECONDS))
            .build();

    public static final PropertyDescriptor AWS_WEB_IDENTITY_STS_REGION = new PropertyDescriptor.Builder()
            .name("AWS Web Identity STS Region")
            .description("Region identifier used for the AWS Security Token Service when exchanging Web Identity tokens.")
            .dependsOn(
                    KafkaClientComponent.AWS_ROLE_SOURCE,
                    AwsRoleSource.WEB_IDENTITY_TOKEN
            )
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor AWS_WEB_IDENTITY_STS_ENDPOINT = new PropertyDescriptor.Builder()
            .name("AWS Web Identity STS Endpoint")
            .description("Optional endpoint override for the AWS Security Token Service.")
            .dependsOn(
                    KafkaClientComponent.AWS_ROLE_SOURCE,
                    AwsRoleSource.WEB_IDENTITY_TOKEN
            )
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor AWS_WEB_IDENTITY_SSL_CONTEXT_PROVIDER = new PropertyDescriptor.Builder()
            .name("AWS Web Identity SSL Context Provider")
            .description("SSL Context Service used when communicating with AWS STS for Web Identity federation.")
            .identifiesControllerService(SSLContextProvider.class)
            .dependsOn(
                    KafkaClientComponent.AWS_ROLE_SOURCE,
                    AwsRoleSource.WEB_IDENTITY_TOKEN
            )
            .required(false)
            .build();

    private final List<PropertyDescriptor> supportedPropertyDescriptors;

    public AmazonMSKConnectionService() {
        final List<PropertyDescriptor> propertyDescriptors = new ArrayList<>(super.getSupportedPropertyDescriptors());

        final ListIterator<PropertyDescriptor> descriptors = propertyDescriptors.listIterator();
        while (descriptors.hasNext()) {
            final PropertyDescriptor propertyDescriptor = descriptors.next();
            if (SASL_MECHANISM.equals(propertyDescriptor)) {
                descriptors.remove();
                // Add AWS MSK properties
                descriptors.add(AWS_SASL_MECHANISM);
                descriptors.add(KafkaClientComponent.AWS_ROLE_SOURCE);
                descriptors.add(KafkaClientComponent.AWS_PROFILE_NAME);
                descriptors.add(KafkaClientComponent.AWS_ASSUME_ROLE_ARN);
                descriptors.add(KafkaClientComponent.AWS_ASSUME_ROLE_SESSION_NAME);
                descriptors.add(AWS_WEB_IDENTITY_TOKEN_PROVIDER);
                descriptors.add(AWS_WEB_IDENTITY_SESSION_TIME);
                descriptors.add(AWS_WEB_IDENTITY_STS_REGION);
                descriptors.add(AWS_WEB_IDENTITY_STS_ENDPOINT);
                descriptors.add(AWS_WEB_IDENTITY_SSL_CONTEXT_PROVIDER);
            }
        }

        supportedPropertyDescriptors = propertyDescriptors;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return supportedPropertyDescriptors;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final Collection<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));

        final AwsRoleSource roleSource = validationContext.getProperty(KafkaClientComponent.AWS_ROLE_SOURCE).asAllowableValue(AwsRoleSource.class);
        if (roleSource == AwsRoleSource.WEB_IDENTITY_TOKEN) {
            if (!validationContext.getProperty(AWS_WEB_IDENTITY_TOKEN_PROVIDER).isSet()) {
                results.add(new ValidationResult.Builder()
                        .subject(AWS_WEB_IDENTITY_TOKEN_PROVIDER.getDisplayName())
                        .valid(false)
                        .explanation("AWS Web Identity Token Provider must be configured when AWS Role Source is set to Web Identity Provider")
                        .build());
            }
        }

        return results;
    }

    @Override
    protected Properties getProducerProperties(final PropertyContext propertyContext, final Properties defaultProperties) {
        final Properties properties = super.getProducerProperties(propertyContext, defaultProperties);
        setAuthenticationProperties(properties, propertyContext);
        return properties;
    }

    @Override
    protected Properties getConsumerProperties(final PropertyContext propertyContext, final Properties defaultProperties) {
        final Properties properties = super.getConsumerProperties(propertyContext, defaultProperties);
        setAuthenticationProperties(properties, propertyContext);
        return properties;
    }

    @Override
    protected Properties getClientProperties(final PropertyContext propertyContext) {
        final Properties properties = super.getClientProperties(propertyContext);
        setAuthenticationProperties(properties, propertyContext);
        return properties;
    }

    private void setAuthenticationProperties(final Properties properties, final PropertyContext propertyContext) {
        final AwsRoleSource roleSource = propertyContext.getProperty(KafkaClientComponent.AWS_ROLE_SOURCE).asAllowableValue(AwsRoleSource.class);
        if (roleSource == AwsRoleSource.WEB_IDENTITY_TOKEN) {
            final AwsCredentialsProvider credentialsProvider = createWebIdentityCredentialsProvider(propertyContext);
            properties.put(AmazonMSKProperty.NIFI_AWS_MSK_CREDENTIALS_PROVIDER.getProperty(), credentialsProvider);
        }
    }

    private AwsCredentialsProvider createWebIdentityCredentialsProvider(final PropertyContext propertyContext) {
        final OAuth2AccessTokenProvider tokenProvider = propertyContext.getProperty(AWS_WEB_IDENTITY_TOKEN_PROVIDER).asControllerService(OAuth2AccessTokenProvider.class);
        if (tokenProvider == null) {
            throw new IllegalStateException("AWS Web Identity Token Provider is required when AWS Role Source is set to Web Identity Provider");
        }

        final String roleArn = propertyContext.getProperty(KafkaClientComponent.AWS_ASSUME_ROLE_ARN).getValue();
        final String roleSessionName = propertyContext.getProperty(KafkaClientComponent.AWS_ASSUME_ROLE_SESSION_NAME).getValue();
        final Long sessionTimeSeconds = propertyContext.getProperty(AWS_WEB_IDENTITY_SESSION_TIME).asTimePeriod(TimeUnit.SECONDS);
        final Integer sessionSeconds = sessionTimeSeconds == null ? null : sessionTimeSeconds.intValue();
        final String stsRegionId = propertyContext.getProperty(AWS_WEB_IDENTITY_STS_REGION).getValue();
        final String stsEndpoint = propertyContext.getProperty(AWS_WEB_IDENTITY_STS_ENDPOINT).getValue();
        final SSLContextProvider sslContextProvider = propertyContext.getProperty(AWS_WEB_IDENTITY_SSL_CONTEXT_PROVIDER).asControllerService(SSLContextProvider.class);

        final ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder();

        if (sslContextProvider != null) {
            final SSLContext sslContext = sslContextProvider.createContext();
            httpClientBuilder.socketFactory(new SSLConnectionSocketFactory(sslContext));
        }

        final StsClientBuilder stsClientBuilder = StsClient.builder().httpClient(httpClientBuilder.build());

        if (!StringUtils.isBlank(stsRegionId)) {
            stsClientBuilder.region(Region.of(stsRegionId));
        }

        if (!StringUtils.isBlank(stsEndpoint)) {
            stsClientBuilder.endpointOverride(URI.create(stsEndpoint));
        }

        final StsClient stsClient = stsClientBuilder.build();

        return new WebIdentityCredentialsProvider(stsClient, tokenProvider, roleArn, roleSessionName, sessionSeconds);
    }

    private static final class WebIdentityCredentialsProvider implements AwsCredentialsProvider, AutoCloseable {
        private static final Duration SKEW = Duration.ofSeconds(60);

        private final StsClient stsClient;
        private final OAuth2AccessTokenProvider tokenProvider;
        private final String roleArn;
        private final String roleSessionName;
        private final Integer sessionSeconds;

        private volatile AwsSessionCredentials cachedCredentials;
        private volatile Instant expiration;

        private WebIdentityCredentialsProvider(final StsClient stsClient,
                                               final OAuth2AccessTokenProvider tokenProvider,
                                               final String roleArn,
                                               final String roleSessionName,
                                               final Integer sessionSeconds) {
            this.stsClient = Objects.requireNonNull(stsClient, "stsClient required");
            this.tokenProvider = Objects.requireNonNull(tokenProvider, "tokenProvider required");
            this.roleArn = Objects.requireNonNull(roleArn, "roleArn required");
            this.roleSessionName = Objects.requireNonNull(roleSessionName, "roleSessionName required");
            this.sessionSeconds = sessionSeconds;
        }

        @Override
        public AwsCredentials resolveCredentials() {
            final Instant now = Instant.now();
            final AwsSessionCredentials currentCredentials = cachedCredentials;
            final Instant currentExpiration = expiration;

            AwsSessionCredentials resolvedCredentials;
            if (isCacheValid(now, currentCredentials, currentExpiration)) {
                resolvedCredentials = currentCredentials;
            } else {
                synchronized (this) {
                    final Instant refreshedNow = Instant.now();
                    if (isCacheValid(refreshedNow, cachedCredentials, expiration)) {
                        resolvedCredentials = cachedCredentials;
                    } else {
                        resolvedCredentials = refreshCredentials();
                    }
                }
            }

            return resolvedCredentials;
        }

        private String getWebIdentityToken() {
            final AccessToken accessToken = tokenProvider.getAccessDetails();
            if (accessToken == null) {
                throw new IllegalStateException("OAuth2AccessTokenProvider returned null AccessToken");
            }

            final Map<String, Object> additionalParameters = accessToken.getAdditionalParameters();
            String tokenValue = null;
            if (additionalParameters != null) {
                final Object idTokenValue = additionalParameters.get("id_token");
                if (idTokenValue instanceof String idToken) {
                    if (StringUtils.isBlank(idToken)) {
                        throw new IllegalStateException("OAuth2AccessTokenProvider returned an empty id_token");
                    }
                    tokenValue = idToken;
                }
            }

            if (tokenValue == null) {
                final String accessTokenValue = accessToken.getAccessToken();
                if (StringUtils.isBlank(accessTokenValue)) {
                    throw new IllegalStateException("No usable token found in AccessToken (id_token or access_token)");
                }
                tokenValue = accessTokenValue;
            }

            return tokenValue;
        }

        private boolean isCacheValid(final Instant referenceTime, final AwsSessionCredentials credentials, final Instant credentialsExpiration) {
            return credentials != null
                    && credentialsExpiration != null
                    && referenceTime.isBefore(credentialsExpiration.minus(SKEW));
        }

        private AwsSessionCredentials refreshCredentials() {
            final String webIdentityToken = getWebIdentityToken();

            final AssumeRoleWithWebIdentityRequest.Builder requestBuilder = AssumeRoleWithWebIdentityRequest.builder()
                    .roleArn(roleArn)
                    .roleSessionName(roleSessionName)
                    .webIdentityToken(webIdentityToken);

            if (sessionSeconds != null) {
                requestBuilder.durationSeconds(sessionSeconds);
            }

            final AssumeRoleWithWebIdentityResponse response = stsClient.assumeRoleWithWebIdentity(requestBuilder.build());
            final Credentials temporaryCredentials = response.credentials();
            final AwsSessionCredentials sessionCredentials = AwsSessionCredentials.create(
                    temporaryCredentials.accessKeyId(), temporaryCredentials.secretAccessKey(), temporaryCredentials.sessionToken());

            cachedCredentials = sessionCredentials;
            expiration = temporaryCredentials.expiration();
            return sessionCredentials;
        }

        @Override
        public void close() {
            stsClient.close();
        }
    }

    @Override
    public void migrateProperties(final PropertyConfiguration config) {
        // For backward compatibility: if an AWS Profile Name was configured previously,
        // set AWS Role Source to SPECIFIED_PROFILE
        if (config.isPropertySet(KafkaClientComponent.AWS_PROFILE_NAME)) {
            config.setProperty(KafkaClientComponent.AWS_ROLE_SOURCE, AwsRoleSource.SPECIFIED_PROFILE.name());
        }
    }
}
