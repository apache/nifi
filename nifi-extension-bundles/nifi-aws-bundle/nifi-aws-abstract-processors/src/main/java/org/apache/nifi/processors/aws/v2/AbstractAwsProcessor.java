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
package org.apache.nifi.processors.aws.v2;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.migration.ProxyServiceMigration;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.ssl.SSLContextService;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.core.SdkClient;
import software.amazon.awssdk.core.client.builder.SdkClientBuilder;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.FileStoreTlsKeyManagersProvider;
import software.amazon.awssdk.http.TlsKeyManagersProvider;
import software.amazon.awssdk.regions.Region;

import javax.net.ssl.TrustManager;
import java.net.Proxy;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Base class for aws processors using the AWS v2 SDK.
 *
 * @param <T> client type
 *
 * @see <a href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/auth/credentials/AwsCredentialsProvider.html">AwsCredentialsProvider</a>
 */
public abstract class AbstractAwsProcessor<T extends SdkClient> extends AbstractSessionFactoryProcessor implements VerifiableProcessor {
    private static final String CREDENTIALS_SERVICE_CLASSNAME = "org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService";

    // Obsolete property names
    private static final String OBSOLETE_ACCESS_KEY = "Access Key";
    private static final String OBSOLETE_SECRET_KEY = "Secret Key";
    private static final String OBSOLETE_CREDENTIALS_FILE = "Credentials File";
    private static final String OBSOLETE_PROXY_HOST = "Proxy Host";
    private static final String OBSOLETE_PROXY_PORT = "Proxy Host Port";
    private static final String OBSOLETE_PROXY_USERNAME = "proxy-user-name";
    private static final String OBSOLETE_PROXY_PASSWORD = "proxy-user-password";

    // Controller Service property names
    private static final String AUTH_SERVICE_ACCESS_KEY = "Access Key";
    private static final String AUTH_SERVICE_SECRET_KEY = "Secret Key";
    private static final String AUTH_SERVICE_CREDENTIALS_FILE = "Credentials File";
    private static final String AUTH_SERVICE_ANONYMOUS_CREDENTIALS = "anonymous-credentials";


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles are routed to success relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles are routed to failure relationship")
            .build();

    private static final Set<Relationship> relationships = Set.of(REL_SUCCESS, REL_FAILURE);

    public static final PropertyDescriptor REGION = new PropertyDescriptor.Builder()
            .name("Region")
            .required(true)
            .allowableValues(RegionUtilV2.getAvailableRegions())
            .defaultValue(RegionUtilV2.createAllowableValue(Region.US_WEST_2).getValue())
            .build();

    public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
            .name("Communications Timeout")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("30 secs")
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("Specifies an optional SSL Context Service that, if provided, will be used to create connections")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final PropertyDescriptor ENDPOINT_OVERRIDE = new PropertyDescriptor.Builder()
            .name("Endpoint Override URL")
            .description("Endpoint URL to use instead of the AWS default including scheme, host, port, and path. " +
                    "The AWS libraries select an endpoint URL based on the AWS region, but this property overrides " +
                    "the selected endpoint URL, allowing use with other S3-compatible endpoints.")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(false)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor AWS_CREDENTIALS_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
        .name("AWS Credentials Provider service")
        .displayName("AWS Credentials Provider Service")
        .description("The Controller Service that is used to obtain AWS credentials provider")
        .required(true)
        .identifiesControllerService(AWSCredentialsProviderService.class)
        .build();

    public static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE = new PropertyDescriptor.Builder()
        .name("proxy-configuration-service")
        .displayName("Proxy Configuration Service")
        .description("Specifies the Proxy Configuration Controller Service to proxy network requests.")
        .identifiesControllerService(ProxyConfigurationService.class)
        .required(false)
        .build();


    protected static final String DEFAULT_USER_AGENT = "NiFi";

    private final Cache<Region, T> clientCache = Caffeine.newBuilder().build();

    /**
     * Configure the http client on the builder.
     * @param <B> The builder type
     * @param clientBuilder The client builder
     * @param context The process context
     */
    protected abstract <B extends AwsClientBuilder> void configureHttpClient(B clientBuilder, ProcessContext context);

    /*
     * Allow optional override of onTrigger with the ProcessSessionFactory where required for AWS processors (e.g. ConsumeKinesisStream)
     *
     * @see AbstractProcessor
     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();
        try {
            onTrigger(context, session);
            session.commitAsync();
        } catch (final Throwable t) {
            session.rollback(true);
            throw t;
        }
    }

    /*
     * Default to requiring the "standard" onTrigger with a single ProcessSession
     */
    public abstract void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException;

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void migrateProperties(final PropertyConfiguration config) {
        migrateAuthenticationProperties(config);
        ProxyServiceMigration.migrateProxyProperties(config, PROXY_CONFIGURATION_SERVICE, OBSOLETE_PROXY_HOST, OBSOLETE_PROXY_PORT, OBSOLETE_PROXY_USERNAME, OBSOLETE_PROXY_PASSWORD);
    }

    private void migrateAuthenticationProperties(final PropertyConfiguration config) {
        if (!config.isPropertySet(AWS_CREDENTIALS_PROVIDER_SERVICE)) {
            if (config.isPropertySet(OBSOLETE_ACCESS_KEY) && config.isPropertySet(OBSOLETE_SECRET_KEY)) {
                final String serviceId = config.createControllerService(CREDENTIALS_SERVICE_CLASSNAME, Map.of(
                        AUTH_SERVICE_ACCESS_KEY, config.getRawPropertyValue(OBSOLETE_ACCESS_KEY).get(),
                        AUTH_SERVICE_SECRET_KEY, config.getRawPropertyValue(OBSOLETE_SECRET_KEY).get()));

                config.setProperty(AWS_CREDENTIALS_PROVIDER_SERVICE.getName(), serviceId);
            } else if (config.isPropertySet(OBSOLETE_CREDENTIALS_FILE)) {
                final String serviceId = config.createControllerService(CREDENTIALS_SERVICE_CLASSNAME, Map.of(
                        AUTH_SERVICE_CREDENTIALS_FILE, config.getRawPropertyValue(OBSOLETE_CREDENTIALS_FILE).get()));

                config.setProperty(AWS_CREDENTIALS_PROVIDER_SERVICE, serviceId);
            } else {
                final String serviceId = config.createControllerService(CREDENTIALS_SERVICE_CLASSNAME, Map.of(
                        AUTH_SERVICE_ANONYMOUS_CREDENTIALS, "true"));
                config.setProperty(AWS_CREDENTIALS_PROVIDER_SERVICE, serviceId);
            }
        }

        config.removeProperty(OBSOLETE_ACCESS_KEY);
        config.removeProperty(OBSOLETE_SECRET_KEY);
        config.removeProperty(OBSOLETE_CREDENTIALS_FILE);
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        getClient(context);
    }

    @OnStopped
    public void onStopped() {
        clientCache.asMap().values().forEach(SdkClient::close);
        clientCache.invalidateAll();
        clientCache.cleanUp();
    }

    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext context, final ComponentLog verificationLogger, final Map<String, String> attributes) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        try (final T ignored = createClient(context)) {
            results.add(new ConfigVerificationResult.Builder()
                    .outcome(Outcome.SUCCESSFUL)
                    .verificationStepName("Create Client")
                    .explanation("Successfully created AWS Client")
                    .build());
        } catch (final Exception e) {
            verificationLogger.error("Failed to create AWS Client", e);
            results.add(new ConfigVerificationResult.Builder()
                    .outcome(Outcome.FAILED)
                    .verificationStepName("Create Client")
                    .explanation("Failed to crete AWS Client: " + e.getMessage())
                    .build());
        }

        return results;
    }

    /**
     * Creates an AWS service client from the context or returns an existing client from the cache
     * @return The created or cached client
     */
    protected T getClient(final ProcessContext context, final Region region) {
        return clientCache.get(region, ignored -> createClient(context, region));
    }

    protected T getClient(final ProcessContext context) {
        return getClient(context, getRegion(context));
    }

    protected <C extends SdkClient, B extends AwsClientBuilder<B, C>>
    void configureClientBuilder(final B clientBuilder, final Region region, final ProcessContext context) {
        configureClientBuilder(clientBuilder, region, context, ENDPOINT_OVERRIDE);
    }

    protected <C extends SdkClient, B extends AwsClientBuilder<B, C>>
    void configureClientBuilder(final B clientBuilder, final Region region, final ProcessContext context, final PropertyDescriptor endpointOverrideDescriptor) {
        clientBuilder.overrideConfiguration(builder -> builder.putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX, DEFAULT_USER_AGENT));
        clientBuilder.overrideConfiguration(builder -> builder.retryPolicy(RetryPolicy.none()));
        this.configureHttpClient(clientBuilder, context);

        if (region != null) {
            clientBuilder.region(region);
        }
        configureEndpoint(context, clientBuilder, endpointOverrideDescriptor);

        final AwsCredentialsProvider credentialsProvider = getCredentialsProvider(context);
        clientBuilder.credentialsProvider(credentialsProvider);
    }

    protected void configureSdkHttpClient(final ProcessContext context, final AwsHttpClientConfigurer httpClientConfigurer) {
        final int communicationsTimeout = context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        httpClientConfigurer.configureBasicSettings(Duration.ofMillis(communicationsTimeout), context.getMaxConcurrentTasks());

        if (this.getSupportedPropertyDescriptors().contains(SSL_CONTEXT_SERVICE)) {
            final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
            if (sslContextService != null) {
                final TrustManager[] trustManagers = new TrustManager[] { sslContextService.createTrustManager() };
                final TlsKeyManagersProvider keyManagersProvider = FileStoreTlsKeyManagersProvider
                        .create(Path.of(sslContextService.getKeyStoreFile()), sslContextService.getKeyStoreType(), sslContextService.getKeyStorePassword());
                httpClientConfigurer.configureTls(trustManagers, keyManagersProvider);
            }
        }

        final ProxyConfiguration proxyConfig = ProxyConfiguration.getConfiguration(context, () -> {
            if (context.getProperty(ProxyConfigurationService.PROXY_CONFIGURATION_SERVICE).isSet()) {
                final ProxyConfigurationService configurationService = context.getProperty(ProxyConfigurationService.PROXY_CONFIGURATION_SERVICE).asControllerService(ProxyConfigurationService.class);
                return configurationService.getConfiguration();
            }
            return ProxyConfiguration.DIRECT_CONFIGURATION;
        });

        if (Proxy.Type.HTTP.equals(proxyConfig.getProxyType())) {
            httpClientConfigurer.configureProxy(proxyConfig);
        }
    }

    protected Region getRegion(final ProcessContext context) {
        final Region region;
        // if the processor supports REGION, get the configured region.
        if (getSupportedPropertyDescriptors().contains(REGION)) {
            final String regionValue = context.getProperty(REGION).getValue();
            if (regionValue != null) {
                region = Region.of(regionValue);
            } else {
                region = null;
            }
        } else {
            region = null;
        }
        return region;
    }

    protected void configureEndpoint(final ProcessContext context, final SdkClientBuilder<?, ?> clientBuilder, final PropertyDescriptor endpointOverrideDescriptor) {
        // if the endpoint override has been configured, set the endpoint.
        // (per Amazon docs this should only be configured at client creation)
        if (endpointOverrideDescriptor != null && getSupportedPropertyDescriptors().contains(endpointOverrideDescriptor)) {
            final String endpointOverride = StringUtils.trimToEmpty(context.getProperty(endpointOverrideDescriptor).evaluateAttributeExpressions().getValue());

            if (!endpointOverride.isEmpty()) {
                getLogger().info("Overriding endpoint with {}", endpointOverride);

                clientBuilder.endpointOverride(URI.create(endpointOverride));
            }
        }
    }


    /**
     * Get credentials provider using the {@link AwsCredentialsProvider}
     * @param context the process context
     * @return AwsCredentialsProvider the credential provider
     */
    protected AwsCredentialsProvider getCredentialsProvider(final ProcessContext context) {
        final AWSCredentialsProviderService awsCredentialsProviderService = context.getProperty(AWS_CREDENTIALS_PROVIDER_SERVICE).asControllerService(AWSCredentialsProviderService.class);
        return awsCredentialsProviderService.getAwsCredentialsProvider();
    }


    protected T createClient(final ProcessContext context) {
        return createClient(context, getRegion(context));
    }

    /**
     * Creates an AWS client using process context and AWS client details.
     *
     * @param context process context
     * @param region  the AWS Region
     * @return AWS client
     */
    protected abstract T createClient(final ProcessContext context, final Region region);

}