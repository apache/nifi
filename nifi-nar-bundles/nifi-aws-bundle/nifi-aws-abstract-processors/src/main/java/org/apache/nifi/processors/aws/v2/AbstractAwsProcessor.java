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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.credentials.provider.PropertiesCredentialsProvider;
import org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.proxy.ProxySpec;
import org.apache.nifi.ssl.SSLContextService;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder;
import software.amazon.awssdk.core.SdkClient;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.FileStoreTlsKeyManagersProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.TlsKeyManagersProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;

import javax.net.ssl.TrustManager;
import java.io.File;
import java.net.Proxy;
import java.net.URI;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
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
public abstract class AbstractAwsProcessor<T extends SdkClient, U extends AwsSyncClientBuilder<U, T> & AwsClientBuilder<U, T>>
        extends AbstractProcessor implements VerifiableProcessor, AwsClientProvider<T> {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles are routed to success relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles are routed to failure relationship")
            .build();

    private static final Set<Relationship> relationships = Collections.unmodifiableSet(
            new LinkedHashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE))
    );

    public static final PropertyDescriptor CREDENTIALS_FILE = CredentialPropertyDescriptors.CREDENTIALS_FILE;

    public static final PropertyDescriptor ACCESS_KEY = CredentialPropertyDescriptors.ACCESS_KEY;

    public static final PropertyDescriptor SECRET_KEY = CredentialPropertyDescriptors.SECRET_KEY;

    public static final PropertyDescriptor PROXY_HOST = new PropertyDescriptor.Builder()
            .name("Proxy Host")
            .description("Proxy host name or IP")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROXY_HOST_PORT = new PropertyDescriptor.Builder()
            .name("Proxy Host Port")
            .description("Proxy host port")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROXY_USERNAME = new PropertyDescriptor.Builder()
            .name("proxy-user-name")
            .displayName("Proxy Username")
            .description("Proxy username")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROXY_PASSWORD = new PropertyDescriptor.Builder()
            .name("proxy-user-password")
            .displayName("Proxy Password")
            .description("Proxy password")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor REGION = new PropertyDescriptor.Builder()
            .name("Region")
            .required(true)
            .allowableValues(RegionUtil.getAvailableRegions())
            .defaultValue(RegionUtil.createAllowableValue(Region.US_WEST_2).getValue())
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
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    /**
     * AWS credentials provider service
     *
     * @see  <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html">AWSCredentialsProvider</a>
     * @see  <a href="https://sdk.amazonaws.com/java/api/2.0.0/software/amazon/awssdk/auth/credentials/AwsCredentialsProvider.html">AwsCredentialsProvider</a>
     */
    public static final PropertyDescriptor AWS_CREDENTIALS_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
            .name("AWS Credentials Provider service")
            .displayName("AWS Credentials Provider Service")
            .description("The Controller Service that is used to obtain AWS credentials provider")
            .required(false)
            .identifiesControllerService(AWSCredentialsProviderService.class)
            .build();

    protected static final String DEFAULT_USER_AGENT = "NiFi";

    private static final ProxySpec[] PROXY_SPECS = {ProxySpec.HTTP_AUTH};

    public static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE = ProxyConfiguration.createProxyConfigPropertyDescriptor(true, PROXY_SPECS);

    protected volatile T client;

    protected volatile Region region;

    private final AwsClientCache<T> awsClientCache = new AwsClientCache<>();

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(validationContext));

        final boolean accessKeySet = validationContext.getProperty(ACCESS_KEY).isSet();
        final boolean secretKeySet = validationContext.getProperty(SECRET_KEY).isSet();
        if ((accessKeySet && !secretKeySet) || (secretKeySet && !accessKeySet)) {
            validationResults.add(new ValidationResult.Builder().input("Access Key").valid(false).explanation("If setting Secret Key or Access Key, must set both").build());
        }

        final boolean credentialsFileSet = validationContext.getProperty(CREDENTIALS_FILE).isSet();
        if ((secretKeySet || accessKeySet) && credentialsFileSet) {
            validationResults.add(new ValidationResult.Builder().input("Access Key").valid(false).explanation("Cannot set both Credentials File and Secret Key/Access Key").build());
        }

        final boolean proxyHostSet = validationContext.getProperty(PROXY_HOST).isSet();
        final boolean proxyPortSet = validationContext.getProperty(PROXY_HOST_PORT).isSet();
        final boolean proxyConfigServiceSet = validationContext.getProperty(ProxyConfigurationService.PROXY_CONFIGURATION_SERVICE).isSet();

        if ((proxyHostSet && !proxyPortSet) || (!proxyHostSet && proxyPortSet)) {
            validationResults.add(new ValidationResult.Builder().subject("Proxy Host and Port").valid(false).explanation("If Proxy Host or Proxy Port is set, both must be set").build());
        }

        final boolean proxyUserSet = validationContext.getProperty(PROXY_USERNAME).isSet();
        final boolean proxyPwdSet = validationContext.getProperty(PROXY_PASSWORD).isSet();

        if ((proxyUserSet && !proxyPwdSet) || (!proxyUserSet && proxyPwdSet)) {
            validationResults.add(new ValidationResult.Builder().subject("Proxy User and Password").valid(false).explanation("If Proxy Username or Proxy Password is set, both must be set").build());
        }

        if (proxyUserSet && !proxyHostSet) {
            validationResults.add(new ValidationResult.Builder().subject("Proxy").valid(false).explanation("If Proxy Username or Proxy Password").build());
        }

        ProxyConfiguration.validateProxySpec(validationContext, validationResults, PROXY_SPECS);

        if (proxyHostSet && proxyConfigServiceSet) {
            validationResults.add(new ValidationResult.Builder().subject("Proxy Configuration Service").valid(false)
                    .explanation("Either Proxy Username and Proxy Password must be set or Proxy Configuration Service but not both").build());
        }

        return validationResults;
    }

    @OnShutdown
    public void onShutDown() {
        if (this.client != null) {
            this.client.close();
        }
    }

    @OnStopped
    public void onStopped() {
        this.awsClientCache.clearCache();
    }

    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext context, final ComponentLog verificationLogger, final Map<String, String> attributes) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        try {
            createClient(context, null);
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
     * Creates the AWS SDK client.
     * @param context The process context
     * @param flowFile the flowfile
     * @return The created client
     */
    @Override
    public T createClient(final ProcessContext context, final FlowFile flowFile) {
        final U clientBuilder = createClientBuilder(context);
        this.configureClientBuilder(clientBuilder, context, flowFile);
        return clientBuilder.build();
    }

    /**
     * Creates the AWS SDK client.
     * @param context The process context
     * @return The created client
     */
    @Override
    public T createClient(final ProcessContext context) {
        final U clientBuilder = createClientBuilder(context);
        this.configureClientBuilder(clientBuilder, context, null);
        return clientBuilder.build();
    }

    protected void configureClientBuilder(final U clientBuilder, final ProcessContext context, final FlowFile flowFile) {
        clientBuilder.overrideConfiguration(builder -> builder.putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX, DEFAULT_USER_AGENT));
        clientBuilder.overrideConfiguration(builder -> builder.retryPolicy(RetryPolicy.none()));
        clientBuilder.httpClient(createSdkHttpClient(context));
        final Region region = getRegion(context, flowFile);

        if (region != null) {
            clientBuilder.region(region);
        }
        configureEndpoint(context, clientBuilder);

        final AwsCredentialsProvider credentialsProvider = getCredentialsProvider(context);
        clientBuilder.credentialsProvider(credentialsProvider);
    }

    /**
    * Creates an AWS service client from the context or returns an existing client from the cache
    * @param context The process context
    * @param  awsClientDetails details of the AWS client
    * @param flowFile the flowfile
    * @return The created client
    */
    protected T getClient(final ProcessContext context, final AwsClientDetails awsClientDetails, FlowFile flowFile) {
         return this.awsClientCache.getOrCreateClient(context, awsClientDetails, this, flowFile);
    }
    protected T getClient(final ProcessContext context, FlowFile  flowFile) {
        final AwsClientDetails awsClientDetails = new AwsClientDetails(getRegion(context, flowFile));
        return getClient(context, awsClientDetails, flowFile);
    }

    protected T getClient(final ProcessContext context) {
        final AwsClientDetails awsClientDetails = new AwsClientDetails(getRegion(context, null));
        return getClient(context, awsClientDetails, null);
    }

    /**
     * Construct the AWS SDK client builder and perform any service-specific configuration of the builder.
     * @param context The process context
     * @return The SDK client builder
     */
    protected abstract U createClientBuilder(final ProcessContext context);

    protected Region getRegion(final ProcessContext context, final FlowFile flowfile) {
        final Region region;
        // if the processor supports REGION, get the configured region.
        if (getSupportedPropertyDescriptors().contains(REGION)) {
            final String regionValue = context.getProperty(REGION).evaluateAttributeExpressions(flowfile).getValue();
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

    protected void configureEndpoint(final ProcessContext context, final U clientBuilder) {
        // if the endpoint override has been configured, set the endpoint.
        // (per Amazon docs this should only be configured at client creation)
        if (getSupportedPropertyDescriptors().contains(ENDPOINT_OVERRIDE)) {
            final String endpointOverride = StringUtils.trimToEmpty(context.getProperty(ENDPOINT_OVERRIDE).evaluateAttributeExpressions().getValue());

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
        final AWSCredentialsProviderService awsCredentialsProviderService =
              context.getProperty(AWS_CREDENTIALS_PROVIDER_SERVICE).asControllerService(AWSCredentialsProviderService.class);

        return awsCredentialsProviderService != null ? awsCredentialsProviderService.getAwsCredentialsProvider() : createStaticCredentialsProvider(context);

    }

    protected AwsCredentialsProvider createStaticCredentialsProvider(final PropertyContext context) {
        final String accessKey = context.getProperty(ACCESS_KEY).evaluateAttributeExpressions().getValue();
        final String secretKey = context.getProperty(SECRET_KEY).evaluateAttributeExpressions().getValue();

        final String credentialsFile = context.getProperty(CREDENTIALS_FILE).getValue();

        if (credentialsFile != null) {
            return new PropertiesCredentialsProvider(new File(credentialsFile));
        }

        if (accessKey != null && secretKey != null) {
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
        }

        return AnonymousCredentialsProvider.create();
    }

    private SdkHttpClient createSdkHttpClient(final ProcessContext context) {
        final ApacheHttpClient.Builder builder = ApacheHttpClient.builder();

        final int communicationsTimeout = context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        builder.connectionTimeout(Duration.ofMillis(communicationsTimeout));
        builder.socketTimeout(Duration.ofMillis(communicationsTimeout));
        builder.maxConnections(context.getMaxConcurrentTasks());

        if (this.getSupportedPropertyDescriptors().contains(SSL_CONTEXT_SERVICE)) {
            final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
            if (sslContextService != null) {
                final TrustManager[] trustManagers = new TrustManager[] { sslContextService.createTrustManager() };
                final TlsKeyManagersProvider keyManagersProvider = FileStoreTlsKeyManagersProvider
                        .create(Paths.get(sslContextService.getKeyStoreFile()), sslContextService.getKeyStoreType(), sslContextService.getKeyStorePassword());
                builder.tlsTrustManagersProvider(() -> trustManagers);
                builder.tlsKeyManagersProvider(keyManagersProvider);
            }
        }

        final ProxyConfiguration proxyConfig = ProxyConfiguration.getConfiguration(context, () -> {
            if (context.getProperty(PROXY_HOST).isSet()) {
                final ProxyConfiguration componentProxyConfig = new ProxyConfiguration();
                final String proxyHost = context.getProperty(PROXY_HOST).evaluateAttributeExpressions().getValue();
                final Integer proxyPort = context.getProperty(PROXY_HOST_PORT).evaluateAttributeExpressions().asInteger();
                final String proxyUsername = context.getProperty(PROXY_USERNAME).evaluateAttributeExpressions().getValue();
                final String proxyPassword = context.getProperty(PROXY_PASSWORD).evaluateAttributeExpressions().getValue();
                componentProxyConfig.setProxyType(Proxy.Type.HTTP);
                componentProxyConfig.setProxyServerHost(proxyHost);
                componentProxyConfig.setProxyServerPort(proxyPort);
                componentProxyConfig.setProxyUserName(proxyUsername);
                componentProxyConfig.setProxyUserPassword(proxyPassword);
                return componentProxyConfig;
            } else if (context.getProperty(ProxyConfigurationService.PROXY_CONFIGURATION_SERVICE).isSet()) {
                final ProxyConfigurationService configurationService = context.getProperty(ProxyConfigurationService.PROXY_CONFIGURATION_SERVICE).asControllerService(ProxyConfigurationService.class);
                return configurationService.getConfiguration();
            }
            return ProxyConfiguration.DIRECT_CONFIGURATION;
        });

        if (Proxy.Type.HTTP.equals(proxyConfig.getProxyType())) {
            final software.amazon.awssdk.http.apache.ProxyConfiguration.Builder proxyConfigBuilder = software.amazon.awssdk.http.apache.ProxyConfiguration.builder()
                    .endpoint(URI.create(String.format("%s:%s", proxyConfig.getProxyServerHost(), proxyConfig.getProxyServerPort())));

            if (proxyConfig.hasCredential()) {
                proxyConfigBuilder.username(proxyConfig.getProxyUserName());
                proxyConfigBuilder.password(proxyConfig.getProxyUserPassword());
            }
            builder.proxyConfiguration(proxyConfigBuilder.build());
        }

        return builder.build();
    }
}