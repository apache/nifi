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
package org.apache.nifi.processors.aws;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.http.conn.ssl.SdkTLSSocketFactory;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.proxy.ProxySpec;
import org.apache.nifi.ssl.SSLContextService;

import javax.net.ssl.SSLContext;
import java.net.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Base class for AWS processors that uses AWSCredentialsProvider interface for creating AWS clients.
 *
 * @param <ClientType> client type
 *
 * @see <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html">AWSCredentialsProvider</a>
 */
public abstract class AbstractAWSCredentialsProviderProcessor<ClientType extends AmazonWebServiceClient> extends AbstractProcessor
        implements VerifiableProcessor {

    private static final String CREDENTIALS_SERVICE_CLASSNAME = "org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService";

    public static final PropertyDescriptor PROXY_HOST = new PropertyDescriptor.Builder()
            .name("Proxy Host")
            .description("Proxy host name or IP")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROXY_HOST_PORT = new PropertyDescriptor.Builder()
            .name("Proxy Host Port")
            .description("Proxy host port")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(false)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROXY_USERNAME = new PropertyDescriptor.Builder()
            .name("proxy-user-name")
            .displayName("Proxy Username")
            .description("Proxy username")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROXY_PASSWORD = new PropertyDescriptor.Builder()
            .name("proxy-user-password")
            .displayName("Proxy Password")
            .description("Proxy password")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor REGION = new PropertyDescriptor.Builder()
            .name("Region")
            .description("The AWS Region to connect to.")
            .required(true)
            .allowableValues(getAvailableRegions())
            .defaultValue(createAllowableValue(Regions.DEFAULT_REGION).getValue())
            .build();

    public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
            .name("Communications Timeout")
            .description("The amount of time to wait in order to establish a connection to AWS or receive data from AWS before timing out.")
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


    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles are routed to this Relationship after they have been successfully processed.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If the Processor is unable to process a given FlowFile, it will be routed to this Relationship.")
            .build();

    public static final Set<Relationship> relationships = Set.of(REL_SUCCESS, REL_FAILURE);


    // Constants
    private static final ProxySpec[] PROXY_SPECS = {ProxySpec.HTTP_AUTH};
    public static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE = ProxyConfiguration.createProxyConfigPropertyDescriptor(true, PROXY_SPECS);


    // Member variables
    private final Cache<String, ClientType> clientCache = Caffeine.newBuilder()
            .maximumSize(10)
            .build();


    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        getClient(context);
    }

    @OnStopped
    public void onStopped() {
        this.clientCache.invalidateAll();
        this.clientCache.cleanUp();
    }


    public static AllowableValue createAllowableValue(final Regions region) {
        return new AllowableValue(region.getName(), region.getDescription(), "AWS Region Code : " + region.getName());
    }

    public static AllowableValue[] getAvailableRegions() {
        final List<AllowableValue> values = new ArrayList<>();
        for (final Regions region : Regions.values()) {
            values.add(createAllowableValue(region));
        }
        return values.toArray(new AllowableValue[0]);
    }


    @Override
    public void migrateProperties(final PropertyConfiguration config) {
        if (config.isPropertySet("Access Key") && config.isPropertySet("Secret Key")) {
            final String serviceId = config.createControllerService(CREDENTIALS_SERVICE_CLASSNAME, Map.of(
                "Access Key", config.getRawPropertyValue("Access Key").get(),
                "Secret Key", config.getRawPropertyValue("Secret Key").get()));
            config.setProperty(AWS_CREDENTIALS_PROVIDER_SERVICE.getName(), serviceId);
        } else if (config.isPropertySet("Credentials File")) {
            final String serviceId = config.createControllerService(CREDENTIALS_SERVICE_CLASSNAME, Map.of(
                "Credentials File", config.getRawPropertyValue("CredentialsFile").get()));
            config.setProperty(AWS_CREDENTIALS_PROVIDER_SERVICE, serviceId);
        } else if (!config.isPropertySet(AWS_CREDENTIALS_PROVIDER_SERVICE)) {
            final String serviceId = config.createControllerService(CREDENTIALS_SERVICE_CLASSNAME, Map.of(
                "default-credentials", "true"));
            config.setProperty(AWS_CREDENTIALS_PROVIDER_SERVICE, serviceId);
        }

        config.removeProperty("Access Key");
        config.removeProperty("Secret Key");
        config.removeProperty("Credentials File");
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(validationContext));

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


    protected ClientConfiguration createConfiguration(final ProcessContext context) {
        return createConfiguration(context, context.getMaxConcurrentTasks());
    }

    protected ClientConfiguration createConfiguration(final PropertyContext context, final int maxConcurrentTasks) {
        final ClientConfiguration config = new ClientConfiguration();
        config.setMaxConnections(maxConcurrentTasks);
        config.setMaxErrorRetry(0);
        config.setUserAgentPrefix("NiFi");
        config.setProtocol(Protocol.HTTPS);

        final int commsTimeout = context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        config.setConnectionTimeout(commsTimeout);
        config.setSocketTimeout(commsTimeout);

        if (this.getSupportedPropertyDescriptors().contains(SSL_CONTEXT_SERVICE)) {
            final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
            if (sslContextService != null) {
                final SSLContext sslContext = sslContextService.createContext();
                // NIFI-3788: Changed hostnameVerifier from null to DHV (BrowserCompatibleHostnameVerifier is deprecated)
                SdkTLSSocketFactory sdkTLSSocketFactory = new SdkTLSSocketFactory(sslContext, new DefaultHostnameVerifier());
                config.getApacheHttpClientConfig().setSslSocketFactory(sdkTLSSocketFactory);
            }
        }

        final ProxyConfiguration proxyConfig = ProxyConfiguration.getConfiguration(context, () -> {
            if (context.getProperty(PROXY_HOST).isSet()) {
                final ProxyConfiguration componentProxyConfig = new ProxyConfiguration();
                String proxyHost = context.getProperty(PROXY_HOST).evaluateAttributeExpressions().getValue();
                Integer proxyPort = context.getProperty(PROXY_HOST_PORT).evaluateAttributeExpressions().asInteger();
                String proxyUsername = context.getProperty(PROXY_USERNAME).evaluateAttributeExpressions().getValue();
                String proxyPassword = context.getProperty(PROXY_PASSWORD).evaluateAttributeExpressions().getValue();
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
            config.setProxyHost(proxyConfig.getProxyServerHost());
            config.setProxyPort(proxyConfig.getProxyServerPort());

            if (proxyConfig.hasCredential()) {
                config.setProxyUsername(proxyConfig.getProxyUserName());
                config.setProxyPassword(proxyConfig.getProxyUserPassword());
            }
        }

        return config;
    }


    protected ClientType createClient(final ProcessContext context) {
        return createClient(context, getRegion(context));
    }

    /**
     * Attempts to create the client using the controller service first before falling back to the standard configuration.
     * @param context The process context
     * @return The created client
     */
    public ClientType createClient(final ProcessContext context, final Region region) {
        getLogger().debug("Using AWS credentials provider service for creating client");
        final AWSCredentialsProvider credentialsProvider = getCredentialsProvider(context);
        final ClientConfiguration configuration = createConfiguration(context);
        final ClientType createdClient = createClient(context, credentialsProvider, region, configuration, getEndpointConfiguration(context, region));
        return createdClient;
    }

    protected AwsClientBuilder.EndpointConfiguration getEndpointConfiguration(final ProcessContext context, final Region region) {
        final PropertyValue overrideValue = context.getProperty(ENDPOINT_OVERRIDE);
        if (overrideValue == null || !overrideValue.isSet()) {
            return null;
        }

        final String endpointOverride = overrideValue.getValue();
        return new AwsClientBuilder.EndpointConfiguration(endpointOverride, region.getName());
    }



    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext context, final ComponentLog verificationLogger, final Map<String, String> attributes) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        try {
            createClient(context);

            results.add(new ConfigVerificationResult.Builder()
                    .outcome(Outcome.SUCCESSFUL)
                    .verificationStepName("Create Client and Configure Region")
                    .explanation("Successfully created AWS Client and configured Region")
                    .build());
        } catch (final Exception e) {
            verificationLogger.error("Failed to create AWS Client", e);
            results.add(new ConfigVerificationResult.Builder()
                    .outcome(Outcome.FAILED)
                    .verificationStepName("Create Client and Configure Region")
                    .explanation("Failed to crete AWS Client or configure Region: " + e.getMessage())
                    .build());
        }

        return results;
    }

    protected AWSCredentialsProvider getCredentialsProvider(final ProcessContext context) {
        final AWSCredentialsProviderService awsCredentialsProviderService = context.getProperty(AWS_CREDENTIALS_PROVIDER_SERVICE).asControllerService(AWSCredentialsProviderService.class);
        return awsCredentialsProviderService.getCredentialsProvider();
    }


    protected Region getRegion(final ProcessContext context) {
        // if the processor supports REGION, get the configured region.
        if (getSupportedPropertyDescriptors().contains(REGION)) {
            final String regionValue = context.getProperty(REGION).getValue();
            if (regionValue != null) {
                return Region.getRegion(Regions.fromName(regionValue));
            }
        }

        return null;
    }

    /**
     * Creates an AWS service client from the context or returns an existing client from the cache
     */
    protected ClientType getClient(final ProcessContext context, final Region region) {
        final String regionName = region == null ? "" : region.getName();
        return clientCache.get(regionName, ignored -> createClient(context, region));
    }

    protected ClientType getClient(final ProcessContext context) {
        return getClient(context, getRegion(context));
    }

    /**
     * Abstract method to create AWS client using credentials provider. This is the preferred method
     * for creating AWS clients
     * @param context process context
     * @param credentialsProvider AWS credentials provider
     * @param config AWS client configuration
     * @return ClientType the client
     */
    protected abstract ClientType createClient(ProcessContext context, AWSCredentialsProvider credentialsProvider, Region region, ClientConfiguration config,
                                               AwsClientBuilder.EndpointConfiguration endpointConfiguration);

}