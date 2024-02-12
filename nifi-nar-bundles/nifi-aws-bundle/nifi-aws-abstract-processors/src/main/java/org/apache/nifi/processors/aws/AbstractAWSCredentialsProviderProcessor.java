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
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.migration.ProxyServiceMigration;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.ssl.SSLContextService;

import javax.net.ssl.SSLContext;
import java.net.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.processors.aws.util.RegionUtilV1.REGION;

/**
 * Base class for AWS processors that uses AWSCredentialsProvider interface for creating AWS clients.
 *
 * @param <ClientType> client type
 *
 * @see <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html">AWSCredentialsProvider</a>
 */
public abstract class AbstractAWSCredentialsProviderProcessor<ClientType extends AmazonWebServiceClient> extends AbstractProcessor implements VerifiableProcessor {

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


    // Property Descriptors
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

    public static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE = new PropertyDescriptor.Builder()
        .name("proxy-configuration-service")
        .displayName("Proxy Configuration Service")
        .description("Specifies the Proxy Configuration Controller Service to proxy network requests.")
        .identifiesControllerService(ProxyConfigurationService.class)
        .required(false)
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
            if (context.getProperty(PROXY_CONFIGURATION_SERVICE).isSet()) {
                final ProxyConfigurationService configurationService = context.getProperty(PROXY_CONFIGURATION_SERVICE).asControllerService(ProxyConfigurationService.class);
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