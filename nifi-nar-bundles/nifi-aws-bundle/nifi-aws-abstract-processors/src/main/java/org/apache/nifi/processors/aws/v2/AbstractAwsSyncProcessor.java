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
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.ssl.SSLContextService;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
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
import java.net.Proxy;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Base class for aws sync client processors using the AWS v2 SDK.
 *
 * @param <T> client type
 * @param <U> client builder type
 *
 * @see <a href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/auth/credentials/AwsCredentialsProvider.html">AwsCredentialsProvider</a>
 */
public abstract class AbstractAwsSyncProcessor<
            T extends SdkClient,
            U extends AwsSyncClientBuilder<U, T> & AwsClientBuilder<U, T>
        >
        extends AbstractAwsProcessor<T> {

    /**
     * Construct the AWS SDK client builder and perform any service-specific configuration of the builder.
     * @param context The process context
     * @return The SDK client builder
     */
    protected abstract U createClientBuilder(final ProcessContext context);

    /**
     * Creates the AWS SDK client.
     * @param context The process context
     * @return The created client
     */
    @Override
    public T createClient(final ProcessContext context) {
        final U clientBuilder = createClientBuilder(context);
        this.configureClientBuilder(clientBuilder, context);
        return clientBuilder.build();
    }

    protected void configureClientBuilder(final U clientBuilder, final ProcessContext context) {
        clientBuilder.overrideConfiguration(builder -> builder.putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX, DEFAULT_USER_AGENT));
        clientBuilder.overrideConfiguration(builder -> builder.retryPolicy(RetryPolicy.none()));
        clientBuilder.httpClient(createSdkHttpClient(context));

        final Region region = getRegion(context);
        if (region != null) {
            clientBuilder.region(region);
        }
        configureEndpoint(context, clientBuilder);

        final AwsCredentialsProvider credentialsProvider = getCredentialsProvider(context);
        clientBuilder.credentialsProvider(credentialsProvider);
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
                        .create(Path.of(sslContextService.getKeyStoreFile()), sslContextService.getKeyStoreType(), sslContextService.getKeyStorePassword());
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