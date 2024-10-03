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

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.proxy.ProxyConfiguration;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder;
import software.amazon.awssdk.core.SdkClient;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.TlsKeyManagersProvider;
import software.amazon.awssdk.http.TlsTrustManagersProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;

import java.net.URI;
import java.time.Duration;

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
    public T createClient(final ProcessContext context) {
        return createClient(context, getRegion(context));
    }

    public T createClient(final ProcessContext context, final Region region) {
        final U clientBuilder = createClientBuilder(context);
        this.configureClientBuilder(clientBuilder, region, context);
        return clientBuilder.build();
    }

    @Override
    protected <B extends AwsClientBuilder> void configureHttpClient(final B clientBuilder, final ProcessContext context) {
        ((AwsSyncClientBuilder) clientBuilder).httpClient(createSdkHttpClient(context));
    }

    private SdkHttpClient createSdkHttpClient(final ProcessContext context) {
        final ApacheHttpClient.Builder builder = ApacheHttpClient.builder();

        final AwsHttpClientConfigurer configurer = new AwsHttpClientConfigurer() {
            @Override
            public void configureBasicSettings(final Duration communicationsTimeout, final int maxConcurrentTasks) {
                builder.connectionTimeout(communicationsTimeout);
                builder.socketTimeout(communicationsTimeout);
                builder.maxConnections(context.getMaxConcurrentTasks());
            }

            @Override
            public void configureTls(final TlsTrustManagersProvider trustManagersProvider, final TlsKeyManagersProvider keyManagersProvider) {
                builder.tlsTrustManagersProvider(trustManagersProvider);
                builder.tlsKeyManagersProvider(keyManagersProvider);
            }

            @Override
            public void configureProxy(final ProxyConfiguration proxyConfiguration) {
                final software.amazon.awssdk.http.apache.ProxyConfiguration.Builder proxyConfigBuilder = software.amazon.awssdk.http.apache.ProxyConfiguration.builder()
                        .endpoint(URI.create(String.format("http://%s:%s", proxyConfiguration.getProxyServerHost(), proxyConfiguration.getProxyServerPort())));

                if (proxyConfiguration.hasCredential()) {
                    proxyConfigBuilder.username(proxyConfiguration.getProxyUserName());
                    proxyConfigBuilder.password(proxyConfiguration.getProxyUserPassword());
                }
                builder.proxyConfiguration(proxyConfigBuilder.build());
            }
        };

        this.configureSdkHttpClient(context, configurer);

        return builder.build();
    }

}