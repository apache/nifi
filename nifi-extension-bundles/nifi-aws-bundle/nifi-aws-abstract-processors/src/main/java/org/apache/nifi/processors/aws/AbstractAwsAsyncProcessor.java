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

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.proxy.ProxyConfiguration;
import software.amazon.awssdk.awscore.AwsClient;
import software.amazon.awssdk.awscore.client.builder.AwsAsyncClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.http.TlsKeyManagersProvider;
import software.amazon.awssdk.http.TlsTrustManagersProvider;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;

import java.time.Duration;

/**
 * Base class for aws async client processors using the AWS v2 SDK.
 *
 * @param <C> client type
 * @param <B> client builder type
 *
 * @see <a href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/auth/credentials/AwsCredentialsProvider.html">AwsCredentialsProvider</a>
 */
public abstract class AbstractAwsAsyncProcessor<
            C extends AwsClient,
            B extends AwsClientBuilder<B, C> & AwsAsyncClientBuilder<B, C>
        >
        extends AbstractAwsProcessor<C, B> {

    @Override
    protected void configureHttpClient(final AwsClientBuilder<?, ?> clientBuilder, final ProcessContext context) {
        ((AwsAsyncClientBuilder<?, ?>) clientBuilder).httpClient(createSdkAsyncHttpClient(clientBuilder, context));
    }

    private <B extends AwsClientBuilder<?, ?>> SdkAsyncHttpClient createSdkAsyncHttpClient(final B clientBuilder, final ProcessContext context) {
        final NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();

        final AwsHttpClientConfigurer configurer = new AwsHttpClientConfigurer() {
            @Override
            public void configureBasicSettings(final Duration communicationsTimeout, final int maxConcurrentTasks) {
                builder.connectionTimeout(communicationsTimeout);
                builder.readTimeout(communicationsTimeout);
                builder.maxConcurrency(maxConcurrentTasks);
            }

            @Override
            public void configureTls(final TlsTrustManagersProvider trustManagersProvider, final TlsKeyManagersProvider keyManagersProvider) {
                builder.tlsTrustManagersProvider(trustManagersProvider);
                builder.tlsKeyManagersProvider(keyManagersProvider);
            }

            @Override
            public void configureProxy(final ProxyConfiguration proxyConfiguration) {
                final software.amazon.awssdk.http.nio.netty.ProxyConfiguration.Builder proxyConfigBuilder = software.amazon.awssdk.http.nio.netty.ProxyConfiguration.builder()
                        .host(proxyConfiguration.getProxyServerHost())
                        .port(proxyConfiguration.getProxyServerPort());

                if (proxyConfiguration.hasCredential()) {
                    proxyConfigBuilder.username(proxyConfiguration.getProxyUserName());
                    proxyConfigBuilder.password(proxyConfiguration.getProxyUserPassword());
                }
                builder.proxyConfiguration(proxyConfigBuilder.build());
            }
        };

        this.configureSdkHttpClient(context, configurer);
        this.customizeAsyncHttpClientBuilderConfiguration(context, builder, clientBuilder.getClass());

        return builder.build();
    }

    /**
     * Customize the {@link NettyNioAsyncHttpClient.Builder} for the given {@link AwsClientBuilder} class.
     *
     * @param context the process context
     * @param builder the {@link NettyNioAsyncHttpClient.Builder} to customize
     * @param customizationTargetClass for which the HTTP client builder is being customized
     */
    protected void customizeAsyncHttpClientBuilderConfiguration(
            final ProcessContext context,
            final NettyNioAsyncHttpClient.Builder builder,
            final Class<? extends AwsClientBuilder> customizationTargetClass) {
        // no-op
    }

}
