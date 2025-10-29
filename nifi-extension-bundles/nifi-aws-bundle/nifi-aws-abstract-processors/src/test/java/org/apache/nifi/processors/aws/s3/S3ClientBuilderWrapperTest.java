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
package org.apache.nifi.processors.aws.s3;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.defaultsmode.DefaultsMode;
import software.amazon.awssdk.core.SdkPlugin;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.auth.spi.scheme.AuthScheme;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.encryption.s3.S3EncryptionClient;

import java.net.URI;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class S3ClientBuilderWrapperTest {

    private static final Boolean FLAG = Boolean.FALSE;

    @Mock
    private S3Configuration s3Configuration;

    @Mock
    private AwsCredentialsProvider credentialsProvider;

    @Mock
    private IdentityProvider<? extends AwsCredentialsIdentity> identityProvider;

    @Mock
    private Region region;

    @Mock
    private DefaultsMode defaultsMode;

    @Mock
    private ClientOverrideConfiguration overrideConfiguration;

    @Mock
    private Consumer<ClientOverrideConfiguration.Builder> overrideConfigurationBuilderConsumer;

    @Mock
    private URI endpointOverride;

    @Mock
    private AuthScheme<?> authScheme;

    @Mock
    private SdkPlugin plugin;

    @Mock
    private List<SdkPlugin> pluginList;

    @Mock
    private SdkHttpClient httpClient;

    @Mock
    private SdkHttpClient.Builder<?> httpClientBuilder;

    @Mock
    private S3Client s3Client;

    @Mock
    private S3EncryptionClient s3EncryptionClient;

    @Test
    void testS3ClientBuilder() {
        S3ClientBuilder builder = mock(S3ClientBuilder.class);

        S3ClientBuilderWrapper wrapper = new S3ClientBuilderWrapper(builder);

        lenient().when(builder.overrideConfiguration()).thenReturn(overrideConfiguration);
        when(builder.plugins()).thenReturn(pluginList);
        when(builder.build()).thenReturn(s3Client);

        wrapper.serviceConfiguration(s3Configuration);
        verify(builder).serviceConfiguration(same(s3Configuration));

        wrapper.credentialsProvider(credentialsProvider);
        verify(builder).credentialsProvider(same(credentialsProvider));

        wrapper.credentialsProvider(identityProvider);
        verify(builder).credentialsProvider(same(identityProvider));

        wrapper.region(region);
        verify(builder).region(same(region));

        wrapper.defaultsMode(defaultsMode);
        verify(builder).defaultsMode(same(defaultsMode));

        wrapper.dualstackEnabled(FLAG);
        verify(builder).dualstackEnabled(same(FLAG));

        wrapper.fipsEnabled(FLAG);
        verify(builder).fipsEnabled(same(FLAG));

        wrapper.overrideConfiguration(overrideConfiguration);
        verify(builder).overrideConfiguration(same(overrideConfiguration));

        wrapper.overrideConfiguration(overrideConfigurationBuilderConsumer);
        verify(builder).overrideConfiguration(same(overrideConfigurationBuilderConsumer));

        assertSame(overrideConfiguration, wrapper.overrideConfiguration());

        wrapper.endpointOverride(endpointOverride);
        verify(builder).endpointOverride(same(endpointOverride));

        wrapper.putAuthScheme(authScheme);
        verify(builder).putAuthScheme(same(authScheme));

        assertSame(pluginList, wrapper.plugins());

        wrapper.addPlugin(plugin);
        verify(builder).addPlugin(same(plugin));

        wrapper.httpClient(httpClient);
        verify(builder).httpClient(same(httpClient));

        wrapper.httpClientBuilder(httpClientBuilder);
        verify(builder).httpClientBuilder(same(httpClientBuilder));

        assertSame(s3Client, wrapper.build());
    }

    @Test
    void testS3EncryptionClientBuilder() {
        S3EncryptionClient.Builder builder = mock(S3EncryptionClient.Builder.class);

        S3ClientBuilderWrapper wrapper = new S3ClientBuilderWrapper(builder);

        lenient().when(builder.overrideConfiguration()).thenReturn(overrideConfiguration);
        when(builder.plugins()).thenReturn(pluginList);
        when(builder.build()).thenReturn(s3EncryptionClient);

        wrapper.serviceConfiguration(s3Configuration);
        verify(builder).serviceConfiguration(same(s3Configuration));

        wrapper.credentialsProvider(credentialsProvider);
        verify(builder).credentialsProvider(same(credentialsProvider));

        wrapper.credentialsProvider(identityProvider);
        verify(builder).credentialsProvider(same(identityProvider));

        wrapper.region(region);
        verify(builder).region(same(region));

        wrapper.defaultsMode(defaultsMode);
        verify(builder).defaultsMode(same(defaultsMode));

        wrapper.dualstackEnabled(FLAG);
        verify(builder).dualstackEnabled(same(FLAG));

        wrapper.fipsEnabled(FLAG);
        verify(builder).fipsEnabled(same(FLAG));

        wrapper.overrideConfiguration(overrideConfiguration);
        verify(builder).overrideConfiguration(same(overrideConfiguration));

        wrapper.overrideConfiguration(overrideConfigurationBuilderConsumer);
        verify(builder).overrideConfiguration(same(overrideConfigurationBuilderConsumer));

        assertSame(overrideConfiguration, wrapper.overrideConfiguration());

        wrapper.endpointOverride(endpointOverride);
        verify(builder).endpointOverride(same(endpointOverride));

        wrapper.putAuthScheme(authScheme);
        verify(builder).putAuthScheme(same(authScheme));

        assertSame(pluginList, wrapper.plugins());

        wrapper.addPlugin(plugin);
        verify(builder).addPlugin(same(plugin));

        wrapper.httpClient(httpClient);
        verify(builder).httpClient(same(httpClient));

        wrapper.httpClientBuilder(httpClientBuilder);
        verify(builder).httpClientBuilder(same(httpClientBuilder));

        assertSame(s3EncryptionClient, wrapper.build());
    }
}
