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

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder;
import software.amazon.awssdk.awscore.defaultsmode.DefaultsMode;
import software.amazon.awssdk.core.SdkPlugin;
import software.amazon.awssdk.core.client.builder.SdkSyncClientBuilder;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.auth.spi.scheme.AuthScheme;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3BaseClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.encryption.s3.S3EncryptionClient;

import java.net.URI;
import java.util.List;
import java.util.function.Consumer;

/**
 * Wrapper object around S3ClientBuilder and S3EncryptionClient.Builder in order to be able to define the client builder generic type in AbstractS3Processor.
 */
public class S3ClientBuilderWrapper implements AwsClientBuilder<S3ClientBuilderWrapper, S3Client>, AwsSyncClientBuilder<S3ClientBuilderWrapper, S3Client> {

    private final S3BaseClientBuilder<? extends S3BaseClientBuilder<?, ?>, ? extends S3Client> builder;

    S3ClientBuilderWrapper(S3ClientBuilder s3ClientBuilder) {
        this.builder = s3ClientBuilder;
    }

    S3ClientBuilderWrapper(S3EncryptionClient.Builder s3EncryptionClientBuilder) {
        this.builder = s3EncryptionClientBuilder;
    }

    S3ClientBuilderWrapper serviceConfiguration(S3Configuration serviceConfiguration) {
        builder.serviceConfiguration(serviceConfiguration);
        return this;
    }

    @Override
    public S3ClientBuilderWrapper credentialsProvider(AwsCredentialsProvider credentialsProvider) {
        builder.credentialsProvider(credentialsProvider);
        return this;
    }

    @Override
    public S3ClientBuilderWrapper credentialsProvider(IdentityProvider<? extends AwsCredentialsIdentity> credentialsProvider) {
        builder.credentialsProvider(credentialsProvider);
        return this;
    }

    @Override
    public S3ClientBuilderWrapper region(Region region) {
        builder.region(region);
        return this;
    }

    @Override
    public S3ClientBuilderWrapper defaultsMode(DefaultsMode defaultsMode) {
        builder.defaultsMode(defaultsMode);
        return this;
    }

    @Override
    public S3ClientBuilderWrapper dualstackEnabled(Boolean dualstackEndpointEnabled) {
        builder.dualstackEnabled(dualstackEndpointEnabled);
        return this;
    }

    @Override
    public S3ClientBuilderWrapper fipsEnabled(Boolean fipsEndpointEnabled) {
        builder.fipsEnabled(fipsEndpointEnabled);
        return this;
    }

    @Override
    public S3ClientBuilderWrapper overrideConfiguration(ClientOverrideConfiguration overrideConfiguration) {
        builder.overrideConfiguration(overrideConfiguration);
        return this;
    }

    @Override
    public S3ClientBuilderWrapper overrideConfiguration(Consumer<ClientOverrideConfiguration.Builder> overrideConfiguration) {
        builder.overrideConfiguration(overrideConfiguration);
        return this;
    }

    @Override
    public ClientOverrideConfiguration overrideConfiguration() {
        return builder.overrideConfiguration();
    }

    @Override
    public S3ClientBuilderWrapper endpointOverride(URI endpointOverride) {
        builder.endpointOverride(endpointOverride);
        return this;
    }

    @Override
    public S3ClientBuilderWrapper putAuthScheme(AuthScheme<?> authScheme) {
        builder.putAuthScheme(authScheme);
        return this;
    }

    @Override
    public List<SdkPlugin> plugins() {
        return builder.plugins();
    }

    @Override
    public S3ClientBuilderWrapper addPlugin(SdkPlugin plugin) {
        builder.addPlugin(plugin);
        return this;
    }

    @Override
    public S3ClientBuilderWrapper httpClient(SdkHttpClient httpClient) {
        ((SdkSyncClientBuilder<?, ?>) builder).httpClient(httpClient);
        return this;
    }

    @Override
    public S3ClientBuilderWrapper httpClientBuilder(SdkHttpClient.Builder httpClientBuilder) {
        ((SdkSyncClientBuilder<?, ?>) builder).httpClientBuilder(httpClientBuilder);
        return this;
    }

    @Override
    public S3Client build() {
        return builder.build();
    }
}
