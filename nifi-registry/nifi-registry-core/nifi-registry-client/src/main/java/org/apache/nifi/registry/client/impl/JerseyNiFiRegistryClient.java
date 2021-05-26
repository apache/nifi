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
package org.apache.nifi.registry.client.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.bucket.BucketItem;
import org.apache.nifi.registry.client.AccessClient;
import org.apache.nifi.registry.client.BucketClient;
import org.apache.nifi.registry.client.BundleClient;
import org.apache.nifi.registry.client.BundleVersionClient;
import org.apache.nifi.registry.client.ExtensionClient;
import org.apache.nifi.registry.client.ExtensionRepoClient;
import org.apache.nifi.registry.client.FlowClient;
import org.apache.nifi.registry.client.FlowSnapshotClient;
import org.apache.nifi.registry.client.ItemsClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryClientConfig;
import org.apache.nifi.registry.client.PoliciesClient;
import org.apache.nifi.registry.client.RequestConfig;
import org.apache.nifi.registry.client.TenantsClient;
import org.apache.nifi.registry.client.UserClient;
import org.apache.nifi.registry.client.impl.request.ProxiedEntityRequestConfig;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.RequestEntityProcessing;
import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import java.io.IOException;
import java.net.URI;

/**
 * A NiFiRegistryClient that uses Jersey Client.
 */
public class JerseyNiFiRegistryClient implements NiFiRegistryClient {

    static final String NIFI_REGISTRY_CONTEXT = "nifi-registry-api";
    static final int DEFAULT_CONNECT_TIMEOUT = 10000;
    static final int DEFAULT_READ_TIMEOUT = 10000;

    private final Client client;
    private final WebTarget baseTarget;

    private final BucketClient bucketClient;
    private final FlowClient flowClient;
    private final FlowSnapshotClient flowSnapshotClient;
    private final ItemsClient itemsClient;

    private JerseyNiFiRegistryClient(final NiFiRegistryClient.Builder builder) {
        final NiFiRegistryClientConfig registryClientConfig = builder.getConfig();
        if (registryClientConfig == null) {
            throw new IllegalArgumentException("NiFiRegistryClientConfig cannot be null");
        }

        String baseUrl = registryClientConfig.getBaseUrl();
        if (StringUtils.isBlank(baseUrl)) {
            throw new IllegalArgumentException("Base URL cannot be blank");
        }

        if (baseUrl.endsWith("/")) {
            baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
        }

        if (!baseUrl.endsWith(NIFI_REGISTRY_CONTEXT)) {
            baseUrl = baseUrl + "/" + NIFI_REGISTRY_CONTEXT;
        }

        try {
            new URI(baseUrl);
        } catch (final Exception e) {
            throw new IllegalArgumentException("Invalid base URL: " + e.getMessage(), e);
        }

        final SSLContext sslContext = registryClientConfig.getSslContext();
        final HostnameVerifier hostnameVerifier = registryClientConfig.getHostnameVerifier();

        final ClientBuilder clientBuilder = ClientBuilder.newBuilder();
        if (sslContext != null) {
            clientBuilder.sslContext(sslContext);
        }
        if (hostnameVerifier != null) {
            clientBuilder.hostnameVerifier(hostnameVerifier);
        }

        final int connectTimeout = registryClientConfig.getConnectTimeout() == null ? DEFAULT_CONNECT_TIMEOUT : registryClientConfig.getConnectTimeout();
        final int readTimeout = registryClientConfig.getReadTimeout() == null ? DEFAULT_READ_TIMEOUT : registryClientConfig.getReadTimeout();

        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.property(ClientProperties.CONNECT_TIMEOUT, connectTimeout);
        clientConfig.property(ClientProperties.READ_TIMEOUT, readTimeout);
        clientConfig.property(ClientProperties.REQUEST_ENTITY_PROCESSING, RequestEntityProcessing.CHUNKED);
        clientConfig.register(jacksonJaxbJsonProvider());
        clientBuilder.withConfig(clientConfig);

        this.client = clientBuilder
                .register(MultiPartFeature.class)
                .build();

        this.baseTarget = client.target(baseUrl);
        this.bucketClient = new JerseyBucketClient(baseTarget);
        this.flowClient = new JerseyFlowClient(baseTarget);
        this.flowSnapshotClient = new JerseyFlowSnapshotClient(baseTarget);
        this.itemsClient = new JerseyItemsClient(baseTarget);
    }

    @Override
    public BucketClient getBucketClient() {
        return this.bucketClient;
    }

    @Override
    public BucketClient getBucketClient(String... proxiedEntity) {
        final RequestConfig requestConfig = new ProxiedEntityRequestConfig(proxiedEntity);
        return new JerseyBucketClient(baseTarget, requestConfig);
    }

    @Override
    public BucketClient getBucketClient(RequestConfig requestConfig) {
        return new JerseyBucketClient(baseTarget, requestConfig);
    }

    @Override
    public FlowClient getFlowClient() {
        return this.flowClient;
    }

    @Override
    public FlowClient getFlowClient(String... proxiedEntity) {
        final RequestConfig requestConfig = new ProxiedEntityRequestConfig(proxiedEntity);
        return new JerseyFlowClient(baseTarget, requestConfig);
    }

    @Override
    public FlowClient getFlowClient(RequestConfig requestConfig) {
        return new JerseyFlowClient(baseTarget, requestConfig);
    }

    @Override
    public FlowSnapshotClient getFlowSnapshotClient() {
        return this.flowSnapshotClient;
    }

    @Override
    public FlowSnapshotClient getFlowSnapshotClient(String... proxiedEntity) {
        final RequestConfig requestConfig = new ProxiedEntityRequestConfig(proxiedEntity);
        return new JerseyFlowSnapshotClient(baseTarget, requestConfig);
    }

    @Override
    public FlowSnapshotClient getFlowSnapshotClient(RequestConfig requestConfig) {
        return new JerseyFlowSnapshotClient(baseTarget, requestConfig);
    }

    @Override
    public ItemsClient getItemsClient() {
        return this.itemsClient;
    }

    @Override
    public ItemsClient getItemsClient(String... proxiedEntity) {
        final RequestConfig requestConfig = new ProxiedEntityRequestConfig(proxiedEntity);
        return new JerseyItemsClient(baseTarget, requestConfig);
    }

    @Override
    public ItemsClient getItemsClient(RequestConfig requestConfig) {
        return new JerseyItemsClient(baseTarget, requestConfig);
    }

    @Override
    public UserClient getUserClient() {
        return new JerseyUserClient(baseTarget);
    }

    @Override
    public UserClient getUserClient(String... proxiedEntity) {
        final RequestConfig requestConfig = new ProxiedEntityRequestConfig(proxiedEntity);
        return new JerseyUserClient(baseTarget, requestConfig);
    }

    @Override
    public UserClient getUserClient(RequestConfig requestConfig) {
        return new JerseyUserClient(baseTarget, requestConfig);
    }

    @Override
    public BundleClient getBundleClient() {
        return new JerseyBundleClient(baseTarget);
    }

    @Override
    public BundleClient getBundleClient(String... proxiedEntity) {
        final RequestConfig requestConfig = new ProxiedEntityRequestConfig(proxiedEntity);
        return new JerseyBundleClient(baseTarget, requestConfig);
    }

    @Override
    public BundleClient getBundleClient(RequestConfig requestConfig) {
        return new JerseyBundleClient(baseTarget, requestConfig);
    }

    @Override
    public BundleVersionClient getBundleVersionClient() {
        return new JerseyBundleVersionClient(baseTarget);
    }

    @Override
    public BundleVersionClient getBundleVersionClient(String... proxiedEntity) {
        final RequestConfig requestConfig = new ProxiedEntityRequestConfig(proxiedEntity);
        return new JerseyBundleVersionClient(baseTarget, requestConfig);
    }

    @Override
    public BundleVersionClient getBundleVersionClient(RequestConfig requestConfig) {
        return new JerseyBundleVersionClient(baseTarget, requestConfig);
    }

    @Override
    public ExtensionRepoClient getExtensionRepoClient() {
        return new JerseyExtensionRepoClient(baseTarget);
    }

    @Override
    public ExtensionRepoClient getExtensionRepoClient(String... proxiedEntity) {
        final RequestConfig requestConfig = new ProxiedEntityRequestConfig(proxiedEntity);
        return new JerseyExtensionRepoClient(baseTarget, requestConfig);
    }

    @Override
    public ExtensionRepoClient getExtensionRepoClient(RequestConfig requestConfig) {
        return new JerseyExtensionRepoClient(baseTarget, requestConfig);
    }

    @Override
    public ExtensionClient getExtensionClient() {
        return new JerseyExtensionClient(baseTarget);
    }

    @Override
    public ExtensionClient getExtensionClient(String... proxiedEntity) {
        final RequestConfig requestConfig = new ProxiedEntityRequestConfig(proxiedEntity);
        return new JerseyExtensionClient(baseTarget, requestConfig);
    }

    @Override
    public ExtensionClient getExtensionClient(RequestConfig requestConfig) {
        return new JerseyExtensionClient(baseTarget, requestConfig);
    }

    @Override
    public TenantsClient getTenantsClient() {
        return new JerseyTenantsClient(baseTarget);
    }

    @Override
    public TenantsClient getTenantsClient(String... proxiedEntity) {
        final RequestConfig requestConfig = new ProxiedEntityRequestConfig(proxiedEntity);
        return new JerseyTenantsClient(baseTarget, requestConfig);
    }

    @Override
    public TenantsClient getTenantsClient(RequestConfig requestConfig) {
        return new JerseyTenantsClient(baseTarget, requestConfig);
    }

    @Override
    public PoliciesClient getPoliciesClient() {
        return new JerseyPoliciesClient(baseTarget);
    }

    @Override
    public PoliciesClient getPoliciesClient(String... proxiedEntity) {
        final RequestConfig requestConfig = new ProxiedEntityRequestConfig(proxiedEntity);
        return new JerseyPoliciesClient(baseTarget, requestConfig);
    }

    @Override
    public PoliciesClient getPoliciesClient(RequestConfig requestConfig) {
        return new JerseyPoliciesClient(baseTarget, requestConfig);
    }

    @Override
    public AccessClient getAccessClient() {
        return new JerseyAccessClient(baseTarget);
    }

    @Override
    public void close() throws IOException {
        if (this.client != null) {
            try {
                this.client.close();
            } catch (Exception e) {

            }
        }
    }

    /**
     * Builder for creating a JerseyNiFiRegistryClient.
     */
    public static class Builder implements NiFiRegistryClient.Builder {

        private NiFiRegistryClientConfig clientConfig;

        @Override
        public Builder config(final NiFiRegistryClientConfig clientConfig) {
            this.clientConfig = clientConfig;
            return this;
        }

        @Override
        public NiFiRegistryClientConfig getConfig() {
            return clientConfig;
        }

        @Override
        public NiFiRegistryClient build() {
            return new JerseyNiFiRegistryClient(this);
        }

    }

    private static JacksonJaxbJsonProvider jacksonJaxbJsonProvider() {
        JacksonJaxbJsonProvider jacksonJaxbJsonProvider = new JacksonJaxbJsonProvider();

        ObjectMapper mapper = new ObjectMapper();
        mapper.setPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.NON_NULL));
        mapper.setAnnotationIntrospector(new JaxbAnnotationIntrospector(mapper.getTypeFactory()));
        // Ignore unknown properties so that deployed client remain compatible with future versions of NiFi Registry that add new fields
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        SimpleModule module = new SimpleModule();
        module.addDeserializer(BucketItem[].class, new BucketItemDeserializer());
        mapper.registerModule(module);

        jacksonJaxbJsonProvider.setMapper(mapper);
        return jacksonJaxbJsonProvider;
    }
}
