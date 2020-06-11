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
package org.apache.nifi.toolkit.cli.impl.client;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.bucket.BucketItem;
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
import org.apache.nifi.registry.client.UserClient;
import org.apache.nifi.registry.client.impl.BucketItemDeserializer;
import org.apache.nifi.registry.security.util.ProxiedEntitiesUtils;
import org.apache.nifi.toolkit.cli.impl.client.registry.PoliciesClient;
import org.apache.nifi.toolkit.cli.impl.client.registry.TenantsClient;
import org.apache.nifi.toolkit.cli.impl.client.registry.impl.JerseyPoliciesClient;
import org.apache.nifi.toolkit.cli.impl.client.registry.impl.JerseyTenantsClient;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Decorator around (Jersey based) NiFiRegistryClient in order to extend it's capabilities without
 * actually changing it.
 */
public class JerseyExtendedNiFiRegistryClient implements ExtendedNiFiRegistryClient {
    // Copied from JerseyNiFiRegistryClient!
    static final String NIFI_REGISTRY_CONTEXT = "nifi-registry-api";
    static final int DEFAULT_CONNECT_TIMEOUT = 10000;
    static final int DEFAULT_READ_TIMEOUT = 10000;

    private final NiFiRegistryClient delegate;
    private final Client client;
    private final WebTarget baseTarget;
    private final TenantsClient tenantsClient;
    private final PoliciesClient policiesClient;

    public JerseyExtendedNiFiRegistryClient(final NiFiRegistryClient delegate, final NiFiRegistryClientConfig registryClientConfig) {
        this.delegate = delegate;

        // Copied from JerseyNiFiRegistryClient!
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

        this.tenantsClient = new JerseyTenantsClient(baseTarget);
        this.policiesClient = new JerseyPoliciesClient(baseTarget);
    }

    @Override
    public TenantsClient getTenantsClient() {
        return tenantsClient;
    }

    @Override
    public TenantsClient getTenantsClient(final String... proxiedEntity) {
        final Map<String,String> headers = getHeaders(proxiedEntity);
        return new JerseyTenantsClient(baseTarget, headers);
    }

    @Override
    public PoliciesClient getPoliciesClient() {
        return policiesClient;
    }

    @Override
    public PoliciesClient getPoliciesClient(final String... proxiedEntity) {
        final Map<String,String> headers = getHeaders(proxiedEntity);
        return new JerseyPoliciesClient(baseTarget, headers);
    }

    @Override
    public BucketClient getBucketClient() {
        return delegate.getBucketClient();
    }

    @Override
    public BucketClient getBucketClient(final String... proxiedEntity) {
        return delegate.getBucketClient(proxiedEntity);
    }

    @Override
    public FlowClient getFlowClient() {
        return delegate.getFlowClient();
    }

    @Override
    public FlowClient getFlowClient(final String... proxiedEntity) {
        return delegate.getFlowClient(proxiedEntity);
    }

    @Override
    public FlowSnapshotClient getFlowSnapshotClient() {
        return delegate.getFlowSnapshotClient();
    }

    @Override
    public FlowSnapshotClient getFlowSnapshotClient(final String... proxiedEntity) {
        return delegate.getFlowSnapshotClient(proxiedEntity);
    }

    @Override
    public ItemsClient getItemsClient() {
        return delegate.getItemsClient();
    }

    @Override
    public ItemsClient getItemsClient(final String... proxiedEntity) {
        return delegate.getItemsClient(proxiedEntity);
    }

    @Override
    public UserClient getUserClient() {
        return delegate.getUserClient();
    }

    @Override
    public UserClient getUserClient(final String... proxiedEntity) {
        return delegate.getUserClient(proxiedEntity);
    }

    @Override
    public BundleClient getBundleClient() {
        return delegate.getBundleClient();
    }

    @Override
    public BundleClient getBundleClient(final String... proxiedEntity) {
        return delegate.getBundleClient(proxiedEntity);
    }

    @Override
    public BundleVersionClient getBundleVersionClient() {
        return delegate.getBundleVersionClient();
    }

    @Override
    public BundleVersionClient getBundleVersionClient(final String... proxiedEntity) {
        return delegate.getBundleVersionClient(proxiedEntity);
    }

    @Override
    public ExtensionRepoClient getExtensionRepoClient() {
        return delegate.getExtensionRepoClient();
    }

    @Override
    public ExtensionRepoClient getExtensionRepoClient(final String... proxiedEntity) {
        return delegate.getExtensionRepoClient(proxiedEntity);
    }

    @Override
    public ExtensionClient getExtensionClient() {
        return delegate.getExtensionClient();
    }

    @Override
    public ExtensionClient getExtensionClient(final String... proxiedEntity) {
        return delegate.getExtensionClient(proxiedEntity);
    }

    @Override
    public void close() throws IOException {
        delegate.close();

        if (this.client != null) {
            try {
                this.client.close();
            } catch (Exception e) {

            }
        }
    }

    // Copied from JerseyNiFiRegistryClient!
    private Map<String,String> getHeaders(String[] proxiedEntities) {
        final String proxiedEntitiesValue = getProxiedEntitesValue(proxiedEntities);

        final Map<String,String> headers = new HashMap<>();
        if (proxiedEntitiesValue != null) {
            headers.put(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN, proxiedEntitiesValue);
        }
        return headers;
    }

    // Copied from JerseyNiFiRegistryClient!
    private String getProxiedEntitesValue(String[] proxiedEntities) {
        if (proxiedEntities == null) {
            return null;
        }

        final List<String> proxiedEntityChain = Arrays.stream(proxiedEntities).map(ProxiedEntitiesUtils::formatProxyDn).collect(Collectors.toList());
        return StringUtils.join(proxiedEntityChain, "");
    }

    // Copied from JerseyNiFiRegistryClient!
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
