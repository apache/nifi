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
package org.apache.nifi.toolkit.cli.impl.client.nifi.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.security.util.ProxiedEntitiesUtils;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ConnectionClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ControllerClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ControllerServicesClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.CountersClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.FlowClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.InputPortClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientConfig;
import org.apache.nifi.toolkit.cli.impl.client.nifi.OutputPortClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ParamContextClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.PoliciesClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ProcessGroupClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ProcessorClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ProvenanceClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.RemoteProcessGroupClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ReportingTasksClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.TemplatesClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.TenantsClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.VersionsClient;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJaxbJsonProvider;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Jersey implementation of NiFiClient.
 */
public class JerseyNiFiClient implements NiFiClient {

    static final String NIFI_CONTEXT = "nifi-api";
    static final int DEFAULT_CONNECT_TIMEOUT = 10000;
    static final int DEFAULT_READ_TIMEOUT = 10000;

    static final String AUTHORIZATION_HEADER = "Authorization";
    static final String BEARER = "Bearer";

    private final Client client;
    private final WebTarget baseTarget;

    private JerseyNiFiClient(final Builder builder) {
        final NiFiClientConfig clientConfig = builder.getConfig();
        if (clientConfig == null) {
            throw new IllegalArgumentException("NiFiClientConfig cannot be null");
        }

        String baseUrl = clientConfig.getBaseUrl();
        if (StringUtils.isBlank(baseUrl)) {
            throw new IllegalArgumentException("Base URL cannot be blank");
        }

        if (baseUrl.endsWith("/")) {
            baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
        }

        if (!baseUrl.endsWith(NIFI_CONTEXT)) {
            baseUrl = baseUrl + "/" + NIFI_CONTEXT;
        }

        try {
            new URI(baseUrl);
        } catch (final Exception e) {
            throw new IllegalArgumentException("Invalid base URL: " + e.getMessage(), e);
        }

        final SSLContext sslContext = clientConfig.getSslContext();
        final HostnameVerifier hostnameVerifier = clientConfig.getHostnameVerifier();

        final ClientBuilder clientBuilder = ClientBuilder.newBuilder();
        if (sslContext != null) {
            clientBuilder.sslContext(sslContext);
        }
        if (hostnameVerifier != null) {
            clientBuilder.hostnameVerifier(hostnameVerifier);
        }

        final int connectTimeout = clientConfig.getConnectTimeout() == null ? DEFAULT_CONNECT_TIMEOUT : clientConfig.getConnectTimeout();
        final int readTimeout = clientConfig.getReadTimeout() == null ? DEFAULT_READ_TIMEOUT : clientConfig.getReadTimeout();

        final ClientConfig jerseyClientConfig = new ClientConfig();
        jerseyClientConfig.property(ClientProperties.CONNECT_TIMEOUT, connectTimeout);
        jerseyClientConfig.property(ClientProperties.READ_TIMEOUT, readTimeout);
        jerseyClientConfig.register(jacksonJaxbJsonProvider());
        clientBuilder.withConfig(jerseyClientConfig);
        this.client = clientBuilder.build();

        this.baseTarget = client.target(baseUrl);
    }

    @Override
    public ControllerClient getControllerClient() {
        return new JerseyControllerClient(baseTarget);
    }

    @Override
    public ControllerClient getControllerClientForProxiedEntities(final String... proxiedEntity) {
        final Map<String,String> headers = getHeaders(proxiedEntity);
        return new JerseyControllerClient(baseTarget, headers);
    }

    @Override
    public ControllerClient getControllerClientForToken(final String base64token) {
        final Map<String,String> headers = getHeadersWithToken(base64token);
        return new JerseyControllerClient(baseTarget, headers);
    }

    @Override
    public ControllerServicesClient getControllerServicesClient() {
        return new JerseyControllerServicesClient(baseTarget);
    }

    @Override
    public ControllerServicesClient getControllerServicesClientForProxiedEntities(final String... proxiedEntity) {
        final Map<String, String> headers = getHeaders(proxiedEntity);
        return new JerseyControllerServicesClient(baseTarget, headers);
    }

    @Override
    public ControllerServicesClient getControllerServicesClientForToken(final String base64token) {
        final Map<String, String> headers = getHeadersWithToken(base64token);
        return new JerseyControllerServicesClient(baseTarget, headers);
    }

    @Override
    public FlowClient getFlowClient() {
        return new JerseyFlowClient(baseTarget);
    }

    @Override
    public FlowClient getFlowClientForProxiedEntities(String... proxiedEntity) {
        final Map<String,String> headers = getHeaders(proxiedEntity);
        return new JerseyFlowClient(baseTarget, headers);
    }

    @Override
    public FlowClient getFlowClientForToken(String base64token) {
        final Map<String,String> headers = getHeadersWithToken(base64token);
        return new JerseyFlowClient(baseTarget, headers);
    }

    @Override
    public ProcessGroupClient getProcessGroupClient() {
        return new JerseyProcessGroupClient(baseTarget);
    }

    @Override
    public ProcessGroupClient getProcessGroupClientForProxiedEntities(String... proxiedEntity) {
        final Map<String,String> headers = getHeaders(proxiedEntity);
        return new JerseyProcessGroupClient(baseTarget, headers);
    }

    @Override
    public ProcessGroupClient getProcessGroupClientForToken(String base64token) {
        final Map<String,String> headers = getHeadersWithToken(base64token);
        return new JerseyProcessGroupClient(baseTarget, headers);
    }

    @Override
    public ProcessorClient getProcessorClient() {
        return new JerseyProcessorClient(baseTarget);
    }

    @Override
    public ProcessorClient getProcessorClientForProxiedEntities(final String... proxiedEntity) {
        final Map<String,String> headers = getHeaders(proxiedEntity);
        return new JerseyProcessorClient(baseTarget, headers);
    }

    @Override
    public ProcessorClient getProcessorClientForToken(final String token) {
        final Map<String,String> headers = getHeadersWithToken(token);
        return new JerseyProcessorClient(baseTarget, headers);
    }

    @Override
    public VersionsClient getVersionsClient() {
        return new JerseyVersionsClient(baseTarget);
    }

    @Override
    public VersionsClient getVersionsClientForProxiedEntities(String... proxiedEntity) {
        final Map<String,String> headers = getHeaders(proxiedEntity);
        return new JerseyVersionsClient(baseTarget, headers);
    }

    @Override
    public VersionsClient getVersionsClientForToken(String base64token) {
        final Map<String,String> headers = getHeadersWithToken(base64token);
        return new JerseyVersionsClient(baseTarget, headers);
    }

    @Override
    public TenantsClient getTenantsClient() {
        return new JerseyTenantsClient(baseTarget);
    }

    @Override
    public TenantsClient getTenantsClientForProxiedEntities(String... proxiedEntity) {
        final Map<String, String> headers = getHeaders(proxiedEntity);
        return new JerseyTenantsClient(baseTarget, headers);
    }

    @Override
    public TenantsClient getTenantsClientForToken(String base64token) {
        final Map<String, String> headers = getHeadersWithToken(base64token);
        return new JerseyTenantsClient(baseTarget, headers);
    }

    @Override
    public PoliciesClient getPoliciesClient() {
        return new JerseyPoliciesClient(baseTarget);
    }

    @Override
    public PoliciesClient getPoliciesClientForProxiedEntities(String... proxiedEntity) {
        final Map<String, String> headers = getHeaders(proxiedEntity);
        return new JerseyPoliciesClient(baseTarget, headers);
    }

    @Override
    public PoliciesClient getPoliciesClientForToken(String base64token) {
        final Map<String, String> headers = getHeadersWithToken(base64token);
        return new JerseyPoliciesClient(baseTarget, headers);
    }

    @Override
    public TemplatesClient getTemplatesClient() {
        return new JerseyTemplatesClient(baseTarget);
    }

    @Override
    public TemplatesClient getTemplatesClientForProxiedEntities(String... proxiedEntity) {
        final Map<String, String> headers = getHeaders(proxiedEntity);
        return new JerseyTemplatesClient(baseTarget, headers);
    }

    @Override
    public TemplatesClient getTemplatesClientForToken(String base64token) {
        final Map<String, String> headers = getHeadersWithToken(base64token);
        return new JerseyTemplatesClient(baseTarget, headers);
    }

    @Override
    public ReportingTasksClient getReportingTasksClient() {
        return new JerseyReportingTasksClient(baseTarget);
    }

    @Override
    public ReportingTasksClient getReportingTasksClientForProxiedEntities(String... proxiedEntity) {
        final Map<String, String> headers = getHeaders(proxiedEntity);
        return new JerseyReportingTasksClient(baseTarget, headers);
    }

    @Override
    public ReportingTasksClient getReportingTasksClientForToken(String base64token) {
        final Map<String, String> headers = getHeadersWithToken(base64token);
        return new JerseyReportingTasksClient(baseTarget, headers);
    }

    @Override
    public ParamContextClient getParamContextClient() {
        return new JerseyParamContextClient(baseTarget);
    }

    @Override
    public ParamContextClient getParamContextClientForProxiedEntities(String... proxiedEntity) {
        final Map<String, String> headers = getHeaders(proxiedEntity);
        return new JerseyParamContextClient(baseTarget, headers);
    }

    @Override
    public ParamContextClient getParamContextClientForToken(String base64token) {
        final Map<String, String> headers = getHeadersWithToken(base64token);
        return new JerseyParamContextClient(baseTarget, headers);
    }

    @Override
    public CountersClient getCountersClient() {
        return new JerseyCountersClient(baseTarget);
    }

    @Override
    public CountersClient getCountersClientForProxiedEntities(final String... proxiedEntity) {
        final Map<String, String> headers = getHeaders(proxiedEntity);
        return new JerseyCountersClient(baseTarget, headers);
    }

    @Override
    public CountersClient getCountersClientForToken(final String token) {
        final Map<String, String> headers = getHeadersWithToken(token);
        return new JerseyCountersClient(baseTarget, headers);
    }

    @Override
    public ConnectionClient getConnectionClient() {
        return new JerseyConnectionClient(baseTarget);
    }

    @Override
    public ConnectionClient getConnectionClientForProxiedEntities(final String... proxiedEntity) {
        final Map<String, String> headers = getHeaders(proxiedEntity);
        return new JerseyConnectionClient(baseTarget, headers);
    }

    @Override
    public ConnectionClient getConnectionClientForToken(final String token) {
        final Map<String, String> headers = getHeadersWithToken(token);
        return new JerseyConnectionClient(baseTarget, headers);
    }

    @Override
    public RemoteProcessGroupClient getRemoteProcessGroupClient() {
        return new JerseyRemoteProcessGroupClient(baseTarget);
    }

    @Override
    public RemoteProcessGroupClient getRemoteProcessGroupClientForProxiedEntities(final String... proxiedEntity) {
        final Map<String, String> headers = getHeaders(proxiedEntity);
        return new JerseyRemoteProcessGroupClient(baseTarget, headers);
    }

    @Override
    public RemoteProcessGroupClient getRemoteProcessGroupClientForToken(final String token) {
        final Map<String, String> headers = getHeadersWithToken(token);
        return new JerseyRemoteProcessGroupClient(baseTarget, headers);
    }

    @Override
    public InputPortClient getInputPortClient() {
        return new JerseyInputPortClient(baseTarget);
    }

    @Override
    public InputPortClient getInputPortClientForProxiedEntities(final String... proxiedEntity) {
        final Map<String, String> headers = getHeaders(proxiedEntity);
        return new JerseyInputPortClient(baseTarget, headers);
    }

    @Override
    public InputPortClient getInputPortClientForToken(final String token) {
        final Map<String, String> headers = getHeadersWithToken(token);
        return new JerseyInputPortClient(baseTarget, headers);
    }

    @Override
    public OutputPortClient getOutputPortClient() {
        return new JerseyOutputPortClient(baseTarget);
    }

    @Override
    public OutputPortClient getOutputPortClientForProxiedEntities(final String... proxiedEntity) {
        final Map<String, String> headers = getHeaders(proxiedEntity);
        return new JerseyOutputPortClient(baseTarget, headers);
    }

    @Override
    public OutputPortClient getOutputPortClientForToken(final String token) {
        final Map<String, String> headers = getHeadersWithToken(token);
        return new JerseyOutputPortClient(baseTarget, headers);
    }

    @Override
    public ProvenanceClient getProvenanceClient() {
        return new JerseyProvenanceClient(baseTarget);
    }

    @Override
    public ProvenanceClient getProvenanceClientForProxiedEntities(final String... proxiedEntity) {
        final Map<String, String> headers = getHeaders(proxiedEntity);
        return new JerseyProvenanceClient(baseTarget, headers);
    }

    @Override
    public ProvenanceClient getProvenanceClientForToken(final String token) {
        final Map<String, String> headers = getHeadersWithToken(token);
        return new JerseyProvenanceClient(baseTarget, headers);
    }

    @Override
    public void close() {
        if (this.client != null) {
            try {
                this.client.close();
            } catch (Exception e) {

            }
        }
    }

    private Map<String,String> getHeadersWithToken(final String base64token) {
        if (StringUtils.isBlank(base64token)) {
            throw new IllegalArgumentException("Token cannot be null");
        }

        final Map<String,String> headers = new HashMap<>();
        headers.put(AUTHORIZATION_HEADER, BEARER + " " + base64token);
        return headers;
    }

    private Map<String,String> getHeaders(final String[] proxiedEntities) {
        final String proxiedEntitiesValue = getProxiedEntitesValue(proxiedEntities);

        final Map<String,String> headers = new HashMap<>();
        if (proxiedEntitiesValue != null) {
            headers.put(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN, proxiedEntitiesValue);
        }
        return headers;
    }

    private String getProxiedEntitesValue(final String[] proxiedEntities) {
        if (proxiedEntities == null) {
            return null;
        }

        final List<String> proxiedEntityChain = Arrays.stream(proxiedEntities)
                .map(ProxiedEntitiesUtils::formatProxyDn).collect(Collectors.toList());
        return StringUtils.join(proxiedEntityChain, "");
    }

    /**
     * Builder for creating a JerseyNiFiClient.
     */
    public static class Builder implements NiFiClient.Builder {

        private NiFiClientConfig clientConfig;

        @Override
        public JerseyNiFiClient.Builder config(final NiFiClientConfig clientConfig) {
            this.clientConfig = clientConfig;
            return this;
        }

        @Override
        public NiFiClientConfig getConfig() {
            return clientConfig;
        }

        @Override
        public NiFiClient build() {
            return new JerseyNiFiClient(this);
        }

    }

    private static JacksonJaxbJsonProvider jacksonJaxbJsonProvider() {
        JacksonJaxbJsonProvider jacksonJaxbJsonProvider = new JacksonJaxbJsonProvider();

        ObjectMapper mapper = new ObjectMapper();
        mapper.setDefaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.NON_NULL));
        mapper.setAnnotationIntrospector(new JaxbAnnotationIntrospector(mapper.getTypeFactory()));
        // Ignore unknown properties so that deployed client remain compatible with future versions of NiFi that add new fields
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        jacksonJaxbJsonProvider.setMapper(mapper);
        return jacksonJaxbJsonProvider;
    }
}
