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
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.impl.client.nifi.AccessClient;
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
import org.apache.nifi.toolkit.cli.impl.client.nifi.RequestConfig;
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

/**
 * Jersey implementation of NiFiClient.
 */
public class JerseyNiFiClient implements NiFiClient {

    static final String NIFI_CONTEXT = "nifi-api";
    static final int DEFAULT_CONNECT_TIMEOUT = 10000;
    static final int DEFAULT_READ_TIMEOUT = 10000;

    @VisibleForTesting
    public final Client client;
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
    public ControllerClient getControllerClient(RequestConfig requestConfig) {
        return new JerseyControllerClient(baseTarget, requestConfig);
    }

    @Override
    public ControllerServicesClient getControllerServicesClient() {
        return new JerseyControllerServicesClient(baseTarget);
    }

    @Override
    public ControllerServicesClient getControllerServicesClient(RequestConfig requestConfig) {
        return new JerseyControllerServicesClient(baseTarget, requestConfig);
    }

    @Override
    public FlowClient getFlowClient() {
        return new JerseyFlowClient(baseTarget);
    }

    @Override
    public FlowClient getFlowClient(RequestConfig requestConfig) {
        return new JerseyFlowClient(baseTarget, requestConfig);
    }

    @Override
    public ProcessGroupClient getProcessGroupClient() {
        return new JerseyProcessGroupClient(baseTarget);
    }

    @Override
    public ProcessGroupClient getProcessGroupClient(RequestConfig requestConfig) {
        return new JerseyProcessGroupClient(baseTarget, requestConfig);
    }

    @Override
    public ProcessorClient getProcessorClient() {
        return new JerseyProcessorClient(baseTarget);
    }

    @Override
    public ProcessorClient getProcessorClient(RequestConfig requestConfig) {
        return new JerseyProcessorClient(baseTarget, requestConfig);
    }

    @Override
    public VersionsClient getVersionsClient() {
        return new JerseyVersionsClient(baseTarget);
    }

    @Override
    public VersionsClient getVersionsClient(RequestConfig requestConfig) {
        return new JerseyVersionsClient(baseTarget, requestConfig);
    }

    @Override
    public TenantsClient getTenantsClient() {
        return new JerseyTenantsClient(baseTarget);
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
    public PoliciesClient getPoliciesClient(RequestConfig requestConfig) {
        return new JerseyPoliciesClient(baseTarget, requestConfig);
    }

    @Override
    public TemplatesClient getTemplatesClient() {
        return new JerseyTemplatesClient(baseTarget);
    }

    @Override
    public TemplatesClient getTemplatesClient(RequestConfig requestConfig) {
        return new JerseyTemplatesClient(baseTarget, requestConfig);
    }

    @Override
    public ReportingTasksClient getReportingTasksClient() {
        return new JerseyReportingTasksClient(baseTarget);
    }

    @Override
    public ReportingTasksClient getReportingTasksClient(RequestConfig requestConfig) {
        return new JerseyReportingTasksClient(baseTarget, requestConfig);
    }

    @Override
    public ParamContextClient getParamContextClient() {
        return new JerseyParamContextClient(baseTarget);
    }

    @Override
    public ParamContextClient getParamContextClient(RequestConfig requestConfig) {
        return new JerseyParamContextClient(baseTarget, requestConfig);
    }

    @Override
    public CountersClient getCountersClient() {
        return new JerseyCountersClient(baseTarget);
    }

    @Override
    public CountersClient getCountersClient(RequestConfig requestConfig) {
        return new JerseyCountersClient(baseTarget, requestConfig);
    }

    @Override
    public ConnectionClient getConnectionClient() {
        return new JerseyConnectionClient(baseTarget);
    }

    @Override
    public ConnectionClient getConnectionClient(RequestConfig requestConfig) {
        return new JerseyConnectionClient(baseTarget, requestConfig);
    }

    @Override
    public RemoteProcessGroupClient getRemoteProcessGroupClient() {
        return new JerseyRemoteProcessGroupClient(baseTarget);
    }

    @Override
    public RemoteProcessGroupClient getRemoteProcessGroupClient(RequestConfig requestConfig) {
        return new JerseyRemoteProcessGroupClient(baseTarget, requestConfig);
    }

    @Override
    public InputPortClient getInputPortClient() {
        return new JerseyInputPortClient(baseTarget);
    }

    @Override
    public InputPortClient getInputPortClient(RequestConfig requestConfig) {
        return new JerseyInputPortClient(baseTarget, requestConfig);
    }

    @Override
    public OutputPortClient getOutputPortClient() {
        return new JerseyOutputPortClient(baseTarget);
    }

    @Override
    public OutputPortClient getOutputPortClient(RequestConfig requestConfig) {
        return new JerseyOutputPortClient(baseTarget, requestConfig);
    }

    @Override
    public ProvenanceClient getProvenanceClient() {
        return new JerseyProvenanceClient(baseTarget);
    }

    @Override
    public ProvenanceClient getProvenanceClient(RequestConfig requestConfig) {
        return new JerseyProvenanceClient(baseTarget, requestConfig);
    }

    @Override
    public AccessClient getAccessClient() {
        return new JerseyAccessClient(baseTarget);
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
