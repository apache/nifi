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

import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.nifi.registry.security.util.KeystoreType;
import org.apache.nifi.toolkit.cli.api.ClientFactory;
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
import org.apache.nifi.toolkit.cli.impl.client.nifi.impl.JerseyNiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.impl.request.BasicAuthRequestConfig;
import org.apache.nifi.toolkit.cli.impl.client.nifi.impl.request.BearerTokenRequestConfig;
import org.apache.nifi.toolkit.cli.impl.client.nifi.impl.request.ProxiedEntityRequestConfig;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;

import java.io.IOException;
import java.util.Properties;

/**
 * Factory for obtaining an instance of NiFiClient from the given properties.
 */
public class NiFiClientFactory implements ClientFactory<NiFiClient> {

    @Override
    public NiFiClient createClient(final Properties properties) throws MissingOptionException {
        final String url = properties.getProperty(CommandOption.URL.getLongName());
        if (StringUtils.isBlank(url)) {
            throw new MissingOptionException("Missing required option '" + CommandOption.URL.getLongName() + "'");
        }

        final String connectionTimeout = properties.getProperty(CommandOption.CONNECTION_TIMEOUT.getLongName());
        final String readTimeout = properties.getProperty(CommandOption.READ_TIMEOUT.getLongName());

        final String keystore = properties.getProperty(CommandOption.KEYSTORE.getLongName());
        final String keystoreType = properties.getProperty(CommandOption.KEYSTORE_TYPE.getLongName());
        final String keystorePasswd = properties.getProperty(CommandOption.KEYSTORE_PASSWORD.getLongName());
        final String keyPasswd = properties.getProperty(CommandOption.KEY_PASSWORD.getLongName());

        final String truststore = properties.getProperty(CommandOption.TRUSTSTORE.getLongName());
        final String truststoreType = properties.getProperty(CommandOption.TRUSTSTORE_TYPE.getLongName());
        final String truststorePasswd = properties.getProperty(CommandOption.TRUSTSTORE_PASSWORD.getLongName());

        final String proxiedEntity = properties.getProperty(CommandOption.PROXIED_ENTITY.getLongName());
        final String protocol = properties.getProperty(CommandOption.PROTOCOL.getLongName());

        final String basicAuthUsername = properties.getProperty(CommandOption.BASIC_AUTH_USER.getLongName());
        final String basicAuthPassword = properties.getProperty(CommandOption.BASIC_AUTH_PASSWORD.getLongName());

        final String bearerToken = properties.getProperty(CommandOption.BEARER_TOKEN.getLongName());

        final boolean secureUrl = url.startsWith("https");

        if (secureUrl && (StringUtils.isBlank(truststore)
                || StringUtils.isBlank(truststoreType)
                || StringUtils.isBlank(truststorePasswd))
                ) {
            throw new MissingOptionException(CommandOption.TRUSTSTORE.getLongName() + ", " + CommandOption.TRUSTSTORE_TYPE.getLongName()
                    + ", and " + CommandOption.TRUSTSTORE_PASSWORD.getLongName() + " are required when using an https url");
        }

        if (!StringUtils.isBlank(proxiedEntity) && (!StringUtils.isBlank(basicAuthUsername) || !StringUtils.isBlank(basicAuthPassword))) {
            throw new IllegalStateException(CommandOption.PROXIED_ENTITY.getLongName() + " and basic authentication can not be used together");
        }

        if (!StringUtils.isBlank(proxiedEntity) && !StringUtils.isBlank(bearerToken)) {
            throw new IllegalStateException(CommandOption.PROXIED_ENTITY.getLongName() + " and "
                    + CommandOption.BEARER_TOKEN.getLongName() + " can not be used together");
        }

        if (!StringUtils.isBlank(bearerToken) && (!StringUtils.isBlank(basicAuthUsername) || !StringUtils.isBlank(basicAuthPassword))) {
            throw new IllegalStateException(CommandOption.BEARER_TOKEN.getLongName() + " and basic authentication can not be used together");
        }

        if (!StringUtils.isBlank(basicAuthUsername) && StringUtils.isBlank(basicAuthPassword)) {
            throw new MissingOptionException(CommandOption.BASIC_AUTH_PASSWORD.getLongName()
                    + " is required when specifying " + CommandOption.BASIC_AUTH_USER.getLongName());
        }

        if (!StringUtils.isBlank(basicAuthPassword) && StringUtils.isBlank(basicAuthUsername)) {
            throw new MissingOptionException(CommandOption.BASIC_AUTH_USER.getLongName()
                    + " is required when specifying " + CommandOption.BASIC_AUTH_PASSWORD.getLongName());
        }

        final NiFiClientConfig.Builder clientConfigBuilder = new NiFiClientConfig.Builder()
                .baseUrl(url);

        if (secureUrl) {
            if (!StringUtils.isBlank(keystore)) {
                clientConfigBuilder.keystoreFilename(keystore);
            }
            if (!StringUtils.isBlank(keystoreType)) {
                clientConfigBuilder.keystoreType(KeystoreType.valueOf(keystoreType.toUpperCase()));
            }
            if (!StringUtils.isBlank(keystorePasswd)) {
                clientConfigBuilder.keystorePassword(keystorePasswd);
            }
            if (!StringUtils.isBlank(keyPasswd)) {
                clientConfigBuilder.keyPassword(keyPasswd);
            }
            if (!StringUtils.isBlank(truststore)) {
                clientConfigBuilder.truststoreFilename(truststore);
            }
            if (!StringUtils.isBlank(truststoreType)) {
                clientConfigBuilder.truststoreType(KeystoreType.valueOf(truststoreType.toUpperCase()));
            }
            if (!StringUtils.isBlank(truststorePasswd)) {
                clientConfigBuilder.truststorePassword(truststorePasswd);
            }
            if (!StringUtils.isBlank(protocol)) {
                clientConfigBuilder.protocol(protocol);
            }
        }

        if (!StringUtils.isBlank(connectionTimeout)) {
            try {
                Integer timeout = Integer.valueOf(connectionTimeout);
                clientConfigBuilder.connectTimeout(timeout);
            } catch(Exception e) {
                throw new MissingOptionException("connectionTimeout has to be an integer");
            }
        }

        if (!StringUtils.isBlank(readTimeout)) {
            try {
                Integer timeout = Integer.valueOf(readTimeout);
                clientConfigBuilder.readTimeout(timeout);
            } catch(Exception e) {
                throw new MissingOptionException("readTimeout has to be an integer");
            }
        }

        final NiFiClient client = new JerseyNiFiClient.Builder().config(clientConfigBuilder.build()).build();

        // return a wrapped client based which arguments were provided, otherwise return the regular client

        if (!StringUtils.isBlank(proxiedEntity)) {
            final RequestConfig proxiedEntityConfig = new ProxiedEntityRequestConfig(proxiedEntity);
            return new NiFiClientWithRequestConfig(client, proxiedEntityConfig);
        } else if (!StringUtils.isBlank(bearerToken)) {
            final RequestConfig bearerTokenConfig = new BearerTokenRequestConfig(bearerToken);
            return new NiFiClientWithRequestConfig(client, bearerTokenConfig);
        } else if (!StringUtils.isBlank(basicAuthUsername) && !StringUtils.isBlank(basicAuthPassword)) {
            final RequestConfig basicAuthConfig = new BasicAuthRequestConfig(basicAuthUsername, basicAuthPassword);
            return new NiFiClientWithRequestConfig(client, basicAuthConfig);
        } else {
            return client;
        }
    }

    /**
     * Wraps a NiFiClient and ensures that all methods to obtain a more specific client will
     * call the RequestConfig variation so that callers don't have to pass in the config on every call.
     */
    private static class NiFiClientWithRequestConfig implements NiFiClient {

        private final NiFiClient wrappedClient;
        private final RequestConfig requestConfig;

        public NiFiClientWithRequestConfig(final NiFiClient wrappedClient, final RequestConfig requestConfig) {
            this.wrappedClient = wrappedClient;
            this.requestConfig = Validate.notNull(requestConfig);
        }

        @Override
        public ControllerClient getControllerClient() {
            return wrappedClient.getControllerClient(requestConfig);
        }

        @Override
        public ControllerClient getControllerClient(RequestConfig requestConfig) {
            return wrappedClient.getControllerClient(requestConfig);
        }

        @Override
        public ControllerServicesClient getControllerServicesClient() {
            return wrappedClient.getControllerServicesClient(requestConfig);
        }

        @Override
        public ControllerServicesClient getControllerServicesClient(RequestConfig requestConfig) {
            return wrappedClient.getControllerServicesClient(requestConfig);
        }

        @Override
        public FlowClient getFlowClient() {
            return wrappedClient.getFlowClient(requestConfig);
        }

        @Override
        public FlowClient getFlowClient(RequestConfig requestConfig) {
            return wrappedClient.getFlowClient(requestConfig);
        }

        @Override
        public ProcessGroupClient getProcessGroupClient() {
            return wrappedClient.getProcessGroupClient(requestConfig);
        }

        @Override
        public ProcessGroupClient getProcessGroupClient(RequestConfig requestConfig) {
            return wrappedClient.getProcessGroupClient(requestConfig);
        }

        @Override
        public ProcessorClient getProcessorClient() {
            return wrappedClient.getProcessorClient(requestConfig);
        }

        @Override
        public ProcessorClient getProcessorClient(RequestConfig requestConfig) {
            return wrappedClient.getProcessorClient(requestConfig);
        }

        @Override
        public VersionsClient getVersionsClient() {
            return wrappedClient.getVersionsClient(requestConfig);
        }

        @Override
        public VersionsClient getVersionsClient(RequestConfig requestConfig) {
            return wrappedClient.getVersionsClient(requestConfig);
        }

        @Override
        public TenantsClient getTenantsClient() {
            return wrappedClient.getTenantsClient(requestConfig);
        }

        @Override
        public TenantsClient getTenantsClient(RequestConfig requestConfig) {
            return wrappedClient.getTenantsClient(requestConfig);
        }

        @Override
        public PoliciesClient getPoliciesClient() {
            return wrappedClient.getPoliciesClient(requestConfig);
        }

        @Override
        public PoliciesClient getPoliciesClient(RequestConfig requestConfig) {
            return wrappedClient.getPoliciesClient(requestConfig);
        }

        @Override
        public TemplatesClient getTemplatesClient() {
            return wrappedClient.getTemplatesClient(requestConfig);
        }

        @Override
        public TemplatesClient getTemplatesClient(RequestConfig requestConfig) {
            return wrappedClient.getTemplatesClient(requestConfig);
        }

        @Override
        public ReportingTasksClient getReportingTasksClient() {
            return wrappedClient.getReportingTasksClient(requestConfig);
        }

        @Override
        public ReportingTasksClient getReportingTasksClient(RequestConfig requestConfig) {
            return wrappedClient.getReportingTasksClient(requestConfig);
        }

        @Override
        public ParamContextClient getParamContextClient() {
            return wrappedClient.getParamContextClient(requestConfig);
        }

        @Override
        public ParamContextClient getParamContextClient(RequestConfig requestConfig) {
            return wrappedClient.getParamContextClient(requestConfig);
        }

        @Override
        public CountersClient getCountersClient() {
            return wrappedClient.getCountersClient(requestConfig);
        }

        @Override
        public CountersClient getCountersClient(RequestConfig requestConfig) {
            return wrappedClient.getCountersClient(requestConfig);
        }

        @Override
        public ConnectionClient getConnectionClient() {
            return wrappedClient.getConnectionClient(requestConfig);
        }

        @Override
        public ConnectionClient getConnectionClient(RequestConfig requestConfig) {
            return wrappedClient.getConnectionClient(requestConfig);
        }

        @Override
        public RemoteProcessGroupClient getRemoteProcessGroupClient() {
            return wrappedClient.getRemoteProcessGroupClient(requestConfig);
        }

        @Override
        public RemoteProcessGroupClient getRemoteProcessGroupClient(RequestConfig requestConfig) {
            return wrappedClient.getRemoteProcessGroupClient(requestConfig);
        }

        @Override
        public InputPortClient getInputPortClient() {
            return wrappedClient.getInputPortClient(requestConfig);
        }

        @Override
        public InputPortClient getInputPortClient(RequestConfig requestConfig) {
            return wrappedClient.getInputPortClient(requestConfig);
        }

        @Override
        public OutputPortClient getOutputPortClient() {
            return wrappedClient.getOutputPortClient(requestConfig);
        }

        @Override
        public OutputPortClient getOutputPortClient(RequestConfig requestConfig) {
            return wrappedClient.getOutputPortClient(requestConfig);
        }

        @Override
        public ProvenanceClient getProvenanceClient() {
            return wrappedClient.getProvenanceClient(requestConfig);
        }

        @Override
        public ProvenanceClient getProvenanceClient(RequestConfig requestConfig) {
            return wrappedClient.getProvenanceClient(requestConfig);
        }

        @Override
        public AccessClient getAccessClient() {
            return wrappedClient.getAccessClient();
        }

        @Override
        public void close() throws IOException {
            wrappedClient.close();
        }
    }
}
