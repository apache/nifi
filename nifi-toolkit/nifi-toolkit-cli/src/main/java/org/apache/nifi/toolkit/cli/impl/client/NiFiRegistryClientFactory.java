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
import org.apache.nifi.registry.client.impl.JerseyNiFiRegistryClient;
import org.apache.nifi.registry.client.impl.request.BasicAuthRequestConfig;
import org.apache.nifi.registry.client.impl.request.BearerTokenRequestConfig;
import org.apache.nifi.registry.client.impl.request.ProxiedEntityRequestConfig;
import org.apache.nifi.registry.security.util.KeystoreType;
import org.apache.nifi.toolkit.cli.api.ClientFactory;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;

import java.io.IOException;
import java.util.Properties;

/**
 * Creates a NiFiRegistryClient from the given properties.
 */
public class NiFiRegistryClientFactory implements ClientFactory<NiFiRegistryClient> {

    @Override
    public NiFiRegistryClient createClient(final Properties properties) throws MissingOptionException {
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

        final NiFiRegistryClientConfig.Builder clientConfigBuilder = new NiFiRegistryClientConfig.Builder()
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

        final NiFiRegistryClientConfig clientConfig = clientConfigBuilder.build();
        final NiFiRegistryClient client = new JerseyNiFiRegistryClient.Builder().config(clientConfig).build();

        // return a wrapped client based which arguments were provided, otherwise return the regular client

        if (!StringUtils.isBlank(proxiedEntity)) {
            final RequestConfig proxiedEntityConfig = new ProxiedEntityRequestConfig(proxiedEntity);
            return new NiFiRegistryClientWithRequestConfig(client, proxiedEntityConfig);
        } else if (!StringUtils.isBlank(bearerToken)) {
            final RequestConfig bearerTokenConfig = new BearerTokenRequestConfig(bearerToken);
            return new NiFiRegistryClientWithRequestConfig(client, bearerTokenConfig);
        } else if (!StringUtils.isBlank(basicAuthUsername) && !StringUtils.isBlank(basicAuthPassword)) {
            final RequestConfig basicAuthConfig = new BasicAuthRequestConfig(basicAuthUsername, basicAuthPassword);
            return new NiFiRegistryClientWithRequestConfig(client, basicAuthConfig);
        } else {
            return client;
        }
    }

    /**
     * Wraps a NiFiRegistryClient and ensures that all methods to obtain a more specific client will
     * call the RequestConfig variation so that callers don't have to pass in the config one every call.
     */
    private static class NiFiRegistryClientWithRequestConfig implements NiFiRegistryClient {

        private final NiFiRegistryClient client;
        private final RequestConfig requestConfig;

        public NiFiRegistryClientWithRequestConfig(final NiFiRegistryClient client, final RequestConfig requestConfig) {
            this.client = client;
            this.requestConfig = requestConfig;
        }

        //----------------------------------------------------------------------

        @Override
        public BucketClient getBucketClient() {
            return client.getBucketClient(requestConfig);
        }

        @Override
        public BucketClient getBucketClient(String... proxiedEntities) {
            final RequestConfig requestConfig = new ProxiedEntityRequestConfig(proxiedEntities);
            return client.getBucketClient(requestConfig);
        }

        @Override
        public BucketClient getBucketClient(RequestConfig requestConfig) {
            return client.getBucketClient(requestConfig);
        }

        //----------------------------------------------------------------------

        @Override
        public FlowClient getFlowClient() {
            return client.getFlowClient(requestConfig);
        }

        @Override
        public FlowClient getFlowClient(String... proxiedEntities) {
            final RequestConfig requestConfig = new ProxiedEntityRequestConfig(proxiedEntities);
            return client.getFlowClient(requestConfig);
        }

        @Override
        public FlowClient getFlowClient(RequestConfig requestConfig) {
            return client.getFlowClient(requestConfig);
        }

        //----------------------------------------------------------------------

        @Override
        public FlowSnapshotClient getFlowSnapshotClient() {
            return client.getFlowSnapshotClient(requestConfig);
        }

        @Override
        public FlowSnapshotClient getFlowSnapshotClient(String... proxiedEntities) {
            final RequestConfig requestConfig = new ProxiedEntityRequestConfig(proxiedEntities);
            return client.getFlowSnapshotClient(requestConfig);
        }

        @Override
        public FlowSnapshotClient getFlowSnapshotClient(RequestConfig requestConfig) {
            return client.getFlowSnapshotClient(requestConfig);
        }

        //----------------------------------------------------------------------

        @Override
        public ItemsClient getItemsClient() {
            return client.getItemsClient(requestConfig);
        }

        @Override
        public ItemsClient getItemsClient(String... proxiedEntities) {
            final RequestConfig requestConfig = new ProxiedEntityRequestConfig(proxiedEntities);
            return client.getItemsClient(requestConfig);
        }

        @Override
        public ItemsClient getItemsClient(RequestConfig requestConfig) {
            return client.getItemsClient(requestConfig);
        }

        //----------------------------------------------------------------------

        @Override
        public UserClient getUserClient() {
            return client.getUserClient(requestConfig);
        }

        @Override
        public UserClient getUserClient(String... proxiedEntities) {
            final RequestConfig requestConfig = new ProxiedEntityRequestConfig(proxiedEntities);
            return client.getUserClient(requestConfig);
        }

        @Override
        public UserClient getUserClient(RequestConfig requestConfig) {
            return client.getUserClient(requestConfig);
        }

        //----------------------------------------------------------------------

        @Override
        public BundleClient getBundleClient() {
            return client.getBundleClient(requestConfig);
        }

        @Override
        public BundleClient getBundleClient(String... proxiedEntities) {
            final RequestConfig requestConfig = new ProxiedEntityRequestConfig(proxiedEntities);
            return client.getBundleClient(requestConfig);
        }

        @Override
        public BundleClient getBundleClient(RequestConfig requestConfig) {
            return client.getBundleClient(requestConfig);
        }

        //----------------------------------------------------------------------

        @Override
        public BundleVersionClient getBundleVersionClient() {
            return client.getBundleVersionClient(requestConfig);
        }

        @Override
        public BundleVersionClient getBundleVersionClient(String... proxiedEntities) {
            final RequestConfig requestConfig = new ProxiedEntityRequestConfig(proxiedEntities);
            return client.getBundleVersionClient(requestConfig);
        }

        @Override
        public BundleVersionClient getBundleVersionClient(RequestConfig requestConfig) {
            return client.getBundleVersionClient(requestConfig);
        }

        //----------------------------------------------------------------------

        @Override
        public ExtensionRepoClient getExtensionRepoClient() {
            return client.getExtensionRepoClient(requestConfig);
        }

        @Override
        public ExtensionRepoClient getExtensionRepoClient(String... proxiedEntities) {
            final RequestConfig requestConfig = new ProxiedEntityRequestConfig(proxiedEntities);
            return client.getExtensionRepoClient(requestConfig);
        }

        @Override
        public ExtensionRepoClient getExtensionRepoClient(RequestConfig requestConfig) {
            return client.getExtensionRepoClient(requestConfig);
        }

        //----------------------------------------------------------------------

        @Override
        public ExtensionClient getExtensionClient() {
            return client.getExtensionClient(requestConfig);
        }

        @Override
        public ExtensionClient getExtensionClient(String... proxiedEntities) {
            final RequestConfig requestConfig = new ProxiedEntityRequestConfig(proxiedEntities);
            return client.getExtensionClient(requestConfig);
        }

        @Override
        public ExtensionClient getExtensionClient(RequestConfig requestConfig) {
            return client.getExtensionClient(requestConfig);
        }

        //----------------------------------------------------------------------

        @Override
        public TenantsClient getTenantsClient() {
            return client.getTenantsClient(requestConfig);
        }

        @Override
        public TenantsClient getTenantsClient(String... proxiedEntities) {
            final RequestConfig requestConfig = new ProxiedEntityRequestConfig(proxiedEntities);
            return client.getTenantsClient(requestConfig);
        }

        @Override
        public TenantsClient getTenantsClient(RequestConfig requestConfig) {
            return client.getTenantsClient(requestConfig);
        }

        //----------------------------------------------------------------------

        @Override
        public PoliciesClient getPoliciesClient() {
            return client.getPoliciesClient(requestConfig);
        }

        @Override
        public PoliciesClient getPoliciesClient(String... proxiedEntities) {
            final RequestConfig requestConfig = new ProxiedEntityRequestConfig(proxiedEntities);
            return client.getPoliciesClient(requestConfig);
        }

        @Override
        public PoliciesClient getPoliciesClient(RequestConfig requestConfig) {
            return client.getPoliciesClient(requestConfig);
        }

        //----------------------------------------------------------------------

        @Override
        public AccessClient getAccessClient() {
            return client.getAccessClient();
        }

        //----------------------------------------------------------------------

        @Override
        public void close() throws IOException {
            client.close();
        }
    }
}
