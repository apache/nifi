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
import org.apache.nifi.registry.client.BucketClient;
import org.apache.nifi.registry.client.FlowClient;
import org.apache.nifi.registry.client.FlowSnapshotClient;
import org.apache.nifi.registry.client.ItemsClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryClientConfig;
import org.apache.nifi.registry.client.UserClient;
import org.apache.nifi.registry.client.impl.JerseyNiFiRegistryClient;
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

        final String keystore = properties.getProperty(CommandOption.KEYSTORE.getLongName());
        final String keystoreType = properties.getProperty(CommandOption.KEYSTORE_TYPE.getLongName());
        final String keystorePasswd = properties.getProperty(CommandOption.KEYSTORE_PASSWORD.getLongName());
        final String keyPasswd = properties.getProperty(CommandOption.KEY_PASSWORD.getLongName());

        final String truststore = properties.getProperty(CommandOption.TRUSTSTORE.getLongName());
        final String truststoreType = properties.getProperty(CommandOption.TRUSTSTORE_TYPE.getLongName());
        final String truststorePasswd = properties.getProperty(CommandOption.TRUSTSTORE_PASSWORD.getLongName());

        final String proxiedEntity = properties.getProperty(CommandOption.PROXIED_ENTITY.getLongName());

        final boolean secureUrl = url.startsWith("https");

        if (secureUrl && (StringUtils.isBlank(truststore)
                || StringUtils.isBlank(truststoreType)
                || StringUtils.isBlank(truststorePasswd))
                ) {
            throw new MissingOptionException(CommandOption.TRUSTSTORE.getLongName() + ", " + CommandOption.TRUSTSTORE_TYPE.getLongName()
                    + ", and " + CommandOption.TRUSTSTORE_PASSWORD.getLongName() + " are required when using an https url");
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
        }

        final NiFiRegistryClient client = new JerseyNiFiRegistryClient.Builder().config(clientConfigBuilder.build()).build();

        // if a proxied entity was specified then return a wrapped client, otherwise return the regular client
        if (!StringUtils.isBlank(proxiedEntity)) {
            return new ProxiedNiFiRegistryClient(client, proxiedEntity);
        } else {
            return client;
        }
    }

    /**
     * Wraps a NiFiRegistryClient and ensures that all methods to obtain a more specific client will
     * call the proxied-entity variation so that callers don't have to care if proxying is taking place.
     */
    private static class ProxiedNiFiRegistryClient implements NiFiRegistryClient {

        private final NiFiRegistryClient client;
        private final String proxiedEntity;

        public ProxiedNiFiRegistryClient(final NiFiRegistryClient client, final String proxiedEntity) {
            this.client = client;
            this.proxiedEntity = proxiedEntity;
        }

        @Override
        public BucketClient getBucketClient() {
            return getBucketClient(proxiedEntity);
        }

        @Override
        public BucketClient getBucketClient(String... proxiedEntity) {
            return client.getBucketClient(proxiedEntity);
        }

        @Override
        public FlowClient getFlowClient() {
            return getFlowClient(proxiedEntity);
        }

        @Override
        public FlowClient getFlowClient(String... proxiedEntity) {
            return client.getFlowClient(proxiedEntity);
        }

        @Override
        public FlowSnapshotClient getFlowSnapshotClient() {
            return getFlowSnapshotClient(proxiedEntity);
        }

        @Override
        public FlowSnapshotClient getFlowSnapshotClient(String... proxiedEntity) {
            return client.getFlowSnapshotClient(proxiedEntity);
        }

        @Override
        public ItemsClient getItemsClient() {
            return getItemsClient(proxiedEntity);
        }

        @Override
        public ItemsClient getItemsClient(String... proxiedEntity) {
            return client.getItemsClient(proxiedEntity);
        }

        @Override
        public UserClient getUserClient() {
            return getUserClient(proxiedEntity);
        }

        @Override
        public UserClient getUserClient(String... proxiedEntity) {
            return client.getUserClient(proxiedEntity);
        }

        @Override
        public void close() throws IOException {
            client.close();
        }
    }

}
