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
package org.apache.nifi.registry.extension;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.registry.client.BundleVersionClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryClientConfig;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.RequestConfig;
import org.apache.nifi.registry.client.impl.JerseyNiFiRegistryClient;
import org.apache.nifi.registry.client.impl.request.ProxiedEntityRequestConfig;
import org.apache.nifi.registry.extension.bundle.BundleVersionFilterParams;
import org.apache.nifi.registry.extension.bundle.BundleVersionMetadata;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * NiFi Registry implementation of ExtensionRegistry.
 */
public class NiFiRegistryExtensionRegistry extends AbstractExtensionRegistry<NiFiRegistryExtensionBundleMetadata> {

    private NiFiRegistryClient registryClient;

    public NiFiRegistryExtensionRegistry(final String identifier, final String url, final String name, final SSLContext sslContext) {
        super(identifier, url, name, sslContext);
    }

    private synchronized NiFiRegistryClient getRegistryClient() {
        if (registryClient != null) {
            return registryClient;
        }

        final NiFiRegistryClientConfig config = new NiFiRegistryClientConfig.Builder()
                .connectTimeout(30000)
                .readTimeout(30000)
                .sslContext(getSSLContext())
                .baseUrl(getURL())
                .build();

        registryClient = new JerseyNiFiRegistryClient.Builder()
                .config(config)
                .build();

        return registryClient;
    }

    private synchronized void invalidateClient() {
        this.registryClient = null;
    }

    @Override
    public void setURL(final String url) {
        super.setURL(url);
        invalidateClient();
    }

    @Override
    public Set<NiFiRegistryExtensionBundleMetadata> getExtensionBundleMetadata(final NiFiUser user)
            throws IOException, ExtensionRegistryException {
        final RequestConfig requestConfig = getRequestConfig(user);
        final NiFiRegistryClient registryClient = getRegistryClient();
        final BundleVersionClient bundleVersionClient = registryClient.getBundleVersionClient(requestConfig);

        try {
            final List<BundleVersionMetadata> bundleVersions = bundleVersionClient.getBundleVersions(BundleVersionFilterParams.empty());
            return bundleVersions.stream().map(bv -> map(bv)).collect(Collectors.toSet());
        } catch (final NiFiRegistryException nre) {
            throw new ExtensionRegistryException(nre.getMessage(), nre);
        }
    }

    @Override
    public InputStream getExtensionBundleContent(final NiFiUser user, final NiFiRegistryExtensionBundleMetadata bundleMetadata)
            throws IOException, ExtensionRegistryException {
        final RequestConfig requestConfig = getRequestConfig(user);
        final NiFiRegistryClient registryClient = getRegistryClient();
        final BundleVersionClient bundleVersionClient = registryClient.getBundleVersionClient(requestConfig);

        try {
            return bundleVersionClient.getBundleVersionContent(bundleMetadata.getBundleIdentifier(), bundleMetadata.getVersion());
        } catch (NiFiRegistryException nre) {
            throw new ExtensionRegistryException(nre.getMessage(), nre);
        }
    }

    private RequestConfig getRequestConfig(final NiFiUser user) {
        final String identity = getIdentity(user);
        return identity == null ? null : new ProxiedEntityRequestConfig(identity);
    }

    private String getIdentity(final NiFiUser user) {
        return (user == null || user.isAnonymous()) ? null : user.getIdentity();
    }

    private NiFiRegistryExtensionBundleMetadata map(final BundleVersionMetadata bundleVersionMetadata) {
        return new NiFiRegistryExtensionBundleMetadata.Builder()
                .group(bundleVersionMetadata.getGroupId())
                .artifact(bundleVersionMetadata.getArtifactId())
                .version(bundleVersionMetadata.getVersion())
                .bundleIdentifier(bundleVersionMetadata.getBundleId())
                .timestamp(bundleVersionMetadata.getTimestamp())
                .registryIdentifier(getIdentifier())
                .build();
    }

}