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

import org.apache.nifi.flow.resource.ExternalResourceDescriptor;
import org.apache.nifi.flow.resource.ExternalResourceProvider;
import org.apache.nifi.flow.resource.ExternalResourceProviderInitializationContext;
import org.apache.nifi.flow.resource.ImmutableExternalResourceDescriptor;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

public class NiFiRegistryExternalResourceProvider implements ExternalResourceProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(NiFiRegistryExternalResourceProvider.class);

    private static final String NIFI_REGISTRY_CLIENT_ID = "nifi-registry-nar-provider";
    private static final String NIFI_REGISTRY_CLIENT_NAME = "NiFi Registry NAR Provider";

    static final String URL_PROPERTY = "url";

    private volatile NiFiRegistryExtensionRegistry extensionRegistry;
    private volatile boolean initialized = false;

    @Override
    public void initialize(final ExternalResourceProviderInitializationContext context) {
        final String url = context.getProperties().get(URL_PROPERTY);
        if (StringUtils.isBlank(url)) {
            throw new IllegalArgumentException("url property required");
        }

        final SSLContext sslContext = context.getSSLContext();
        if (url.startsWith("https") && sslContext == null) {
            throw new IllegalStateException("NiFi TLS properties must be specified in order to connect to NiFi Registry via https");
        }

        extensionRegistry = new NiFiRegistryExtensionRegistry(NIFI_REGISTRY_CLIENT_ID, url, NIFI_REGISTRY_CLIENT_NAME, sslContext);
        initialized = true;
    }

    @Override
    public Collection<ExternalResourceDescriptor> listResources() throws IOException {
        if (!initialized) {
            LOGGER.error("Provider is not initialized");
        }

        try {
            final Set<NiFiRegistryExtensionBundleMetadata> bundleMetadata = extensionRegistry.getExtensionBundleMetadata(null);
            return bundleMetadata.stream().map(bm ->  new ImmutableExternalResourceDescriptor(bm.toLocationString(), bm.getTimestamp())).collect(Collectors.toSet());
        } catch (final ExtensionRegistryException ere) {
            LOGGER.error("Unable to retrieve listing of NARs from NiFi Registry at [{}]", extensionRegistry.getURL(), ere);
            return Collections.emptySet();
        }
    }

    @Override
    public InputStream fetchExternalResource(final ExternalResourceDescriptor descriptor) throws IOException {
        if (!initialized) {
            LOGGER.error("Provider is not initialized");
        }

        final NiFiRegistryExtensionBundleMetadata bundleMetadata = NiFiRegistryExtensionBundleMetadata.fromLocationString(descriptor.getLocation())
                .registryIdentifier(extensionRegistry.getIdentifier())
                .build();

        LOGGER.debug("Fetching NAR contents for bundleIdentifier [{}] and version [{}]",
                bundleMetadata.getBundleIdentifier(), bundleMetadata.getVersion());

        try {
            return extensionRegistry.getExtensionBundleContent(null, bundleMetadata);
        } catch (ExtensionRegistryException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
