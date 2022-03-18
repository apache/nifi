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

import org.apache.nifi.nar.NarProvider;
import org.apache.nifi.nar.NarProviderInitializationContext;
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

/**
 * NarProvider implementation that retrieves NARs from NiFi Registry. The current implementation will retrieve bundles
 * from all buckets that the NiFi server is authorized to read from (generally will be all buckets).
 *
 * Example configuration for nifi.properties:
 *   nifi.nar.library.provider.nifi-registry.implementation=org.apache.nifi.registry.extension.NiFiRegistryNarProvider
 *   nifi.nar.library.provider.nifi-registry.url=http://localhost:18080
 *
 */
public class NiFiRegistryNarProvider implements NarProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(NiFiRegistryNarProvider.class);

    private static final String NIFI_REGISTRY_CLIENT_ID = "nifi-registry-nar-provider";
    private static final String NIFI_REGISTRY_CLIENT_NAME = "NiFi Registry NAR Provider";

    static final String URL_PROPERTY = "url";

    private volatile NiFiRegistryExtensionRegistry extensionRegistry;
    private volatile boolean initialized = false;

    @Override
    public void initialize(final NarProviderInitializationContext initializationContext) {
        final String url = initializationContext.getProperties().get(URL_PROPERTY);
        if (StringUtils.isBlank(url)) {
            throw new IllegalArgumentException("NiFiRegistryNarProvider requires a `url` property");
        }

        final SSLContext sslContext = initializationContext.getNiFiSSLContext();
        if (url.startsWith("https") && sslContext == null) {
            throw new IllegalStateException("NiFi TLS properties must be specified in order to connect to NiFi Registry via https");
        }

        extensionRegistry = new NiFiRegistryExtensionRegistry(NIFI_REGISTRY_CLIENT_ID, url, NIFI_REGISTRY_CLIENT_NAME, sslContext);
        initialized = true;
    }

    @Override
    public Collection<String> listNars() throws IOException {
        if (!initialized) {
            LOGGER.error("Provider is not initialized");
        }

        try {
            final Set<NiFiRegistryExtensionBundleMetadata> bundleMetadata = extensionRegistry.getExtensionBundleMetadata(null);
            return bundleMetadata.stream().map(bm -> bm.toLocationString()).collect(Collectors.toSet());
        } catch (final ExtensionRegistryException ere) {
            LOGGER.error("Unable to retrieve listing of NARs from NiFi Registry at [{}]", extensionRegistry.getURL(), ere);
            return Collections.emptySet();
        }
    }

    @Override
    public InputStream fetchNarContents(final String location) throws IOException {
        if (!initialized) {
            LOGGER.error("Provider is not initialized");
        }

        final NiFiRegistryExtensionBundleMetadata bundleMetadata = NiFiRegistryExtensionBundleMetadata.fromLocationString(location)
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
