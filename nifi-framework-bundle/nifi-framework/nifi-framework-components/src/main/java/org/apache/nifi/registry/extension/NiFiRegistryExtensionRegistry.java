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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.module.jakarta.xmlbind.JakartaXmlBindAnnotationIntrospector;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.registry.extension.bundle.BundleVersionMetadata;
import org.apache.nifi.security.proxied.entity.StandardProxiedEntityEncoder;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;

/**
 * NiFi Registry implementation of ExtensionRegistry.
 */
public class NiFiRegistryExtensionRegistry extends AbstractExtensionRegistry<NiFiRegistryExtensionBundleMetadata> {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private static final String FORWARD_SLASH = "/";

    private static final String PROXIED_ENTITIES_CHAIN_HEADER = "X-ProxiedEntitiesChain";

    private static final ObjectMapper objectMapper = new ObjectMapper()
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .setAnnotationIntrospector(new JakartaXmlBindAnnotationIntrospector(TypeFactory.defaultInstance()));

    private HttpClient httpClient;

    public NiFiRegistryExtensionRegistry(final String identifier, final String url, final String name, final SSLContext sslContext) {
        super(identifier, url, name, sslContext);
    }

    private synchronized HttpClient getConfiguredHttpClient() {
        if (httpClient != null) {
            return httpClient;
        }

        final HttpClient.Builder builder = HttpClient.newBuilder();
        builder.connectTimeout(TIMEOUT);

        final SSLContext sslContext = getSSLContext();
        if (sslContext != null) {
            builder.sslContext(sslContext);
        }
        httpClient = builder.build();

        return httpClient;
    }

    private synchronized void invalidateClient() {
        this.httpClient = null;
    }

    @Override
    public void setURL(final String url) {
        super.setURL(url);
        invalidateClient();
    }

    @Override
    public Set<NiFiRegistryExtensionBundleMetadata> getExtensionBundleMetadata(final NiFiUser user) throws IOException, ExtensionRegistryException {
        final URI versionsUri = getUri("bundles/versions");
        final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(versionsUri);

        try (InputStream inputStream = sendRequest(requestBuilder, user)) {
            final BundleVersionMetadata[] bundleVersions = objectMapper.readValue(inputStream, BundleVersionMetadata[].class);
            return Arrays.stream(bundleVersions).map(this::map).collect(Collectors.toSet());
        }
    }

    @Override
    public InputStream getExtensionBundleContent(final NiFiUser user, final NiFiRegistryExtensionBundleMetadata bundleMetadata) throws ExtensionRegistryException {
        final String bundleId = bundleMetadata.getBundleIdentifier();
        final String version = bundleMetadata.getVersion();
        final URI versionsUri = getUri("bundles/%s/versions/%s/content".formatted(bundleId, version));
        final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(versionsUri);

        return sendRequest(requestBuilder, user);
    }

    private InputStream sendRequest(final HttpRequest.Builder requestBuilder, final NiFiUser user) throws ExtensionRegistryException {
        final HttpClient configuredHttpClient = getConfiguredHttpClient();

        if (user != null && !user.isAnonymous()) {
            final String identity = user.getIdentity();
            final String proxiedEntities = StandardProxiedEntityEncoder.getInstance().getEncodedEntity(identity);
            requestBuilder.setHeader(PROXIED_ENTITIES_CHAIN_HEADER, proxiedEntities);
        }

        requestBuilder.timeout(TIMEOUT);
        final HttpRequest request = requestBuilder.build();

        final URI uri = request.uri();
        try {
            final HttpResponse<InputStream> response = configuredHttpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
            final int statusCode = response.statusCode();
            if (HttpURLConnection.HTTP_OK == statusCode) {
                return response.body();
            } else {
                throw new ExtensionRegistryException("Registry request failed with HTTP %d [%s]".formatted(statusCode, uri));
            }
        } catch (final IOException e) {
            throw new ExtensionRegistryException("Registry request failed [%s]".formatted(uri), e);
        } catch (final InterruptedException e) {
            throw new ExtensionRegistryException("Registry requested interrupted [%s]".formatted(uri), e);
        }
    }

    private URI getUri(final String path) {
        final StringBuilder builder = new StringBuilder();
        final String baseUrl = getURL();
        builder.append(baseUrl);
        if (!baseUrl.endsWith(FORWARD_SLASH)) {
            builder.append(FORWARD_SLASH);
        }
        builder.append(path);
        return URI.create(builder.toString());
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
