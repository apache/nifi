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
package org.apache.nifi.registry.flow;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flow.VersionedFlowCoordinates;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.client.BucketClient;
import org.apache.nifi.registry.client.FlowClient;
import org.apache.nifi.registry.client.FlowSnapshotClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryClientConfig;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.impl.JerseyNiFiRegistryClient;
import org.apache.nifi.registry.client.impl.request.ProxiedEntityRequestConfig;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.ssl.SSLContextService;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class NifiRegistryFlowRegistryClient extends AbstractFlowRegistryClient {

    public final static PropertyDescriptor PROPERTY_URL = new PropertyDescriptor.Builder()
            .name("url")
            .displayName("URL")
            .description("URL of the NiFi Registry")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .required(true)
            .build();
    static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("ssl-context-service")
            .displayName("SSL Context Service")
            .description("Specifies the SSL Context Service to use for communicating with NiFiRegistry")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    private volatile String registryUrl;
    private volatile NiFiRegistryClient registryClient;

    private synchronized NiFiRegistryClient getRegistryClient(final FlowRegistryClientConfigurationContext context) {
        final String proposedUrl = getProposedUri(context);

        if (!proposedUrl.equals(registryUrl)) {
            registryUrl = proposedUrl;
            invalidateClient();
        }

        if (registryClient != null) {
            return registryClient;
        }

        final NiFiRegistryClientConfig config = new NiFiRegistryClientConfig.Builder()
                .connectTimeout(30000)
                .readTimeout(30000)
                .sslContext(extractSSLContext(context))
                .baseUrl(registryUrl)
                .build();
        registryClient = new JerseyNiFiRegistryClient.Builder()
                .config(config)
                .build();

        return registryClient;
    }

    private String getProposedUri(final FlowRegistryClientConfigurationContext context) {
        final String configuredUrl = context.getProperty(PROPERTY_URL).evaluateAttributeExpressions().getValue();

        final URI uri;

        try {
            final URI fullUri = URI.create(configuredUrl);
            final int port = fullUri.getPort();
            final String portSuffix = port < 0 ? "" : ":" + port;
            final String uriString = fullUri.getScheme() + "://" + fullUri.getHost() + portSuffix;
            uri = URI.create(uriString);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("The given Registry URL is not valid: " + configuredUrl);
        }

        final String uriScheme = uri.getScheme();

        if (uriScheme == null) {
            throw new IllegalArgumentException("The given Registry URL is not valid: " + configuredUrl);
        }

        return uri.toString();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(
                PROPERTY_URL,
                SSL_CONTEXT_SERVICE
        );
    }

    private synchronized void invalidateClient() {
        this.registryClient = null;
    }

    private String extractIdentity(final FlowRegistryClientConfigurationContext context) {
        return context.getNiFiUserIdentity().orElse(null);
    }

    private SSLContext extractSSLContext(final FlowRegistryClientConfigurationContext context) {
        if (context.getProperty(SSL_CONTEXT_SERVICE).isSet()) {
            final TlsConfiguration tlsConfiguration = createTlsConfigurationFromContext(context);

            try {
                return SslContextFactory.createSslContext(tlsConfiguration);
            } catch (final TlsException e) {
                throw new IllegalStateException("Could not instantiate flow registry client because of TLS issues", e);
            }
        } else {
            return getSystemSslContext().orElse(null);
        }
    }

    @Override
    public boolean isStorageLocationApplicable(final FlowRegistryClientConfigurationContext context, final String location) {
        return location != null && location.startsWith(getProposedUri(context));
    }

    @Override
    public Set<FlowRegistryBucket> getBuckets(final FlowRegistryClientConfigurationContext context) throws FlowRegistryException, IOException {
        try {
            final BucketClient bucketClient = getBucketClient(context);
            return bucketClient.getAll().stream().map(NifiRegistryUtil::convert).collect(Collectors.toSet());
        } catch (final NiFiRegistryException e) {
            throw new FlowRegistryException(e.getMessage(), e);
        }
    }

    @Override
    public FlowRegistryBucket getBucket(final FlowRegistryClientConfigurationContext context, final String bucketId) throws FlowRegistryException, IOException {
        try {
            final BucketClient bucketClient = getBucketClient(context);
            final Bucket bucket = bucketClient.get(bucketId);

            if (bucket == null) {
                throw new NoSuchBucketException(String.format("Bucket %s does not exist in the registry", bucketId));
            }

            return NifiRegistryUtil.convert(bucket);
        } catch (final NiFiRegistryException e) {
            throw new FlowRegistryException(e.getMessage(), e);
        }
    }

    @Override
    public RegisteredFlow registerFlow(final FlowRegistryClientConfigurationContext context, final RegisteredFlow flow) throws FlowRegistryException, IOException {
        try {
            final FlowClient flowClient = getFlowClient(context);

            final List<VersionedFlow> versionedFlows = flowClient.getByBucket(flow.getBucketIdentifier());
            final boolean matched = versionedFlows.stream()
                .anyMatch(versionedFlow -> Objects.equals(versionedFlow.getName(), flow.getName()));
            if (matched) {
                throw new FlowAlreadyExistsException("Flow %s within bucket %s already exists".formatted(flow.getName(), flow.getBucketIdentifier()));
            }

            return NifiRegistryUtil.convert(flowClient.create(NifiRegistryUtil.convert(flow)));
        } catch (final NiFiRegistryException e) {
            throw new FlowRegistryException(e.getMessage(), e);
        }
    }

    @Override
    public RegisteredFlow deregisterFlow(final FlowRegistryClientConfigurationContext context, final String bucketId, final String flowId) throws FlowRegistryException, IOException {
        try {
            final FlowClient flowClient = getFlowClient(context);
            return NifiRegistryUtil.convert(flowClient.delete(bucketId, flowId));
        } catch (final NiFiRegistryException e) {
            throw new FlowRegistryException(e.getMessage(), e);
        }
    }

    @Override
    public RegisteredFlow getFlow(final FlowRegistryClientConfigurationContext context, final String bucketId, final String flowId) throws FlowRegistryException, IOException {
        try {
            final FlowClient flowClient = getFlowClient(context);
            final VersionedFlow flow = flowClient.get(bucketId, flowId);

            if (flow == null) {
                throw new NoSuchFlowException(String.format("Flow %s does not exist in bucket %s", flowId, bucketId));
            }

            return NifiRegistryUtil.convert(flow);
        } catch (final NiFiRegistryException e) {
            throw new FlowRegistryException(e.getMessage(), e);
        }
    }

    @Override
    public Set<RegisteredFlow> getFlows(final FlowRegistryClientConfigurationContext context, final String bucketId) throws FlowRegistryException, IOException {
        try {
            final FlowClient flowClient = getFlowClient(context);
            return flowClient.getByBucket(bucketId).stream().map(NifiRegistryUtil::convert).collect(Collectors.toSet());
        } catch (final NiFiRegistryException e) {
            throw new FlowRegistryException(e.getMessage(), e);
        }
    }

    @Override
    public RegisteredFlowSnapshot getFlowContents(
            final FlowRegistryClientConfigurationContext context, final String bucketId, final String flowId, final int version
    ) throws FlowRegistryException, IOException {
        try {
            final FlowSnapshotClient snapshotClient = getFlowSnapshotClient(context);
            final VersionedFlowSnapshot snapshot = snapshotClient.get(bucketId, flowId, version);

            if (snapshot == null) {
                throw new NoSuchFlowVersionException(String.format("Version %d of flow %s does not exist in bucket %s", version, flowId, bucketId));
            }

            return NifiRegistryUtil.convert(snapshot);
        } catch (final NiFiRegistryException e) {
            throw new FlowRegistryException(e.getMessage(), e);
        }
    }

    @Override
    public RegisteredFlowSnapshot registerFlowSnapshot(FlowRegistryClientConfigurationContext context, RegisteredFlowSnapshot flowSnapshot) throws FlowRegistryException, IOException {
        try {
            final FlowSnapshotClient snapshotClient = getFlowSnapshotClient(context);
            final VersionedFlowSnapshot versionedFlowSnapshot = snapshotClient.create(NifiRegistryUtil.convert(flowSnapshot));
            final VersionedFlowCoordinates versionedFlowCoordinates = new VersionedFlowCoordinates();

            final String bucketId = versionedFlowSnapshot.getFlow().getBucketIdentifier();
            final String flowId = versionedFlowSnapshot.getFlow().getIdentifier();
            final int version = (int) versionedFlowSnapshot.getFlow().getVersionCount();

            versionedFlowCoordinates.setRegistryId(getIdentifier());
            versionedFlowCoordinates.setBucketId(bucketId);
            versionedFlowCoordinates.setFlowId(flowId);
            versionedFlowCoordinates.setVersion(version);
            versionedFlowCoordinates.setStorageLocation(getProposedUri(context) + "/nifi-registry-api/buckets/" + bucketId + "/flows/" + flowId + "/versions/" + version);
            versionedFlowSnapshot.getFlowContents().setVersionedFlowCoordinates(versionedFlowCoordinates);
            return NifiRegistryUtil.convert(versionedFlowSnapshot);
        } catch (NiFiRegistryException e) {
            throw new FlowRegistryException(e.getMessage(), e);
        }
    }

    @Override
    public Set<RegisteredFlowSnapshotMetadata> getFlowVersions(
            final FlowRegistryClientConfigurationContext context, final String bucketId, final String flowId
    ) throws FlowRegistryException, IOException {
        try {
            final FlowSnapshotClient snapshotClient = getFlowSnapshotClient(context);
            return snapshotClient.getSnapshotMetadata(bucketId, flowId).stream().map(NifiRegistryUtil::convert).collect(Collectors.toSet());
        } catch (NiFiRegistryException e) {
            throw new FlowRegistryException(e.getMessage(), e);
        }
    }

    @Override
    public int getLatestVersion(final FlowRegistryClientConfigurationContext context, final String bucketId, final String flowId) throws FlowRegistryException, IOException {
        try {
            return (int) getFlowClient(context).get(bucketId, flowId).getVersionCount();
        } catch (NiFiRegistryException e) {
            throw new FlowRegistryException(e.getMessage(), e);
        }
    }

    private BucketClient getBucketClient(final FlowRegistryClientConfigurationContext context) {
        final NiFiRegistryClient registryClient = getRegistryClient(context);
        return context.getNiFiUserIdentity().isPresent()
                ? registryClient.getBucketClient(new ProxiedEntityRequestConfig(extractIdentity(context)))
                : registryClient.getBucketClient();
    }

    private FlowSnapshotClient getFlowSnapshotClient(final FlowRegistryClientConfigurationContext context) {
        final NiFiRegistryClient registryClient = getRegistryClient(context);
        return context.getNiFiUserIdentity().isPresent()
                ? registryClient.getFlowSnapshotClient(new ProxiedEntityRequestConfig(extractIdentity(context)))
                : registryClient.getFlowSnapshotClient();
    }

    private FlowClient getFlowClient(final FlowRegistryClientConfigurationContext context) {
        final NiFiRegistryClient registryClient = getRegistryClient(context);
        return context.getNiFiUserIdentity().isPresent()
                ? registryClient.getFlowClient(new ProxiedEntityRequestConfig(extractIdentity(context)))
                : registryClient.getFlowClient();
    }

    private static TlsConfiguration createTlsConfigurationFromContext(final FlowRegistryClientConfigurationContext context) {
        return new StandardTlsConfiguration(context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class).createTlsConfiguration());
    }
}
