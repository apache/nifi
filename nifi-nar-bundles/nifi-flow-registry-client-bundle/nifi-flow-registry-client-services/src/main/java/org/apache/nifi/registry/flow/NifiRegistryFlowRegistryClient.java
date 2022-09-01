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

import org.apache.http.client.utils.URIBuilder;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
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
import org.apache.nifi.security.util.KeystoreType;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsException;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
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
    public final static PropertyDescriptor PROPERTY_KEYSTORE_PATH = new PropertyDescriptor.Builder()
            .name("keystorePath")
            .displayName("Keystore Path")
            .description("The fully-qualified filename of the Keystore")
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .required(false)
            .build();
    public final static PropertyDescriptor PROPERTY_KEYSTORE_PASSWORD = new PropertyDescriptor.Builder()
            .name("keystorePassword")
            .displayName("Keystore Password")
            .description("The password for the Keystore")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .required(false)
            .build();
    public final static PropertyDescriptor PROPERTY_KEY_PASSWORD = new PropertyDescriptor.Builder()
            .name("keyPassword")
            .displayName("Key Password")
            .description("The password for the key. If this is not specified, but the Keystore Filename, Password, and Type are specified, "
                    + "then the Keystore Password will be assumed to be the same as the Key Password.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .required(false)
            .build();
    public final static PropertyDescriptor PROPERTY_KEYSTORE_TYPE = new PropertyDescriptor.Builder()
            .name("keystoreType")
            .displayName("Keystore Type")
            .description("The Type of the Keystore")
            .allowableValues(KeystoreType.values())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();
    public final static PropertyDescriptor PROPERTY_TRUSTSTORE_PATH = new PropertyDescriptor.Builder()
            .name("truststorePath")
            .displayName("Truststore Path")
            .description("The fully-qualified filename of the Truststore")
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .required(false)
            .build();
    public final static PropertyDescriptor PROPERTY_TRUSTSTORE_PASSWORD = new PropertyDescriptor.Builder()
            .name("truststorePassword")
            .displayName("Truststore Password")
            .description("The password for the Truststore")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .required(false)
            .build();
    public final static PropertyDescriptor PROPERTY_TRUSTSTORE_TYPE = new PropertyDescriptor.Builder()
            .name("truststoreType")
            .displayName("Truststore Type")
            .description("The Type of the Truststore")
            .allowableValues(KeystoreType.values())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    private volatile String registryUrl;
    private volatile NiFiRegistryClient registryClient;

    private synchronized NiFiRegistryClient getRegistryClient(final FlowRegistryClientConfigurationContext context) {
        final String configuredUrl = context.getProperty(PROPERTY_URL).evaluateAttributeExpressions().getValue();

        final URI uri;

        try {
            // Handles case where the URI entered has a trailing slash, or includes the trailing /nifi-registry-api
            uri = new URIBuilder(configuredUrl).setPath("").removeQuery().build();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("The given Registry URL is not valid: " + configuredUrl);
        }

        final String uriScheme = uri.getScheme();
        if (uriScheme == null) {
            throw new IllegalArgumentException("The given Registry URL is not valid: " + configuredUrl);
        }

        final String proposedUrl = uri.toString();;

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

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(
                PROPERTY_URL,
                PROPERTY_KEYSTORE_PATH,
                PROPERTY_KEYSTORE_TYPE,
                PROPERTY_KEYSTORE_PASSWORD,
                PROPERTY_KEY_PASSWORD,
                PROPERTY_TRUSTSTORE_PATH,
                PROPERTY_TRUSTSTORE_TYPE,
                PROPERTY_TRUSTSTORE_PASSWORD
        );
    }

    private synchronized void invalidateClient() {
        this.registryClient = null;
    }

    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final Collection<ValidationResult> result = new HashSet<>();

        boolean isKeystorePathSet = context.getProperty(PROPERTY_KEYSTORE_PATH).isSet();
        boolean isKeystorePassword = context.getProperty(PROPERTY_KEYSTORE_PASSWORD).isSet();
        boolean isKeyPassword = context.getProperty(PROPERTY_KEY_PASSWORD).isSet();
        boolean isKeystoreType = context.getProperty(PROPERTY_KEYSTORE_TYPE).isSet();
        boolean isTruststorePath = context.getProperty(PROPERTY_TRUSTSTORE_PATH).isSet();
        boolean isTruststorePassword = context.getProperty(PROPERTY_TRUSTSTORE_PASSWORD).isSet();
        boolean isTruststoreType = context.getProperty(PROPERTY_TRUSTSTORE_TYPE).isSet();

        boolean sslContextIsSet = isKeystorePathSet && isKeystorePassword && isKeyPassword && isKeystoreType && isTruststorePath && isTruststorePassword && isTruststoreType;
        boolean sslContextIsNotSet = !(isKeystorePathSet || isKeystorePassword || isKeyPassword || isKeystoreType || isTruststorePath || isTruststorePassword || isTruststoreType);

        if (!(sslContextIsSet || sslContextIsNotSet)) {
            result.add(new ValidationResult.Builder().subject(this.getClass().getSimpleName())
                .valid(false)
                .explanation("It is expected to either set all the properties for the SSLContext or set none")
                .build());
        }

        return result;
    }

    private String extractIdentity(final FlowRegistryClientConfigurationContext context) {
        return context.getNiFiUserIdentity().orElse(null);
    }

    private SSLContext extractSSLContext(final FlowRegistryClientConfigurationContext context) {
        if (context.getProperty(PROPERTY_KEYSTORE_PATH).isSet()) {
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

            if (flowClient.getByBucket(flow.getBucketIdentifier()).stream().map(f -> f.getName()).collect(Collectors.toSet()).contains(flow.getName())) {
                throw new FlowAlreadyExistsException(String.format("Flow %s within bucket %s already exists", flow.getName(), flow.getBucketName()));
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
            return NifiRegistryUtil.convert(snapshotClient.create(NifiRegistryUtil.convert(flowSnapshot)));
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
        String keystorePath = context.getProperty(PROPERTY_KEYSTORE_PATH).getValue();
        String keystorePassword = context.getProperty(PROPERTY_KEYSTORE_PASSWORD).getValue();
        String keyPassword = context.getProperty(PROPERTY_KEY_PASSWORD).getValue();
        String keystoreType = context.getProperty(PROPERTY_KEYSTORE_TYPE).getValue();
        String truststorePath = context.getProperty(PROPERTY_TRUSTSTORE_PATH).getValue();
        String truststorePassword = context.getProperty(PROPERTY_TRUSTSTORE_PASSWORD).getValue();
        String truststoreType = context.getProperty(PROPERTY_TRUSTSTORE_TYPE).getValue();
        return new StandardTlsConfiguration(keystorePath, keystorePassword, keyPassword, keystoreType, truststorePath, truststorePassword, truststoreType, TlsConfiguration.TLS_PROTOCOL);
    }
}
