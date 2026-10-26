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
package org.apache.nifi.kubernetes.client;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.http.HttpClient;
import io.fabric8.kubernetes.client.internal.CertUtils;
import io.fabric8.kubernetes.client.jdkhttp.JdkHttpClientFactory;
import io.fabric8.kubernetes.client.utils.Utils;
import org.apache.nifi.security.ssl.PemCertificateKeyStoreBuilder;
import org.apache.nifi.security.ssl.StandardTrustManagerBuilder;

import java.io.InputStream;
import java.security.KeyStore;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;

/**
 * Kubernetes HTTP Client Factory that builds the SSL Context from the configured Certificate Authority using default Key Store Type
 */
public final class KubernetesHttpClientFactory extends JdkHttpClientFactory {

    /** Priority higher than the Fabric8 JDK implementation, which uses the interface default of 0 */
    private static final int PRIORITY = 1;

    /**
     * Get factory priority indicating precedence over the default Kubernetes Client implementations
     *
     * @return factory priority
     */
    @Override
    public int priority() {
        return PRIORITY;
    }

    /**
     * Create an HTTP Client Builder configured with an SSL Context containing a Trust Manager built from the configured
     * Certificate Authority, or the default builder when the default SSL Context handling is required
     *
     * @param config Kubernetes Client configuration
     * @return HTTP Client Builder
     */
    @Override
    public HttpClient.Builder newBuilder(final Config config) {
        final HttpClient.Builder builder;

        if (isDefaultSslContextRequired(config)) {
            builder = super.newBuilder(config);
        } else {
            builder = super.newBuilder(getConfigWithoutCertificateAuthority(config));

            final TrustManager[] trustManagers = getTrustManagers(config);
            builder.sslContext(null, trustManagers);
        }

        return builder;
    }

    /**
     * Determine whether the default SSL Context handling is required instead of the Certificate Authority Trust
     * Managers built from PEM certificates
     *
     * @param config Kubernetes Client configuration
     * @return whether the default SSL Context handling is required
     */
    static boolean isDefaultSslContextRequired(final Config config) {
        final boolean trustStoreConfigured = Utils.isNotNullOrEmpty(config.getTrustStoreFile());
        final boolean certificateAuthorityNotConfigured = Utils.isNullOrEmpty(config.getCaCertData()) && Utils.isNullOrEmpty(config.getCaCertFile());
        // Client Certificates require default SSLContext handling to support different types of Private Keys
        final boolean clientCertificateConfigured = Utils.isNotNullOrEmpty(config.getClientCertData()) || Utils.isNotNullOrEmpty(config.getClientCertFile());

        return config.isTrustCerts() || trustStoreConfigured || certificateAuthorityNotConfigured || clientCertificateConfigured;
    }

    /**
     * Copy the client configuration without the Certificate Authority to avoid default trust store handling
     *
     * @param config Kubernetes Client configuration
     * @return Kubernetes Client configuration without Certificate Authority properties
     */
    static Config getConfigWithoutCertificateAuthority(final Config config) {
        // Disable automatic configuration when copying to avoid restoring the Certificate Authority from environment sources
        final Config configWithoutCertificateAuthority = new ConfigBuilder(config)
                .withAutoConfigure(false)
                .withCaCertData(null)
                .withCaCertFile(null)
                .build();

        // Restore the automatic configuration status required for refreshing Service Account tokens
        configWithoutCertificateAuthority.setAutoConfigure(config.getAutoConfigure());

        return configWithoutCertificateAuthority;
    }

    /**
     * Get Trust Managers containing the certificates read from the configured Certificate Authority
     *
     * @param config Kubernetes Client configuration
     * @return Trust Managers for the configured Certificate Authority
     */
    static TrustManager[] getTrustManagers(final Config config) {
        final KeyStore trustStore = getTrustStore(config);
        final X509ExtendedTrustManager trustManager = new StandardTrustManagerBuilder().trustStore(trustStore).build();
        return new TrustManager[] {trustManager};
    }

    /**
     * Get a Trust Store containing the certificates read from the configured Certificate Authority
     *
     * @param config Kubernetes Client configuration
     * @return Trust Store for the configured Certificate Authority
     */
    static KeyStore getTrustStore(final Config config) {
        try (InputStream inputStream = CertUtils.getInputStreamFromDataOrFile(config.getCaCertData(), config.getCaCertFile())) {
            return new PemCertificateKeyStoreBuilder().inputStream(inputStream).build();
        } catch (final Exception e) {
            throw new KubernetesClientException("Kubernetes Client Certificate Authority reading failed", e);
        }
    }
}
