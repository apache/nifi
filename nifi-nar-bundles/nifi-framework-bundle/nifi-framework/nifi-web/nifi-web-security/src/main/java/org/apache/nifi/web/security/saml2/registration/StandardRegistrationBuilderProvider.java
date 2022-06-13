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
package org.apache.nifi.web.security.saml2.registration;

import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.saml2.SamlConfigurationException;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;
import org.springframework.security.saml2.provider.service.registration.RelyingPartyRegistration;
import org.springframework.security.saml2.provider.service.registration.RelyingPartyRegistrations;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Standard Registration Builder Provider implementation based on NiFi Application Properties
 */
class StandardRegistrationBuilderProvider implements RegistrationBuilderProvider {
    static final String NIFI_TRUST_STORE_STRATEGY = "NIFI";

    private static final String HTTP_SCHEME_PREFIX = "http";

    private static final ResourceLoader resourceLoader = new DefaultResourceLoader();

    private final NiFiProperties properties;

    public StandardRegistrationBuilderProvider(final NiFiProperties properties) {
        this.properties = Objects.requireNonNull(properties, "Properties required");
    }

    /**
     * Get Registration Builder from configured location supporting local files or HTTP services
     *
     * @return Registration Builder
     */
    @Override
    public RelyingPartyRegistration.Builder getRegistrationBuilder() {
        final String metadataUrl = Objects.requireNonNull(properties.getSamlIdentityProviderMetadataUrl(), "Metadata URL required");

        try (final InputStream inputStream = getInputStream(metadataUrl)) {
            return RelyingPartyRegistrations.fromMetadata(inputStream);
        } catch (final IOException e) {
            throw new SamlConfigurationException(String.format("SAML Metadata loading failed [%s]", metadataUrl), e);
        }
    }

    private InputStream getInputStream(final String metadataUrl) throws IOException {
        final InputStream inputStream;
        if (metadataUrl.startsWith(HTTP_SCHEME_PREFIX)) {
            inputStream = getRemoteInputStream(metadataUrl);
        } else {
            inputStream = resourceLoader.getResource(metadataUrl).getInputStream();
        }
        return inputStream;
    }

    private InputStream getRemoteInputStream(final String metadataUrl) {
        final OkHttpClient client = getHttpClient();

        final Request request = new Request.Builder().get().url(metadataUrl).build();
        final Call call = client.newCall(request);
        try {
            final Response response = call.execute();
            if (response.isSuccessful()) {
                final ResponseBody body = Objects.requireNonNull(response.body(), "SAML Metadata response not found");
                return body.byteStream();
            } else {
                response.close();
                throw new SamlConfigurationException(String.format("SAML Metadata retrieval failed [%s] HTTP %d", metadataUrl, response.code()));
            }
        } catch (final IOException e) {
            throw new SamlConfigurationException(String.format("SAML Metadata retrieval failed [%s]", metadataUrl), e);
        }
    }

    private OkHttpClient getHttpClient() {
        final Duration connectTimeout = Duration.ofMillis(
                (long) FormatUtils.getPreciseTimeDuration(properties.getSamlHttpClientConnectTimeout(), TimeUnit.MILLISECONDS)
        );
        final Duration readTimeout = Duration.ofMillis(
                (long) FormatUtils.getPreciseTimeDuration(properties.getSamlHttpClientReadTimeout(), TimeUnit.MILLISECONDS)
        );

        final OkHttpClient.Builder builder = new OkHttpClient.Builder()
                .connectTimeout(connectTimeout)
                .readTimeout(readTimeout);

        if (NIFI_TRUST_STORE_STRATEGY.equals(properties.getSamlHttpClientTruststoreStrategy())) {
            setSslSocketFactory(builder);
        }

        return builder.build();
    }

    private void setSslSocketFactory(final OkHttpClient.Builder builder) {
        final TlsConfiguration tlsConfiguration = StandardTlsConfiguration.fromNiFiProperties(properties);

        try {
            final X509TrustManager trustManager = Objects.requireNonNull(SslContextFactory.getX509TrustManager(tlsConfiguration), "TrustManager required");
            final TrustManager[] trustManagers = new TrustManager[] { trustManager };
            final SSLContext sslContext = Objects.requireNonNull(SslContextFactory.createSslContext(tlsConfiguration, trustManagers), "SSLContext required");
            final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();
            builder.sslSocketFactory(sslSocketFactory, trustManager);
        } catch (final TlsException e) {
            throw new SamlConfigurationException("SAML Metadata HTTP TLS configuration failed", e);
        }
    }
}
