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

import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.client.StandardWebClientService;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpResponseStatus;
import org.apache.nifi.web.client.api.WebClientService;
import org.apache.nifi.web.client.ssl.TlsContext;
import org.apache.nifi.web.security.saml2.SamlConfigurationException;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;
import org.springframework.security.saml2.provider.service.registration.RelyingPartyRegistration;
import org.springframework.security.saml2.provider.service.registration.RelyingPartyRegistrations;

import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Standard Registration Builder Provider implementation based on NiFi Application Properties
 */
class StandardRegistrationBuilderProvider implements RegistrationBuilderProvider {
    static final String NIFI_TRUST_STORE_STRATEGY = "NIFI";

    private static final String HTTP_SCHEME_PREFIX = "http";

    private static final String TLS_PROTOCOL = "TLS";

    private static final ResourceLoader resourceLoader = new DefaultResourceLoader();

    private final NiFiProperties properties;

    private final X509KeyManager keyManager;

    private final X509TrustManager trustManager;

    public StandardRegistrationBuilderProvider(
            final NiFiProperties properties,
            final X509KeyManager keyManager,
            final X509TrustManager trustManager
    ) {
        this.properties = Objects.requireNonNull(properties, "Properties required");
        this.keyManager = keyManager;
        this.trustManager = trustManager;
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
        final WebClientService webClientService = getWebClientService();

        final URI uri = URI.create(metadataUrl);

        try {
            final HttpResponseEntity responseEntity = webClientService.get().uri(uri).retrieve();
            final int statusCode = responseEntity.statusCode();

            if (HttpResponseStatus.OK.getCode() == statusCode) {
                return responseEntity.body();
            } else {
                responseEntity.close();
                throw new SamlConfigurationException(String.format("SAML Metadata retrieval failed [%s] HTTP %d", metadataUrl, statusCode));
            }
        } catch (final IOException e) {
            throw new SamlConfigurationException(String.format("SAML Metadata retrieval failed [%s]", metadataUrl), e);
        }
    }

    private WebClientService getWebClientService() {
        final Duration connectTimeout = Duration.ofMillis(
                (long) FormatUtils.getPreciseTimeDuration(properties.getSamlHttpClientConnectTimeout(), TimeUnit.MILLISECONDS)
        );
        final Duration readTimeout = Duration.ofMillis(
                (long) FormatUtils.getPreciseTimeDuration(properties.getSamlHttpClientReadTimeout(), TimeUnit.MILLISECONDS)
        );

        final StandardWebClientService webClientService = new StandardWebClientService();
        webClientService.setConnectTimeout(connectTimeout);
        webClientService.setReadTimeout(readTimeout);

        if (NIFI_TRUST_STORE_STRATEGY.equals(properties.getSamlHttpClientTruststoreStrategy())) {
            webClientService.setTlsContext(new TlsContext() {
                @Override
                public String getProtocol() {
                    return TLS_PROTOCOL;
                }

                @Override
                public X509TrustManager getTrustManager() {
                    return trustManager;
                }

                @Override
                public Optional<X509KeyManager> getKeyManager() {
                    return Optional.ofNullable(keyManager);
                }
            });
        }

        return webClientService;
    }
}
