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
package org.apache.nifi.web.security.configuration;

import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.oidc.registration.ClientRegistrationProvider;
import org.apache.nifi.web.security.oidc.registration.DisabledClientRegistrationRepository;
import org.apache.nifi.web.security.oidc.registration.StandardClientRegistrationProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.security.oauth2.client.http.OAuth2ErrorResponseErrorHandler;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.oauth2.client.registration.InMemoryClientRegistrationRepository;
import org.springframework.security.oauth2.core.http.converter.OAuth2AccessTokenResponseHttpMessageConverter;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestTemplate;

import javax.net.ssl.SSLContext;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * OpenID Connect Client Registration configuration with supporting components
 */
@Configuration
public class ClientRegistrationConfiguration {

    private static final Duration DEFAULT_SOCKET_TIMEOUT = Duration.ofSeconds(5);

    private static final String NIFI_TRUSTSTORE_STRATEGY = "NIFI";

    private final NiFiProperties properties;

    private final SSLContext sslContext;

    public ClientRegistrationConfiguration(
            @Autowired final NiFiProperties properties,
            @Autowired(required = false) final SSLContext sslContext
    ) {
        this.properties = Objects.requireNonNull(properties, "Application properties required");
        this.sslContext = sslContext;
    }

    /**
     * Client Registration Repository for OpenID Connect Discovery
     *
     * @return Client Registration Repository
     */
    @Bean
    public ClientRegistrationRepository clientRegistrationRepository() {
        final ClientRegistrationRepository clientRegistrationRepository;
        if (properties.isOidcEnabled()) {
            final ClientRegistrationProvider clientRegistrationProvider = new StandardClientRegistrationProvider(properties, oidcRestClient());
            final ClientRegistration clientRegistration = clientRegistrationProvider.getClientRegistration();
            clientRegistrationRepository = new InMemoryClientRegistrationRepository(clientRegistration);
        } else {
            clientRegistrationRepository = new DisabledClientRegistrationRepository();
        }
        return clientRegistrationRepository;
    }

    /**
     * OpenID Connect REST Client for communication with Authorization Servers
     *
     * @return REST Client
     */
    @Bean
    public RestClient oidcRestClient() {
        return RestClient.create(oidcRestOperations());
    }

    /**
     * OpenID Connect REST Operations for communication with Authorization Servers
     *
     * @return REST Operations
     */
    @Bean
    public RestTemplate oidcRestOperations() {
        final RestTemplate restTemplate = new RestTemplate(oidcClientHttpRequestFactory());
        restTemplate.setErrorHandler(new OAuth2ErrorResponseErrorHandler());
        restTemplate.setMessageConverters(
                Arrays.asList(
                        new FormHttpMessageConverter(),
                        new OAuth2AccessTokenResponseHttpMessageConverter(),
                        new StringHttpMessageConverter(),
                        new MappingJackson2HttpMessageConverter()
                )
        );
        return restTemplate;
    }

    /**
     * OpenID Connect Client HTTP Request Factory for communication with Authorization Servers
     *
     * @return Client HTTP Request Factory
     */
    @Bean
    public ClientHttpRequestFactory oidcClientHttpRequestFactory() {
        final HttpClient httpClient = getHttpClient();
        final JdkClientHttpRequestFactory clientHttpRequestFactory = new JdkClientHttpRequestFactory(httpClient);
        final Duration readTimeout = getTimeout(properties.getOidcReadTimeout());
        clientHttpRequestFactory.setReadTimeout(readTimeout);
        return clientHttpRequestFactory;
    }

    private HttpClient getHttpClient() {
        final Duration connectTimeout = getTimeout(properties.getOidcConnectTimeout());
        final HttpClient.Builder builder = HttpClient.newBuilder().connectTimeout(connectTimeout);

        if (NIFI_TRUSTSTORE_STRATEGY.equals(properties.getOidcClientTruststoreStrategy())) {
            builder.sslContext(sslContext);
        }

        return builder.build();
    }

    private Duration getTimeout(final String timeoutExpression) {
        try {
            final double duration = FormatUtils.getPreciseTimeDuration(timeoutExpression, TimeUnit.MILLISECONDS);
            final long rounded = Math.round(duration);
            return Duration.ofMillis(rounded);
        } catch (final RuntimeException e) {
            return DEFAULT_SOCKET_TIMEOUT;
        }
    }
}
