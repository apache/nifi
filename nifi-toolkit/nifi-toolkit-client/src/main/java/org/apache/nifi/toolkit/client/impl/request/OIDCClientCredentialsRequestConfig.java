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
package org.apache.nifi.toolkit.client.impl.request;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.Form;
import org.apache.nifi.toolkit.client.NiFiClientConfig;
import org.apache.nifi.toolkit.client.RequestConfig;
import org.apache.nifi.toolkit.client.impl.request.util.AccessToken;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

/**
 * Implementation of RequestConfig when using the OAuth Client Credentials Flow
 */
public class OIDCClientCredentialsRequestConfig implements RequestConfig {

    public static final String AUTHORIZATION_HEADER = "Authorization";
    public static final String BEARER = "Bearer";

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);

    private AccessToken token;
    private final NiFiClientConfig niFiClientConfig;
    private final String oidcTokenUrl;
    private final String oidcClientId;
    private final String oidcClientSecret;

    public OIDCClientCredentialsRequestConfig(NiFiClientConfig niFiClientConfig, final String oidcTokenUrl, final String oidcClientId, final String oidcClientSecret) {
        Objects.requireNonNull(oidcTokenUrl);
        Objects.requireNonNull(oidcClientId);
        Objects.requireNonNull(oidcClientSecret);
        Objects.requireNonNull(niFiClientConfig);

        this.niFiClientConfig = niFiClientConfig;
        this.oidcTokenUrl = oidcTokenUrl;
        this.oidcClientId = oidcClientId;
        this.oidcClientSecret = oidcClientSecret;
        setNewToken();
    }

    @Override
    public Map<String, String> getHeaders() {
        if (isRefreshRequired()) {
            setNewToken();
        }
        final Map<String, String> headers = new HashMap<>();
        headers.put(AUTHORIZATION_HEADER, BEARER + " " + token.getAccessToken());
        return headers;
    }

    private void setNewToken() {
        final SSLContext sslContext = niFiClientConfig.getSslContext();
        final HostnameVerifier hostnameVerifier = niFiClientConfig.getHostnameVerifier();

        final ClientBuilder clientBuilder = ClientBuilder.newBuilder();
        if (sslContext != null) {
            clientBuilder.sslContext(sslContext);
        }
        if (hostnameVerifier != null) {
            clientBuilder.hostnameVerifier(hostnameVerifier);
        }

        final Form form = new Form();
        form.param("grant_type", "client_credentials");
        form.param("client_id", oidcClientId);
        form.param("client_secret", oidcClientSecret);

        final WebTarget target = clientBuilder.build().target(oidcTokenUrl);
        final String response = target.request().post(Entity.form(form), String.class);

        try {
            this.token = OBJECT_MAPPER.readValue(response, AccessToken.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isRefreshRequired() {
        final Instant expirationRefreshTime = token.getFetchTime()
                .plusSeconds(token.getExpiresIn())
                .minusSeconds(60); // Refresh 60 seconds before expiration
        return Instant.now().isAfter(expirationRefreshTime);
    }
}
