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

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.Form;
import org.apache.nifi.toolkit.client.NiFiClientConfig;
import org.apache.nifi.toolkit.client.RequestConfig;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Implementation of RequestConfig when using the OAuth Client Credentials Flow
 */
public class OIDCClientCredentialsRequestConfig implements RequestConfig {

    public static final String AUTHORIZATION_HEADER = "Authorization";
    public static final String BEARER = "Bearer";

    private final String token;

    public OIDCClientCredentialsRequestConfig(NiFiClientConfig niFiClientConfig, final String oidcTokenUrl, final String oidcClientId, final String oidcClientSecret) {
        Objects.requireNonNull(oidcTokenUrl);
        Objects.requireNonNull(oidcClientId);
        Objects.requireNonNull(oidcClientSecret);
        Objects.requireNonNull(niFiClientConfig);

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

        ObjectMapper mapper = new ObjectMapper();
        try {
            this.token = mapper.readTree(response).get("access_token").textValue();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, String> getHeaders() {
        final Map<String, String> headers = new HashMap<>();
        headers.put(AUTHORIZATION_HEADER, BEARER + " " + token);
        return headers;
    }
}
