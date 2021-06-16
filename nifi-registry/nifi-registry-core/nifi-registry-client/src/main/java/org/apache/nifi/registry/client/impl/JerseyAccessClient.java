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
package org.apache.nifi.registry.client.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.client.AccessClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.RequestConfig;
import org.apache.nifi.registry.client.impl.request.BasicAuthRequestConfig;
import org.apache.nifi.registry.client.impl.request.BearerTokenRequestConfig;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import java.io.IOException;
import java.util.Map;

/**
 * Jersey implementation of AccessClient.
 */
public class JerseyAccessClient extends AbstractJerseyClient implements AccessClient {

    private final WebTarget accessTarget;

    public JerseyAccessClient(final WebTarget baseTarget) {
        super(null);
        this.accessTarget = baseTarget.path("/access");
    }

    @Override
    public String getToken(final String username, final String password) throws NiFiRegistryException, IOException {
        if (StringUtils.isBlank(username)) {
            throw new IllegalArgumentException("Username is required");
        }

        if (StringUtils.isBlank(password)) {
            throw new IllegalArgumentException("Password is required");
        }

        return executeAction("Error performing login", () -> {
            final WebTarget target = accessTarget.path("token/login");
            final Invocation.Builder requestBuilder = getRequestBuilder(target);

            final RequestConfig basicCredsConfig = new BasicAuthRequestConfig(username, password);
            final Map<String,String> basicAuthHeaders = basicCredsConfig.getHeaders();
            basicAuthHeaders.entrySet().stream().forEach(e -> requestBuilder.header(e.getKey(), e.getValue()));

            return requestBuilder.post(Entity.json(null), String.class);
        });
    }

    @Override
    public String getTokenFromKerberosTicket() throws NiFiRegistryException, IOException {
        return executeAction("Error performing kerberos login", () -> {
            final WebTarget target = accessTarget.path("token/kerberos");
            return getRequestBuilder(target).post(Entity.json(null), String.class);
        });
    }

    @Override
    public void logout(final String token) throws IOException, NiFiRegistryException {
        if (StringUtils.isBlank(token)) {
            throw new IllegalArgumentException("Token is required");
        }

        executeAction("Error performing logout", () -> {
            final WebTarget target = accessTarget.path("logout");
            final Invocation.Builder requestBuilder = getRequestBuilder(target);

            final RequestConfig tokenConfig = new BearerTokenRequestConfig(token);
            final Map<String,String> bearerHeaders = tokenConfig.getHeaders();
            bearerHeaders.entrySet().stream().forEach(e -> requestBuilder.header(e.getKey(), e.getValue()));

            requestBuilder.delete();
            return null;
        });
    }
}
