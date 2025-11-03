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
package org.apache.nifi.services.iceberg.catalog;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.nifi.logging.ComponentLog;

import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Standard implementation of REST Client Provider supporting configurable Access Delegation
 */
class StandardRESTClientProvider implements RESTClientProvider {
    /** Access Delegation HTTP Request Header added in Apache Iceberg 1.5.0 */
    static final String ICEBERG_ACCESS_DELEGATION_HEADER = "X-Iceberg-Access-Delegation";

    private static final Pattern CLIENT_CREDENTIALS_PATTERN = Pattern.compile("^([^:]+):(.+)$");

    private static final int CLIENT_ID_GROUP = 1;

    private static final int CLIENT_SECRET_GROUP = 2;

    private final ComponentLog log;

    StandardRESTClientProvider(final ComponentLog log) {
        this.log = Objects.requireNonNull(log, "Component Log required");
    }

    @Override
    public RESTClient build(final Map<String, String> properties) {
        Objects.requireNonNull(properties, "Properties required");
        final String uri = properties.get(CatalogProperties.URI);

        // Set empty Authentication Session to avoid runtime exceptions in BaseHTTPClient class
        final HTTPClient.Builder builder = HTTPClient.builder(properties).uri(uri).withAuthSession(AuthSession.EMPTY);

        if (properties.containsKey(ICEBERG_ACCESS_DELEGATION_HEADER)) {
            final String accessDelegationHeader = properties.get(ICEBERG_ACCESS_DELEGATION_HEADER);
            builder.withHeader(ICEBERG_ACCESS_DELEGATION_HEADER, accessDelegationHeader);
        }

        final RESTClient restClient;

        if (properties.containsKey(OAuth2Properties.CREDENTIAL)) {
            final String credential = properties.get(OAuth2Properties.CREDENTIAL);
            final Matcher clientCredentialsMatcher = CLIENT_CREDENTIALS_PATTERN.matcher(credential);
            if (clientCredentialsMatcher.matches()) {
                final String clientId = clientCredentialsMatcher.group(CLIENT_ID_GROUP);
                final String clientSecret = clientCredentialsMatcher.group(CLIENT_SECRET_GROUP);
                restClient = new CredentialsRefreshRESTClient(log, builder.build(), clientId, clientSecret);
            } else {
                throw new IllegalStateException("OAuth 2 Client Credentials format not found");
            }
        } else {
            restClient = builder.build();
        }

        return restClient;
    }
}
