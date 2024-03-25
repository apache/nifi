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
package org.apache.nifi.apicurio.schemaregistry.client;

import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

import java.io.InputStream;
import java.net.URI;

public class SchemaRegistryApiClient {

    private final WebClientServiceProvider webClientServiceProvider;
    private final String baseUrl;
    private final String groupId;

    public SchemaRegistryApiClient(final WebClientServiceProvider webClientServiceProvider, final String baseUrl, final String groupId) {
        this.webClientServiceProvider = webClientServiceProvider;
        this.baseUrl = baseUrl;
        this.groupId = groupId;
    }

    public InputStream retrieveResponse(final URI uri) {
        return webClientServiceProvider.getWebClientService()
                .get()
                .uri(uri)
                .retrieve()
                .body();
    }

    public HttpUriBuilder buildBaseUri() {
        final URI uri = URI.create(baseUrl);
        return webClientServiceProvider.getHttpUriBuilder()
                .scheme(uri.getScheme())
                .host(uri.getHost())
                .port(uri.getPort())
                .addPathSegment("apis")
                .addPathSegment("registry")
                .addPathSegment("v2");
    }

    private HttpUriBuilder buildBaseSchemaUri() {
        return buildBaseUri()
                .addPathSegment("groups")
                .addPathSegment(this.groupId);
    }

    private HttpUriBuilder buildBaseSchemaArtifactUri(final String artifactId) {
        return buildBaseSchemaUri()
                .addPathSegment("artifacts")
                .addPathSegment(artifactId);
    }

    public URI buildSchemaArtifactUri(final String artifactId) {
        return buildBaseSchemaArtifactUri(artifactId).build();
    }

    public URI buildSchemaVersionUri(final String artifactId, final int version) {
        return buildBaseSchemaArtifactUri(artifactId)
                .addPathSegment("versions")
                .addPathSegment(String.valueOf(version))
                .build();
    }
}
