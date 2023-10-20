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

    public SchemaRegistryApiClient(final WebClientServiceProvider webClientServiceProvider, final String baseUrl) {
        this.webClientServiceProvider = webClientServiceProvider;
        this.baseUrl = baseUrl;
    }

    public InputStream retrieveResponse(URI uri) {
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

    public URI buildSearchUri(final String schemaName) {
        return buildBaseUri()
                .addPathSegment("search")
                .addPathSegment("artifacts")
                .addQueryParameter("name", schemaName)
                .addQueryParameter("limit", "1")
                .build();
    }

    public URI buildMetaDataUri(final String groupId, final String artifactId) {
        return buildGroupArtifactsUri(groupId, artifactId)
                .addPathSegment("meta")
                .build();
    }

    public URI buildSchemaUri(final String groupId, final String artifactId) {
        return buildGroupArtifactsUri(groupId, artifactId).build();
    }

    private HttpUriBuilder buildGroupArtifactsUri(final String groupId, final String artifactId) {
        return buildBaseUri()
                .addPathSegment("groups")
                .addPathSegment(groupId)
                .addPathSegment("artifacts")
                .addPathSegment(artifactId);
    }

}
