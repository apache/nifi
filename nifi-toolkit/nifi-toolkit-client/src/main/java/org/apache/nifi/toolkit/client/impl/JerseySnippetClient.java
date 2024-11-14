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

package org.apache.nifi.toolkit.client.impl;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.toolkit.client.RequestConfig;
import org.apache.nifi.toolkit.client.SnippetClient;
import org.apache.nifi.web.api.entity.SnippetEntity;

import java.io.IOException;

public class JerseySnippetClient extends AbstractJerseyClient implements SnippetClient {
    private final WebTarget snippetTarget;

    public JerseySnippetClient(final WebTarget baseTarget) {
        this(baseTarget, null);
    }

    public JerseySnippetClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
        super(requestConfig);
        this.snippetTarget = baseTarget.path("/snippets");
    }

    @Override
    public SnippetEntity createSnippet(final SnippetEntity snippet) throws NiFiClientException, IOException {
        if (snippet == null) {
            throw new IllegalArgumentException("Snippet entity cannot be null");
        }

        return executeAction("Error creating snippet", () -> getRequestBuilder(snippetTarget).post(
                Entity.entity(snippet, MediaType.APPLICATION_JSON),
                SnippetEntity.class));
    }

    @Override
    public SnippetEntity updateSnippet(final SnippetEntity snippet) throws NiFiClientException, IOException {
        if (snippet == null) {
            throw new IllegalArgumentException("Snippet entity cannot be null");
        }

        return executeAction("Error updating snippet", () -> {
            final WebTarget target = snippetTarget
                    .path("/{id}")
                    .resolveTemplate("id", snippet.getSnippet().getId());

            final Entity<SnippetEntity> entity = Entity.entity(snippet, MediaType.APPLICATION_JSON);
            return getRequestBuilder(target).put(entity, SnippetEntity.class);
        });
    }
}
