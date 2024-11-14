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
import jakarta.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.toolkit.client.ParamContextClient;
import org.apache.nifi.toolkit.client.RequestConfig;
import org.apache.nifi.web.api.entity.AssetEntity;
import org.apache.nifi.web.api.entity.AssetsEntity;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterContextUpdateRequestEntity;
import org.apache.nifi.web.api.entity.ParameterContextsEntity;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class JerseyParamContextClient extends AbstractJerseyClient implements ParamContextClient {

    private final WebTarget flowTarget;
    private final WebTarget paramContextTarget;

    public JerseyParamContextClient(final WebTarget baseTarget) {
        this(baseTarget, null);
    }

    public JerseyParamContextClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
        super(requestConfig);
        this.flowTarget = baseTarget.path("/flow");
        this.paramContextTarget = baseTarget.path("/parameter-contexts");
    }

    @Override
    public ParameterContextsEntity getParamContexts() throws NiFiClientException, IOException {
        return executeAction("Error retrieving parameter contexts", () -> {
            final WebTarget target = flowTarget.path("/parameter-contexts");
            return getRequestBuilder(target).get(ParameterContextsEntity.class);
        });
    }

    @Override
    public ParameterContextEntity getParamContext(final String id, final boolean includeInheritedParameters) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Parameter context id cannot be null or blank");
        }

        return executeAction("Error retrieving parameter context", () -> {
            final WebTarget target = paramContextTarget.path("{id}")
                    .resolveTemplate("id", id)
                    .queryParam("includeInheritedParameters", String.valueOf(includeInheritedParameters));
            return getRequestBuilder(target).get(ParameterContextEntity.class);
        });
    }

    @Override
    public ParameterContextEntity createParamContext(final ParameterContextEntity paramContext) throws NiFiClientException, IOException {
        if (paramContext == null) {
            throw new IllegalArgumentException("Parameter context cannot be null or blank");
        }

        return executeAction("Error creating parameter context", () -> {
            final WebTarget target = paramContextTarget;
            return getRequestBuilder(target).post(
                    Entity.entity(paramContext, MediaType.APPLICATION_JSON),
                    ParameterContextEntity.class);
        });
    }

    @Override
    public ParameterContextEntity deleteParamContext(final String id, final String version) throws NiFiClientException, IOException {
        return deleteParamContext(id, version, false);
    }

    @Override
    public ParameterContextEntity deleteParamContext(final String id, final String version, final boolean disconnectedNodeAcknowledged) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Parameter context id cannot be null or blank");
        }

        if (StringUtils.isBlank(version)) {
            throw new IllegalArgumentException("Version cannot be null or blank");
        }

        return executeAction("Error deleting parameter context", () -> {
            WebTarget target = paramContextTarget.path("{id}")
                    .resolveTemplate("id", id)
                    .queryParam("version", version);

            if (disconnectedNodeAcknowledged) {
                target = target.queryParam("disconnectedNodeAcknowledged", "true");
            }

            return getRequestBuilder(target).delete(ParameterContextEntity.class);
        });
    }

    @Override
    public ParameterContextUpdateRequestEntity updateParamContext(final ParameterContextEntity paramContext)
            throws NiFiClientException, IOException {
        if (paramContext == null) {
            throw new IllegalArgumentException("Parameter context entity cannot be null");
        }

        if (paramContext.getComponent() == null) {
            throw new IllegalArgumentException("Parameter context DTO cannot be null");
        }

        final String paramContextId = paramContext.getComponent().getId();
        if (StringUtils.isBlank(paramContextId)) {
            throw new IllegalArgumentException("Parameter context id cannot be null or blank");
        }

        return executeAction("Error creating parameter context update request", () -> {
            final WebTarget target = paramContextTarget.path("{id}/update-requests")
                    .resolveTemplate("id", paramContextId);
            return getRequestBuilder(target).post(
                    Entity.entity(paramContext, MediaType.APPLICATION_JSON),
                    ParameterContextUpdateRequestEntity.class);
        });
    }

    @Override
    public ParameterContextUpdateRequestEntity getParamContextUpdateRequest(final String contextId, final String updateRequestId)
            throws NiFiClientException, IOException {
        if (StringUtils.isBlank(updateRequestId)) {
            throw new IllegalArgumentException("Parameter context update request id cannot be null or blank");
        }

        return executeAction("Error retrieving parameter context", () -> {
            final WebTarget target = paramContextTarget.path("{context-id}/update-requests/{request-id}")
                    .resolveTemplate("context-id", contextId)
                    .resolveTemplate("request-id", updateRequestId);
            return getRequestBuilder(target).get(ParameterContextUpdateRequestEntity.class);
        });
    }

    @Override
    public ParameterContextUpdateRequestEntity deleteParamContextUpdateRequest(final String contextId, final String updateRequestId)
            throws NiFiClientException, IOException {
        if (StringUtils.isBlank(updateRequestId)) {
            throw new IllegalArgumentException("Parameter context update request id cannot be null or blank");
        }

        return executeAction("Error deleting parameter context update request", () -> {
            final WebTarget target = paramContextTarget.path("{context-id}/update-requests/{request-id}")
                    .resolveTemplate("context-id", contextId)
                    .resolveTemplate("request-id", updateRequestId);
            return getRequestBuilder(target).delete(ParameterContextUpdateRequestEntity.class);
        });
    }

    @Override
    public AssetEntity createAsset(final String contextId, final String assetName, final File file) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(contextId)) {
            throw new IllegalArgumentException("Parameter context id cannot be null or blank");
        }
        if (StringUtils.isBlank(assetName)) {
            throw new IllegalArgumentException("Asset name cannot be null or blank");
        }
        if (file == null) {
            throw new IllegalArgumentException("File cannot be null");
        }
        if (!file.exists()) {
            throw new FileNotFoundException(file.getAbsolutePath());
        }

        try (final InputStream assetInputStream = new FileInputStream(file)) {
            return executeAction("Error Creating Asset " + assetName + " for Parameter Context " + contextId, () -> {
                final WebTarget target = paramContextTarget.path("{context-id}/assets")
                        .resolveTemplate("context-id", contextId);

                return getRequestBuilder(target)
                        .header("Filename", assetName)
                        .post(
                                Entity.entity(assetInputStream, MediaType.APPLICATION_OCTET_STREAM_TYPE),
                                AssetEntity.class);
            });
        }
    }

    @Override
    public AssetsEntity getAssets(final String contextId) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(contextId)) {
            throw new IllegalArgumentException("Parameter context id cannot be null or blank");
        }
        return executeAction("Error retrieving parameter context assets", () -> {
            final WebTarget target = paramContextTarget.path("{context-id}/assets")
                    .resolveTemplate("context-id", contextId);
            return getRequestBuilder(target).get(AssetsEntity.class);
        });
    }

    @Override
    public Path getAssetContent(final String contextId, final String assetId, final File outputDirectory)
            throws NiFiClientException, IOException {
        if (StringUtils.isBlank(contextId)) {
            throw new IllegalArgumentException("Parameter context id cannot be null or blank");
        }
        if (StringUtils.isBlank(assetId)) {
            throw new IllegalArgumentException("Asset id cannot be null or blank");
        }
        return executeAction("Error getting asset content", () -> {
            final WebTarget target = paramContextTarget.path("{context-id}/assets/{asset-id}")
                    .resolveTemplate("context-id", contextId)
                    .resolveTemplate("asset-id", assetId);

            final Response response = getRequestBuilder(target)
                    .accept(MediaType.APPLICATION_OCTET_STREAM_TYPE)
                    .get();

            final String filename = getContentDispositionFilename(response);
            final File assetFile = new File(outputDirectory, filename);

            try (final InputStream responseInputStream = response.readEntity(InputStream.class)) {
                Files.copy(responseInputStream, assetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                return assetFile.toPath();
            }
        });
    }

    @Override
    public AssetEntity deleteAsset(final String contextId, final String assetId) throws NiFiClientException, IOException {
        return deleteAsset(contextId, assetId, false);
    }

    @Override
    public AssetEntity deleteAsset(final String contextId, final String assetId, final boolean disconnectedNodeAcknowledged)
            throws NiFiClientException, IOException {
        if (StringUtils.isBlank(contextId)) {
            throw new IllegalArgumentException("Parameter context id cannot be null or blank");
        }
        if (StringUtils.isBlank(assetId)) {
            throw new IllegalArgumentException("Asset id cannot be null or blank");
        }
        return executeAction("Error deleting asset", () -> {
            WebTarget target = paramContextTarget.path("{context-id}/assets/{asset-id}")
                    .resolveTemplate("context-id", contextId)
                    .resolveTemplate("asset-id", assetId);

            if (disconnectedNodeAcknowledged) {
                target = target.queryParam("disconnectedNodeAcknowledged", "true");
            }

            return getRequestBuilder(target).delete(AssetEntity.class);
        });
    }
}
