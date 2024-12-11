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
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.toolkit.client.ParamProviderClient;
import org.apache.nifi.toolkit.client.RequestConfig;
import org.apache.nifi.web.api.entity.ParameterProviderApplyParametersRequestEntity;
import org.apache.nifi.web.api.entity.ParameterProviderEntity;
import org.apache.nifi.web.api.entity.ParameterProviderParameterApplicationEntity;
import org.apache.nifi.web.api.entity.ParameterProviderParameterFetchEntity;
import org.apache.nifi.web.api.entity.VerifyConfigRequestEntity;

import java.io.IOException;

public class JerseyParamProviderClient extends AbstractJerseyClient implements ParamProviderClient {

    private final WebTarget paramProviderTarget;

    public JerseyParamProviderClient(final WebTarget baseTarget) {
        this(baseTarget, null);
    }

    public JerseyParamProviderClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
        super(requestConfig);
        this.paramProviderTarget = baseTarget.path("/parameter-providers");
    }

    @Override
    public ParameterProviderEntity getParamProvider(final String id) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Parameter provider id cannot be null or blank");
        }

        return executeAction("Error retrieving parameter provider", () -> {
            final WebTarget target = paramProviderTarget.path("{id}")
                    .resolveTemplate("id", id);
            return getRequestBuilder(target).get(ParameterProviderEntity.class);
        });
    }

    @Override
    public ParameterProviderEntity updateParamProvider(final ParameterProviderEntity entity) throws NiFiClientException, IOException {
        if (entity == null) {
            throw new IllegalArgumentException("Parameter provider entity cannot be null");
        }

        return executeAction("Error updating parameter provider", () -> {
            final WebTarget target = paramProviderTarget.path("{id}").resolveTemplate("id", entity.getId());

            return getRequestBuilder(target).put(
                    Entity.entity(entity, MediaType.APPLICATION_JSON_TYPE),
                    ParameterProviderEntity.class);
        });
    }

    @Override
    public ParameterProviderEntity deleteParamProvider(final String id, final String version) throws NiFiClientException, IOException {
        return deleteParamProvider(id, version, false);
    }

    @Override
    public ParameterProviderEntity deleteParamProvider(final String id, final String version, final boolean disconnectedNodeAcknowledged) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Parameter provider id cannot be null or blank");
        }

        if (StringUtils.isBlank(version)) {
            throw new IllegalArgumentException("Version cannot be null or blank");
        }

        return executeAction("Error deleting parameter provider", () -> {
            WebTarget target = paramProviderTarget.path("{id}")
                    .resolveTemplate("id", id)
                    .queryParam("version", version);

            if (disconnectedNodeAcknowledged) {
                target = target.queryParam("disconnectedNodeAcknowledged", "true");
            }

            return getRequestBuilder(target).delete(ParameterProviderEntity.class);
        });
    }

    @Override
    public ParameterProviderEntity fetchParameters(final ParameterProviderParameterFetchEntity parameterFetchEntity) throws NiFiClientException, IOException {
        if (parameterFetchEntity == null) {
            throw new IllegalArgumentException("Parameter fetch entity cannot be null");
        }

        if (parameterFetchEntity.getRevision() == null) {
            throw new IllegalArgumentException("Parameter provider revision cannot be null");
        }

        final String paramProviderId = parameterFetchEntity.getId();
        if (StringUtils.isBlank(paramProviderId)) {
            throw new IllegalArgumentException("Parameter provider id cannot be null or blank");
        }

        return executeAction("Error fetching parameters", () -> {
            final WebTarget target = paramProviderTarget.path("{id}/parameters/fetch-requests")
                    .resolveTemplate("id", paramProviderId);
            return getRequestBuilder(target).post(
                    Entity.entity(parameterFetchEntity, MediaType.APPLICATION_JSON),
                    ParameterProviderEntity.class);
        });
    }

    @Override
    public ParameterProviderApplyParametersRequestEntity applyParameters(final ParameterProviderParameterApplicationEntity providerParameterApplicationEntity)
            throws NiFiClientException, IOException {
        if (providerParameterApplicationEntity == null) {
            throw new IllegalArgumentException("Parameter application entity cannot be null");
        }

        if (providerParameterApplicationEntity.getRevision() == null) {
            throw new IllegalArgumentException("Parameter provider revision cannot be null");
        }

        final String paramProviderId = providerParameterApplicationEntity.getId();
        if (StringUtils.isBlank(paramProviderId)) {
            throw new IllegalArgumentException("Parameter provider id cannot be null or blank");
        }

        return executeAction("Error creating parameter provider application request", () -> {
            final WebTarget target = paramProviderTarget.path("{id}/apply-parameters-requests")
                    .resolveTemplate("id", paramProviderId);
            return getRequestBuilder(target).post(
                    Entity.entity(providerParameterApplicationEntity, MediaType.APPLICATION_JSON),
                    ParameterProviderApplyParametersRequestEntity.class);
        });
    }

    @Override
    public ParameterProviderApplyParametersRequestEntity getParamProviderApplyParametersRequest(final String providerId, final String applyParametersRequestId)
            throws NiFiClientException, IOException {
        if (StringUtils.isBlank(applyParametersRequestId)) {
            throw new IllegalArgumentException("Parameter provider apply parameters request id cannot be null or blank");
        }

        return executeAction("Error retrieving parameter provider", () -> {
            final WebTarget target = paramProviderTarget.path("{provider-id}/apply-parameters-requests/{request-id}")
                    .resolveTemplate("provider-id", providerId)
                    .resolveTemplate("request-id", applyParametersRequestId);
            return getRequestBuilder(target).get(ParameterProviderApplyParametersRequestEntity.class);
        });
    }

    @Override
    public ParameterProviderApplyParametersRequestEntity deleteParamProviderApplyParametersRequest(final String providerId, final String updateRequestId)
            throws NiFiClientException, IOException {
        if (StringUtils.isBlank(updateRequestId)) {
            throw new IllegalArgumentException("Parameter provider apply parameters request id cannot be null or blank");
        }

        return executeAction("Error deleting parameter provider apply parameters request", () -> {
            final WebTarget target = paramProviderTarget.path("{provider-id}/apply-parameters-requests/{request-id}")
                    .resolveTemplate("provider-id", providerId)
                    .resolveTemplate("request-id", updateRequestId);
            return getRequestBuilder(target).delete(ParameterProviderApplyParametersRequestEntity.class);
        });
    }

    @Override
    public VerifyConfigRequestEntity submitConfigVerificationRequest(final VerifyConfigRequestEntity configRequestEntity) throws NiFiClientException, IOException {
        if (configRequestEntity == null) {
            throw new IllegalArgumentException("Config Request Entity cannot be null");
        }
        if (configRequestEntity.getRequest() == null) {
            throw new IllegalArgumentException("Config Request DTO cannot be null");
        }
        if (configRequestEntity.getRequest().getComponentId() == null) {
            throw new IllegalArgumentException("Parameter Provider ID cannot be null");
        }
        if (configRequestEntity.getRequest().getProperties() == null) {
            throw new IllegalArgumentException("Parameter Provider properties cannot be null");
        }

        return executeAction("Error submitting Config Verification Request", () -> {
            final WebTarget target = paramProviderTarget
                    .path("{id}/config/verification-requests")
                    .resolveTemplate("id", configRequestEntity.getRequest().getComponentId());

            return getRequestBuilder(target).post(
                    Entity.entity(configRequestEntity, MediaType.APPLICATION_JSON_TYPE),
                    VerifyConfigRequestEntity.class);
        });
    }

    @Override
    public VerifyConfigRequestEntity getConfigVerificationRequest(final String taskId, final String verificationRequestId) throws NiFiClientException, IOException {
        if (verificationRequestId == null) {
            throw new IllegalArgumentException("Verification Request ID cannot be null");
        }

        return executeAction("Error retrieving Config Verification Request", () -> {
            final WebTarget target = paramProviderTarget
                    .path("{id}/config/verification-requests/{requestId}")
                    .resolveTemplate("id", taskId)
                    .resolveTemplate("requestId", verificationRequestId);

            return getRequestBuilder(target).get(VerifyConfigRequestEntity.class);
        });
    }

    @Override
    public VerifyConfigRequestEntity deleteConfigVerificationRequest(final String taskId, final String verificationRequestId) throws NiFiClientException, IOException {
        if (verificationRequestId == null) {
            throw new IllegalArgumentException("Verification Request ID cannot be null");
        }

        return executeAction("Error deleting Config Verification Request", () -> {
            final WebTarget target = paramProviderTarget
                    .path("{id}/config/verification-requests/{requestId}")
                    .resolveTemplate("id", taskId)
                    .resolveTemplate("requestId", verificationRequestId);

            return getRequestBuilder(target).delete(VerifyConfigRequestEntity.class);
        });
    }
}
