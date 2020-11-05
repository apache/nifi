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
package org.apache.nifi.toolkit.cli.impl.client.nifi.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ParamContextClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.RequestConfig;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterContextUpdateRequestEntity;
import org.apache.nifi.web.api.entity.ParameterContextsEntity;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

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
    public ParameterContextEntity getParamContext(final String id) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Parameter context id cannot be null or blank");
        }

        return executeAction("Error retrieving parameter context", () -> {
            final WebTarget target = paramContextTarget.path("{id}")
                    .resolveTemplate("id", id);
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
                    ParameterContextEntity.class
            );
        });
    }

    @Override
    public ParameterContextEntity deleteParamContext(final String id, final String version) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Parameter context id cannot be null or blank");
        }

        if (StringUtils.isBlank(version)) {
            throw new IllegalArgumentException("Version cannot be null or blank");
        }

        return executeAction("Error deleting parameter context", () -> {
            final WebTarget target = paramContextTarget.path("{id}")
                    .resolveTemplate("id", id)
                    .queryParam("version", version);
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
                    ParameterContextUpdateRequestEntity.class
            );
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
}
