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
package org.apache.nifi.cluster.coordination.http.endpoints;

import org.apache.nifi.cluster.coordination.http.EndpointResponseMerger;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.PermissionsDtoMerger;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.ComponentValidationResultDTO;
import org.apache.nifi.web.api.dto.ParameterContextValidationRequestDTO;
import org.apache.nifi.web.api.entity.ComponentValidationResultEntity;
import org.apache.nifi.web.api.entity.ComponentValidationResultsEntity;
import org.apache.nifi.web.api.entity.ParameterContextValidationRequestEntity;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class ParameterContextValidationMerger extends AbstractSingleEntityEndpoint<ParameterContextValidationRequestEntity> implements EndpointResponseMerger {
    public static final Pattern REQUESTS_URI_PATTERN = Pattern.compile("/nifi-api/parameter-contexts/validation-requests");
    public static final Pattern REQUEST_BY_ID_URI_PATTERN = Pattern.compile("/nifi-api/parameter-contexts/validation-requests/[a-f0-9\\-]{36}");

    @Override
    public boolean canHandle(final URI uri, final String method) {
        if ("POST".equalsIgnoreCase(method) && REQUESTS_URI_PATTERN.matcher(uri.getPath()).matches()) {
            return true;
        }

        return (REQUEST_BY_ID_URI_PATTERN.matcher(uri.getPath()).matches());
    }

    @Override
    protected Class<ParameterContextValidationRequestEntity> getEntityClass() {
        return ParameterContextValidationRequestEntity.class;
    }

    @Override
    protected void mergeResponses(final ParameterContextValidationRequestEntity clientEntity, final Map<NodeIdentifier, ParameterContextValidationRequestEntity> entityMap,
                                  final Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses) {

        final ParameterContextValidationRequestDTO validationRequest = clientEntity.getRequest();

        for (final ParameterContextValidationRequestEntity requestEntity : entityMap.values()) {
            final ParameterContextValidationRequestDTO requestDto = requestEntity.getRequest();

            if (!requestDto.isComplete()) {
                validationRequest.setComplete(false);
            }
            if (requestDto.getFailureReason() != null) {
                validationRequest.setFailureReason(requestDto.getFailureReason());
            }
            if (requestDto.getLastUpdated() != null && (validationRequest.getLastUpdated() == null || requestDto.getLastUpdated().after(validationRequest.getLastUpdated()))) {
                validationRequest.setLastUpdated(requestDto.getLastUpdated());
            }
            if (requestDto.getPercentCompleted() < validationRequest.getPercentCompleted()) {
                validationRequest.setPercentCompleted(requestDto.getPercentCompleted());
                validationRequest.setState(requestDto.getState());
            }
        }

        final ComponentValidationResultsEntity resultsEntity = validationRequest.getComponentValidationResults();
        if (resultsEntity == null) {
            return;
        }

        final List<ComponentValidationResultEntity> resultsEntities = resultsEntity.getValidationResults();
        if (resultsEntities == null) {
            return;
        }

        final Map<String, ComponentValidationResultEntity> resultsById = new HashMap<>();
        for (final ComponentValidationResultEntity resultEntity : resultsEntities) {
            resultsById.put(resultEntity.getId(), resultEntity);
        }

        for (final ParameterContextValidationRequestEntity requestEntity : entityMap.values()) {
            final ComponentValidationResultsEntity validationResultsEntity = requestEntity.getRequest().getComponentValidationResults();
            if (validationResultsEntity == null) {
                continue;
            }

            for (final ComponentValidationResultEntity resultEntity : validationResultsEntity.getValidationResults()) {
                final ComponentValidationResultEntity mergedResultEntity = resultsById.get(resultEntity.getId());
                if (mergedResultEntity == null) {
                    resultsById.put(resultEntity.getId(), resultEntity);
                } else {
                    merge(mergedResultEntity, resultEntity);
                }
            }
        }

        resultsEntity.setValidationResults(new ArrayList<>(resultsById.values()));
    }

    private void merge(final ComponentValidationResultEntity merged, final ComponentValidationResultEntity additional) {
        if (merged.getComponent() == null) {
            return;
        }

        PermissionsDtoMerger.mergePermissions(merged.getPermissions(), additional.getPermissions());

        // If either entity doesn't have the component, then we cannot return the component because it was filtered out due to permissions.
        if (additional.getComponent() == null) {
            merged.setComponent(null);
            return;
        }

        final ComponentValidationResultDTO mergedResultDto = merged.getComponent();
        final ComponentValidationResultDTO additionalResultDto = additional.getComponent();

        mergedResultDto.setActiveThreadCount(mergedResultDto.getActiveThreadCount() + additionalResultDto.getActiveThreadCount());

        // Merge validation errors
        Collection<String> mergedValidationErrors = mergedResultDto.getValidationErrors();
        if (mergedValidationErrors == null) {
            mergedValidationErrors = new ArrayList<>();
            mergedResultDto.setValidationErrors(mergedValidationErrors);
        }
        if (additionalResultDto.getValidationErrors() != null) {
            mergedValidationErrors.addAll(additionalResultDto.getValidationErrors());
        }

        // Merge resultant validation errors
        Collection<String> mergedResultantValidationErrors = mergedResultDto.getResultantValidationErrors();
        if (mergedResultantValidationErrors == null) {
            mergedResultantValidationErrors = new ArrayList<>();
            mergedResultDto.setResultantValidationErrors(mergedResultantValidationErrors);
        }
        if (additionalResultDto.getResultantValidationErrors() != null) {
            mergedResultantValidationErrors.addAll(additionalResultDto.getResultantValidationErrors());
        }

        // Merge currently valid & results valid fields
        mergedResultDto.setCurrentlyValid(mergedValidationErrors.isEmpty());
        mergedResultDto.setResultsValid(mergedResultantValidationErrors.isEmpty());
    }
}
