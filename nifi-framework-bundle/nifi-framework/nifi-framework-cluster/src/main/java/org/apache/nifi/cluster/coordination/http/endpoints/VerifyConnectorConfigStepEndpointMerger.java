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

import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.ConfigVerificationResultDTO;
import org.apache.nifi.web.api.dto.VerifyConnectorConfigStepRequestDTO;
import org.apache.nifi.web.api.entity.VerifyConnectorConfigStepRequestEntity;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class VerifyConnectorConfigStepEndpointMerger extends AbstractSingleEntityEndpoint<VerifyConnectorConfigStepRequestEntity> {
    public static final Pattern VERIFY_CONNECTOR_CONFIG_STEP_URI_PATTERN =
            Pattern.compile("/nifi-api/connectors/[a-f0-9\\-]{36}/configuration-steps/[^/]+/verify-config(/[a-f0-9\\-]{36})?");

    @Override
    protected Class<VerifyConnectorConfigStepRequestEntity> getEntityClass() {
        return VerifyConnectorConfigStepRequestEntity.class;
    }

    @Override
    public boolean canHandle(final URI uri, final String method) {
        return VERIFY_CONNECTOR_CONFIG_STEP_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    protected void mergeResponses(final VerifyConnectorConfigStepRequestEntity clientEntity,
                                  final Map<NodeIdentifier, VerifyConnectorConfigStepRequestEntity> entityMap,
                                  final Set<NodeResponse> successfulResponses,
                                  final Set<NodeResponse> problematicResponses) {

        final VerifyConnectorConfigStepRequestDTO requestDto = clientEntity.getRequest();
        final List<ConfigVerificationResultDTO> results = requestDto.getResults();

        if (results == null) {
            return;
        }

        final ConfigVerificationResultMerger resultMerger = new ConfigVerificationResultMerger();
        for (final Map.Entry<NodeIdentifier, VerifyConnectorConfigStepRequestEntity> entry : entityMap.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final VerifyConnectorConfigStepRequestEntity entity = entry.getValue();

            final List<ConfigVerificationResultDTO> nodeResults = entity.getRequest().getResults();
            resultMerger.addNodeResults(nodeId, nodeResults);
        }

        final List<ConfigVerificationResultDTO> aggregateResults = resultMerger.computeAggregateResults();

        clientEntity.getRequest().setResults(aggregateResults);
    }
}

