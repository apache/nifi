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
import org.apache.nifi.web.api.entity.ClearComponentStateResultEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class ClearComponentStateEndpointMerger extends AbstractSingleEntityEndpoint<ClearComponentStateResultEntity> {
    public static final Pattern PROCESSOR_STATE_URI_PATTERN = Pattern.compile("/nifi-api/processors/[a-f0-9\\-]{36}/state/clear-requests");
    public static final Pattern CONTROLLER_SERVICE_STATE_URI_PATTERN = Pattern.compile("/nifi-api/controller-services/[a-f0-9\\-]{36}/state/clear-requests");
    public static final Pattern REPORTING_TASK_STATE_URI_PATTERN = Pattern.compile("/nifi-api/reporting-tasks/[a-f0-9\\-]{36}/state/clear-requests");

    private static final Logger logger = LoggerFactory.getLogger(ClearComponentStateEndpointMerger.class);

    @Override
    public boolean canHandle(URI uri, String method) {
        if (!"POST".equalsIgnoreCase(method)) {
            return false;
        }

        return PROCESSOR_STATE_URI_PATTERN.matcher(uri.getPath()).matches()
            || CONTROLLER_SERVICE_STATE_URI_PATTERN.matcher(uri.getPath()).matches()
            || REPORTING_TASK_STATE_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    protected Class<ClearComponentStateResultEntity> getEntityClass() {
        return ClearComponentStateResultEntity.class;
    }

    @Override
    protected void mergeResponses(ClearComponentStateResultEntity clientEntity, Map<NodeIdentifier,
            ClearComponentStateResultEntity> entityMap, Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses) {

        if (logger.isDebugEnabled()) {
            logger.debug("entityMap={}, successfulResponse={}, problematicResponse={}", entityMap, successfulResponses, problematicResponses);
            entityMap.forEach((id, res) -> logger.debug("nodeId={}, res.isCleared={}", id, res.isCleared()));
        }

        // If there's a uncleared response, use that.
        final ClearComponentStateResultEntity unclearedRes = entityMap.values().stream().filter(res -> !res.isCleared()).findFirst().orElse(null);
        if (unclearedRes != null) {
            clientEntity.setCleared(unclearedRes.isCleared());
            clientEntity.setMessage(unclearedRes.getMessage());
        }

    }
}
