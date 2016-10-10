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
import org.apache.nifi.cluster.manager.StatusMerger;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.status.ControllerStatusDTO;
import org.apache.nifi.web.api.entity.ControllerStatusEntity;

import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class ControllerStatusEndpointMerger extends AbstractSingleDTOEndpoint<ControllerStatusEntity, ControllerStatusDTO> {
    public static final Pattern CONTROLLER_STATUS_URI_PATTERN = Pattern.compile("/nifi-api/flow/status");

    @Override
    public boolean canHandle(URI uri, String method) {
        return "GET".equalsIgnoreCase(method) && CONTROLLER_STATUS_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    protected Class<ControllerStatusEntity> getEntityClass() {
        return ControllerStatusEntity.class;
    }

    @Override
    protected ControllerStatusDTO getDto(ControllerStatusEntity entity) {
        return entity.getControllerStatus();
    }

    @Override
    protected void mergeResponses(ControllerStatusDTO clientDto, Map<NodeIdentifier, ControllerStatusDTO> dtoMap, Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses) {
        ControllerStatusDTO mergedStatus = clientDto;
        for (final Map.Entry<NodeIdentifier, ControllerStatusDTO> entry : dtoMap.entrySet()) {
            final ControllerStatusDTO nodeStatus = entry.getValue();

            if (nodeStatus == mergedStatus) {
                continue;
            }

            StatusMerger.merge(mergedStatus, nodeStatus);
        }
    }

}
