/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.cluster.coordination.http.endpoints;

import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.DocumentedTypeDTO;
import org.apache.nifi.web.api.entity.NarDetailsEntity;

import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class NarDetailsEndpointMerger extends AbstractSingleEntityEndpoint<NarDetailsEntity> {

    public static final Pattern NAR_DETAILS_URI_PATTERN = Pattern.compile("/nifi-api/controller/nar-manager/nars/[a-f0-9\\-]{36}/details");

    @Override
    public boolean canHandle(final URI uri, final String method) {
        return "GET".equals(method) && NAR_DETAILS_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    protected Class<NarDetailsEntity> getEntityClass() {
        return NarDetailsEntity.class;
    }

    @Override
    protected void mergeResponses(final NarDetailsEntity clientEntity, final Map<NodeIdentifier, NarDetailsEntity> entityMap,
                                  final Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses) {

        for (final Map.Entry<NodeIdentifier, NarDetailsEntity> entry : entityMap.entrySet()) {
            final NarDetailsEntity nodeEntity = entry.getValue();
            merge(clientEntity.getProcessorTypes(), nodeEntity.getProcessorTypes());
            merge(clientEntity.getControllerServiceTypes(), nodeEntity.getControllerServiceTypes());
            merge(clientEntity.getReportingTaskTypes(), nodeEntity.getReportingTaskTypes());
            merge(clientEntity.getParameterProviderTypes(), nodeEntity.getParameterProviderTypes());
            merge(clientEntity.getFlowRegistryClientTypes(), nodeEntity.getFlowRegistryClientTypes());
            merge(clientEntity.getFlowAnalysisRuleTypes(), nodeEntity.getFlowAnalysisRuleTypes());
        }
    }

    private void merge(final Set<DocumentedTypeDTO> clientTypes, final Set<DocumentedTypeDTO> nodeTypes) {
        if (clientTypes == null || nodeTypes == null) {
            return;
        }
        clientTypes.retainAll(nodeTypes);
    }
}
