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

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.nifi.cluster.coordination.http.EndpointResponseMerger;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.ProcessorDiagnosticsEntityMerger;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.entity.ProcessorDiagnosticsEntity;

public class ProcessorDiagnosticsEndpointMerger implements EndpointResponseMerger {
    public static final Pattern PROCESSOR_DIAGNOSTICS_URI_PATTERN = Pattern.compile("/nifi-api/processors/[a-f0-9\\-]{36}/diagnostics");

    private final ProcessorDiagnosticsEntityMerger diagnosticsEntityMerger;

    public ProcessorDiagnosticsEndpointMerger(final long componentStatusSnapshotMillis) {
        diagnosticsEntityMerger = new ProcessorDiagnosticsEntityMerger(componentStatusSnapshotMillis);
    }

    @Override
    public boolean canHandle(final URI uri, final String method) {
        if (!"GET".equalsIgnoreCase(method)) {
            return false;
        }

        return PROCESSOR_DIAGNOSTICS_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    public NodeResponse merge(URI uri, String method, Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses, NodeResponse clientResponse) {
        final ProcessorDiagnosticsEntity clientEntity = clientResponse.getClientResponse().readEntity(ProcessorDiagnosticsEntity.class);

        // Unmarshall each response into an entity.
        final Map<NodeIdentifier, ProcessorDiagnosticsEntity> entityMap = new HashMap<>();
        for (final NodeResponse nodeResponse : successfulResponses) {
            final ProcessorDiagnosticsEntity nodeResponseEntity = nodeResponse == clientResponse ? clientEntity : nodeResponse.getClientResponse().readEntity(ProcessorDiagnosticsEntity.class);
            entityMap.put(nodeResponse.getNodeId(), nodeResponseEntity);
        }

        diagnosticsEntityMerger.merge(clientEntity, entityMap);
        return new NodeResponse(clientResponse, clientEntity);
    }

}
