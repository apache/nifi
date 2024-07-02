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

import org.apache.nifi.cluster.manager.NarSummaryDtoMerger;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.NarSummaryDTO;
import org.apache.nifi.web.api.entity.NarSummaryEntity;

import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class NarSummaryEndpointMerger extends AbstractSingleDTOEndpoint<NarSummaryEntity, NarSummaryDTO> {

    private static final String NAR_MANAGER_PATH = "/nifi-api/controller/nar-manager";
    private static final String NAR_UPLOAD_PATH = NAR_MANAGER_PATH + "/uploads";
    public static final Pattern NAR_URI_PATTERN = Pattern.compile(NAR_MANAGER_PATH + "/nars/[a-f0-9\\-]{36}");

    @Override
    public boolean canHandle(final URI uri, final String method) {
        if ("POST".equals(method) && NAR_UPLOAD_PATH.equals(uri.getPath())) {
            return true;
        }
        return ("DELETE".equals(method) || "GET".equals(method)) && NAR_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    protected Class<NarSummaryEntity> getEntityClass() {
        return NarSummaryEntity.class;
    }

    @Override
    protected NarSummaryDTO getDto(final NarSummaryEntity entity) {
        return entity.getNarSummary();
    }

    @Override
    protected void mergeResponses(final NarSummaryDTO clientDto, final Map<NodeIdentifier, NarSummaryDTO> dtoMap, final Set<NodeResponse> successfulResponses,
                                  final Set<NodeResponse> problematicResponses) {
        for (final NarSummaryDTO nodeDto : dtoMap.values()) {
            NarSummaryDtoMerger.merge(clientDto, nodeDto);
        }
    }
}
