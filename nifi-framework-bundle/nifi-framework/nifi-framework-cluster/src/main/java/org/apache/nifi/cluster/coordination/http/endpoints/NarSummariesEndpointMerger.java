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

import org.apache.nifi.cluster.manager.NarSummariesMerger;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.entity.NarSummariesEntity;
import org.apache.nifi.web.api.entity.NarSummaryEntity;

import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class NarSummariesEndpointMerger extends AbstractSingleDTOEndpoint<NarSummariesEntity, Collection<NarSummaryEntity>> {

    private static final String NAR_SUMMARIES_PATH = "/nifi-api/controller/nar-manager/nars";

    @Override
    public boolean canHandle(final URI uri, final String method) {
        return "GET".equals(method) && NAR_SUMMARIES_PATH.equals(uri.getPath());
    }

    @Override
    protected Class<NarSummariesEntity> getEntityClass() {
        return NarSummariesEntity.class;
    }

    @Override
    protected Collection<NarSummaryEntity> getDto(final NarSummariesEntity entity) {
        return entity.getNarSummaries();
    }

    @Override
    protected void mergeResponses(final Collection<NarSummaryEntity> clientDto, final Map<NodeIdentifier, Collection<NarSummaryEntity>> dtoMap,
                                  final Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses) {
        NarSummariesMerger.mergeResponses(clientDto, dtoMap);
    }

}
