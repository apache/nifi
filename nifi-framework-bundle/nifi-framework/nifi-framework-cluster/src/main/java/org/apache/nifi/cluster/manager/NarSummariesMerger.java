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

package org.apache.nifi.cluster.manager;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.NarSummaryDTO;
import org.apache.nifi.web.api.entity.NarSummaryEntity;

import java.util.Collection;
import java.util.Map;

public class NarSummariesMerger {

    public static void mergeResponses(final Collection<NarSummaryEntity> clientDto, final Map<NodeIdentifier, Collection<NarSummaryEntity>> dtoMap) {
        dtoMap.values().forEach(clientDto::retainAll);

        clientDto.stream()
                .map(NarSummaryEntity::getNarSummary)
                .forEach(summaryDTO -> mergeResponses(summaryDTO, dtoMap));
    }

    public static void mergeResponses(final NarSummaryDTO clientDto, final Map<NodeIdentifier, Collection<NarSummaryEntity>> dtoMap) {
        for (final Collection<NarSummaryEntity> nodeSummaries : dtoMap.values()) {
            for (final NarSummaryEntity nodeSummary : nodeSummaries) {
                final NarSummaryDTO nodeDto = nodeSummary.getNarSummary();
                if (clientDto.getIdentifier().equals(nodeDto.getIdentifier())) {
                    NarSummaryDtoMerger.merge(clientDto, nodeDto);
                }
            }
        }
    }
}
