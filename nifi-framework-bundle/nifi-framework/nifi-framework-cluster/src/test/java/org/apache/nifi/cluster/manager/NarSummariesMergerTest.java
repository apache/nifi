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
import org.apache.nifi.web.api.dto.NarCoordinateDTO;
import org.apache.nifi.web.api.dto.NarSummaryDTO;
import org.apache.nifi.web.api.entity.NarSummaryEntity;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NarSummariesMergerTest {

    public static final String NIFI_GROUP_ID = "org.apache.nifi";
    public static final String NIFI_NAR_FORMAT = "nifi-nar-%s";
    public static final String VERSION = "1.0.0";

    @Test
    public void testMergeNarSummaries() {
        final NarSummaryEntity narSummary1 = createNarSummaryEntity(NIFI_GROUP_ID, NIFI_NAR_FORMAT.formatted("1"), VERSION);
        final NarSummaryEntity narSummary2 = createNarSummaryEntity(NIFI_GROUP_ID, NIFI_NAR_FORMAT.formatted("2"), VERSION);
        final NarSummaryEntity narSummary3 = createNarSummaryEntity(NIFI_GROUP_ID, NIFI_NAR_FORMAT.formatted("3"), VERSION);
        final NarSummaryEntity narSummary4 = createNarSummaryEntity(NIFI_GROUP_ID, NIFI_NAR_FORMAT.formatted("4"), VERSION);

        final Collection<NarSummaryEntity> mergedResults = new ArrayList<>();
        mergedResults.add(narSummary1);
        mergedResults.add(narSummary2);
        mergedResults.add(narSummary3);

        final NodeIdentifier node1Identifier = new NodeIdentifier("node1", "localhost", 8080, "localhost", 8081, "localhost", 8082, 8083, false);
        final Collection<NarSummaryEntity> node1Response = new ArrayList<>();
        node1Response.add(narSummary2);
        node1Response.add(narSummary3);

        final NodeIdentifier node2Identifier = new NodeIdentifier("node2", "localhost", 8080, "localhost", 8081, "localhost", 8082, 8083, false);
        final Collection<NarSummaryEntity> node2Response = new ArrayList<>();
        node2Response.add(narSummary1);
        node2Response.add(narSummary3);
        node2Response.add(narSummary4);

        final Map<NodeIdentifier, Collection<NarSummaryEntity>> nodeMap = Map.of(
                node1Identifier, node1Response,
                node2Identifier, node2Response
        );

        NarSummariesMerger.mergeResponses(mergedResults, nodeMap);

        assertEquals(1, mergedResults.size());
        assertEquals(narSummary3, mergedResults.stream().findFirst().orElse(null));
    }

    private NarSummaryEntity createNarSummaryEntity(final String group, final String artifact, final String version) {
        final NarSummaryDTO summaryDTO = new NarSummaryDTO();
        summaryDTO.setIdentifier(UUID.randomUUID().toString());
        summaryDTO.setCoordinate(new NarCoordinateDTO(group, artifact, version));
        return new NarSummaryEntity(summaryDTO);
    }
}
