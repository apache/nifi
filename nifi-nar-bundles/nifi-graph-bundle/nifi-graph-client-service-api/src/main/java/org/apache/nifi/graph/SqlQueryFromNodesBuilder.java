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
package org.apache.nifi.graph;

import org.apache.nifi.provenance.ProvenanceEventType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SqlQueryFromNodesBuilder {

    public List<GraphQuery> getQueries(final List<Map<String, Object>> nodeList) {
        final List<GraphQuery> queryList = new ArrayList<>(nodeList.size());
        StringBuilder queryBuilder;
        queryList.add(new GraphQuery("CREATE VERTEX TYPE NiFiProvenanceEvent IF NOT EXISTS", GraphClientService.SQL));
        queryList.add(new GraphQuery("CREATE VERTEX TYPE FlowFile IF NOT EXISTS", GraphClientService.SQL));

        // Create the event types as vertex types in ArcadeDB if they haven't been created already
        for (ProvenanceEventType provenanceEventType : ProvenanceEventType.values()) {
            queryBuilder = new StringBuilder();
            queryBuilder.append("CREATE VERTEX TYPE `");
            queryBuilder.append(provenanceEventType.name());
            queryBuilder.append("` IF NOT EXISTS EXTENDS `NiFiProvenanceEvent`");
            queryList.add(new GraphQuery(queryBuilder.toString(), GraphClientService.SQL));
        }
        // Add the edge types
        queryList.add(new GraphQuery("CREATE EDGE TYPE `next` IF NOT EXISTS", GraphClientService.SQL));
        queryList.add(new GraphQuery("CREATE EDGE TYPE `parentOf` IF NOT EXISTS", GraphClientService.SQL));

        // Add the vertices
        for (final Map<String, Object> eventNode : nodeList) {
            queryBuilder = new StringBuilder();
            queryBuilder.append("UPDATE `");
            queryBuilder.append(eventNode.get("eventType"));
            queryBuilder.append("` SET nodeType = 'NiFiProvenanceEvent'");

            for (final Map.Entry<String, Object> properties : eventNode.entrySet()) {
                queryBuilder.append(", ");
                queryBuilder.append(properties.getKey());
                queryBuilder.append("= '");
                queryBuilder.append(properties.getValue());
                queryBuilder.append("'\n");
            }
            queryBuilder.append(" UPSERT WHERE eventOrdinal = ");
            queryBuilder.append(eventNode.get("eventOrdinal"));
            queryList.add(new GraphQuery(queryBuilder.toString(), GraphClientService.SQL));

            // If there are previous event IDs, add edges
            List<Long> previousEventIds = (List<Long>) eventNode.get("previousEventIds");
            if (previousEventIds != null) {
                queryBuilder = new StringBuilder();
                // Match the source (previous event) and target (this event) and create the edge if it doesn't exist
                for (Long previousEventId : previousEventIds) {
                    queryBuilder.append("CREATE EDGE next\nFROM (SELECT * FROM `NiFiProvenanceEvent` WHERE eventOrdinal = ");
                    queryBuilder.append(previousEventId);
                    queryBuilder.append(") TO\n(SELECT * FROM  `NiFiProvenanceEvent` WHERE eventOrdinal = ");
                    queryBuilder.append(eventNode.get("eventOrdinal"));
                    queryBuilder.append(") IF NOT EXISTS");
                    // Add edge to graph
                    queryList.add(new GraphQuery(queryBuilder.toString(), GraphClientService.SQL));
                }
            }

            // Parent-child "next" edges will be created using the the previousEventIds, except when a processor has two of the same relationships
            // to different downstream processors
            /*List<String> parentFlowFileIds = (List<String>) eventNode.get("parentIds");
            if (parentFlowFileIds != null) {
                queryBuilder = new StringBuilder();
                // Match the source (previous event) and target (this event) and create the edge if it doesn't exist
                for (String parentFlowFileId : parentFlowFileIds) {
                    final Long eventOrdinal = (Long) eventNode.get("eventOrdinal");

                    queryBuilder.append("CREATE EDGE next\nFROM (SELECT * FROM `NiFiProvenanceEvent` WHERE entityId = '");
                    queryBuilder.append(parentFlowFileId);
                    queryBuilder.append("' AND eventOrdinal != ");
                    queryBuilder.append(eventOrdinal);
                    queryBuilder.append(") TO\n(SELECT * FROM `");
                    queryBuilder.append(eventNode.get("eventType"));
                    queryBuilder.append("` WHERE eventOrdinal = ");
                    queryBuilder.append(eventOrdinal);
                    queryBuilder.append(") IF NOT EXISTS");
                    // Add edge to graph
                    queryList.add(new GraphQuery(queryBuilder.toString(), GraphClientService.SQL));
                }
            }*/
        }
        return queryList;
    }
}
