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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CypherQueryFromNodesBuilder {
    public List<GraphQuery> getQueries(final List<Map<String, Object>> nodeList) {
        final List<GraphQuery> queryList = new ArrayList<>(nodeList.size());
        for (Map<String, Object> eventNode : nodeList) {
            StringBuilder queryBuilder = new StringBuilder("MERGE (p:" + eventNode.get("eventType") + " {");
            final List<String> propertyDefinitions = new ArrayList<>(eventNode.entrySet().size());

            for (Map.Entry<String,Object> properties : eventNode.entrySet()) {
                propertyDefinitions.add(properties.getKey() + ": \"" + properties.getValue() + "\"");
            }
            propertyDefinitions.add("nodeType: \"NiFiProvenanceEvent\"");

            queryBuilder.append(String.join(",", propertyDefinitions));
            queryBuilder.append("})");
            queryList.add(new GraphQuery(queryBuilder.toString(), GraphClientService.CYPHER));

            List<Long> previousEventIds = (List<Long>) eventNode.get("previousEventIds");

            // If there are previous event IDs, add edges
            if (previousEventIds != null) {
                queryBuilder = new StringBuilder();
                // Match the source (previous event) and target (this event) and create the edge if it doesn't exist
                for (Long previousEventId : previousEventIds) {
                    queryBuilder.append("MATCH\n(x {nodeType: 'NiFiProvenanceEvent', eventOrdinal: '");
                    queryBuilder.append(previousEventId);
                    queryBuilder.append("'}),\n(y {nodeType: 'NiFiProvenanceEvent', eventOrdinal: '");
                    queryBuilder.append(eventNode.get("eventOrdinal"));
                    queryBuilder.append("'})\nMERGE(x) -[:next]-> (y)");
                    // Add edge to graph
                    queryList.add(new GraphQuery(queryBuilder.toString(), GraphClientService.CYPHER));
                }
            }
        }
        return queryList;
    }
}
