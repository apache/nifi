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

public class SqlQueryFromNodesBuilder {

    public List<GraphQuery> getQueries(final List<Map<String, Object>> nodeList) {
        final List<GraphQuery> queryList = new ArrayList<>(nodeList.size());
        for (final Map<String, Object> eventNode : nodeList) {
            StringBuilder queryBuilder = new StringBuilder();
            queryBuilder.append("UPDATE NiFiProvenanceEvent SET ");
            final String eventIdClause = "eventId = '" + eventNode.get("eventId") + "'";
            queryBuilder.append(eventIdClause);

            for (final Map.Entry<String, Object> properties : eventNode.entrySet()) {
                queryBuilder.append(", ");
                queryBuilder.append(properties.getKey());
                queryBuilder.append("= '");
                queryBuilder.append(properties.getValue());
                queryBuilder.append("'");
            }
            queryBuilder.append(" UPSERT WHERE ");
            queryBuilder.append(eventIdClause);
            queryList.add(new GraphQuery(queryBuilder.toString(), GraphClientService.SQL));
        }
        return queryList;
    }
}
