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

import org.apache.nifi.graph.exception.GraphClientMethodNotSupported;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class GremlinQueryFromNodesBuilder implements QueryFromNodesBuilder {

    private String databaseName;

    @Override
    public List<GraphQuery> getProvenanceQueries(final List<Map<String, Object>> nodeList, final boolean includeFlowGraph) {
        final List<GraphQuery> queryList = new ArrayList<>(nodeList.size());
        for (final Map<String, Object> eventNode : nodeList) {
            StringBuilder queryBuilder = new StringBuilder();
            queryBuilder.append("g.V().has('");
            queryBuilder.append(eventNode.get("eventType"));
            queryBuilder.append("').has('eventId', '");
            queryBuilder.append(eventNode.get("eventId"));
            queryBuilder.append("').fold().coalesce(unfold(), addV('");
            queryBuilder.append(eventNode.get("eventType"));
            queryBuilder.append("').property('nodeType' , 'NiFiProvenanceEvent')");

            for (final Map.Entry<String, Object> properties : eventNode.entrySet()) {
                queryBuilder.append(".property('");
                queryBuilder.append(properties.getKey());
                queryBuilder.append("', '");
                queryBuilder.append(properties.getValue());
                queryBuilder.append("')");
            }
            queryBuilder.append(");\n");
            queryList.add(new GraphQuery(queryBuilder.toString(), GraphClientService.GREMLIN));

            // Add edges
            List<Long> previousEventIds = (List<Long>) eventNode.remove("previousEventIds");
            for (Long previousEventId : previousEventIds) {
                queryBuilder = new StringBuilder();
                queryBuilder.append("g.V().has('eventType', '");
                queryBuilder.append(eventNode.get("eventType"));
                queryBuilder.append("').has('eventOrdinal', '");
                queryBuilder.append(eventNode.get("eventOrdinal"));
                queryBuilder.append("').inE('next').where(outV().has('eventOrdinal', '");
                queryBuilder.append(previousEventId);
                queryBuilder.append("')).fold().coalesce(unfold(), addE('next').from(V().has('eventOrdinal', '");
                queryBuilder.append(previousEventId);
                queryBuilder.append("')).to(V().has('eventType', '");
                queryBuilder.append(eventNode.get("eventType"));
                queryBuilder.append("').has('eventOrdinal', '");
                queryBuilder.append(eventNode.get("eventOrdinal"));
                queryBuilder.append("')));\n");
                queryList.add(new GraphQuery(queryBuilder.toString(), GraphClientService.GREMLIN));
            }
        }
        return queryList;
    }

    @Override
    public List<GraphQuery> getFlowGraphQueries(List<Map<String, Object>> nodeList) {
        return Collections.emptyList();
    }

    @Override
    public List<GraphQuery> convertActionsToQueries(List<Map<String, Object>> nodeList) {
        if (nodeList == null) {
            return Collections.emptyList();
        }
        final List<GraphQuery> queryList = new ArrayList<>(nodeList.size());
        for (Map<String, Object> actionNode : nodeList) {
            final String operation = actionNode.get("operation").toString();
            StringBuilder queryBuilder;
            if ("Remove".equals(operation)) {
                // TODO move to a "history" database? If so match its nearest neighbors
                // Remove the node from the graph if it has been replaced
                queryBuilder = new StringBuilder("g.V().has('id','")
                        .append(actionNode.get("componentId"))
                        .append("').drop()");
                queryList.add(new GraphQuery(queryBuilder.toString(), GraphClientService.GREMLIN));
            }
        }

        return queryList;
    }

    @Override
    public List<GraphQuery> generateCreateDatabaseQueries(String databaseName, boolean isCompositeDatabase) throws GraphClientMethodNotSupported {
        throw new GraphClientMethodNotSupported("Gremlin does not support creating databases");
    }

    @Override
    public List<GraphQuery> generateCreateIndexQueries(String databaseName, boolean isCompositeDatabase) throws GraphClientMethodNotSupported {
        throw new GraphClientMethodNotSupported("Gremlin does not support creating indexes");
    }

    @Override
    public List<GraphQuery> generateInitialVertexTypeQueries(String databaseName, boolean isCompositeDatabase) {
        if (null != databaseName && !databaseName.isEmpty()) {
            this.databaseName = databaseName;
        }
        return Collections.emptyList();
    }

    @Override
    public List<GraphQuery> generateInitialEdgeTypeQueries(String databaseName, boolean isCompositeDatabase) {
        if (null != databaseName && !databaseName.isEmpty()) {
            this.databaseName = databaseName;
        }
        return Collections.emptyList();
    }

    public String generateUseClause(final String databaseName) {
        return "";
    }
}
