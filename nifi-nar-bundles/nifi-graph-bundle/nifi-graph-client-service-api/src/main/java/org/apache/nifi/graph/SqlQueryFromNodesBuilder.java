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

import org.apache.nifi.flow.ComponentType;
import org.apache.nifi.graph.exception.GraphClientMethodNotSupported;
import org.apache.nifi.provenance.ProvenanceEventType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SqlQueryFromNodesBuilder implements QueryFromNodesBuilder {

    private String databaseName;

    public List<GraphQuery> getProvenanceQueries(final List<Map<String, Object>> nodeList, final boolean includeFlowGraph) {
        final List<GraphQuery> queryList = new ArrayList<>(nodeList.size());
        StringBuilder queryBuilder;

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

            // Parent-child "next" edges will be created using the previousEventIds, except when a processor has two of the same relationships
            // to different downstream processors
            /*List<String> parentFlowFileIds = (List<String>) eventNode.get("parentIds");
            if (parentFlowFileIds != null) {
                queryBuilder = new StringBuilder();
                // Match the source (previous event) and target (this event) and create the edge if it doesn't exist
                for (String parentFlowFileId : parentFlowFileIds) {
                    final Long eventOrdinal = (Long) eventNode.get("eventOrdinal");

                    queryBuilder.append("CREATE EDGE next UPSERT\nFROM (SELECT * FROM `NiFiProvenanceEvent` WHERE entityId = '");
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

    @Override
    public List<GraphQuery> getFlowGraphQueries(List<Map<String, Object>> nodeList) {
        final List<GraphQuery> queryList = new ArrayList<>();
        for (Map<String, Object> componentStatusNode : nodeList) {
            StringBuilder queryBuilder;
            // Skip Connection nodes, put all the information into the edge
            if (!"Connection".equals(componentStatusNode.get("type"))) {
                final String id = componentStatusNode.get("id").toString();
                queryBuilder = new StringBuilder("UPDATE `" + componentStatusNode.get("type") + "`\n\tSET id = '");
                queryBuilder.append(id);
                queryBuilder.append("',\n\t");
                queryBuilder.append(joinPropertyEntries(componentStatusNode, null, " =", ",\n\t", ""));
                queryBuilder.append("\nUPSERT WHERE id = '");
                queryBuilder.append(id);
                queryBuilder.append("'");
                queryList.add(new GraphQuery(queryBuilder.toString(), GraphClientService.SQL));

                // Add an edge to the parent process group if appropriate
                if (componentStatusNode.containsKey("groupId")) {
                    queryBuilder = new StringBuilder("CREATE EDGE `parent` FROM (SELECT * FROM `" + componentStatusNode.get("type") + "` WHERE `id` = '");
                    queryBuilder.append(componentStatusNode.get("id"));
                    queryBuilder.append("') TO (SELECT * FROM ProcessGroup WHERE `id` = '");
                    queryBuilder.append(componentStatusNode.get("groupId"));
                    queryBuilder.append("') IF NOT EXISTS");
                    queryList.add(new GraphQuery(queryBuilder.toString(), GraphClientService.SQL));
                }
            } else {
                // Add edges between source and destination processors going through the connection node
                final String sourceId = componentStatusNode.get("sourceId").toString();
                final String sourceName = componentStatusNode.get("sourceName").toString();
                // Check for funnels, they do not offer status so they have to be added manually as encountered
                if ("Funnel".equals(sourceName)) {
                    String addFunnelBuilder = "UPDATE Funnel SET id = '" + sourceId + "', name = 'Funnel' UPSERT)";
                    queryList.add(new GraphQuery(addFunnelBuilder, GraphClientService.SQL));
                }

                queryBuilder = new StringBuilder("CREATE EDGE TYPE `");
                queryBuilder.append(componentStatusNode.get("name"));
                queryBuilder.append("` IF NOT EXISTS EXTENDS NiFiFlowConnection");

                queryBuilder = new StringBuilder("CREATE EDGE `");
                queryBuilder.append(componentStatusNode.get("name"));
                queryBuilder.append("` FROM\n(SELECT * FROM NiFiComponent ");
                queryBuilder.append(" WHERE id = '");
                queryBuilder.append(sourceId);
                queryBuilder.append("')\nTO (SELECT * FROM NiFiComponent WHERE id = '");
                final String destinationId = componentStatusNode.get("destinationId").toString();
                final String destinationName = componentStatusNode.get("destinationName").toString();

                // Inject a destination for funnels
                if ("Funnel".equals(destinationName)) {
                    String addFunnelBuilder = "UPDATE Funnel SET id = '" + destinationId + "', name = 'Funnel' UPSERT";
                    queryList.add(new GraphQuery(addFunnelBuilder, GraphClientService.SQL));
                }

                // Continue with the CREATE EDGE query
                queryBuilder.append(destinationId);
                queryBuilder.append("') IF NOT EXISTS SET\n\t");

                final Map<String, Object> connectionProperties = new HashMap<>();
                connectionProperties.put("groupId", componentStatusNode.get("groupId"));
                connectionProperties.put("backPressureBytesThreshold", componentStatusNode.get("backPressureBytesThreshold"));
                connectionProperties.put("backPressureObjectThreshold", componentStatusNode.get("backPressureObjectThreshold"));
                connectionProperties.put("backPressureDataSizeThreshold", componentStatusNode.get("backPressureDataSizeThreshold"));
                connectionProperties.put("type", "Connection");
                final String properties = joinPropertyEntries(connectionProperties, null, " = ", ",\n\t", "");
                queryBuilder.append(properties);
                queryList.add(new GraphQuery(queryBuilder.toString(), GraphClientService.SQL));
            }
        }
        return queryList;
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
                // Remove the node from the graph, if it has been replaced
                queryBuilder = new StringBuilder(generateUseClause(databaseName) + "DELETE FROM Processor WHERE id = '");
                queryBuilder.append(actionNode.get("componentId"));
                queryBuilder.append("'");
                queryList.add(new GraphQuery(queryBuilder.toString(), GraphClientService.SQL));
            }
        }
        return queryList;
    }

    public List<GraphQuery> generateCreateDatabaseQueries(final String databaseName, final boolean isCompositeDatabase) throws GraphClientMethodNotSupported {
        throw new GraphClientMethodNotSupported("SQL does not support creating databases");
    }

    public List<GraphQuery> generateInitialVertexTypeQueries(final String databaseName, final boolean isCompositeDatabase) throws GraphClientMethodNotSupported {
        if (null != databaseName && !databaseName.isEmpty()) {
            this.databaseName = databaseName;
        }
        final List<GraphQuery> queryList = new ArrayList<>();
        StringBuilder queryBuilder;
        // Add the vertex types
        queryList.add(new GraphQuery("CREATE VERTEX TYPE NiFiProvenanceEvent IF NOT EXISTS", GraphClientService.SQL));
        queryList.add(new GraphQuery("CREATE VERTEX TYPE `org.apache.nifi.flowfile.FlowFile` IF NOT EXISTS", GraphClientService.SQL));
        queryList.add(new GraphQuery("CREATE VERTEX TYPE NiFiComponent IF NOT EXISTS", GraphClientService.SQL));

        for (ComponentType componentType : ComponentType.values()) {
            queryList.add(new GraphQuery("CREATE VERTEX TYPE `" + componentType.getTypeName().replace(" ", "") + "` IF NOT EXISTS EXTENDS NiFiComponent", GraphClientService.SQL));
        }

        // Create the event types as vertex types if they haven't been created already
        for (ProvenanceEventType provenanceEventType : ProvenanceEventType.values()) {
            queryBuilder = new StringBuilder();
            queryBuilder.append("CREATE VERTEX TYPE `");
            queryBuilder.append(provenanceEventType.name());
            queryBuilder.append("` IF NOT EXISTS EXTENDS `NiFiProvenanceEvent`");
            queryList.add(new GraphQuery(queryBuilder.toString(), GraphClientService.SQL));
        }
        return queryList;
    }

    public List<GraphQuery> generateInitialEdgeTypeQueries(final String databaseName, final boolean isCompositeDatabase) throws GraphClientMethodNotSupported {
        if (null != databaseName && !databaseName.isEmpty()) {
            this.databaseName = databaseName;
        }
        final List<GraphQuery> queryList = new ArrayList<>();
        // Add the edge types
        queryList.add(new GraphQuery("CREATE EDGE TYPE NiFiFlowConnection IF NOT EXISTS", GraphClientService.SQL));
        queryList.add(new GraphQuery("CREATE EDGE TYPE `next` IF NOT EXISTS EXTENDS NiFiFlowConnection", GraphClientService.SQL));
        queryList.add(new GraphQuery("CREATE EDGE TYPE `parent` IF NOT EXISTS EXTENDS NiFiFlowConnection", GraphClientService.SQL));
        return queryList;
    }

    @Override
    public List<GraphQuery> generateCreateIndexQueries(String databaseName, boolean isCompositeDatabase) throws GraphClientMethodNotSupported {
        throw new GraphClientMethodNotSupported("SQL does not support creating indexes");
    }

    public String generateUseClause(final String databaseName) {
        if (databaseName == null) {
            return "";
        }
        return "USE `" + databaseName + "` ";
    }
}
