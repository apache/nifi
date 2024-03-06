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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CypherQueryFromNodesBuilder implements QueryFromNodesBuilder {

    private String databaseName;

    @Override
    public List<GraphQuery> getProvenanceQueries(final List<Map<String, Object>> nodeList, final boolean includeFlowGraph) {
        // Assume at least four queries per node
        final List<GraphQuery> queryList = new ArrayList<>(nodeList.size() * 4);
        for (Map<String, Object> eventNode : nodeList) {
            StringBuilder queryBuilder = new StringBuilder(generateUseClause(databaseName) + "MERGE (p:`" + eventNode.get("eventType") + "`: NiFiProvenanceEvent {id: '");
            queryBuilder.append(eventNode.get("eventId"));
            queryBuilder.append("'})\nON CREATE SET \n\t");
            queryBuilder.append(joinPropertyEntries(eventNode, "p", " =", ",\n\t", ""));
            queryBuilder.append("\nON MATCH SET \n\t");
            queryBuilder.append(joinPropertyEntries(eventNode, "p", " =", ",\n\t", ""));
            queryList.add(new GraphQuery(queryBuilder.toString(), GraphClientService.CYPHER));

            // Add its entity (FlowFile, e.g.) to the graph
            final Object entityTypeObject = eventNode.get("entityType");
            final String entityType = entityTypeObject != null ? entityTypeObject.toString() : "unknown";
            queryBuilder = new StringBuilder(generateUseClause(databaseName) + "MERGE (ff:`");
            queryBuilder.append(entityType);
            queryBuilder.append("` {id: '");
            queryBuilder.append(eventNode.get("entityId"));
            queryBuilder.append("', type:'");
            // Add a "type" property with just the class name (not the fully-qualified name)
            queryBuilder.append(entityType.substring(entityType.lastIndexOf(".") + 1));
            queryBuilder.append("'})");
            queryList.add(new GraphQuery(queryBuilder.toString(), GraphClientService.CYPHER));

            // Add its component to the graph
            queryBuilder = new StringBuilder(generateUseClause(databaseName) + "MERGE (p:`" + eventNode.get("componentType") + "` {");
            queryBuilder.append("componentId: '");
            queryBuilder.append(eventNode.get("componentId"));
            queryBuilder.append("'})\nON CREATE SET \n\t");
            queryBuilder.append("componentName = '");
            queryBuilder.append(eventNode.get("componentName"));
            queryBuilder.append("'\nON MATCH SET \n\t");

            // Add an "entity" edge between them
            final String eventOrdinal = eventNode.get("eventOrdinal").toString();
            queryBuilder = new StringBuilder(generateUseClause(databaseName) + "MATCH (x:`" + eventNode.get("entityType") + "` {id: '");
            queryBuilder.append(eventNode.get("entityId"));
            queryBuilder.append("'}),\n(y:NiFiProvenanceEvent {eventOrdinal: '");
            queryBuilder.append(eventOrdinal);
            queryBuilder.append("'})\nMERGE(x) <-[:entity]- (y)");
            queryList.add(new GraphQuery(queryBuilder.toString(), GraphClientService.CYPHER));

            List<Long> previousEventIds = (List<Long>) eventNode.get("previousEventIds");

            // If there are previous event IDs, add edges
            if (previousEventIds != null) {
                // Match the source (previous event) and target (this event) and create the edge if it doesn't exist
                for (Long previousEventId : previousEventIds) {
                    queryBuilder = new StringBuilder(generateUseClause(databaseName) + "MATCH (x:NiFiProvenanceEvent {eventOrdinal: '");
                    queryBuilder.append(previousEventId);
                    queryBuilder.append("'}), (y:NiFiProvenanceEvent {eventOrdinal: '");
                    queryBuilder.append(eventOrdinal);
                    queryBuilder.append("'})\nMERGE (x)-[z:next]->(y)\n");
                    queryBuilder.append("ON CREATE SET\n\tz.relationship = x.relationship,\n\tz.sourceEventOrdinal = x.eventOrdinal,\n\tz.destinationEventOrdinal = y.eventOrdinal\n");
                    queryBuilder.append("ON MATCH SET \n\tz.relationship = x.relationship,\n\tz.sourceEventOrdinal = x.eventOrdinal,\n\tz.destinationEventOrdinal = y.eventOrdinal");

                    // Add edge to graph
                    queryList.add(new GraphQuery(queryBuilder.toString(), GraphClientService.CYPHER));
                }
            }

            // Connect to flow graph nodes if specified
            if (includeFlowGraph) {
                // Add a link between the
                queryBuilder = new StringBuilder(generateUseClause(databaseName) + "MATCH (x {id: '");
                queryBuilder.append(eventNode.get("componentId"));
                queryBuilder.append("'}),\n(y {eventOrdinal: '");
                queryBuilder.append(eventOrdinal);
                queryBuilder.append("'})\nMERGE(x) -[:reported]-> (y)");
                queryList.add(new GraphQuery(queryBuilder.toString(), GraphClientService.CYPHER));
            }
        }
        return queryList;
    }

    @Override
    public List<GraphQuery> getFlowGraphQueries(final List<Map<String, Object>> nodeList) {
        final List<GraphQuery> queryList = new ArrayList<>();
        for (Map<String, Object> componentStatusNode : nodeList) {
            StringBuilder queryBuilder;
            // Skip Connection nodes, put all the information into the edge
            if (!"Connection".equals(componentStatusNode.get("type"))) {
                queryBuilder = new StringBuilder(generateUseClause(databaseName) + "MERGE (p:`" + componentStatusNode.get("type") + "` {id: '");
                queryBuilder.append(componentStatusNode.get("id"));
                queryBuilder.append("'})\nON CREATE SET \n\t");
                final String properties = joinPropertyEntries(componentStatusNode, "p", " =", ",\n\t", "");
                queryBuilder.append(properties);
                queryBuilder.append("\nON MATCH SET \n\t");
                queryBuilder.append(properties);
                queryList.add(new GraphQuery(queryBuilder.toString(), GraphClientService.CYPHER));

                // Add an edge to the parent process group if appropriate
                if (componentStatusNode.containsKey("groupId")) {
                    queryBuilder = new StringBuilder(generateUseClause(databaseName) + "MATCH (x:`" + componentStatusNode.get("type") + "` {id: '");
                    queryBuilder.append(componentStatusNode.get("id"));
                    queryBuilder.append("'}),\n(y:ProcessGroup {id: '");
                    queryBuilder.append(componentStatusNode.get("groupId"));
                    queryBuilder.append("'})\nMERGE(x)-[:parent]->(y)");
                    queryList.add(new GraphQuery(queryBuilder.toString(), GraphClientService.CYPHER));
                }
            } else {
                // Add edges between source and destination processors going through the connection node
                final String sourceId = componentStatusNode.get("sourceId").toString();
                final String sourceName = componentStatusNode.get("sourceName").toString();
                // Check for funnels, they do not offer status so they have to be added manually as encountered
                if ("Funnel".equals(sourceName)) {
                    String addFunnelBuilder = generateUseClause(databaseName) + "MERGE (p:Funnel {id: '" + sourceId + "'})";
                    queryList.add(new GraphQuery(addFunnelBuilder, GraphClientService.CYPHER));
                }

                queryBuilder = new StringBuilder(generateUseClause(databaseName) + "MATCH (x {id: '");
                queryBuilder.append(sourceId);
                queryBuilder.append("'}),\n");
                final String destinationId = componentStatusNode.get("destinationId").toString();
                final String destinationName = componentStatusNode.get("destinationName").toString();
                if ("Funnel".equals(destinationName)) {
                    String addFunnelBuilder = generateUseClause(databaseName) + "MERGE (p:Funnel {id: '" + destinationId + "', name:'Funnel'})";
                    queryList.add(new GraphQuery(addFunnelBuilder, GraphClientService.CYPHER));
                }
                queryBuilder.append("(z {id: '");
                queryBuilder.append(destinationId);
                queryBuilder.append("'})\nMERGE (x)-[y:`");
                final String componentName = componentStatusNode.get("name").toString().isEmpty()
                        ? "funnel"
                        : componentStatusNode.get("name").toString();
                queryBuilder.append(componentName);
                queryBuilder.append("`:NiFiFlowConnection]->(z)\nON CREATE SET\n\t");
                final Map<String, Object> connectionProperties = new HashMap<>();
                connectionProperties.put("groupId", componentStatusNode.get("groupId"));
                connectionProperties.put("backPressureBytesThreshold", componentStatusNode.get("backPressureBytesThreshold"));
                connectionProperties.put("backPressureObjectThreshold", componentStatusNode.get("backPressureObjectThreshold"));
                connectionProperties.put("backPressureDataSizeThreshold", componentStatusNode.get("backPressureDataSizeThreshold"));
                connectionProperties.put("type", "Connection");
                final String properties = joinPropertyEntries(connectionProperties, "y", " = ", ",\n\t", "");
                queryBuilder.append(properties);
                queryBuilder.append("\nON MATCH SET \n\t");
                queryBuilder.append(properties);
                queryList.add(new GraphQuery(queryBuilder.toString(), GraphClientService.CYPHER));
            }
        }
        return queryList;
    }

    @Override
    public List<GraphQuery> convertActionsToQueries(final List<Map<String, Object>> nodeList) {
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
                queryBuilder = new StringBuilder(generateUseClause(databaseName) + "MATCH (p:Processor {id: '");
                queryBuilder.append(actionNode.get("componentId"));
                queryBuilder.append("'} DETACH DELETE p");
                queryList.add(new GraphQuery(queryBuilder.toString(), GraphClientService.CYPHER));
            }
        }
        return queryList;
    }

    @Override
    public List<GraphQuery> generateCreateDatabaseQueries(String databaseName, boolean isCompositeDatabase) {
        final List<GraphQuery> queryList = new ArrayList<>();
        StringBuilder queryBuilder;
        // If the provenance database name is not specified, use the default database in the graph DB, otherwise create and use it
        if (null != databaseName && !databaseName.isEmpty()) {
            this.databaseName = databaseName;
            queryBuilder = new StringBuilder("CREATE ");
            if (isCompositeDatabase) {
                queryBuilder.append("COMPOSITE ");
            }
            queryBuilder.append("DATABASE `");
            queryBuilder.append(databaseName);
            queryBuilder.append("` IF NOT EXISTS");
            queryList.add(new GraphQuery(queryBuilder.toString(), GraphClientService.CYPHER));
        }
        return queryList;
    }

    @Override
    public List<GraphQuery> generateCreateIndexQueries(String databaseName, boolean isCompositeDatabase) throws GraphClientMethodNotSupported {
        if (null != databaseName && !databaseName.isEmpty()) {
            this.databaseName = databaseName;
            return Arrays.asList(
                    new GraphQuery(generateUseClause(databaseName)
                            + "CREATE INDEX prov_event_id_index IF NOT EXISTS FOR (n:NiFiProvenanceEvent) ON (n.id)", GraphClientService.CYPHER),
                    new GraphQuery(generateUseClause(databaseName)
                            + "CREATE INDEX prov_event_entityId_index IF NOT EXISTS FOR (n:NiFiProvenanceEvent) ON (n.entityId)", GraphClientService.CYPHER),
                    new GraphQuery(generateUseClause(databaseName)
                            + "CREATE INDEX prov_event_componentId_index IF NOT EXISTS FOR (n:NiFiProvenanceEvent) ON (n.componentId)", GraphClientService.CYPHER),
                    new GraphQuery(generateUseClause(databaseName)
                            + "CREATE INDEX prov_event_next_index IF NOT EXISTS\nFOR ()-[r:next]-() ON (r.sourceEventOrdinal,r.destinationEventOrdinal)", GraphClientService.CYPHER),
                    new GraphQuery(generateUseClause(databaseName)
                            + "CREATE INDEX component_connection_index IF NOT EXISTS\nFOR ()-[r:NiFiFlowConnection]-() ON (r.sourceId,r.destinationId)", GraphClientService.CYPHER)
            );
        } else {
            throw new GraphClientMethodNotSupported("Could not CREATE INDEX queries for Cypher as no database was specified");
        }
    }

    @Override
    public List<GraphQuery> generateInitialVertexTypeQueries(final String databaseName, final boolean isCompositeDatabase) {
        // Vertex types are added as nodes are created
        return Collections.emptyList();
    }

    @Override
    public List<GraphQuery> generateInitialEdgeTypeQueries(final String databaseName, final boolean isCompositeDatabase) {
        // Edge types are added as edges are created
        return Collections.emptyList();
    }

    public String generateUseClause(final String databaseName) {
        if (databaseName == null) {
            return "";
        }
        return "USE `" + databaseName + "` ";
    }
}
