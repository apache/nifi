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

package org.apache.nifi.processors.graph;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.graph.CypherQueryFromNodesBuilder;
import org.apache.nifi.graph.GraphClientService;
import org.apache.nifi.graph.GraphQuery;
import org.apache.nifi.graph.GraphQueryResultCallback;
import org.apache.nifi.graph.QueryFromNodesBuilder;
import org.apache.nifi.graph.exception.GraphClientMethodNotSupported;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockCypherClientService extends AbstractControllerService implements GraphClientService {

    protected volatile QueryFromNodesBuilder cypherQueryFromNodesBuilder = new CypherQueryFromNodesBuilder();

    @Override
    public Map<String, String> executeQuery(GraphQuery graphQuery, Map<String, Object> parameters, GraphQueryResultCallback handler) {
        handler.process(new HashMap<String, Object>(){{
            put("name", "John Smith");
            put("age", 40);
        }}, true);
        handler.process(new HashMap<String, Object>(){{
            put("name", "John Smith");
            put("age", 40);
        }}, false);

        Map<String,String> resultAttributes = new HashMap<>();
        resultAttributes.put(NODES_CREATED,String.valueOf(1));
        resultAttributes.put(RELATIONS_CREATED,String.valueOf(1));
        resultAttributes.put(LABELS_ADDED,String.valueOf(1));
        resultAttributes.put(NODES_DELETED,String.valueOf(1));
        resultAttributes.put(RELATIONS_DELETED,String.valueOf(1));
        resultAttributes.put(PROPERTIES_SET, String.valueOf(1));
        resultAttributes.put(ROWS_RETURNED, String.valueOf(1));

        return resultAttributes;
    }

    @Override
    public String getTransitUrl() {
        return "mock://localhost:12345/fake_database";
    }

    @Override
    public List<GraphQuery> convertActionsToQueries(final List<Map<String, Object>> nodeList) {
        return Collections.emptyList();
    }

    @Override
    public List<GraphQuery> buildFlowGraphQueriesFromNodes(List<Map<String, Object>> eventList, Map<String, Object> parameters) {
        // Build queries from event list
        return cypherQueryFromNodesBuilder.getFlowGraphQueries(eventList);
    }

    @Override
    public List<GraphQuery> buildProvenanceQueriesFromNodes(List<Map<String, Object>> eventList, Map<String, Object> parameters, final boolean includeFlowGraph) {
        // Build queries from event list
        return cypherQueryFromNodesBuilder.getProvenanceQueries(eventList, includeFlowGraph);
    }

    @Override
    public List<GraphQuery> generateCreateDatabaseQueries(final String databaseName, final boolean isCompositeDatabase) throws GraphClientMethodNotSupported {
        return cypherQueryFromNodesBuilder.generateCreateDatabaseQueries(databaseName, isCompositeDatabase);
    }

    @Override
    public List<GraphQuery> generateCreateIndexQueries(final String databaseName, final boolean isCompositeDatabase) throws GraphClientMethodNotSupported {
        return cypherQueryFromNodesBuilder.generateCreateIndexQueries(databaseName, isCompositeDatabase);
    }

    @Override
    public List<GraphQuery> generateInitialVertexTypeQueries(final String databaseName, final boolean isCompositeDatabase) throws GraphClientMethodNotSupported {
        return cypherQueryFromNodesBuilder.generateInitialVertexTypeQueries(databaseName, isCompositeDatabase);
    }

    @Override
    public List<GraphQuery> generateInitialEdgeTypeQueries(final String databaseName, final boolean isCompositeDatabase) throws GraphClientMethodNotSupported {
        return cypherQueryFromNodesBuilder.generateInitialEdgeTypeQueries(databaseName, isCompositeDatabase);
    }

    @Override
    public String getDatabaseName() {
        return "mockDB";
    }
}
