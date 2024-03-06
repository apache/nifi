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
package org.apache.nifi.processors.graph.util;

import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.graph.GraphClientService;
import org.apache.nifi.graph.GraphQuery;
import org.apache.nifi.graph.GraphQueryResultCallback;
import org.apache.nifi.graph.GremlinQueryFromNodesBuilder;
import org.apache.nifi.graph.QueryFromNodesBuilder;
import org.apache.nifi.graph.exception.GraphClientMethodNotSupported;
import org.apache.nifi.graph.exception.GraphQueryException;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class InMemoryGraphClient extends AbstractControllerService implements GraphClientService {
    private Graph graph;

    private final QueryFromNodesBuilder gremlinQueryFromNodesBuilder = new GremlinQueryFromNodesBuilder();
    private boolean generateExceptionOnQuery = false;

    private String databaseName;

    public InMemoryGraphClient() {
        this(false);
    }

    public InMemoryGraphClient(final boolean generateExceptionOnQuery) {
        this.generateExceptionOnQuery = generateExceptionOnQuery;
    }

    @OnEnabled
    void onEnabled(ConfigurationContext context) {
        graph = buildGraph();
    }

    private static JanusGraph buildGraph() {
        return JanusGraphFactory.build().set("storage.backend", "inmemory").open();
    }

    @Override
    public Map<String, String> executeQuery(GraphQuery graphQuery, Map<String, Object> parameters, GraphQueryResultCallback graphQueryResultCallback) throws GraphQueryException {
        if(generateExceptionOnQuery) {
            throw new GraphQueryException("Generated test exception");
        }
        ScriptEngine engine = new ScriptEngineManager().getEngineByName("groovy");
        parameters.forEach(engine::put);
        if (graph == null) {
            graph = buildGraph();
        }
        engine.put("graph", graph);
        engine.put("g", graph.traversal());

        Object response;
        try {
            response = engine.eval(graphQuery.getQuery());
        } catch (ScriptException ex) {
            throw new GraphQueryException(ex);
        }

        if (response instanceof Map) {
            //The below logic helps with the handling of complex Map<String, Object> relationships
            Map<String, Object> resultMap = (Map<String, Object>) response;
            if (!resultMap.isEmpty()) {
                // Convertex a resultMap to an entrySet iterator
                // this loops over the outermost map
                for (Object o : resultMap.entrySet()) {
                    Map.Entry<String, Object> innerResultSet = (Map.Entry<String, Object>) o;
                    // this is for edge case handling where innerResultSet is also a Map
                    if (innerResultSet.getValue() instanceof Map) {
                        Iterator<Map.Entry<String, Object>> resultSet = ((Map) innerResultSet.getValue()).entrySet().iterator();
                        // looping over each result in the inner map
                        while (resultSet.hasNext()) {
                            Map.Entry<String, Object> tempResult = resultSet.next();
                            Map<String, Object> tempRetObject = new HashMap<>();
                            tempRetObject.put(tempResult.getKey(), tempResult.getValue());
                            SimpleEntry<String, Object> returnObject = new SimpleEntry<>(tempResult.getKey(), tempRetObject);
                            Map<String, Object> resultReturnMap = new HashMap<>();
                            resultReturnMap.put(innerResultSet.getKey(), returnObject);
                            if (getLogger().isDebugEnabled()) {
                                getLogger().debug(resultReturnMap.toString());
                            }
                            // return the object to the graphQueryResultCallback object
                            graphQueryResultCallback.process(resultReturnMap, resultSet.hasNext());
                        }
                    } else {
                        // for non-maps, return objects need to be a map<string, object> this simply converts the object
                        // to a map to be return to the graphQueryResultCallback object
                        Map<String, Object> resultReturnMap = new HashMap<>();
                        resultReturnMap.put(innerResultSet.getKey(), innerResultSet.getValue());
                        graphQueryResultCallback.process(resultReturnMap, false);
                    }
                }
            }
        }

        return new HashMap<>();
    }

    @Override
    public String getTransitUrl() {
        return "memory://localhost/graph";
    }

    @Override
    public List<GraphQuery> convertActionsToQueries(final List<Map<String, Object>> nodeList) {
        return Collections.emptyList();
    }

    @Override
    public List<GraphQuery> buildFlowGraphQueriesFromNodes(List<Map<String, Object>> eventList, Map<String, Object> parameters) {
        // Build queries from event list
        return gremlinQueryFromNodesBuilder.getFlowGraphQueries(eventList);
    }

    @Override
    public List<GraphQuery> buildProvenanceQueriesFromNodes(List<Map<String, Object>> eventList, Map<String, Object> parameters, final boolean includeFlowGraph) {
        // Build queries from event list
        return gremlinQueryFromNodesBuilder.getProvenanceQueries(eventList, includeFlowGraph);
    }

    @Override
    public List<GraphQuery> generateCreateDatabaseQueries(final String databaseName, final boolean isCompositeDatabase) throws GraphClientMethodNotSupported {
        return gremlinQueryFromNodesBuilder.generateCreateDatabaseQueries(databaseName, isCompositeDatabase);
    }

    @Override
    public List<GraphQuery> generateCreateIndexQueries(final String databaseName, final boolean isCompositeDatabase) throws GraphClientMethodNotSupported {
        return gremlinQueryFromNodesBuilder.generateCreateIndexQueries(databaseName, isCompositeDatabase);
    }

    @Override
    public List<GraphQuery> generateInitialVertexTypeQueries(final String databaseName, final boolean isCompositeDatabase) throws GraphClientMethodNotSupported {
        return gremlinQueryFromNodesBuilder.generateInitialVertexTypeQueries(databaseName, isCompositeDatabase);
    }

    @Override
    public List<GraphQuery> generateInitialEdgeTypeQueries(final String databaseName, final boolean isCompositeDatabase) throws GraphClientMethodNotSupported {
        return gremlinQueryFromNodesBuilder.generateInitialEdgeTypeQueries(databaseName, isCompositeDatabase);
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

}
