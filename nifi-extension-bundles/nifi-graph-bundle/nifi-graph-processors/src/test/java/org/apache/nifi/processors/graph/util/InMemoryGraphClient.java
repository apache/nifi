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
import org.apache.nifi.graph.GraphQueryResultCallback;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class InMemoryGraphClient extends AbstractControllerService implements GraphClientService {
    private Graph graph;
    private boolean generateExceptionOnQuery = false;

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
    public Map<String, String> executeQuery(String query, Map<String, Object> parameters, GraphQueryResultCallback graphQueryResultCallback) {
        if(generateExceptionOnQuery) {
            throw new ProcessException("Generated test exception");
        }
        ScriptEngine engine = new ScriptEngineManager().getEngineByName("groovy");
        parameters.entrySet().stream().forEach( it -> {
            engine.put(it.getKey(), it.getValue());
        });
        if (graph == null) {
            graph = buildGraph();
        }
        engine.put("graph", graph);
        engine.put("g", graph.traversal());

        Object response;
        try {
            response = engine.eval(query);
        } catch (ScriptException ex) {
            throw new ProcessException(ex);
        }

        if (response instanceof Map) {
            //The below logic helps with the handling of complex Map<String, Object> relationships
            Map resultMap = (Map) response;
            if (!resultMap.isEmpty()) {
                // Convertex a resultMap to an entrySet iterator
                Iterator outerResultSet = resultMap.entrySet().iterator();
                // this loops over the outermost map
                while(outerResultSet.hasNext()) {
                    Map.Entry<String, Object> innerResultSet = (Map.Entry<String, Object>) outerResultSet.next();
                    // this is for edge case handling where innerResultSet is also a Map
                    if (innerResultSet.getValue() instanceof Map) {
                        Iterator resultSet = ((Map) innerResultSet.getValue()).entrySet().iterator();
                        // looping over each result in the inner map
                        while (resultSet.hasNext()) {
                            Map.Entry<String, Object> tempResult = (Map.Entry<String, Object>) resultSet.next();
                            Map<String, Object> tempRetObject = new HashMap<>();
                            tempRetObject.put(tempResult.getKey(), tempResult.getValue());
                            SimpleEntry returnObject = new SimpleEntry<String, Object>(tempResult.getKey(), tempRetObject);
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
}
