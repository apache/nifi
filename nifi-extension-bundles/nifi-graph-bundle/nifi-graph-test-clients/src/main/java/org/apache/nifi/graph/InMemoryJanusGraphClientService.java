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

import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.core.JanusGraphFactory;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.util.HashMap;
import java.util.Map;

/**
 * This is an in memory implementation of the GraphClientService using JanusGraph. It should in no way be confused for a
 * production-ready capability. It is intended to provide a fairly realistic environment for testing Gremlin script
 * submission.
 */
public class InMemoryJanusGraphClientService extends AbstractControllerService implements GraphClientService {
    private Graph graph;

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        graph = JanusGraphFactory.build().set("storage.backend", "inmemory").open();
    }

    /**
     * Execute the query.
     *
     * This instantiate a new script engine every time to ensure a pristine environment for testing.
     *
     * @param query A gremlin query (Groovy syntax)
     * @param parameters A map of parameters to be injected into the script. This can be structured the way you would
     *                   expect a REST API call to Gremlin Server.
     * @param handler The callback for parsing the rsponse.
     * @return Empty map. This API feature is only filled with values typically when dealing with Cypher clients, Neo4J
     * in particular.
     */
    @Override
    public Map<String, String> executeQuery(String query, Map<String, Object> parameters, GraphQueryResultCallback handler) {
        ScriptEngine engine = new ScriptEngineManager().getEngineByName("groovy");
        parameters.entrySet().forEach(entry -> engine.put(entry.getKey(), entry.getValue()));

        engine.put("graph", graph);
        engine.put("g", graph.traversal());

        try {
            Object response = engine.eval(query);

            if (response instanceof Map) {
                Map resp = (Map) response;
                Map<String, Object> result = new HashMap<>();
                result.put("result", resp.entrySet().iterator().next());
                handler.process(result,false);
            } else {
                Map<String, Object> result = new HashMap<>();
                result.put("result", response);
                handler.process(result, false);
            }

            return new HashMap<>();
        } catch (Exception ex) {
            throw new ProcessException(ex);
        }
    }

    @Override
    public String getTransitUrl() {
        return "janusgraph:memory://localhost";
    }

    /**
     * Getter for accessing the generated JanusGraph object once the client service is activated in a test.
     * The purpose of this is to allow testers to get access to the graph so they can do things like run traversals
     * on it.
     *
     * @return Tinkerpop Graph object representing the in memory graph database.
     */
    public Graph getGraph() {
        return graph;
    }
}
