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

import org.apache.nifi.controller.ControllerService;

import java.util.List;
import java.util.Map;

public interface GraphClientService extends ControllerService {
    String NODES_CREATED = "graph.nodes.created";
    String RELATIONS_CREATED = "graph.relations.created";
    String LABELS_ADDED = "graph.labels.added";
    String NODES_DELETED = "graph.nodes.deleted";
    String RELATIONS_DELETED = "graph.relations.deleted";
    String PROPERTIES_SET = "graph.properties.set";
    String ROWS_RETURNED = "graph.rows.returned";

    // Supported query languages (service-dependent)
    String SQL = "sql";
    String SQL_SCRIPT = "sqlscript";
    String GRAPHQL = "graphql";
    String CYPHER = "cypher";
    String GREMLIN = "gremlin";
    String MONGO = "mongo";

    /**
     * Executes the specified query using the client service, and handles and returned results by calling the specified callback
     *
     * @param query      The query to execute
     * @param parameters A Map of parameter values to use in the query and/or execution
     * @param handler    The callback handler invoked with any returned results
     * @return Any results returned after handling the query response
     */
    Map<String, String> executeQuery(String query, Map<String, Object> parameters, GraphQueryResultCallback handler);

    /**
     * Returns the URL used to submit the query
     *
     * @return the URL (as a string) used to submit the query
     */
    String getTransitUrl();

    /**
     * Builds a list of client-specific queries based on a list of property map nodes. Usually followed by a call to executeQuery
     *
     * @param nodeList   A List of Maps corresponding to property map nodes
     * @param parameters A Map of parameter values to use in the query and/or execution
     * @return A List of queries each corresponding to an operation on the node list
     */
    List<GraphQuery> buildQueryFromNodes(List<Map<String, Object>> nodeList, Map<String, Object> parameters);
}