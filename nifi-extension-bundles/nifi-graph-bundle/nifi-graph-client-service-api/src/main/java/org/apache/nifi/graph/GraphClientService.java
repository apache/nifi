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
import org.apache.nifi.util.Tuple;

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

    String NODES_TYPE = "Nodes";
    String EDGES_TYPE = "Edges";

    /**
     * Executes the given query using the parameters provided and returns a map of results.
     * @param query The query to execute
     * @param parameters The parameter values to be used in the query
     * @param handler A callback handler to process the results of the query
     * @return A map containing the results of the query execution, where the keys are column names and the values are the corresponding data.
     */
    Map<String, String> executeQuery(final String query, final Map<String, Object> parameters, final GraphQueryResultCallback handler);
    String getTransitUrl();

    /**
     * Generates a query/statement for setting properties on matched node(s) in the language associated with the Graph Database
     * @param componentType The type of component that is executing the query, e.g. "Nodes", "Edges", etc.
     * @param identifiersAndValues A tuple containing the name of and value for the property to match on,
     * @param nodeType The type of node to match on, e.g. "Person", "Organization", etc.
     * @param propertyMap A map of key/value pairs of property names and values to set on the matched node(s)
     * @return A query/statement that can be executed against the Graph Database to set the properties on the matched node(s)
     */
    default String generateSetPropertiesStatement(final String componentType, final List<Tuple<String, String>> identifiersAndValues, final String nodeType, final Map<String, Object> propertyMap) {
        throw new UnsupportedOperationException("This capability is not implemented for this GraphClientService");
    }
}
