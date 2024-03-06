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
import java.util.List;
import java.util.Map;

public interface QueryFromNodesBuilder {

    List<GraphQuery> getProvenanceQueries(final List<Map<String, Object>> nodeList, final boolean includeFlowGraph);

    List<GraphQuery> getFlowGraphQueries(final List<Map<String, Object>> nodeList);

    List<GraphQuery> convertActionsToQueries(final List<Map<String, Object>> nodeList);

    List<GraphQuery> generateCreateDatabaseQueries(final String databaseName, final boolean isCompositeDatabase) throws GraphClientMethodNotSupported;

    List<GraphQuery> generateCreateIndexQueries(final String databaseName, final boolean isCompositeDatabase) throws GraphClientMethodNotSupported;

    List<GraphQuery> generateInitialVertexTypeQueries(final String databaseName, final boolean isCompositeDatabase) throws GraphClientMethodNotSupported;

    List<GraphQuery> generateInitialEdgeTypeQueries(final String databaseName, final boolean isCompositeDatabase) throws GraphClientMethodNotSupported;

    default String joinPropertyEntries(final Map<String, Object> componentPropertiesMap, final String nodeName, final String valueSeparator, final String joinSeparator, final String prefix) {
        final List<String> propertyDefinitions = new ArrayList<>(componentPropertiesMap.entrySet().size());

        for (Map.Entry<String, Object> property : componentPropertiesMap.entrySet()) {
            if (property.getValue() instanceof String) {
                propertyDefinitions.add((prefix == null ? "" : prefix) + (nodeName == null ? "" : nodeName + ".") + property.getKey() + valueSeparator + "'" + property.getValue() + "'");
            } else {
                propertyDefinitions.add((prefix == null ? "" : prefix) + (nodeName == null ? "" : nodeName + ".") + property.getKey() + valueSeparator + property.getValue());
            }
        }

        return String.join(joinSeparator, propertyDefinitions);
    }

    String generateUseClause(final String databaseName);
}
