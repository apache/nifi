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
import org.apache.nifi.graph.GraphClientService;
import org.apache.nifi.graph.GraphQueryResultCallback;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockCypherClientService extends AbstractControllerService implements GraphClientService {

    @Override
    public Map<String, String> executeQuery(String query, Map<String, Object> parameters, GraphQueryResultCallback handler) {
        handler.process(Map.of("name", "John Smith", "age", 40, "relationship", "ASSOCIATED_WITH"), true);
        handler.process(Map.of("name", "John Smith", "age", 40, "relationship", "ASSOCIATED_WITH"), false);

        Map<String, String> resultAttributes = new HashMap<>();
        resultAttributes.put(NODES_CREATED, String.valueOf(1));
        resultAttributes.put(RELATIONS_CREATED, String.valueOf(1));
        resultAttributes.put(LABELS_ADDED, String.valueOf(1));
        resultAttributes.put(NODES_DELETED, String.valueOf(1));
        resultAttributes.put(RELATIONS_DELETED, String.valueOf(1));
        resultAttributes.put(PROPERTIES_SET, String.valueOf(1));
        resultAttributes.put(ROWS_RETURNED, String.valueOf(1));

        return resultAttributes;
    }

    @Override
    public String getTransitUrl() {
        return "mock://localhost:12345/fake_database";
    }

    @Override
    public String generateSetPropertiesStatement(final String componentType,
                                                 final List<Tuple<String, String>> identifiersAndValues,
                                                 final String nodeType,
                                                 final Map<String, Object> propertyMap) {

        StringBuilder queryBuilder = switch (componentType) {
            case GraphClientService.NODES_TYPE -> getNodeQueryBuilder(identifiersAndValues, nodeType);
            case GraphClientService.EDGES_TYPE -> getEdgeQueryBuilder(identifiersAndValues, nodeType);
            default -> throw new ProcessException("Unsupported component type: " + componentType);
        };

        queryBuilder.append(")\n")
                .append("ON MATCH SET ");

        List<String> setClauses = new ArrayList<>();
        for (Map.Entry<String, Object> entry : propertyMap.entrySet()) {
            StringBuilder setClause = new StringBuilder("n.")
                    .append(entry.getKey())
                    .append(" = ");
            if (entry.getValue() == null) {
                setClause.append(" NULL");
            } else {
                setClause.append("'")
                        .append(entry.getValue())
                        .append("'");
            }
            setClauses.add(setClause.toString());
        }
        String setClauseString = String.join(", ", setClauses);
        queryBuilder.append(setClauseString)
                .append("\nON CREATE SET ")
                .append(setClauseString);

        return queryBuilder.toString();
    }

    private static StringBuilder getNodeQueryBuilder(List<Tuple<String, String>> identifiersAndValues, String nodeType) {
        StringBuilder queryBuilder = new StringBuilder("MERGE (n");
        if (nodeType != null && !nodeType.isEmpty()) {
            queryBuilder.append(":").append(nodeType);
        }

        buildMatchClause(identifiersAndValues, queryBuilder);
        return queryBuilder;
    }

    private static StringBuilder getEdgeQueryBuilder(List<Tuple<String, String>> identifiersAndValues, String edgeType) {
        StringBuilder queryBuilder = new StringBuilder("MERGE (n)<-[e:");

        if (edgeType == null || edgeType.isEmpty()) {
            throw new ProcessException("Edge type must not be null or empty");
        }
        queryBuilder.append(edgeType);

        buildMatchClause(identifiersAndValues, queryBuilder);
        queryBuilder.append("]-> (x)");
        return queryBuilder;
    }

    private static void buildMatchClause(List<Tuple<String, String>> identifiersAndValues, StringBuilder queryBuilder) {
        if (!identifiersAndValues.isEmpty()) {
            queryBuilder.append(" {");

            List<String> identifierNamesAndValues = new ArrayList<>();
            for (Tuple<String, String> identifierAndValue : identifiersAndValues) {
                if (identifierAndValue == null || identifierAndValue.getKey() == null || identifierAndValue.getValue() == null) {
                    throw new ProcessException("Identifiers and values must not be null");
                }

                final String identifierName = identifierAndValue.getKey();
                final Object identifierObject = identifierAndValue.getValue();
                if (identifierObject != null) {
                    identifierNamesAndValues.add(identifierName + ": '" + identifierObject + "'");
                }
            }
            queryBuilder.append(String.join(", ", identifierNamesAndValues))
                    .append("}");
        }
    }
}
