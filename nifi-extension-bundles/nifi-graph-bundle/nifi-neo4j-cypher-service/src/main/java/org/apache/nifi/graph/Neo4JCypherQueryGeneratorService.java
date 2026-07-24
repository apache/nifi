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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

@Tags({"graph", "neo4j", "cypher", "query", "generator"})
@CapabilityDescription("Generates parameterized Cypher upsert statements for graph elements from identifier and property values.")
public class Neo4JCypherQueryGeneratorService extends AbstractControllerService implements GraphQueryGeneratorService {
    private static final String PROPERTIES_PARAMETER = "properties";
    private static final String NODE_ALIAS = "n";
    private static final String EDGE_ALIAS = "e";

    @Override
    public GraphMutation generateSetPropertiesMutation(final GraphElementType elementType, final Map<String, Object> identifiers, final String elementLabel,
                                                       final Map<String, Object> propertyMap) {
        if (elementType == null) {
            throw new ProcessException("Graph element type must be specified");
        }

        final Map<String, Object> mutationParameters = new HashMap<>();
        final StringBuilder queryBuilder = new StringBuilder();

        if (GraphElementType.NODE == elementType) {
            queryBuilder.append("MERGE (").append(NODE_ALIAS);
            if (!StringUtils.isBlank(elementLabel)) {
                queryBuilder.append(":").append(validateCypherToken(elementLabel, "Element Label"));
            }

            appendIdentifierClause(queryBuilder, identifiers, mutationParameters);
            queryBuilder.append(")\n");
            queryBuilder.append("ON MATCH SET ").append(NODE_ALIAS).append(" += $").append(PROPERTIES_PARAMETER).append("\n");
            queryBuilder.append("ON CREATE SET ").append(NODE_ALIAS).append(" += $").append(PROPERTIES_PARAMETER);
        } else if (GraphElementType.EDGE.equals(elementType)) {
            if (StringUtils.isBlank(elementLabel)) {
                throw new ProcessException("Element Label must be set when enriching edges");
            }

            queryBuilder.append("MATCH ()-[").append(EDGE_ALIAS).append(":").append(validateCypherToken(elementLabel, "Element Label"));
            appendIdentifierClause(queryBuilder, identifiers, mutationParameters);
            queryBuilder.append("]-()\n");
            queryBuilder.append("SET ").append(EDGE_ALIAS).append(" += $").append(PROPERTIES_PARAMETER);
        } else {
            throw new ProcessException("Unsupported graph element type: " + elementType);
        }

        final Map<String, Object> properties = propertyMap == null ? Map.of() : propertyMap;
        mutationParameters.put(PROPERTIES_PARAMETER, properties);

        return new GraphMutation(queryBuilder.toString(), mutationParameters);
    }

    private void appendIdentifierClause(final StringBuilder queryBuilder, final Map<String, Object> identifiers, final Map<String, Object> mutationParameters) {
        if (identifiers == null || identifiers.isEmpty()) {
            return;
        }

        queryBuilder.append(" {");
        int index = 0;
        for (final Map.Entry<String, Object> identifierEntry : identifiers.entrySet()) {
            if (identifierEntry.getKey() == null || identifierEntry.getValue() == null) {
                throw new ProcessException("Identifiers and values must not be null");
            }

            if (index > 0) {
                queryBuilder.append(", ");
            }

            final String identifierName = validateCypherToken(identifierEntry.getKey(), "Identifier name");
            final String parameterName = "identifier_" + index;
            queryBuilder.append(identifierName).append(": $").append(parameterName);
            mutationParameters.put(parameterName, identifierEntry.getValue());
            index++;
        }
        queryBuilder.append("}");
    }

    private String validateCypherToken(final String token, final String tokenType) {
        if (StringUtils.isBlank(token)) {
            throw new ProcessException(tokenType + " must not be blank");
        }

        for (int index = 0; index < token.length(); index++) {
            final char character = token.charAt(index);
            if (!Character.isLetterOrDigit(character) && character != '_') {
                throw new ProcessException(tokenType + " '" + token + "' contains invalid character '" + character + "'. Only [A-Za-z0-9_] are supported.");
            }
        }

        if (Character.isDigit(token.charAt(0))) {
            throw new ProcessException(tokenType + " '" + token + "' must not start with a digit.");
        }

        return token;
    }
}
