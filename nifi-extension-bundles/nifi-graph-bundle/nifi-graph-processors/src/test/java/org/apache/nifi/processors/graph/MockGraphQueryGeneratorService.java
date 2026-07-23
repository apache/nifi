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
import org.apache.nifi.graph.GraphElementType;
import org.apache.nifi.graph.GraphMutation;
import org.apache.nifi.graph.GraphQueryGeneratorService;

import java.util.HashMap;
import java.util.Map;

public class MockGraphQueryGeneratorService extends AbstractControllerService implements GraphQueryGeneratorService {

    @Override
    public GraphMutation generateSetPropertiesMutation(final GraphElementType elementType, final Map<String, Object> identifiers, final String elementLabel,
                                                       final Map<String, Object> propertyMap) {
        final String query;
        if (propertyMap != null && Boolean.TRUE.equals(propertyMap.get("forceTransientFailure"))) {
            query = "TRANSIENT_FAIL";
        } else if (propertyMap != null && Boolean.TRUE.equals(propertyMap.get("forceFailure"))) {
            query = "FAIL";
        } else {
            query = elementType.name() + ":" + (elementLabel == null ? "" : elementLabel);
        }

        final Map<String, Object> parameters = new HashMap<>();
        parameters.put("properties", propertyMap == null ? Map.of() : propertyMap);
        parameters.put("elementType", elementType.name());
        parameters.put("elementLabel", elementLabel);
        parameters.put("identifierCount", identifiers == null ? 0 : identifiers.size());

        return new GraphMutation(query, parameters);
    }
}
