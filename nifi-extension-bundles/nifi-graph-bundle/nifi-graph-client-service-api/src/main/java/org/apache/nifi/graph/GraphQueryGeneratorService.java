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

import java.util.Map;

public interface GraphQueryGeneratorService extends ControllerService {

    /**
     * Generates a query statement for setting properties on matched graph elements in the language associated with the graph database.
     *
     * @param elementType The graph element type to enrich, such as NODE or EDGE
     * @param identifiers The identifier property names and values used to match existing elements
     * @param elementLabel The graph element label to match on, for example "Person", "Organization", or "KNOWS"
     * @param propertyMap The property names and values to set on matched elements
     * @return A {@link GraphMutation} containing the generated query and parameters
     */
    GraphMutation generateSetPropertiesMutation(GraphElementType elementType, Map<String, Object> identifiers, String elementLabel, Map<String, Object> propertyMap);
}
