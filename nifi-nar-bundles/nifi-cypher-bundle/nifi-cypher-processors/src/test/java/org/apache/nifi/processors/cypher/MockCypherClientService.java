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

package org.apache.nifi.processors.cypher;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.cypher.CypherClientService;
import org.apache.nifi.cypher.CypherQueryResultCallback;

import java.util.HashMap;
import java.util.Map;

public class MockCypherClientService extends AbstractControllerService implements CypherClientService {

    @Override
    public Map<String, String> executeQuery(String query, Map<String, Object> parameters, CypherQueryResultCallback handler) {
        handler.process(new HashMap<String, Object>(){{
            put("name", "John Smith");
            put("age", 40);
        }}, true);
        handler.process(new HashMap<String, Object>(){{
            put("name", "John Smith");
            put("age", 40);
        }}, false);

        Map<String,String> resultAttributes = new HashMap<>();
        resultAttributes.put(NODES_CREATED,String.valueOf(1));
        resultAttributes.put(RELATIONS_CREATED,String.valueOf(1));
        resultAttributes.put(LABELS_ADDED,String.valueOf(1));
        resultAttributes.put(NODES_DELETED,String.valueOf(1));
        resultAttributes.put(RELATIONS_DELETED,String.valueOf(1));
        resultAttributes.put(PROPERTIES_SET, String.valueOf(1));
        resultAttributes.put(ROWS_RETURNED, String.valueOf(1));

        return resultAttributes;
    }

    @Override
    public String getTransitUrl() {
        return "mock://localhost:12345/fake_database";
    }
}
