/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.nifi.minifi.toolkit.schema.v2;

import org.apache.nifi.minifi.toolkit.schema.ConfigSchema;
import org.apache.nifi.minifi.toolkit.schema.PortSchema;
import org.apache.nifi.minifi.toolkit.schema.RemoteProcessGroupSchema;
import org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RemoteProcessGroupSchemaV2Test {
    @Test
    public void testNoPropertiesSet() {
        validateIssuesNumMatches(3, new RemoteProcessGroupSchemaV2(new HashMap<>()));
    }

    @Test
    public void testInputPortsRootGroup() {
        Map<String, Object> map = new HashMap<>();
        map.put(CommonPropertyKeys.INPUT_PORTS_KEY, Collections.singletonList(createPortSchema("f94d2469-39f8-4f07-a0d8-acd9396f639e", "testName", ConfigSchema.TOP_LEVEL_NAME).toMap()));
        map.put(RemoteProcessGroupSchema.URL_KEY, "http://localhost:8080/nifi");
        map.put(CommonPropertyKeys.ID_KEY, "a58d2fab-7efe-4cb7-8224-12a60bd8003d");
        validateIssuesNumMatches(0, new RemoteProcessGroupSchemaV2(map));
    }

    @Test
    public void testTransportProtocol() {
        Map<String, Object> map = new HashMap<>();
        map.put(CommonPropertyKeys.INPUT_PORTS_KEY, Collections.singletonList(createPortSchema("f94d2469-39f8-4f07-a0d8-acd9396f639e", "testName", ConfigSchema.TOP_LEVEL_NAME).toMap()));
        map.put(RemoteProcessGroupSchema.URL_KEY, "http://localhost:8080/nifi");
        map.put(CommonPropertyKeys.ID_KEY, "a58d2fab-7efe-4cb7-8224-12a60bd8003d");
        map.put(RemoteProcessGroupSchema.TRANSPORT_PROTOCOL_KEY, "not valid");
        validateIssuesNumMatches(1, new RemoteProcessGroupSchemaV2(map));

        map.put(RemoteProcessGroupSchema.TRANSPORT_PROTOCOL_KEY, "RAW");
        RemoteProcessGroupSchemaV2 first =  new RemoteProcessGroupSchemaV2(map);
        validateIssuesNumMatches(0, first);
        assertEquals(first.getTransportProtocol(), "RAW");

        map.put(RemoteProcessGroupSchema.TRANSPORT_PROTOCOL_KEY, "HTTP");
        RemoteProcessGroupSchemaV2 second =  new RemoteProcessGroupSchemaV2(map);
        validateIssuesNumMatches(0, second);
        assertEquals(second.getTransportProtocol(), "HTTP");
    }

    private PortSchema createPortSchema(String id, String name, String wrapperName) {
        Map<String, Object> map = new HashMap<>();
        map.put(CommonPropertyKeys.ID_KEY, id);
        map.put(CommonPropertyKeys.NAME_KEY, name);
        return new PortSchema(map, wrapperName);
    }

    private void validateIssuesNumMatches(int expected, RemoteProcessGroupSchemaV2 remoteProcessGroupSchema) {
        int actual = remoteProcessGroupSchema.getValidationIssues().size();
        String issues = "[" + System.lineSeparator() + remoteProcessGroupSchema.getValidationIssues().stream().collect(Collectors.joining("," + System.lineSeparator())) + "]";
        assertEquals(expected, actual, "Expected " + expected + " issue(s), got " + actual + ": " + issues);
    }
}
