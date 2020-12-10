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

package org.apache.nifi.minifi.commons.schema;

import org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class ProcessGroupSchemaTest {
    @Test
    public void testNoPortsRootGroup() {
        validateIssuesNumMatches(0, new ProcessGroupSchema(new HashMap<>(), ConfigSchema.TOP_LEVEL_NAME));
    }

    @Test
    public void testInputPortsRootGroup() {
        Map<String, Object> map = new HashMap<>();
        map.put(CommonPropertyKeys.INPUT_PORTS_KEY, Arrays.asList(createPortSchema("testId", "testName", ConfigSchema.TOP_LEVEL_NAME).toMap()));
        validateIssuesNumMatches(1, new ProcessGroupSchema(map, ConfigSchema.TOP_LEVEL_NAME));
    }

    @Test
    public void testOutputPortsRootGroup() {
        Map<String, Object> map = new HashMap<>();
        map.put(CommonPropertyKeys.OUTPUT_PORTS_KEY, Arrays.asList(createPortSchema("testId", "testName", ConfigSchema.TOP_LEVEL_NAME).toMap()));
        validateIssuesNumMatches(1, new ProcessGroupSchema(map, ConfigSchema.TOP_LEVEL_NAME));
    }

    private PortSchema createPortSchema(String id, String name, String wrapperName) {
        Map<String, Object> map = new HashMap<>();
        map.put(CommonPropertyKeys.ID_KEY, id);
        map.put(CommonPropertyKeys.NAME_KEY, name);
        return new PortSchema(map, wrapperName);
    }

    private void validateIssuesNumMatches(int expected, ProcessGroupSchema processGroupSchema) {
        int actual = processGroupSchema.getValidationIssues().size();
        String issues = "[" + System.lineSeparator() + processGroupSchema.getValidationIssues().stream().collect(Collectors.joining("," + System.lineSeparator())) + "]";
        assertEquals("Expected " + expected + " issue(s), got " + actual + ": " + issues, expected, actual);
    }
}
