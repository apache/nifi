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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.cypher.CypherClientService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Cypher unit tests.
 */
public class TestExecuteCypherQuery {
    protected TestRunner runner;

    @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Before
    public void setUp() throws Exception {
        MockCypherClientService service = new MockCypherClientService();
        runner = TestRunners.newTestRunner(ExecuteCypherQuery.class);
        runner.addControllerService("clientService", service);
        runner.enableControllerService(service);
        runner.setProperty(AbstractCypherExecutor.CLIENT_SERVICE, "clientService");
        runner.setProperty(AbstractCypherExecutor.QUERY, "match (n) return n");
    }

    @Test
    public void testExecute() throws Exception {
        runner.setProperty(AbstractCypherExecutor.QUERY, "MATCH (p:person) RETURN p");

        runner.enqueue(new byte[] {});
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(ExecuteCypherQuery.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteCypherQuery.REL_SUCCESS);
        assertEquals("1",flowFiles.get(0).getAttribute(CypherClientService.LABELS_ADDED));
        assertEquals("1",flowFiles.get(0).getAttribute(CypherClientService.NODES_CREATED));
        assertEquals("1",flowFiles.get(0).getAttribute(CypherClientService.NODES_DELETED));
        assertEquals("1",flowFiles.get(0).getAttribute(CypherClientService.RELATIONS_CREATED));
        assertEquals("1",flowFiles.get(0).getAttribute(CypherClientService.RELATIONS_DELETED));
        assertEquals("1",flowFiles.get(0).getAttribute(CypherClientService.PROPERTIES_SET));
        assertEquals("1",flowFiles.get(0).getAttribute(CypherClientService.ROWS_RETURNED));
        byte[] raw = runner.getContentAsByteArray(flowFiles.get(0));
        String str = new String(raw);
        List<Map<String, Object>> parsed = new ObjectMapper().readValue(str, List.class);
        assertNotNull(parsed);
        assertEquals(2, parsed.size());
        for (Map<String, Object> result : parsed) {
            assertEquals(2, result.size());
            assertTrue(result.containsKey("name"));
            assertTrue(result.containsKey("age"));
        }
    }
}