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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.graph.InMemoryJanusGraphClientService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ExecuteGraphQueryIT {
    TestRunner runner;
    public static final String QUERY = "0.upto(9) {\n" +
                                            "g.addV(\"test\").property(\"uuid\", UUID.randomUUID().toString()).next()\n" +
                                        "}\n" +
                                        "g.V().hasLabel(\"test\").count().next()";

    @Before
    public void setUp() throws Exception {
        InMemoryJanusGraphClientService service = new InMemoryJanusGraphClientService();
        runner = TestRunners.newTestRunner(ExecuteGraphQuery.class);
        runner.addControllerService("clientService", service);
        runner.enableControllerService(service);
        runner.setProperty(AbstractGraphExecutor.CLIENT_SERVICE, "clientService");
        runner.setProperty(AbstractGraphExecutor.QUERY, QUERY);
    }

    @Test
    public void test() throws Exception {
        runner.run();
        runner.assertTransferCount(ExecuteGraphQuery.REL_FAILURE, 0);
        runner.assertTransferCount(ExecuteGraphQuery.REL_ORIGINAL, 0);
        runner.assertTransferCount(ExecuteGraphQuery.REL_SUCCESS, 1);

        List<MockFlowFile> flowFileList = runner.getFlowFilesForRelationship(ExecuteGraphQuery.REL_SUCCESS);
        MockFlowFile ff = flowFileList.get(0);
        byte[] raw = runner.getContentAsByteArray(ff);
        String str = new String(raw);
        List<Map<String, Object>> result = new ObjectMapper().readValue(str, List.class);
        assertEquals(1, result.size());
        assertEquals(10, result.get(0).get("result"));
    }
}
