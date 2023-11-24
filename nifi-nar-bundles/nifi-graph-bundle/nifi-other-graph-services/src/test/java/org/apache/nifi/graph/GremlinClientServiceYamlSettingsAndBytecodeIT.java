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

import org.apache.commons.io.IOUtils;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

/*
 * Note: this integration test does not work out of the box
 * because nifi needs to understand/load specific serializers
 * to use when connecting via yaml. If using the yaml in the
 * resources folder, please update line 54 with the path to a jar
 * That contains org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1
 * gremlin-util generally has this available
 */
@Testcontainers
public class GremlinClientServiceYamlSettingsAndBytecodeIT {
    private TestRunner runner;
    private TestableGremlinClientService clientService;

    @BeforeEach
    public void setup() throws Exception {

        String remoteYamlFile = "src/test/resources/gremlin.yml";
        String customJarFile = "src/test/resources/gremlin-util-3.7.0.jar";
        clientService = new TestableGremlinClientService();
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.addControllerService("gremlinService", clientService);
        runner.setProperty(clientService, TinkerpopClientService.CONNECTION_SETTINGS, "yaml-settings");
        runner.setProperty(clientService, TinkerpopClientService.SUBMISSION_TYPE, "bytecode-submission");
        runner.setProperty(clientService, TinkerpopClientService.REMOTE_OBJECTS_FILE, remoteYamlFile);
        runner.setProperty(clientService, TinkerpopClientService.EXTRA_RESOURCE, customJarFile);
        /*Note: This does not seem to have much of an effect. As long as EXTRA_RESOURCE has the
                classes of interest, both the gremlin driver and ExecuteGraphQueryRecord will properly execute.
                However, something like this will be needed if we ever move away from groovy.
        */
//        runner.setProperty(clientService, TinkerpopClientService.EXTENSION_CLASSES, "org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1");
        runner.enableControllerService(clientService);
        runner.assertValid();

        String teardown = IOUtils.toString(getClass().getResourceAsStream("/teardown.gremlin"), "UTF-8");
        clientService.getCluster().connect().submit(teardown);
        String setup = IOUtils.toString(getClass().getResourceAsStream("/setup.gremlin"), "UTF-8");
        clientService.getCluster().connect().submit(setup);

    }

    @AfterEach
    public void tearDown() throws Exception {
        String teardown = IOUtils.toString(getClass().getResourceAsStream("/teardown.gremlin"), "UTF-8");
        clientService.getCluster().connect().submit(teardown);
    }

    @Test
    public void testValueMap() {
        String gremlin = "[result: g.V().hasLabel('dog').valueMap().collect()]";
        AtomicInteger integer = new AtomicInteger();
        Map<String, String> result = clientService.executeQuery(gremlin, new HashMap<>(), (record, isMore) -> {
            Assertions.assertTrue(record.containsKey("result"));
            Assertions.assertTrue(record.get("result") instanceof List);
            ((List) record.get("result")).forEach(it -> {
                integer.incrementAndGet();
            });
        });
    }

    @Test
    public void testCount() {
        String gremlin = "[result: g.V().hasLabel('dog').count().next()]";
        AtomicLong dogCount = new AtomicLong();
        Map<String, String> result = clientService.executeQuery(gremlin, new HashMap<>(), (record, isMore) -> {
            Assertions.assertTrue(record.containsKey("result"));
            dogCount.set((Long) record.get("result"));
        });
        assertEquals(2, dogCount.get());
    }

    @Test
    public void testSubGraph() {
        String gremlin = "[dogInE: g.V().hasLabel('dog').inE().count().next(), dogOutE: g.V().hasLabel('dog').outE().count().next(), " +
                "dogProps: g.V().hasLabel('dog').valueMap().collect()]";
        List<Map<String, Object>> recordSet = new ArrayList<>();
        Map<String, String> result = clientService.executeQuery(gremlin, new HashMap<>(), (record, isMore) -> {
            recordSet.add(record);
        });
        Assertions.assertEquals(3, recordSet.size());
        List<Map<String, Object>> dogInE = recordSet.stream().filter(it -> it.containsKey("dogInE")).toList();
        List<Map<String, Object>> dogOutE = recordSet.stream().filter(it -> it.containsKey("dogOutE")).toList();
        List<Map<String, Object>> dogProps = recordSet.stream().filter(it -> it.containsKey("dogProps")).toList();

        Assertions.assertFalse(dogInE.isEmpty());
        Assertions.assertFalse(dogOutE.isEmpty());
        Assertions.assertFalse(dogProps.isEmpty());

        Assertions.assertEquals(2, (Long) dogOutE.get(0).get("dogOutE"));
        Assertions.assertEquals(4, (Long) dogInE.get(0).get("dogInE"));
        Assertions.assertTrue(dogProps.get(0).get("dogProps") instanceof List);
        Map<String, Object> dogPropsMap = (Map) ((List<?>) dogProps.get(0).get("dogProps")).get(0);
        Assertions.assertTrue(dogPropsMap.containsKey("name"));
        Assertions.assertTrue(dogPropsMap.containsKey("age"));
    }
}