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
package org.apache.nifi.rules.engine;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.rules.Action;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestEasyRulesEngineProvider {

    @Test
    public void testGetRulesEngine() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final RulesEngineProvider service = new MockEasyRulesEngineProvider();
        runner.addControllerService("easy-rules-engine-service-test",service);
        runner.setProperty(service, EasyRulesEngineService.RULES_FILE_PATH, "src/test/resources/test_nifi_rules.yml");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_TYPE, "YAML");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_FORMAT, "NIFI");
        runner.enableControllerService(service);
        runner.assertValid(service);
        Map<String, Object> facts = new HashMap<>();
        facts.put("predictedQueuedCount",60);
        facts.put("predictedTimeToBytesBackpressureMillis",299999);
        RulesEngine engine = service.getRulesEngine();
        assertNotNull(engine);
        List<Action> actions = engine.fireRules(facts);
        assertNotNull(actions);
        assertEquals(actions.size(), 3);

    }

    private static class MockEasyRulesEngineProvider extends EasyRulesEngineProvider {

    }
}
