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
import org.mvel2.PropertyAccessException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class TestEasyRulesEngineService {

    @Test
    public void testYamlNiFiRules() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final RulesEngineService service = new MockEasyRulesEngineService();
        runner.addControllerService("easy-rules-engine-service-test",service);
        runner.setProperty(service, EasyRulesEngineService.RULES_FILE_PATH, "src/test/resources/test_nifi_rules.yml");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_TYPE, "YAML");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_FORMAT, "NIFI");
        runner.enableControllerService(service);
        runner.assertValid(service);
        Map<String, Object> facts = new HashMap<>();
        facts.put("predictedQueuedCount",60);
        facts.put("predictedTimeToBytesBackpressureMillis",299999);
        List<Action> actions = service.fireRules(facts);
        assertNotNull(actions);
        assertEquals(actions.size(), 3);
    }

    @Test
    public void testYamlMvelRules() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final RulesEngineService service = new MockEasyRulesEngineService();
        runner.addControllerService("easy-rules-engine-service-test",service);
        runner.setProperty(service, EasyRulesEngineService.RULES_FILE_PATH, "src/test/resources/test_mvel_rules.yml");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_TYPE, "YAML");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_FORMAT, "MVEL");
        runner.enableControllerService(service);
        runner.assertValid(service);
        Map<String, Object> facts = new HashMap<>();
        facts.put("predictedQueuedCount",60);
        facts.put("predictedTimeToBytesBackpressureMillis",299999);
        List<Action> actions = service.fireRules(facts);
        assertNotNull(actions);
        assertEquals(actions.size(), 2);
    }

    @Test
    public void testYamlSpelRules() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final RulesEngineService service = new MockEasyRulesEngineService();
        runner.addControllerService("easy-rules-engine-service-test",service);
        runner.setProperty(service, EasyRulesEngineService.RULES_FILE_PATH, "src/test/resources/test_spel_rules.yml");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_TYPE, "YAML");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_FORMAT, "SPEL");
        runner.enableControllerService(service);
        runner.assertValid(service);
        Map<String, Object> facts = new HashMap<>();
        facts.put("predictedQueuedCount",60);
        facts.put("predictedTimeToBytesBackpressureMillis",299999);
        List<Action> actions = service.fireRules(facts);
        assertNotNull(actions);
        assertEquals(actions.size(), 2);
    }

    @Test
    public void testJsonNiFiRules() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final RulesEngineService service = new MockEasyRulesEngineService();
        runner.addControllerService("easy-rules-engine-service-test",service);
        runner.setProperty(service, EasyRulesEngineService.RULES_FILE_PATH, "src/test/resources/test_nifi_rules.json");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_TYPE, "JSON");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_FORMAT, "NIFI");
        runner.enableControllerService(service);
        runner.assertValid(service);
        Map<String, Object> facts = new HashMap<>();
        facts.put("predictedQueuedCount",60);
        facts.put("predictedTimeToBytesBackpressureMillis",299999);
        List<Action> actions = service.fireRules(facts);
        assertNotNull(actions);
        assertEquals(actions.size(), 2);
    }

    @Test
    public void testJsonMvelRules() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final RulesEngineService service = new MockEasyRulesEngineService();
        runner.addControllerService("easy-rules-engine-service-test",service);
        runner.setProperty(service, EasyRulesEngineService.RULES_FILE_PATH, "src/test/resources/test_mvel_rules.json");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_TYPE, "JSON");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_FORMAT, "MVEL");
        runner.enableControllerService(service);
        runner.assertValid(service);
        Map<String, Object> facts = new HashMap<>();
        facts.put("predictedQueuedCount",60);
        facts.put("predictedTimeToBytesBackpressureMillis",299999);
        List<Action> actions = service.fireRules(facts);
        assertNotNull(actions);
        assertEquals(actions.size(), 2);
    }

    @Test
    public void testJsonSpelRules() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final RulesEngineService service = new MockEasyRulesEngineService();
        runner.addControllerService("easy-rules-engine-service-test",service);
        runner.setProperty(service, EasyRulesEngineService.RULES_FILE_PATH, "src/test/resources/test_spel_rules.json");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_TYPE, "JSON");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_FORMAT, "SPEL");
        runner.enableControllerService(service);
        runner.assertValid(service);
        Map<String, Object> facts = new HashMap<>();
        facts.put("predictedQueuedCount",60);
        facts.put("predictedTimeToBytesBackpressureMillis",299999);
        List<Action> actions = service.fireRules(facts);
        assertNotNull(actions);
        assertEquals(actions.size(), 2);
    }

    @Test
    public void testJsonSpelRulesAsString() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final RulesEngineService service = new MockEasyRulesEngineService();
        runner.addControllerService("easy-rules-engine-service-test",service);
        String testRules = "[\n" +
                "  {\n" +
                "    \"name\": \"Queue Size\",\n" +
                "    \"description\": \"Queue size check greater than 50\",\n" +
                "    \"priority\": 1,\n" +
                "    \"condition\": \"#predictedQueuedCount > 50\",\n" +
                "    \"actions\": [\"#predictedQueuedCount + 'is large'\"]\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"Time To Back Pressure\",\n" +
                "    \"description\": \"Back pressure time less than 5 minutes\",\n" +
                "    \"priority\": 2,\n" +
                "    \"condition\": \"#predictedTimeToBytesBackpressureMillis < 300000 && #predictedTimeToBytesBackpressureMillis >= 0\",\n" +
                "    \"actions\": [\"'System is approaching backpressure! Predicted time left: ' + #predictedTimeToBytesBackpressureMillis\"]\n" +
                "  }\n" +
                "]";
        runner.setProperty(service, EasyRulesEngineService.RULES_BODY, testRules);
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_TYPE, "JSON");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_FORMAT, "SPEL");
        runner.enableControllerService(service);
        runner.assertValid(service);
        Map<String, Object> facts = new HashMap<>();
        facts.put("predictedQueuedCount",60);
        facts.put("predictedTimeToBytesBackpressureMillis",299999);
        List<Action> actions = service.fireRules(facts);
        assertNotNull(actions);
        assertEquals(actions.size(), 2);
    }

    @Test
    public void testYamlMvelRulesAsString() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final RulesEngineService service = new MockEasyRulesEngineService();
        runner.addControllerService("easy-rules-engine-service-test",service);
        String testYaml = "---\n" +
                "name: \"Queue Size\"\n" +
                "description: \"Queue size check greater than 50\"\n" +
                "priority: 1\n" +
                "condition: \"predictedQueuedCount > 50\"\n" +
                "actions:\n" +
                "  - \"System.out.println(\\\"Queue Size Over 50 is detected!\\\")\"\n" +
                "---\n" +
                "name: \"Time To Back Pressure\"\n" +
                "description: \"Back pressure time less than 5 minutes\"\n" +
                "priority: 2\n" +
                "condition: \"predictedTimeToBytesBackpressureMillis < 300000 && predictedTimeToBytesBackpressureMillis >= 0\"\n" +
                "actions:\n" +
                "  - \"System.out.println(\\\"Back Pressure prediction less than 5 minutes!\\\")\"";

        runner.setProperty(service, EasyRulesEngineService.RULES_BODY, testYaml);
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_TYPE, "YAML");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_FORMAT, "MVEL");
        runner.enableControllerService(service);
        runner.assertValid(service);
        Map<String, Object> facts = new HashMap<>();
        facts.put("predictedQueuedCount",60);
        facts.put("predictedTimeToBytesBackpressureMillis",299999);
        List<Action> actions = service.fireRules(facts);
        assertNotNull(actions);
        assertEquals(actions.size(), 2);
    }



    @Test
    public void testIgnoreConditionErrorsFalseNIFI() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final RulesEngineService service = new MockEasyRulesEngineService();
        runner.addControllerService("easy-rules-engine-service-test",service);
        runner.setProperty(service, EasyRulesEngineService.RULES_FILE_PATH, "src/test/resources/test_nifi_rules.json");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_TYPE, "JSON");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_FORMAT, "NIFI");
        runner.setProperty(service,EasyRulesEngineService.IGNORE_CONDITION_ERRORS,"false");
        runner.enableControllerService(service);
        runner.assertValid(service);
        Map<String, Object> facts = new HashMap<>();
        facts.put("fakeMetric",60);
        facts.put("predictedTimeToBytesBackpressureMillis",299999);
        try {
            service.fireRules(facts);
            fail("Expected exception to be thrown");
        }catch (Exception pae){
            assert true;
        }
    }

    @Test
    public void testIgnoreConditionErrorsTrueNIFI() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final RulesEngineService service = new MockEasyRulesEngineService();
        runner.addControllerService("easy-rules-engine-service-test",service);
        runner.setProperty(service, EasyRulesEngineService.RULES_FILE_PATH, "src/test/resources/test_nifi_rules.json");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_TYPE, "JSON");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_FORMAT, "NIFI");
        runner.setProperty(service,EasyRulesEngineService.IGNORE_CONDITION_ERRORS,"true");
        runner.enableControllerService(service);
        runner.assertValid(service);
        Map<String, Object> facts = new HashMap<>();
        facts.put("fakeMetric",60);
        facts.put("predictedTimeToBytesBackpressureMillis",299999);
        try {
            List<Action> actions = service.fireRules(facts);
            assertNotNull(actions);
            assertEquals(actions.size(), 1);
        }catch (PropertyAccessException pae){
            fail();
        }
    }

    @Test
    public void testIgnoreConditionErrorsFalseMVEL() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final RulesEngineService service = new MockEasyRulesEngineService();
        runner.addControllerService("easy-rules-engine-service-test",service);
        runner.setProperty(service, EasyRulesEngineService.RULES_FILE_PATH, "src/test/resources/test_mvel_rules.json");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_TYPE, "JSON");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_FORMAT, "MVEL");
        runner.setProperty(service,EasyRulesEngineService.IGNORE_CONDITION_ERRORS,"false");
        runner.enableControllerService(service);
        runner.assertValid(service);
        Map<String, Object> facts = new HashMap<>();
        facts.put("fakeMetric",60);
        facts.put("predictedTimeToBytesBackpressureMillis",299999);
        try {
            service.fireRules(facts);
            fail("Expected exception to be thrown");
        }catch (Exception pae){
            assert true;
        }
    }

    @Test
    public void testIgnoreConditionErrorsTrueMVEL() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final RulesEngineService service = new MockEasyRulesEngineService();
        runner.addControllerService("easy-rules-engine-service-test",service);
        runner.setProperty(service, EasyRulesEngineService.RULES_FILE_PATH, "src/test/resources/test_mvel_rules.json");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_TYPE, "JSON");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_FORMAT, "MVEL");
        runner.setProperty(service,EasyRulesEngineService.IGNORE_CONDITION_ERRORS,"true");
        runner.enableControllerService(service);
        runner.assertValid(service);
        Map<String, Object> facts = new HashMap<>();
        facts.put("fakeMetric",60);
        facts.put("predictedTimeToBytesBackpressureMillis",299999);
        try {
            List<Action> actions = service.fireRules(facts);
            assertNotNull(actions);
            assertEquals(actions.size(), 1);
        }catch (PropertyAccessException pae){
            fail();
        }
    }

    @Test
    public void testIgnoreConditionErrorsFalseSPEL() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final RulesEngineService service = new MockEasyRulesEngineService();
        runner.addControllerService("easy-rules-engine-service-test",service);
        runner.setProperty(service, EasyRulesEngineService.RULES_FILE_PATH, "src/test/resources/test_bad_spel_rules.json");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_TYPE, "JSON");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_FORMAT, "SPEL");
        runner.setProperty(service,EasyRulesEngineService.IGNORE_CONDITION_ERRORS,"false");
        runner.enableControllerService(service);
        runner.assertValid(service);
        Map<String, Object> facts = new HashMap<>();
        facts.put("fakeMetric",60);
        facts.put("fakeMetric2",299999);
        try {
            service.fireRules(facts);
            fail("Expected exception to be thrown");
        }catch (Exception pae){
            assert true;
        }
    }

    @Test
    public void testIgnoreConditionErrorsTrueSPEL() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final RulesEngineService service = new MockEasyRulesEngineService();
        runner.addControllerService("easy-rules-engine-service-test",service);
        runner.setProperty(service, EasyRulesEngineService.RULES_FILE_PATH, "src/test/resources/test_bad_spel_rules.json");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_TYPE, "JSON");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_FORMAT, "SPEL");
        runner.setProperty(service,EasyRulesEngineService.IGNORE_CONDITION_ERRORS,"true");
        runner.enableControllerService(service);
        runner.assertValid(service);
        Map<String, Object> facts = new HashMap<>();
        facts.put("predictedQueuedCount",60);
        facts.put("predictedTimeToBytesBackpressureMillis",299999);
        try {
            List<Action> actions = service.fireRules(facts);
            assertNotNull(actions);
            assertEquals(actions.size(), 1);
        }catch (Exception pae){
            fail();
        }
    }

    @Test
    public void testFilterRulesMissingFactsTrue() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final RulesEngineService service = new MockEasyRulesEngineService();
        runner.addControllerService("easy-rules-engine-service-test",service);
        runner.setProperty(service, EasyRulesEngineService.RULES_FILE_PATH, "src/test/resources/test_nifi_rules_filter.json");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_TYPE, "JSON");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_FORMAT, "NIFI");
        runner.setProperty(service,EasyRulesEngineService.FILTER_RULES_MISSING_FACTS, "true");
        runner.enableControllerService(service);
        runner.assertValid(service);
        Map<String, Object> facts = new HashMap<>();
        facts.put("predictedQueuedCount",60);
        List<Action> actions = service.fireRules(facts);
        assertNotNull(actions);
        assertEquals(actions.size(), 1);
    }

    private static class MockEasyRulesEngineService extends EasyRulesEngineService {

    }

}
