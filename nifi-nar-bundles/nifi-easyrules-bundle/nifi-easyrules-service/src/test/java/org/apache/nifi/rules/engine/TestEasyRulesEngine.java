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
import org.apache.nifi.rules.Action;
import org.apache.nifi.rules.Rule;
import org.apache.nifi.rules.RulesFactory;
import org.junit.Test;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class TestEasyRulesEngine {

    @Test
    public void testCheckRules() throws Exception {
        String testYamlFile = "src/test/resources/test_nifi_rules.yml";
        List<Rule> rules = RulesFactory.createRulesFromFile(testYamlFile, "YAML", "NIFI");
        final EasyRulesEngine service = new EasyRulesEngine("NIFI",true,false, rules);
        Map<String, Object> facts = new HashMap<>();
        facts.put("predictedQueuedCount",60);
        facts.put("predictedTimeToBytesBackpressureMillis",311111);
        Map<Rule, Boolean> checkedRules = service.checkRules(facts);
        assertNotNull(checkedRules);
        assertEquals(2,checkedRules.values().size());
    }

    @Test
    public void testFireRules() throws Exception {
        String testYamlFile = "src/test/resources/test_nifi_rules.yml";
        List<Rule> rules = RulesFactory.createRulesFromFile(testYamlFile, "YAML", "NIFI");
        final EasyRulesEngine service = new EasyRulesEngine("NIFI",true,false, rules);
        Map<String, Object> facts = new HashMap<>();
        facts.put("predictedQueuedCount",60);
        facts.put("predictedTimeToBytesBackpressureMillis",299999);
        List<Action> actions = service.fireRules(facts);
        assertNotNull(actions);
        assertEquals(3,actions.size());
    }

    @Test
    public void testIgnoreErrorConditions() throws Exception {
        String testYamlFile = "src/test/resources/test_nifi_rules.yml";
        List<Rule> rules = RulesFactory.createRulesFromFile(testYamlFile, "YAML", "NIFI");
        final EasyRulesEngine service = new EasyRulesEngine("NIFI",false, false, rules);
        Map<String, Object> facts = new HashMap<>();
        facts.put("predictedQueuedCount",60);
        facts.put("predictedTimeToBytesBackpressure",311111);
        try {
            service.fireRules(facts);
            fail("Error condition exception was not thrown");
        }catch (Exception ignored){
        }

    }

    @Test
    public void testFilterRulesMissingFacts() throws Exception {
        String testYamlFile = "src/test/resources/test_nifi_rules.yml";
        List<Rule> rules = RulesFactory.createRulesFromFile(testYamlFile, "YAML", "NIFI");
        final EasyRulesEngine service = new EasyRulesEngine("NIFI",false, true, rules);
        Map<String, Object> facts = new HashMap<>();
        facts.put("predictedQueuedCount",60);
        Map<Rule, Boolean> checkedRules = service.checkRules(facts);
        assertEquals(1, checkedRules.size());
    }

}
