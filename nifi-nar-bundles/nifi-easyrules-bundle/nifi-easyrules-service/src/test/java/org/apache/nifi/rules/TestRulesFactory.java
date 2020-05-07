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
package org.apache.nifi.rules;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class TestRulesFactory {
    @Test
    public void testCreateRulesFromNiFiYaml(){
        try {
            String testYamlFile = "src/test/resources/test_nifi_rules.yml";
            List<Rule> rules = RulesFactory.createRulesFromFile(testYamlFile,"YAML", "NIFI");
            assertEquals(2, rules.size());
            assert confirmEntries(rules);
        }catch (Exception ex){
            fail("Unexpected exception occurred: "+ex.getMessage());
        }
    }

    @Test
    public void testCreateRulesFromMvelYaml(){
        try {
            String testYamlFile = "src/test/resources/test_mvel_rules.yml";
            List<Rule> rules = RulesFactory.createRulesFromFile(testYamlFile,"YAML", "MVEL");
            assertEquals(2, rules.size());
            assert confirmEntries(rules);
            assertSame("EXPRESSION", rules.get(0).getActions().get(0).getType());
        }catch (Exception ex){
            fail("Unexpected exception occurred: "+ex.getMessage());
        }
    }

    @Test
    public void testCreateRulesFromSpelYaml(){
        try {
            String testYamlFile = "src/test/resources/test_spel_rules.yml";
            List<Rule> rules = RulesFactory.createRulesFromFile(testYamlFile,"YAML", "SPEL");
            assertEquals(2, rules.size());
            assertSame("EXPRESSION", rules.get(0).getActions().get(0).getType());
        }catch (Exception ex){
            fail("Unexpected exception occurred: "+ex.getMessage());
        }
    }

    @Test
    public void testCreateRulesFromNiFiJson(){
        try {
            String testJsonFile = "src/test/resources/test_nifi_rules.json";
            List<Rule> rules = RulesFactory.createRulesFromFile(testJsonFile,"JSON", "NIFI");
            assertEquals(2, rules.size());
            assert confirmEntries(rules);
        }catch (Exception ex){
            fail("Unexpected exception occurred: "+ex.getMessage());
        }
    }

    @Test
    public void testCreateRulesFromMvelJson(){
        try {
            String testJsonFile = "src/test/resources/test_mvel_rules.json";
            List<Rule> rules = RulesFactory.createRulesFromFile(testJsonFile,"JSON", "MVEL");
            assertEquals(2, rules.size());
            assertSame("EXPRESSION", rules.get(0).getActions().get(0).getType());
            assert confirmEntries(rules);
        }catch (Exception ex){
            fail("Unexpected exception occurred: "+ex.getMessage());
        }
    }

    @Test
    public void testCreateRulesFromSpelJson(){
        try {
            String testJsonFile = "src/test/resources/test_spel_rules.json";
            List<Rule> rules = RulesFactory.createRulesFromFile(testJsonFile,"JSON", "SPEL");
            assertEquals(2, rules.size());
            assertSame("EXPRESSION", rules.get(0).getActions().get(0).getType());
        }catch (Exception ex){
            fail("Unexpected exception occurred: "+ex.getMessage());
        }
    }

    @Test
    public void testCreateRulesFromStringSpelJson(){
        try {
            String testJson = "[\n" +
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
            List<Rule> rules = RulesFactory.createRulesFromString(testJson,"JSON", "SPEL");
            assertEquals(2, rules.size());
            assertSame("EXPRESSION", rules.get(0).getActions().get(0).getType());
        }catch (Exception ex){
            fail("Unexpected exception occurred: "+ex.getMessage());
        }
    }

    @Test
    public void testCreateRulesFromStringSpelYaml(){
        try {
            String testYaml = "---\n" +
                    "name: \"Queue Size\"\n" +
                    "description: \"Queue size check greater than 50\"\n" +
                    "priority: 1\n" +
                    "condition: \"#predictedQueuedCount > 50\"\n" +
                    "actions:\n" +
                    "  - \"System.out.println(\\\"Queue Size Over 50 is detected!\\\")\"\n" +
                    "---\n" +
                    "name: \"Time To Back Pressure\"\n" +
                    "description: \"Back pressure time less than 5 minutes\"\n" +
                    "priority: 2\n" +
                    "condition: \"#predictedTimeToBytesBackpressureMillis < 300000 && #predictedTimeToBytesBackpressureMillis >= 0\"\n" +
                    "actions:\n" +
                    "  - \"System.out.println(\\\"Back Pressure prediction less than 5 minutes!\\\")\"";
            List<Rule> rules = RulesFactory.createRulesFromString(testYaml,"YAML", "SPEL");
            assertEquals(2, rules.size());
            assertSame("EXPRESSION", rules.get(0).getActions().get(0).getType());
        }catch (Exception ex){
            fail("Unexpected exception occurred: "+ex.getMessage());
        }
    }

    @Test
    public void testFakeTypeNotSupported(){
        try {
            RulesFactory.createRulesFromFile("FAKEFILE", "FAKE", "NIFI");
        }catch (Exception ex){
            return;
        }
        fail("Exception should have been thrown for unexpected type");
    }

    @Test
    public void testFakeFormatNotSupported(){
        try {
            RulesFactory.createRulesFromFile("FAKEFILE", "JSON", "FAKE");
        }catch (Exception ex){
            return;
        }
        fail("Exception should have been thrown for unexpected type");
    }


    private boolean confirmEntries(List<Rule> rules){
        Rule rule1= rules.get(0);
        Rule rule2 = rules.get(1);

        boolean checkDiagnostic = rule1.getName().equals("Queue Size") && rule1.getDescription().equals("Queue size check greater than 50")
                && rule1.getPriority() == 1 && rule1.getCondition().equals("predictedQueuedCount > 50");

        checkDiagnostic = rule2.getName().equals("Time To Back Pressure") && rule2.getDescription().equals("Back pressure time less than 5 minutes")
                && rule2.getPriority() == 2 && rule2.getCondition().equals("predictedTimeToBytesBackpressureMillis < 300000 && predictedTimeToBytesBackpressureMillis >= 0") && checkDiagnostic;

        return checkDiagnostic;

    }
}
