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
package org.apache.nifi.priority;

import org.apache.nifi.controller.MockFlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RulesManagerTest {
    private RulesManager rulesManager;
    private Rule high, medium, low;

    @Before
    public void before() {
        rulesManager = new RulesManager(null, null);
        high = Rule.builder().expression("${priority:equals('0')}").label("high").rateOfThreadUsage(101).build();
        medium = Rule.builder().expression("${priority:equals('1')}").label("medium").rateOfThreadUsage(40).build();
        low = Rule.builder().expression("${priority:equals('2')}").label("low").rateOfThreadUsage(25).build();
    }

    @Test
    public void testGetRule() {
        rulesManager.addRule(high);
        Map<String, String> attributes = new HashMap<String, String>(1) {{put("priority", "0");}};
        FlowFileRecord flowFileRecord = new MockFlowFileRecord(attributes, 0L);
        Rule rule = rulesManager.getRule(flowFileRecord);
        assertNotNull(rule);
        assertEquals("high", rule.getLabel());
    }

    @Test
    public void testAddRules() {
        rulesManager.addRules(Arrays.asList(high, medium));
        rulesManager.addRule(low);

        // Rule.DEFAULT and Rule.UNEVALUATED will also be in the list
        assertEquals(5, rulesManager.getRuleList().size());
        assertTrue(rulesManager.getRuleList().stream().noneMatch(Rule::isExpired));
    }

    @Test
    public void testModifyRule() {
        String shmedium = "shmedium";
        rulesManager.addRules(Arrays.asList(high, medium, low));

        // Case 1: UUID of modifiedRule does not match anything in the list. Therefore nothing was modified or added to the list
        Rule modifiedRule = Rule.builder().expression(medium.getExpression()).label(shmedium).rateOfThreadUsage(50).build();
        rulesManager.modifyRule(modifiedRule);
        assertEquals(5, rulesManager.getRuleList().size());
        assertNotEquals(shmedium, medium.getLabel());
        assertNotEquals(50, medium.getRateOfThreadUsage());

        // Case 2: UUID matched and so did expression. The existing rule is modified
        modifiedRule = Rule.builder().uuid(medium.getUuid()).expression(medium.getExpression()).label(shmedium).rateOfThreadUsage(50).build();
        rulesManager.modifyRule(modifiedRule);
        assertEquals(5, rulesManager.getRuleList().size());
        assertEquals(shmedium, medium.getLabel());
        assertEquals(50, medium.getRateOfThreadUsage());

        // Case 3: UUID matched but the expression did not. The existing rule will be marked as expired and a new one is added
        Rule finalModifiedRule = Rule.builder().uuid(medium.getUuid()).expression("${x:equals('10')}").label("new label").rateOfThreadUsage(55).build();
        rulesManager.modifyRule(finalModifiedRule);
        assertEquals(6, rulesManager.getRuleList().size());
        assertTrue(medium.isExpired());

        // The list will NOT contain modifiedRule (using == is deliberate) because a new rule had to be created. One should exist,
        // however, with the correct label, expression and rate`
        assertTrue(rulesManager.getRuleList().stream().noneMatch(rule -> rule == finalModifiedRule));
        assertNotEquals(medium.getExpression(), finalModifiedRule.getExpression());
        assertNotEquals(medium.getLabel(), finalModifiedRule.getLabel());
        assertNotEquals(medium.getRateOfThreadUsage(), finalModifiedRule.getRateOfThreadUsage());

        assertTrue(rulesManager.getRuleList().stream().anyMatch(rule -> finalModifiedRule.getExpression().equals(rule.getExpression())
                && finalModifiedRule.getLabel().equals(rule.getLabel())
                && finalModifiedRule.getRateOfThreadUsage() == rule.getRateOfThreadUsage()));
    }

    @Test
    public void testReadRules() {
        RulesManager rulesManager = new RulesManager(new File("src/test/resources/priorityRules.json"), null);
        rulesManager.readRules();
        assertEquals(5, rulesManager.getRuleList().size());
    }

    @Test
    public void testWriteRules() throws IOException {
        TemporaryFolder temporaryFolder = new TemporaryFolder();

        try {
            temporaryFolder.create();
            File tempFile = temporaryFolder.newFile();

            List<Rule> newRules = new ArrayList<>(1);
            newRules.add(medium);
            RulesManager rulesManager1 = new RulesManager(tempFile, null);
            rulesManager1.addRules(newRules);
            rulesManager1.writeRules();
            RulesManager rulesManager2 = new RulesManager(tempFile, null);
            rulesManager2.readRules();

            // Validate that we successfully wrote and then read the equivalent rule to/from disk
            assertTrue(rulesManager2.getRuleList().stream().anyMatch(newRules.get(0)::equivalent));
        } finally {
            temporaryFolder.delete();
        }
    }

    @Test
    public void testConvertMethods() {
        String test = rulesManager.convertRuleToJsonString(high);
        Rule newHigh = rulesManager.convertJsonStringToRule(test);
        assertEquals(high, newHigh);
    }
}