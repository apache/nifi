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

import org.jeasy.rules.api.Condition;
import org.jeasy.rules.api.Facts;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestRulesCondition {

    @Test
    public void testRulesMVELConditionPassed(){
        String expression = "predictedTimeToBytesBackpressureMillis <= 14400000";
        Facts facts = new Facts();
        facts.put("predictedTimeToBytesBackpressureMillis",13300000);
        Condition condition = new RulesMVELCondition(expression, false);
        long start = System.currentTimeMillis();
        boolean passed = condition.evaluate(facts);
        long end = System.currentTimeMillis();
        System.out.println("Total Time: " + (end - start));
        assertTrue(passed);
    }

    @Test
    public void testRulesMVELConditionFailed(){
        String expression = "predictedQueuedCount > 50";
        Facts facts = new Facts();
        facts.put("predictedQueuedCount",49);
        Condition condition = new RulesMVELCondition(expression, false);
        assertFalse(condition.evaluate(facts));
    }

    @Test
    public void testRulesMVELConditionError(){
        String expression = "predictedQueuedCount > 50";
        Facts facts = new Facts();
        facts.put("predictedQueued",100);
        Condition condition = new RulesMVELCondition(expression, false);
        try {
            condition.evaluate(facts);
            fail();
        }catch (Exception ignored){
        }
    }

    @Test
    public void testRulesSPELConditionPassed(){
        String expression = "#predictedQueuedCount > 50";
        Facts facts = new Facts();
        facts.put("predictedQueuedCount",100);
        Condition condition = new RulesSPELCondition(expression, false);
        long start = System.currentTimeMillis();
        boolean passed = condition.evaluate(facts);
        long end = System.currentTimeMillis();
        System.out.println("Total Time: " + (end - start));
        assertTrue(passed);
    }

    @Test
    public void testRulesSPELConditionFailed(){
        String expression = "#predictedQueuedCount > 50";
        Facts facts = new Facts();
        facts.put("predictedQueuedCount",49);
        Condition condition = new RulesSPELCondition(expression, false);
        assertFalse(condition.evaluate(facts));
    }

    @Test
    public void testRulesSPELConditionError(){
        String expression = "predictedQueuedCount > 50";
        Facts facts = new Facts();
        facts.put("predictedQueuedCount",100);
        Condition condition = new RulesSPELCondition(expression, false);
        try {
            condition.evaluate(facts);
            fail();
        }catch (Exception ignored){
        }
    }


}
