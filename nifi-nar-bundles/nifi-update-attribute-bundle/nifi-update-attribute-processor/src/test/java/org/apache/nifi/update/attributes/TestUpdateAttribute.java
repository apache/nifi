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
package org.apache.nifi.update.attributes;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.attributes.UpdateAttribute;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.update.attributes.serde.CriteriaSerDe;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.nifi.processors.attributes.UpdateAttribute.STORE_STATE_LOCALLY;
import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TestUpdateAttribute {

    final static private String TEST_CONTENT = "THIS IS TEST CONTENT";

    private Map<String, String> getMap(String... keyValues) {
        final Map<String, String> map = new HashMap<>();
        if (keyValues != null) {
            for (int i = 0; i <= keyValues.length - 2; i += 2) {
                map.put(keyValues[i], keyValues[i + 1]);
            }
        }
        return map;
    }

    private Criteria getCriteria() {
        return new Criteria();
    }

    private void addRule(final Criteria criteria, final String name, final Collection<String> conditions, final Map<String, String> actions) {
        final Rule rule = new Rule();
        rule.setId(UUID.randomUUID().toString());
        rule.setName(name);
        rule.setConditions(new HashSet<Condition>());
        rule.setActions(new HashSet<Action>());

        for (final String expression : conditions) {
            final Condition condition = new Condition();
            condition.setExpression(expression);
            rule.getConditions().add(condition);
        }

        for (final Map.Entry<String, String> entry : actions.entrySet()) {
            final Action action = new Action();
            action.setAttribute(entry.getKey());
            action.setValue(entry.getValue());
            rule.getActions().add(action);
        }

        criteria.addRule(rule);
    }

    private String serialize(final Criteria criteria) {
        return CriteriaSerDe.serialize(criteria);
    }

    @Test
    public void testDefault() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        runner.setProperty("attribute.1", "value.1");
        runner.setProperty("attribute.2", "new.value.2");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attribute.2", "old.value.2");
        runner.enqueue(new byte[0], attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS);
        result.get(0).assertAttributeEquals("attribute.1", "value.1");
        result.get(0).assertAttributeEquals("attribute.2", "new.value.2");
    }

    @Test
    public void testDefaultAddAttribute() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        runner.setProperty("NewAttr", "${one:plus(${two})}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("one", "1");
        attributes.put("two", "2");
        runner.enqueue(new byte[0], attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).get(0).assertAttributeEquals("NewAttr", "3");
    }

    @Test
    public void testBasicState() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        runner.setProperty(UpdateAttribute.STORE_STATE, STORE_STATE_LOCALLY);
        runner.setProperty("count", "${getStateValue('count'):plus(1)}");
        runner.setProperty("sum", "${getStateValue('sum'):plus(${pencils})}");

        runner.assertNotValid();
        runner.setProperty(UpdateAttribute.STATEFUL_VARIABLES_INIT_VALUE, "0");
        runner.assertValid();

        final Map<String, String> attributes2 = new HashMap<>();
        attributes2.put("pencils", "2");

        runner.enqueue(new byte[0],attributes2);
        runner.enqueue(new byte[0],attributes2);

        final Map<String, String> attributes3 = new HashMap<>();
        attributes3.put("pencils", "3");
        runner.enqueue(new byte[0], attributes3);
        runner.enqueue(new byte[0], attributes3);

        final Map<String, String> attributes5 = new HashMap<>();
        attributes5.put("pencils", "5");
        runner.enqueue(new byte[0], attributes5);

        runner.run(5);

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 5);
        runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).get(4).assertAttributeEquals("count", "5");
        runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).get(4).assertAttributeEquals("sum", "15");
    }

    @Test
    public void testStateFailures() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        final UpdateAttribute processor = (UpdateAttribute) runner.getProcessor();
        final ProcessSessionFactory processSessionFactory = runner.getProcessSessionFactory();
        MockStateManager mockStateManager = runner.getStateManager();

        runner.setProperty(UpdateAttribute.STORE_STATE, STORE_STATE_LOCALLY);
        runner.setProperty("count", "${getStateValue('count'):plus(1)}");
        runner.setProperty("sum", "${getStateValue('sum'):plus(${pencils})}");
        runner.setProperty(UpdateAttribute.STATEFUL_VARIABLES_INIT_VALUE, "0");

        processor.onScheduled(runner.getProcessContext());

        final Map<String, String> attributes2 = new HashMap<>();
        attributes2.put("pencils", "2");

        mockStateManager.setFailOnStateGet(Scope.LOCAL, true);

        runner.enqueue(new byte[0],attributes2);
        processor.onTrigger(runner.getProcessContext(), processSessionFactory.createSession());

        runner.assertQueueNotEmpty();

        mockStateManager.setFailOnStateGet(Scope.LOCAL, false);
        mockStateManager.setFailOnStateSet(Scope.LOCAL, true);

        processor.onTrigger(runner.getProcessContext(), processSessionFactory.createSession());

        runner.assertQueueEmpty();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_FAILED_SET_STATE, 1);
        runner.getFlowFilesForRelationship(UpdateAttribute.REL_FAILED_SET_STATE).get(0).assertAttributeEquals("count", "1");
        runner.getFlowFilesForRelationship(UpdateAttribute.REL_FAILED_SET_STATE).get(0).assertAttributeEquals("sum", "2");
    }


    @Test
    public void testStateWithInitValue() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        runner.setProperty(UpdateAttribute.STORE_STATE, STORE_STATE_LOCALLY);
        runner.setProperty(UpdateAttribute.STATEFUL_VARIABLES_INIT_VALUE, "10");
        runner.setProperty("count", "${getStateValue('count'):plus(1)}");
        runner.setProperty("sum", "${getStateValue('sum'):plus(${pencils})}");

        runner.assertValid();

        final Map<String, String> attributes2 = new HashMap<>();
        attributes2.put("pencils", "2");

        runner.enqueue(new byte[0],attributes2);
        runner.enqueue(new byte[0],attributes2);

        final Map<String, String> attributes3 = new HashMap<>();
        attributes3.put("pencils", "3");
        runner.enqueue(new byte[0], attributes3);
        runner.enqueue(new byte[0], attributes3);

        final Map<String, String> attributes5 = new HashMap<>();
        attributes5.put("pencils", "5");
        runner.enqueue(new byte[0], attributes5);

        runner.run(5);

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 5);
        runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).get(4).assertAttributeEquals("count", "15");
        runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).get(4).assertAttributeEquals("sum", "25");
    }

    @Test
    public void testRuleHitWithState() throws Exception {
        final Criteria criteria = getCriteria();
        addRule(criteria, "rule", Arrays.asList(
                // conditions
                "${getStateValue('maxValue'):lt(${value})}"), getMap(
                // actions
                "maxValue", "${value}"));

        TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        runner.setProperty(UpdateAttribute.STORE_STATE, STORE_STATE_LOCALLY);
        runner.setProperty(UpdateAttribute.STATEFUL_VARIABLES_INIT_VALUE, "0");
        runner.setAnnotationData(serialize(criteria));

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("value", "1");
        runner.enqueue(new byte[0], attributes);
        runner.run();
        attributes.put("value", "2");
        runner.enqueue(new byte[0], attributes);
        runner.run();
        attributes.put("value", "4");
        runner.enqueue(new byte[0], attributes);
        runner.run();
        attributes.put("value", "1");
        runner.enqueue(new byte[0], attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 4);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS);
        result.get(2).assertAttributeEquals("maxValue", "4");
        result.get(3).assertAttributeEquals("maxValue", null);
    }

    @Test
    public void testStateFailuresWithRulesUsingOriginal() throws Exception {
        final Criteria criteria = getCriteria();
        criteria.setFlowFilePolicy(FlowFilePolicy.USE_ORIGINAL);
        addRule(criteria, "rule", Collections.singletonList(
                // conditions
                "${getStateValue('maxValue'):lt(${value})}"), getMap(
                // actions
                "maxValue", "${value}"));
        addRule(criteria, "rule2", Collections.singletonList(
                // conditions
                "${getStateValue('maxValue2'):lt(${value})}"), getMap(
                // actions
                "maxValue2", "${value}"));

        TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        final UpdateAttribute processor = (UpdateAttribute) runner.getProcessor();
        final ProcessSessionFactory processSessionFactory = runner.getProcessSessionFactory();
        MockStateManager mockStateManager = runner.getStateManager();

        runner.setProperty(UpdateAttribute.STORE_STATE, STORE_STATE_LOCALLY);
        runner.setProperty(UpdateAttribute.STATEFUL_VARIABLES_INIT_VALUE, "0");
        runner.setAnnotationData(serialize(criteria));


        processor.onScheduled(runner.getProcessContext());

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("value", "1");
        runner.enqueue(new byte[0], attributes);

        mockStateManager.setFailOnStateGet(Scope.LOCAL, true);
        processor.onTrigger(runner.getProcessContext(), processSessionFactory.createSession());

        runner.assertQueueNotEmpty();
        mockStateManager.setFailOnStateGet(Scope.LOCAL, false);
        mockStateManager.setFailOnStateSet(Scope.LOCAL, true);

        processor.onTrigger(runner.getProcessContext(), processSessionFactory.createSession());

        runner.assertQueueEmpty();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_FAILED_SET_STATE, 1);
        runner.getFlowFilesForRelationship(UpdateAttribute.REL_FAILED_SET_STATE).get(0).assertAttributeEquals("maxValue", "1");
        runner.getFlowFilesForRelationship(UpdateAttribute.REL_FAILED_SET_STATE).get(0).assertAttributeEquals("maxValue2", "1");
    }

    @Test
    public void testStateFailuresWithRulesUsingClone() throws Exception {
        final Criteria criteria = getCriteria();
        criteria.setFlowFilePolicy(FlowFilePolicy.USE_CLONE);
        addRule(criteria, "rule", Collections.singletonList(
                // conditions
                "${getStateValue('maxValue'):lt(${value})}"), getMap(
                // actions
                "maxValue", "${value}"));
        addRule(criteria, "rule2", Collections.singletonList(
                // conditions
                "${getStateValue('maxValue2'):lt(${value})}"), getMap(
                // actions
                "maxValue2", "${value}"));

        TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        final UpdateAttribute processor = (UpdateAttribute) runner.getProcessor();
        final ProcessSessionFactory processSessionFactory = runner.getProcessSessionFactory();
        MockStateManager mockStateManager = runner.getStateManager();

        runner.setProperty(UpdateAttribute.STORE_STATE, STORE_STATE_LOCALLY);
        runner.setProperty(UpdateAttribute.STATEFUL_VARIABLES_INIT_VALUE, "0");
        runner.setAnnotationData(serialize(criteria));


        processor.onScheduled(runner.getProcessContext());

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("value", "1");
        runner.enqueue(new byte[0], attributes);

        mockStateManager.setFailOnStateGet(Scope.LOCAL, true);
        processor.onTrigger(runner.getProcessContext(), processSessionFactory.createSession());

        runner.assertQueueNotEmpty();
        mockStateManager.setFailOnStateGet(Scope.LOCAL, false);
        mockStateManager.setFailOnStateSet(Scope.LOCAL, true);

        processor.onTrigger(runner.getProcessContext(), processSessionFactory.createSession());

        runner.assertQueueEmpty();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_FAILED_SET_STATE, 1);
        runner.getFlowFilesForRelationship(UpdateAttribute.REL_FAILED_SET_STATE).get(0).assertAttributeEquals("maxValue", "1");
        runner.getFlowFilesForRelationship(UpdateAttribute.REL_FAILED_SET_STATE).get(0).assertAttributeNotExists("maxValue2");
    }
    @Test
    public void testRuleHitWithStateWithDefault() throws Exception {
        final Criteria criteria = getCriteria();
        addRule(criteria, "rule", Arrays.asList(
                // conditions
                "${getStateValue('maxValue'):lt(${value})}"), getMap(
                // actions
                "maxValue", "${value}"));

        TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        runner.setProperty(UpdateAttribute.STORE_STATE, STORE_STATE_LOCALLY);
        runner.setProperty(UpdateAttribute.STATEFUL_VARIABLES_INIT_VALUE, "0");
        runner.setAnnotationData(serialize(criteria));
        runner.setProperty("maxValue", "${getStateValue('maxValue')}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("value", "1");
        runner.enqueue(new byte[0], attributes);
        runner.run();
        attributes.put("value", "2");
        runner.enqueue(new byte[0], attributes);
        runner.run();
        attributes.put("value", "4");
        runner.enqueue(new byte[0], attributes);
        runner.run();
        attributes.put("value", "1");
        runner.enqueue(new byte[0], attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 4);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS);
        result.get(2).assertAttributeEquals("maxValue", "4");
        result.get(3).assertAttributeEquals("maxValue", "4");
    }

    @Test
    public void testRuleHitWithStateWithInitValue() throws Exception {
        final Criteria criteria = getCriteria();
        addRule(criteria, "rule", Arrays.asList(
                // conditions
                "${getStateValue('minValue'):ge(${value})}"), getMap(
                // actions
                "minValue", "${value}"));

        TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        runner.setProperty(UpdateAttribute.STORE_STATE, STORE_STATE_LOCALLY);
        runner.setProperty(UpdateAttribute.STATEFUL_VARIABLES_INIT_VALUE, "5");
        runner.setAnnotationData(serialize(criteria));

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("value", "1");
        runner.enqueue(new byte[0], attributes);
        runner.run();
        attributes.put("value", "2");
        runner.enqueue(new byte[0], attributes);
        runner.run();
        attributes.put("value", "4");
        runner.enqueue(new byte[0], attributes);
        runner.run();
        attributes.put("value", "1");
        runner.enqueue(new byte[0], attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 4);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS);
        result.get(3).assertAttributeEquals("minValue", "1");
    }

    @Test
    public void testMultipleRulesWithStateAndDelete() throws Exception {
        final Criteria criteria = getCriteria();
        criteria.setFlowFilePolicy(FlowFilePolicy.USE_ORIGINAL);
        addRule(criteria, "rule", Collections.singletonList(
                // conditions
                "${getStateValue('maxValue'):lt(${value})}"), getMap(
                // actions
                "maxValue", "${value}"));
        addRule(criteria, "rule2", Collections.singletonList(
                // conditions
                "${value:mod(2):equals(0)}"), getMap(
                // actions
                "whatIsIt", "even"));

        TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        runner.setProperty(UpdateAttribute.STORE_STATE, STORE_STATE_LOCALLY);
        runner.setProperty(UpdateAttribute.DELETE_ATTRIBUTES, "badValue");
        runner.setProperty(UpdateAttribute.STATEFUL_VARIABLES_INIT_VALUE, "0");
        runner.setAnnotationData(serialize(criteria));
        runner.setProperty("maxValue", "${getStateValue('maxValue')}");
        runner.setProperty("whatIsIt", "odd");
        runner.setProperty("whatWasIt", "${getStateValue('whatIsIt')}");
        runner.setProperty("theCount", "${getStateValue('theCount'):plus(1)}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("value", "1");
        attributes.put("badValue", "10");
        runner.enqueue(new byte[0], attributes);
        runner.run();
        attributes.put("value", "2");
        runner.enqueue(new byte[0], attributes);
        runner.run();
        attributes.put("value", "5");
        runner.enqueue(new byte[0], attributes);
        runner.run();
        attributes.put("value", "1");
        runner.enqueue(new byte[0], attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 4);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS);
        result.get(3).assertAttributeEquals("maxValue", "5");
        result.get(3).assertAttributeEquals("theCount", "4");

        result.get(0).assertAttributeEquals("badValue", null);

        result.get(0).assertAttributeEquals("whatIsIt", "odd");
        result.get(1).assertAttributeEquals("whatIsIt", "even");

        result.get(2).assertAttributeEquals("whatWasIt", "even");
        result.get(3).assertAttributeEquals("whatWasIt", "odd");
    }

    @Test
    public void testDefaultBooleanAsString() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        runner.setProperty("NewAttr", "${a:equals('b'):toString()}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("a", "b");
        runner.enqueue(new byte[0], attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).get(0).assertAttributeEquals("NewAttr", "true");
    }

    @Test
    public void testDefaultEscapeValue() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        runner.setProperty("NewAttr", "$${a}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("a", "b");
        runner.enqueue(new byte[0], attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).get(0).assertAttributeEquals("NewAttr", "${a}");
    }

    @Test
    public void testRuleMissBecauseAttributeMissing() throws Exception {
        final Criteria criteria = getCriteria();
        addRule(criteria, "rule", Arrays.asList(
                // conditions
                "${attribute5:equals('value.5')}"), getMap(
                        // actions
                        "attribute.2", "value.2"));

        final TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        runner.setAnnotationData(serialize(criteria));

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attribute.1", "value.1");
        runner.enqueue(new byte[0], attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS);
        result.get(0).assertAttributeEquals("attribute.1", "value.1");
        result.get(0).assertAttributeNotExists("attribute.2");
    }

    @Test
    public void testRuleMissBecauseValueNoMatch() throws Exception {
        final Criteria criteria = getCriteria();
        addRule(criteria, "rule", Arrays.asList(
                // conditions
                "${attribute1:equals('not.value.1')}"), getMap(
                        // actions
                        "attribute.2", "value.2"));

        final TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        runner.setAnnotationData(serialize(criteria));

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attribute.1", "value.1");
        runner.enqueue(new byte[0], attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS);
        result.get(0).assertAttributeEquals("attribute.1", "value.1");
        result.get(0).assertAttributeNotExists("attribute.2");
    }

    @Test
    public void testRuleHitWithDefaults() throws Exception {
        final Criteria criteria = getCriteria();
        addRule(criteria, "rule", Arrays.asList(
                // conditions
                "${attribute.1:equals('value.1')}"), getMap(
                // actions
                "attribute.2", "value.2"));

        final TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        runner.setAnnotationData(serialize(criteria));
        runner.setProperty("attribute.1", "new.value.1");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attribute.1", "value.1");
        runner.enqueue(new byte[0], attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS);
        result.get(0).assertAttributeEquals("attribute.1", "new.value.1");
        result.get(0).assertAttributeEquals("attribute.2", "value.2");
    }

    @Test
    public void testRuleHitWithAConflictingDefault() throws Exception {
        final Criteria criteria = getCriteria();
        addRule(criteria, "rule", Arrays.asList(
                // conditions
                "${attribute.1:equals('value.1')}"), getMap(
                // actions
                "attribute.2", "value.2"));

        final TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        runner.setAnnotationData(serialize(criteria));
        runner.setProperty("attribute.2", "default.value.2");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attribute.1", "value.1");
        runner.enqueue(new byte[0], attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS);
        result.get(0).assertAttributeEquals("attribute.2", "value.2");
    }

    @Test
    public void testMultipleRuleHitsWithNoFlowFilePolicySpecified() throws Exception {
        final Criteria criteria = getCriteria();
        addRule(criteria, "rule 1", Arrays.asList(
                // conditions
                "${attribute.1:equals('value.1')}"), getMap(
                        // actions
                        "attribute.2", "value.2"));
        addRule(criteria, "rule 2", Arrays.asList(
                // conditions
                "${attribute.1:equals('value.1')}"), getMap(
                        // actions
                        "attribute.3", "value.3"));

        final TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        runner.setAnnotationData(serialize(criteria));
        runner.setProperty("attribute.2", "default.value.2");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attribute.1", "value.1");
        runner.enqueue(TEST_CONTENT.getBytes(StandardCharsets.UTF_8), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 2);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS);

        final MockFlowFile flowfile1 = result.get(0);
        final MockFlowFile flowfile2 = result.get(1);

        // ensure the attributes are as expected
        if ("rule 1".equals(flowfile1.getAttribute(runner.getProcessor().getClass().getSimpleName() + ".matchedRule"))) {
            flowfile1.assertAttributeEquals("attribute.2", "value.2");
            flowfile2.assertAttributeEquals("attribute.3", "value.3");
            flowfile2.assertAttributeEquals("attribute.2", "default.value.2");
        } else {
            flowfile2.assertAttributeEquals("attribute.2", "value.2");
            flowfile1.assertAttributeEquals("attribute.3", "value.3");
            flowfile1.assertAttributeEquals("attribute.2", "default.value.2");
        }

        // ensure the content was copied as well
        flowfile1.assertContentEquals(TEST_CONTENT.getBytes(StandardCharsets.UTF_8));
        flowfile2.assertContentEquals(TEST_CONTENT.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testMultipleRuleHitsWithUseClone() throws Exception {
        final Criteria criteria = getCriteria();
        criteria.setFlowFilePolicy(FlowFilePolicy.USE_CLONE);
        addRule(criteria, "rule 1", Arrays.asList(
                // conditions
                "${attribute.1:equals('value.1')}"), getMap(
                        // actions
                        "attribute.2", "value.2"));
        addRule(criteria, "rule 2", Arrays.asList(
                // conditions
                "${attribute.1:equals('value.1')}"), getMap(
                        // actions
                        "attribute.3", "value.3"));

        final TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        runner.setAnnotationData(serialize(criteria));
        runner.setProperty("attribute.2", "default.value.2");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attribute.1", "value.1");
        runner.enqueue(TEST_CONTENT.getBytes(StandardCharsets.UTF_8), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 2);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS);

        final MockFlowFile flowfile1 = result.get(0);
        final MockFlowFile flowfile2 = result.get(1);

        // ensure the attributes are as expected
        if ("rule 1".equals(flowfile1.getAttribute(runner.getProcessor().getClass().getSimpleName() + ".matchedRule"))) {
            flowfile1.assertAttributeEquals("attribute.2", "value.2");
            flowfile2.assertAttributeEquals("attribute.3", "value.3");
            flowfile2.assertAttributeEquals("attribute.2", "default.value.2");
        } else {
            flowfile2.assertAttributeEquals("attribute.2", "value.2");
            flowfile1.assertAttributeEquals("attribute.3", "value.3");
            flowfile1.assertAttributeEquals("attribute.2", "default.value.2");
        }

        // ensure the content was copied as well
        flowfile1.assertContentEquals(TEST_CONTENT.getBytes(StandardCharsets.UTF_8));
        flowfile2.assertContentEquals(TEST_CONTENT.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testMultipleRuleHitsWithUseOriginal() throws Exception {
        final Criteria criteria = getCriteria();
        criteria.setFlowFilePolicy(FlowFilePolicy.USE_ORIGINAL);
        addRule(criteria, "rule 1", Arrays.asList(
                // conditions
                "${attribute.1:equals('value.1')}"), getMap(
                        // actions
                        "attribute.2", "value.2"));
        addRule(criteria, "rule 2", Arrays.asList(
                // conditions
                "${attribute.1:equals('value.1')}"), getMap(
                        // actions
                        "attribute.3", "value.3"));
        addRule(criteria, "rule 3", Arrays.asList(
                // conditions
                "${attribute.1:equals('value.1')}"), getMap(
                // actions
                "attribute.2", "value.3"));

        final TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        runner.setAnnotationData(serialize(criteria));
        runner.setProperty("attribute.2", "default.value.2");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attribute.1", "value.1");
        runner.enqueue(TEST_CONTENT.getBytes(StandardCharsets.UTF_8), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS);

        final MockFlowFile flowfile = result.get(0);

        // ensure the attributes are as expected
        flowfile.assertAttributeEquals("attribute.2", "value.3");
        flowfile.assertAttributeEquals("attribute.3", "value.3");

        // ensure the content was copied as well
        flowfile.assertContentEquals(TEST_CONTENT.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testMultipleRuleHitsWithUseOriginalDoesntApplyDefaultsRepeatedly() throws Exception {
        final Criteria criteria = getCriteria();
        criteria.setFlowFilePolicy(FlowFilePolicy.USE_ORIGINAL);
        addRule(criteria, "rule 1", Arrays.asList(
                // conditions
                "${attribute.1:equals('value.1')}"), getMap(
                        // actions
                        "attribute.2", "value.2"));
        addRule(criteria, "rule 2", Arrays.asList(
                // conditions
                "${attribute.1:equals('value.1')}"), getMap(
                        // actions
                        "attribute.3", "value.3"));
        addRule(criteria, "rule 3", Arrays.asList(
                // conditions
                "${attribute.1:equals('value.1')}"), getMap(
                        // actions
                        "attribute.2", "value.3"));

        final TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        runner.setAnnotationData(serialize(criteria));
        runner.setProperty("default.attr", "${default.attr}-more-stuff");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attribute.1", "value.1");
        runner.enqueue(TEST_CONTENT.getBytes(StandardCharsets.UTF_8), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS);

        final MockFlowFile flowfile = result.get(0);

        // ensure the attributes are as expected
        flowfile.assertAttributeEquals("default.attr", "-more-stuff");
    }

    @Test
    public void testSimpleDelete() {
        final TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        runner.setProperty(UpdateAttribute.DELETE_ATTRIBUTES, "attribute.2");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attribute.1", "value.1");
        attributes.put("attribute.2", "value.2");
        runner.enqueue(new byte[0], attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS);
        result.get(0).assertAttributeEquals("attribute.1", "value.1");
        result.get(0).assertAttributeNotExists("attribute.2");
    }

    @Test
    public void testRegexDotDelete() {
        final TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        runner.setProperty(UpdateAttribute.DELETE_ATTRIBUTES, "attribute.2");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attribute.1", "value.1");
        attributes.put("attribute.2", "value.2");
        attributes.put("attributex2", "valuex2");
        runner.enqueue(new byte[0], attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS);
        result.get(0).assertAttributeEquals("attribute.1", "value.1");
        result.get(0).assertAttributeNotExists("attribute.2");
        result.get(0).assertAttributeNotExists("attributex2");
    }

    @Test
    public void testRegexLiteralDotDelete() {
        final TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        runner.setProperty(UpdateAttribute.DELETE_ATTRIBUTES, "attribute\\.2");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attribute.1", "value.1");
        attributes.put("attribute.2", "value.2");
        attributes.put("attributex2", "valuex2");
        runner.enqueue(new byte[0], attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS);
        result.get(0).assertAttributeEquals("attribute.1", "value.1");
        result.get(0).assertAttributeNotExists("attribute.2");
        result.get(0).assertAttributeExists("attributex2");
    }

    @Test
    public void testRegexGroupDelete() {
        final TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        runner.setProperty(UpdateAttribute.DELETE_ATTRIBUTES, "(attribute\\.[2-5]|sample.*)");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attribute.1", "value.1");
        attributes.put("attribute.2", "value.2");
        attributes.put("attribute.6", "value.6");
        attributes.put("sampleSize", "value.size");
        attributes.put("sample.1", "value.sample.1");
        attributes.put("simple.1", "value.simple.1");
        runner.enqueue(new byte[0], attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS);
        result.get(0).assertAttributeEquals("attribute.1", "value.1");
        result.get(0).assertAttributeNotExists("attribute.2");
        result.get(0).assertAttributeExists("attribute.6");
        result.get(0).assertAttributeNotExists("sampleSize");
        result.get(0).assertAttributeNotExists("sample.1");
        result.get(0).assertAttributeExists("simple.1");
    }

    @Test
    public void testAttributeKey() {
        final TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        runner.setProperty(UpdateAttribute.DELETE_ATTRIBUTES, "(attribute\\.[2-5]|sample.*)");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attribute.1", "value.1");
        attributes.put("attribute.2", "value.2");
        attributes.put("attribute.6", "value.6");
        attributes.put("sampleSize", "value.size");
        attributes.put("sample.1", "value.sample.1");
        attributes.put("simple.1", "value.simple.1");
        runner.enqueue(new byte[0], attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS);
        result.get(0).assertAttributeEquals("attribute.1", "value.1");
        result.get(0).assertAttributeNotExists("attribute.2");
        result.get(0).assertAttributeExists("attribute.6");
        result.get(0).assertAttributeNotExists("sampleSize");
        result.get(0).assertAttributeNotExists("sample.1");
        result.get(0).assertAttributeExists("simple.1");
    }

    @Test
    public void testExpressionLiteralDelete() {
        final TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        runner.setProperty(UpdateAttribute.DELETE_ATTRIBUTES, "${literal('attribute\\.'):append(${literal(6)})}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attribute.1", "value.1");
        attributes.put("attribute.2", "value.2");
        attributes.put("attribute.6", "value.6");
        attributes.put("sampleSize", "value.size");
        attributes.put("sample.1", "value.sample.1");
        attributes.put("simple.1", "value.simple.1");
        runner.enqueue(new byte[0], attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS);
        result.get(0).assertAttributeEquals("attribute.1", "value.1");
        result.get(0).assertAttributeExists("attribute.2");
        result.get(0).assertAttributeNotExists("attribute.6");
        result.get(0).assertAttributeExists("sampleSize");
        result.get(0).assertAttributeExists("sample.1");
        result.get(0).assertAttributeExists("simple.1");
    }

    @Test
    public void testExpressionRegexDelete() {
        final TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        runner.setProperty(UpdateAttribute.DELETE_ATTRIBUTES, "${literal('(attribute\\.'):append(${literal('[2-5]')}):append(${literal('|sample.*)')})}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attribute.1", "value.1");
        attributes.put("attribute.2", "value.2");
        attributes.put("attribute.6", "value.6");
        attributes.put("sampleSize", "value.size");
        attributes.put("sample.1", "value.sample.1");
        attributes.put("simple.1", "value.simple.1");
        runner.enqueue(new byte[0], attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS);
        result.get(0).assertAttributeEquals("attribute.1", "value.1");
        result.get(0).assertAttributeNotExists("attribute.2");
        result.get(0).assertAttributeExists("attribute.6");
        result.get(0).assertAttributeNotExists("sampleSize");
        result.get(0).assertAttributeNotExists("sample.1");
        result.get(0).assertAttributeExists("simple.1");
    }

    @Test
    public void testAttributeListDelete() {
        final TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        runner.setProperty(UpdateAttribute.DELETE_ATTRIBUTES, "attribute.1|attribute.2|sample.1|simple.1");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attribute.1", "value.1");
        attributes.put("attribute.2", "value.2");
        attributes.put("attribute.6", "value.6");
        attributes.put("sampleSize", "value.size");
        attributes.put("sample.1", "value.sample.1");
        attributes.put("simple.1", "value.simple.1");
        runner.enqueue(new byte[0], attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS);
        result.get(0).assertAttributeNotExists("attribute.1");
        result.get(0).assertAttributeNotExists("attribute.2");
        result.get(0).assertAttributeExists("attribute.6");
        result.get(0).assertAttributeExists("sampleSize");
        result.get(0).assertAttributeNotExists("sample.1");
        result.get(0).assertAttributeNotExists("simple.1");
    }

    @Test
    public void testInvalidRegex() {
        final TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        runner.setProperty(UpdateAttribute.DELETE_ATTRIBUTES, "(");
        runner.assertNotValid();
    }

    @Test
    public void testInvalidRegexInAttribute() {
        final TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        runner.setProperty(UpdateAttribute.DELETE_ATTRIBUTES, "${butter}");
        runner.assertValid();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("butter", "(");
        runner.enqueue(new byte[0], attributes);
        try {
            runner.run();
        } catch (Throwable t) {
            assertEquals(ProcessException.class, t.getCause().getClass());
        }
    }

    @Test
    public void testDataIsTooShort() {
        final TestRunner runner = TestRunners.newTestRunner(new UpdateAttribute());
        runner.setProperty("attribute.1", "${test:substring(1, 20)}");

        runner.assertValid();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("test", "chocolate");
        runner.enqueue(new byte[0], attributes);
        try {
            runner.run();
        } catch (AssertionError e) {
            Assert.assertTrue(e.getMessage().contains("org.apache.nifi.processor.exception.ProcessException"));
        }
    }

}
