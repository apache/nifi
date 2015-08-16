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

import org.apache.nifi.update.attributes.FlowFilePolicy;
import org.apache.nifi.update.attributes.Criteria;
import org.apache.nifi.update.attributes.Condition;
import org.apache.nifi.update.attributes.Action;
import org.apache.nifi.update.attributes.Rule;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.nifi.processors.attributes.UpdateAttribute;
import org.apache.nifi.update.attributes.serde.CriteriaSerDe;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Test;

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
        final Criteria criteria = new Criteria();
        return criteria;
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
        runner.setProperty("NewAttr", "abc${'Hello${Goose}'}!");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("Goose", "Geese");
        attributes.put("HelloGeese", "123");
        runner.enqueue(new byte[0], attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).get(0).assertAttributeEquals("NewAttr", "abc123!");
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
}
