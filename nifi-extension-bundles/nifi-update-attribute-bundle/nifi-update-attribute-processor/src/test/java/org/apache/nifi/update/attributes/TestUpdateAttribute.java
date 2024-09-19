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

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.attributes.UpdateAttribute;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.update.attributes.serde.CriteriaSerDe;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.nifi.processors.attributes.UpdateAttribute.STORE_STATE_LOCALLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestUpdateAttribute {

    final static private String TEST_CONTENT = "THIS IS TEST CONTENT";

    final UpdateAttribute processor = new UpdateAttribute();
    final TestRunner runner = TestRunners.newTestRunner(processor);
    final MockStateManager mockStateManager = runner.getStateManager();
    final ProcessSessionFactory processSessionFactory = runner.getProcessSessionFactory();

    @Test
    public void testDefault() {
        runner.setProperty("attribute.1", "value.1");
        runner.setProperty("attribute.2", "new.value.2");

        runner.enqueue(new byte[0], Map.of("attribute.2", "old.value.2"));
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals("attribute.1", "value.1");
        flowFile.assertAttributeEquals("attribute.2", "new.value.2");
    }

    @Test
    public void testSetEmptyString() {
        runner.setProperty("attribute.1", "");
        runner.assertValid();

        // No attributes on flowFile
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final MockFlowFile flowFile1 = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).getFirst();
        assertTrue(flowFile1.getAttribute("attribute.1").isEmpty());
        runner.clearTransferState();

        // Existing attribute to be replaced on FlowFile
        runner.enqueue(new byte[0], Map.of("attribute.1", "old.value.1"));
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final MockFlowFile flowFile2 = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).getFirst();
        assertTrue(flowFile2.getAttribute("attribute.1").isEmpty());
    }

    @Test
    public void testDefaultAddAttribute() {
        runner.setProperty("NewAttr", "${one:plus(${two})}");

        runner.enqueue(new byte[0], Map.of("one", "1", "two", "2"));
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals("NewAttr", "3");
    }

    @Test
    public void testAddAttributeWithIncorrectExpression() {
        runner.setProperty("NewId", "${UUID(}");
        runner.assertNotValid();
    }

    @Test
    public void testBasicState() {
        runner.setProperty(UpdateAttribute.STORE_STATE, STORE_STATE_LOCALLY);
        runner.setProperty("count", "${getStateValue('count'):plus(1)}");
        runner.setProperty("sum", "${getStateValue('sum'):plus(${pencils})}");

        runner.assertNotValid();
        runner.setProperty(UpdateAttribute.STATEFUL_VARIABLES_INIT_VALUE, "0");
        runner.assertValid();

        runner.enqueue(new byte[0], Map.of("pencils", "2"));
        runner.enqueue(new byte[0], Map.of("pencils", "2"));
        runner.enqueue(new byte[0], Map.of("pencils", "3"));
        runner.enqueue(new byte[0], Map.of("pencils", "3"));
        runner.enqueue(new byte[0], Map.of("pencils", "5"));
        runner.run(5);

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 5);
        runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).get(4).assertAttributeEquals("count", "5");
        runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).get(4).assertAttributeEquals("sum", "15");
    }

    @Test
    public void testStateFailures() throws Exception {
        runner.setProperty(UpdateAttribute.STORE_STATE, STORE_STATE_LOCALLY);
        runner.setProperty("count", "${getStateValue('count'):plus(1)}");
        runner.setProperty("sum", "${getStateValue('sum'):plus(${pencils})}");
        runner.setProperty(UpdateAttribute.STATEFUL_VARIABLES_INIT_VALUE, "0");

        processor.onScheduled(runner.getProcessContext());

        mockStateManager.setFailOnStateGet(Scope.LOCAL, true);

        runner.enqueue(new byte[0], Map.of("pencils", "2"));
        processor.onTrigger(runner.getProcessContext(), processSessionFactory.createSession());
        runner.assertQueueNotEmpty();

        mockStateManager.setFailOnStateGet(Scope.LOCAL, false);
        mockStateManager.setFailOnStateSet(Scope.LOCAL, true);

        processor.onTrigger(runner.getProcessContext(), processSessionFactory.createSession());

        runner.assertQueueEmpty();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_FAILED_SET_STATE, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateAttribute.REL_FAILED_SET_STATE).getFirst();
        flowFile.assertAttributeEquals("count", "1");
        flowFile.assertAttributeEquals("sum", "2");
    }


    @Test
    public void testStateWithInitValue() {
        runner.setProperty(UpdateAttribute.STORE_STATE, STORE_STATE_LOCALLY);
        runner.setProperty(UpdateAttribute.STATEFUL_VARIABLES_INIT_VALUE, "10");
        runner.setProperty("count", "${getStateValue('count'):plus(1)}");
        runner.setProperty("sum", "${getStateValue('sum'):plus(${pencils})}");

        runner.assertValid();

        runner.enqueue(new byte[0], Map.of("pencils", "2"));
        runner.enqueue(new byte[0], Map.of("pencils", "2"));
        runner.enqueue(new byte[0], Map.of("pencils", "3"));
        runner.enqueue(new byte[0], Map.of("pencils", "3"));
        runner.enqueue(new byte[0], Map.of("pencils", "5"));
        runner.run(5);

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 5);
        runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).get(4).assertAttributeEquals("count", "15");
        runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).get(4).assertAttributeEquals("sum", "25");
    }

    @Test
    public void testRuleHitWithState() {
        final Criteria criteria = getCriteria();
        addRule(criteria, "rule",
                // conditions
                List.of("${getStateValue('maxValue'):lt(${value})}"),
                // actions
                Map.of("maxValue", "${value}")
        );

        runner.setProperty(UpdateAttribute.STORE_STATE, STORE_STATE_LOCALLY);
        runner.setProperty(UpdateAttribute.STATEFUL_VARIABLES_INIT_VALUE, "0");
        runner.setAnnotationData(serialize(criteria));

        runner.enqueue(new byte[0], Map.of("value", "1"));
        runner.run();
        runner.enqueue(new byte[0], Map.of("value", "2"));
        runner.run();
        runner.enqueue(new byte[0], Map.of("value", "4"));
        runner.run();
        runner.enqueue(new byte[0], Map.of("value", "1"));
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
        addRule(criteria, "rule",
                // conditions
                List.of("${getStateValue('maxValue'):lt(${value})}"),
                // actions
                Map.of("maxValue", "${value}")
        );
        addRule(criteria, "rule2",
                // conditions
                List.of("${getStateValue('maxValue2'):lt(${value})}"),
                // actions
                Map.of("maxValue2", "${value}")
        );

        runner.setProperty(UpdateAttribute.STORE_STATE, STORE_STATE_LOCALLY);
        runner.setProperty(UpdateAttribute.STATEFUL_VARIABLES_INIT_VALUE, "0");
        runner.setAnnotationData(serialize(criteria));

        processor.onScheduled(runner.getProcessContext());

        runner.enqueue(new byte[0], Map.of("value", "1"));

        mockStateManager.setFailOnStateGet(Scope.LOCAL, true);
        processor.onTrigger(runner.getProcessContext(), processSessionFactory.createSession());

        runner.assertQueueNotEmpty();
        mockStateManager.setFailOnStateGet(Scope.LOCAL, false);
        mockStateManager.setFailOnStateSet(Scope.LOCAL, true);

        processor.onTrigger(runner.getProcessContext(), processSessionFactory.createSession());

        runner.assertQueueEmpty();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_FAILED_SET_STATE, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateAttribute.REL_FAILED_SET_STATE).getFirst();
        flowFile.assertAttributeEquals("maxValue", "1");
        flowFile.assertAttributeEquals("maxValue2", "1");
    }

    @Test
    public void testStateFailuresWithRulesUsingClone() throws Exception {
        final Criteria criteria = getCriteria();
        criteria.setFlowFilePolicy(FlowFilePolicy.USE_CLONE);
        addRule(criteria, "rule",
                // conditions
                List.of("${getStateValue('maxValue'):lt(${value})}"),
                // actions
                Map.of("maxValue", "${value}")
        );
        addRule(criteria, "rule2",
                // conditions
                List.of("${getStateValue('maxValue2'):lt(${value})}"),
                // actions
                Map.of("maxValue2", "${value}")
        );

        runner.setProperty(UpdateAttribute.STORE_STATE, STORE_STATE_LOCALLY);
        runner.setProperty(UpdateAttribute.STATEFUL_VARIABLES_INIT_VALUE, "0");
        runner.setAnnotationData(serialize(criteria));

        processor.onScheduled(runner.getProcessContext());

        runner.enqueue(new byte[0], Map.of("value", "1"));

        mockStateManager.setFailOnStateGet(Scope.LOCAL, true);
        processor.onTrigger(runner.getProcessContext(), processSessionFactory.createSession());

        runner.assertQueueNotEmpty();
        mockStateManager.setFailOnStateGet(Scope.LOCAL, false);
        mockStateManager.setFailOnStateSet(Scope.LOCAL, true);

        processor.onTrigger(runner.getProcessContext(), processSessionFactory.createSession());

        runner.assertQueueEmpty();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_FAILED_SET_STATE, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateAttribute.REL_FAILED_SET_STATE).getFirst();
        flowFile.assertAttributeEquals("maxValue", "1");
        flowFile.assertAttributeNotExists("maxValue2");
    }
    @Test
    public void testRuleHitWithStateWithDefault() {
        final Criteria criteria = getCriteria();
        addRule(criteria, "rule",
                // conditions
                List.of("${getStateValue('maxValue'):lt(${value})}"),
                // actions
                Map.of("maxValue", "${value}")
        );

        runner.setProperty(UpdateAttribute.STORE_STATE, STORE_STATE_LOCALLY);
        runner.setProperty(UpdateAttribute.STATEFUL_VARIABLES_INIT_VALUE, "0");
        runner.setAnnotationData(serialize(criteria));
        runner.setProperty("maxValue", "${getStateValue('maxValue')}");

        runner.enqueue(new byte[0], Map.of("value", "1"));
        runner.run();
        runner.enqueue(new byte[0], Map.of("value", "2"));
        runner.run();
        runner.enqueue(new byte[0], Map.of("value", "4"));
        runner.run();
        runner.enqueue(new byte[0], Map.of("value", "1"));
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 4);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS);
        result.get(2).assertAttributeEquals("maxValue", "4");
        result.get(3).assertAttributeEquals("maxValue", "4");
    }

    @Test
    public void testRuleHitWithStateWithInitValue() {
        final Criteria criteria = getCriteria();
        addRule(criteria, "rule",
                // conditions
                List.of("${getStateValue('minValue'):ge(${value})}"),
                // actions
                Map.of("minValue", "${value}")
        );

        runner.setProperty(UpdateAttribute.STORE_STATE, STORE_STATE_LOCALLY);
        runner.setProperty(UpdateAttribute.STATEFUL_VARIABLES_INIT_VALUE, "5");
        runner.setAnnotationData(serialize(criteria));

        runner.enqueue(new byte[0], Map.of("value", "1"));
        runner.run();
        runner.enqueue(new byte[0], Map.of("value", "2"));
        runner.run();
        runner.enqueue(new byte[0], Map.of("value", "4"));
        runner.run();
        runner.enqueue(new byte[0], Map.of("value", "1"));
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 4);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS);
        result.get(3).assertAttributeEquals("minValue", "1");
    }

    @Test
    public void testMultipleRulesWithStateAndDelete() {
        final Criteria criteria = getCriteria();
        criteria.setFlowFilePolicy(FlowFilePolicy.USE_ORIGINAL);
        addRule(criteria, "rule",
                // conditions
                List.of("${getStateValue('maxValue'):lt(${value})}"),
                // actions
                Map.of("maxValue", "${value}")
        );
        addRule(criteria, "rule2",
                // conditions
                List.of("${value:mod(2):equals(0)}"),
                // actions
                Map.of("whatIsIt", "even")
        );

        runner.setProperty(UpdateAttribute.STORE_STATE, STORE_STATE_LOCALLY);
        runner.setProperty(UpdateAttribute.DELETE_ATTRIBUTES, "badValue");
        runner.setProperty(UpdateAttribute.STATEFUL_VARIABLES_INIT_VALUE, "0");
        runner.setAnnotationData(serialize(criteria));
        runner.setProperty("maxValue", "${getStateValue('maxValue')}");
        runner.setProperty("whatIsIt", "odd");
        runner.setProperty("whatWasIt", "${getStateValue('whatIsIt')}");
        runner.setProperty("theCount", "${getStateValue('theCount'):plus(1)}");

        runner.enqueue(new byte[0], Map.of("value", "1", "badValue", "10"));
        runner.run();
        runner.enqueue(new byte[0], Map.of("value", "2", "badValue", "10"));
        runner.run();
        runner.enqueue(new byte[0], Map.of("value", "5", "badValue", "10"));
        runner.run();
        runner.enqueue(new byte[0], Map.of("value", "1", "badValue", "10"));
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
    public void testDefaultBooleanAsString() {
        runner.setProperty("NewAttr", "${a:equals('b'):toString()}");

        runner.enqueue(new byte[0], Map.of("a", "b"));
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals("NewAttr", "true");
    }

    @Test
    public void testDefaultEscapeValue() {
        runner.setProperty("NewAttr", "$${a}");

        runner.enqueue(new byte[0], Map.of("a", "b"));
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals("NewAttr", "${a}");
    }

    @Test
    public void testRuleMissBecauseAttributeMissing() {
        final Criteria criteria = getCriteria();
        addRule(criteria, "rule",
                // conditions
                List.of("${attribute5:equals('value.5')}"),
                // actions
                Map.of("attribute.2", "value.2")
        );

        runner.setAnnotationData(serialize(criteria));

        runner.enqueue(new byte[0], Map.of("attribute.1", "value.1"));
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals("attribute.1", "value.1");
        flowFile.assertAttributeNotExists("attribute.2");
    }

    @Test
    public void testRuleMissBecauseValueNoMatch() {
        final Criteria criteria = getCriteria();
        addRule(criteria, "rule",
                // conditions
                List.of("${attribute1:equals('not.value.1')}"),
                // actions
                Map.of("attribute.2", "value.2")
        );

        runner.setAnnotationData(serialize(criteria));

        runner.enqueue(new byte[0], Map.of("attribute.1", "value.1"));
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals("attribute.1", "value.1");
        flowFile.assertAttributeNotExists("attribute.2");
    }

    @Test
    public void testRuleHitWithDefaults() {
        final Criteria criteria = getCriteria();
        addRule(criteria, "rule",
                // conditions
                List.of("${attribute.1:equals('value.1')}"),
                // actions
                Map.of("attribute.2", "value.2")
        );

        runner.setAnnotationData(serialize(criteria));
        runner.setProperty("attribute.1", "new.value.1");

        runner.enqueue(new byte[0], Map.of("attribute.1", "value.1"));
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals("attribute.1", "new.value.1");
        flowFile.assertAttributeEquals("attribute.2", "value.2");
    }

    @Test
    public void testRuleHitWithAConflictingDefault() {
        final Criteria criteria = getCriteria();
        addRule(criteria, "rule",
                // conditions
                List.of("${attribute.1:equals('value.1')}"),
                // actions
                Map.of("attribute.2", "value.2")
        );

        runner.setAnnotationData(serialize(criteria));
        runner.setProperty("attribute.2", "default.value.2");

        runner.enqueue(new byte[0], Map.of("attribute.1", "value.1"));
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals("attribute.2", "value.2");
    }

    @Test
    public void testMultipleRuleHitsWithNoFlowFilePolicySpecified() throws Exception {
        final Criteria criteria = getCriteria();
        addRule(criteria, "rule 1",
                // conditions
                List.of("${attribute.1:equals('value.1')}"),
                // actions
                Map.of("attribute.2", "value.2")
        );
        addRule(criteria, "rule 2",
                // conditions
                List.of("${attribute.1:equals('value.1')}"),
                // actions
                Map.of("attribute.3", "value.3")
        );

        runner.setAnnotationData(serialize(criteria));
        runner.setProperty("attribute.2", "default.value.2");

        runner.enqueue(TEST_CONTENT.getBytes(StandardCharsets.UTF_8), Map.of("attribute.1", "value.1"));
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 2);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS);

        final MockFlowFile flowFile1 = result.get(0);
        final MockFlowFile flowFile2 = result.get(1);

        // ensure the attributes are as expected
        if ("rule 1".equals(flowFile1.getAttribute(runner.getProcessor().getClass().getSimpleName() + ".matchedRule"))) {
            flowFile1.assertAttributeEquals("attribute.2", "value.2");
            flowFile2.assertAttributeEquals("attribute.3", "value.3");
            flowFile2.assertAttributeEquals("attribute.2", "default.value.2");
        } else {
            flowFile2.assertAttributeEquals("attribute.2", "value.2");
            flowFile1.assertAttributeEquals("attribute.3", "value.3");
            flowFile1.assertAttributeEquals("attribute.2", "default.value.2");
        }

        // ensure the content was copied as well
        flowFile1.assertContentEquals(TEST_CONTENT.getBytes(StandardCharsets.UTF_8));
        flowFile2.assertContentEquals(TEST_CONTENT.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testMultipleRuleHitsWithUseClone() throws Exception {
        final Criteria criteria = getCriteria();
        criteria.setFlowFilePolicy(FlowFilePolicy.USE_CLONE);
        addRule(criteria, "rule 1",
                // conditions
                List.of("${attribute.1:equals('value.1')}"),
                // actions
                Map.of("attribute.2", "value.2")
        );
        addRule(criteria, "rule 2",
                // conditions
                List.of("${attribute.1:equals('value.1')}"),
                // actions
                Map.of("attribute.3", "value.3")
        );

        runner.setAnnotationData(serialize(criteria));
        runner.setProperty("attribute.2", "default.value.2");

        runner.enqueue(TEST_CONTENT.getBytes(StandardCharsets.UTF_8), Map.of("attribute.1", "value.1"));
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 2);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS);

        final MockFlowFile flowFile1 = result.get(0);
        final MockFlowFile flowFile2 = result.get(1);

        // ensure the attributes are as expected
        if ("rule 1".equals(flowFile1.getAttribute(runner.getProcessor().getClass().getSimpleName() + ".matchedRule"))) {
            flowFile1.assertAttributeEquals("attribute.2", "value.2");
            flowFile2.assertAttributeEquals("attribute.3", "value.3");
            flowFile2.assertAttributeEquals("attribute.2", "default.value.2");
        } else {
            flowFile2.assertAttributeEquals("attribute.2", "value.2");
            flowFile1.assertAttributeEquals("attribute.3", "value.3");
            flowFile1.assertAttributeEquals("attribute.2", "default.value.2");
        }

        // ensure the content was copied as well
        flowFile1.assertContentEquals(TEST_CONTENT.getBytes(StandardCharsets.UTF_8));
        flowFile2.assertContentEquals(TEST_CONTENT.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testMultipleRuleHitsWithUseOriginal() throws Exception {
        final Criteria criteria = getCriteria();
        criteria.setFlowFilePolicy(FlowFilePolicy.USE_ORIGINAL);
        addRule(criteria, "rule 1",
                // conditions
                List.of("${attribute.1:equals('value.1')}"),
                // actions
                Map.of("attribute.2", "value.2")
        );
        addRule(criteria, "rule 2",
                // conditions
                List.of("${attribute.1:equals('value.1')}"),
                // actions
                Map.of("attribute.3", "value.3")
        );
        addRule(criteria, "rule 3",
                // conditions
                List.of("${attribute.1:equals('value.1')}"),
                // actions
                Map.of("attribute.2", "value.3")
        );

        runner.setAnnotationData(serialize(criteria));
        runner.setProperty("attribute.2", "default.value.2");

        runner.enqueue(TEST_CONTENT.getBytes(StandardCharsets.UTF_8), Map.of("attribute.1", "value.1"));
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).getFirst();

        // ensure the attributes are as expected
        flowFile.assertAttributeEquals("attribute.2", "value.3");
        flowFile.assertAttributeEquals("attribute.3", "value.3");

        // ensure the content was copied as well
        flowFile.assertContentEquals(TEST_CONTENT.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testMultipleRuleHitsWithUseOriginalDoesntApplyDefaultsRepeatedly() {
        final Criteria criteria = getCriteria();
        criteria.setFlowFilePolicy(FlowFilePolicy.USE_ORIGINAL);
        addRule(criteria, "rule 1",
                // conditions
                List.of("${attribute.1:equals('value.1')}"),
                // actions
                Map.of("attribute.2", "value.2")
        );
        addRule(criteria, "rule 2",
                // conditions
                List.of("${attribute.1:equals('value.1')}"),
                // actions
                Map.of("attribute.3", "value.3")
        );
        addRule(criteria, "rule 3",
                // conditions
                List.of("${attribute.1:equals('value.1')}"),
                // actions
                Map.of("attribute.2", "value.3")
        );

        runner.setAnnotationData(serialize(criteria));
        runner.setProperty("default.attr", "${default.attr}-more-stuff");

        runner.enqueue(TEST_CONTENT.getBytes(StandardCharsets.UTF_8), Map.of("attribute.1", "value.1"));
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).getFirst();

        // ensure the attributes are as expected
        flowFile.assertAttributeEquals("default.attr", "-more-stuff");
    }

    @Test
    public void testSimpleDelete() {
        runner.setProperty(UpdateAttribute.DELETE_ATTRIBUTES, "attribute.2");

        runner.enqueue(new byte[0], Map.of(
                "attribute.1", "value.1",
                "attribute.2", "value.2")
        );
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals("attribute.1", "value.1");
        flowFile.assertAttributeNotExists("attribute.2");
    }

    @Test
    public void testRegexDotDelete() {
        runner.setProperty(UpdateAttribute.DELETE_ATTRIBUTES, "attribute.2");

        runner.enqueue(new byte[0], Map.of(
                "attribute.1", "value.1",
                "attribute.2", "value.2",
                "attributex2", "valuex2")
        );
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals("attribute.1", "value.1");
        flowFile.assertAttributeNotExists("attribute.2");
        flowFile.assertAttributeNotExists("attributex2");
    }

    @Test
    public void testRegexLiteralDotDelete() {
        runner.setProperty(UpdateAttribute.DELETE_ATTRIBUTES, "attribute\\.2");

        runner.enqueue(new byte[0], Map.of(
                "attribute.1", "value.1",
                "attribute.2", "value.2",
                "attributex2", "valuex2")
        );
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals("attribute.1", "value.1");
        flowFile.assertAttributeNotExists("attribute.2");
        flowFile.assertAttributeExists("attributex2");
    }

    @Test
    public void testRegexGroupDelete() {
        runner.setProperty(UpdateAttribute.DELETE_ATTRIBUTES, "(attribute\\.[2-5]|sample.*)");

        runner.enqueue(new byte[0], Map.of(
                "attribute.1", "value.1",
                "attribute.2", "value.2",
                "attribute.6", "value.6",
                "sampleSize", "value.size",
                "sample.1", "value.sample.1",
                "simple.1", "value.simple.1")
        );
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals("attribute.1", "value.1");
        flowFile.assertAttributeNotExists("attribute.2");
        flowFile.assertAttributeExists("attribute.6");
        flowFile.assertAttributeNotExists("sampleSize");
        flowFile.assertAttributeNotExists("sample.1");
        flowFile.assertAttributeExists("simple.1");
    }

    @Test
    public void testAttributeKey() {
        runner.setProperty(UpdateAttribute.DELETE_ATTRIBUTES, "(attribute\\.[2-5]|sample.*)");

        runner.enqueue(new byte[0], Map.of(
                "attribute.1", "value.1",
                "attribute.2", "value.2",
                "attribute.6", "value.6",
                "sampleSize", "value.size",
                "sample.1", "value.sample.1",
                "simple.1", "value.simple.1")
        );
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals("attribute.1", "value.1");
        flowFile.assertAttributeNotExists("attribute.2");
        flowFile.assertAttributeExists("attribute.6");
        flowFile.assertAttributeNotExists("sampleSize");
        flowFile.assertAttributeNotExists("sample.1");
        flowFile.assertAttributeExists("simple.1");
    }

    @Test
    public void testExpressionLiteralDelete() {
        runner.setProperty(UpdateAttribute.DELETE_ATTRIBUTES, "${literal('attribute\\.'):append(${literal(6)})}");

        runner.enqueue(new byte[0], Map.of(
                "attribute.1", "value.1",
                "attribute.2", "value.2",
                "attribute.6", "value.6",
                "sampleSize", "value.size",
                "sample.1", "value.sample.1",
                "simple.1", "value.simple.1")
        );
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals("attribute.1", "value.1");
        flowFile.assertAttributeExists("attribute.2");
        flowFile.assertAttributeNotExists("attribute.6");
        flowFile.assertAttributeExists("sampleSize");
        flowFile.assertAttributeExists("sample.1");
        flowFile.assertAttributeExists("simple.1");
    }

    @Test
    public void testExpressionRegexDelete() {
        runner.setProperty(UpdateAttribute.DELETE_ATTRIBUTES, "${literal('(attribute\\.'):append(${literal('[2-5]')}):append(${literal('|sample.*)')})}");

        runner.enqueue(new byte[0], Map.of(
                "attribute.1", "value.1",
                "attribute.2", "value.2",
                "attribute.6", "value.6",
                "sampleSize", "value.size",
                "sample.1", "value.sample.1",
                "simple.1", "value.simple.1")
        );
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals("attribute.1", "value.1");
        flowFile.assertAttributeNotExists("attribute.2");
        flowFile.assertAttributeExists("attribute.6");
        flowFile.assertAttributeNotExists("sampleSize");
        flowFile.assertAttributeNotExists("sample.1");
        flowFile.assertAttributeExists("simple.1");
    }

    @Test
    public void testAttributeListDelete() {
        runner.setProperty(UpdateAttribute.DELETE_ATTRIBUTES, "attribute.1|attribute.2|sample.1|simple.1");

        runner.enqueue(new byte[0], Map.of(
                "attribute.1", "value.1",
                "attribute.2", "value.2",
                "attribute.6", "value.6",
                "sampleSize", "value.size",
                "sample.1", "value.sample.1",
                "simple.1", "value.simple.1")
        );
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateAttribute.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(UpdateAttribute.REL_SUCCESS).getFirst();
        flowFile.assertAttributeNotExists("attribute.1");
        flowFile.assertAttributeNotExists("attribute.2");
        flowFile.assertAttributeExists("attribute.6");
        flowFile.assertAttributeExists("sampleSize");
        flowFile.assertAttributeNotExists("sample.1");
        flowFile.assertAttributeNotExists("simple.1");
    }

    @Test
    public void testInvalidRegex() {
        runner.setProperty(UpdateAttribute.DELETE_ATTRIBUTES, "(");
        runner.assertNotValid();
    }

    @Test
    public void testInvalidRegexInAttribute() {
        runner.setProperty(UpdateAttribute.DELETE_ATTRIBUTES, "${butter}");
        runner.assertValid();

        runner.enqueue(new byte[0], Map.of("butter", "("));
        try {
            runner.run();
        } catch (Throwable t) {
            assertEquals(ProcessException.class, t.getCause().getClass());
        }
    }

    @Test
    public void testDataIsTooShort() {
        runner.setProperty("attribute.1", "${test:substring(1, 20)}");
        runner.assertValid();

        runner.enqueue(new byte[0], Map.of("test", "chocolate"));
        try {
            runner.run();
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("org.apache.nifi.processor.exception.ProcessException"));
        }
    }

    private Criteria getCriteria() {
        return new Criteria();
    }

    // overloaded for convenience method for rules without optional comments
    private void addRule(final Criteria criteria, final String name, final Collection<String> conditions, final Map<String, String> actions) {
        addRule(criteria, name, null, conditions, actions);
    }

    private void addRule(final Criteria criteria, final String name, final String comments, final Collection<String> conditions, final Map<String, String> actions) {
        final Rule rule = new Rule();
        rule.setId(UUID.randomUUID().toString());
        rule.setName(name);
        rule.setComments(comments);
        rule.setConditions(new HashSet<>());
        rule.setActions(new HashSet<>());

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
}
