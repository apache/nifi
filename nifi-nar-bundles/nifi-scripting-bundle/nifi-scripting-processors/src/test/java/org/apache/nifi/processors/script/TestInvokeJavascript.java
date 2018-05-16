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
package org.apache.nifi.processors.script;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.script.ScriptingComponentUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockProcessorInitializationContext;
import org.apache.nifi.util.MockValidationContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestInvokeJavascript extends BaseScriptTest {

    @Before
    public void setup() throws Exception {
        super.setupInvokeScriptProcessor();
    }

    /**
     * Tests a scripted processor written in Javascript that reads the first line of text from the flowfiles content
     * and stores the value in an attribute of the outgoing flowfile.
     * Confirms that the scripted processor transfers the incoming flowfile with an attribute added.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testReadFlowFileContentAndStoreInFlowFileAttribute() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "ECMAScript");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, "target/test/resources/javascript/test_reader.js");
        runner.setProperty(ScriptingComponentUtils.MODULES, "target/test/resources/javascript");

        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred("test", 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("test");
        result.get(0).assertAttributeEquals("from-content", "test content");
    }

    /**
     * Tests a scripted processor written in Javascript that reads the first line of text from the flowfiles content and
     * stores the value in an attribute of the outgoing flowfile.
     * Confirms that the scripted processor can return property descriptors defined in it.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testScriptDefinedAttribute() throws Exception {
        InvokeScriptedProcessor processor = new InvokeScriptedProcessor();
        MockProcessContext context = new MockProcessContext(processor);
        MockProcessorInitializationContext initContext = new MockProcessorInitializationContext(processor, context);

        processor.initialize(initContext);

        context.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "ECMAScript");
        context.setProperty(ScriptingComponentUtils.SCRIPT_FILE, "target/test/resources/javascript/test_reader.js");
        context.setProperty(ScriptingComponentUtils.MODULES, "target/test/resources/javascript");
        // State Manger is unused, and a null reference is specified
        processor.customValidate(new MockValidationContext(context));
        processor.setup(context);

        List<PropertyDescriptor> descriptors = processor.getSupportedPropertyDescriptors();
        assertNotNull(descriptors);
        assertTrue(descriptors.size() > 0);
        boolean found = false;
        for (PropertyDescriptor descriptor : descriptors) {
            if (descriptor.getName().equals("test-attribute")) {
                found = true;
                break;
            }
        }
        assertTrue(found);
    }

    /**
     * Tests a scripted processor written in Javascript reads the first line of text from the flowfiles content and
     * stores the value in an attribute of the outgoing flowfile.
     * Confirms that the scripted processor can return relationships defined in it.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testScriptDefinedRelationship() throws Exception {
        InvokeScriptedProcessor processor = new InvokeScriptedProcessor();
        MockProcessContext context = new MockProcessContext(processor);
        MockProcessorInitializationContext initContext = new MockProcessorInitializationContext(processor, context);

        processor.initialize(initContext);

        context.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "ECMAScript");
        context.setProperty(ScriptingComponentUtils.SCRIPT_FILE, "target/test/resources/javascript/test_reader.js");
        context.setProperty(ScriptingComponentUtils.MODULES, "target/test/resources/javascript");

        // State Manger is unused, and a null reference is specified
        processor.customValidate(new MockValidationContext(context));
        processor.setup(context);

        Set<Relationship> relationships = processor.getRelationships();
        assertNotNull(relationships);
        assertTrue(relationships.size() > 0);
        boolean found = false;
        for (Relationship relationship : relationships) {
            if (relationship.getName().equals("test")) {
                found = true;
                break;
            }
        }
        assertTrue(found);
    }

    /**
     * Tests a script that throws a ProcessException within.
     * The expected result is that the exception will be propagated.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test(expected = AssertionError.class)
    public void testInvokeScriptCausesException() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new InvokeScriptedProcessor());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "ECMAScript");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_BODY, getFileContentsAsString(
                TEST_RESOURCE_LOCATION + "javascript/testInvokeScriptCausesException.js")
        );
        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();

    }

    /**
     * Tests a script that routes the FlowFile to failure.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testScriptRoutesToFailure() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "ECMAScript");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_BODY, getFileContentsAsString(
                TEST_RESOURCE_LOCATION + "javascript/testScriptRoutesToFailure.js")
        );
        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred("FAILURE", 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("FAILURE");
        assertFalse(result.isEmpty());
    }
}
