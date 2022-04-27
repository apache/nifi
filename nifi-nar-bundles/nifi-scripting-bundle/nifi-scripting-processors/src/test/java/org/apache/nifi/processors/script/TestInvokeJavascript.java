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
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledForJreRange;
import org.junit.jupiter.api.condition.JRE;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisabledForJreRange(min = JRE.JAVA_15, disabledReason = "Java 15 removed Nashorn Engine")
public class TestInvokeJavascript extends BaseScriptTest {

    @BeforeEach
    public void setup() throws Exception {
        super.setupInvokeScriptProcessor();
    }

    /**
     * Tests a scripted processor written in Javascript that reads the first line of text from the flowfiles content
     * and stores the value in an attribute of the outgoing flowfile.
     * Confirms that the scripted processor transfers the incoming flowfile with an attribute added.
     *
     * @Any error encountered while testing
     */
    @Test
    public void testReadFlowFileContentAndStoreInFlowFileAttribute() {
        setScriptProperties();

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
     * @Any error encountered while testing
     */
    @Test
    public void testScriptDefinedAttribute() {
        setScriptProperties();
        runner.assertValid();

        List<PropertyDescriptor> descriptors = runner.getProcessor().getPropertyDescriptors();
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
     * @Any error encountered while testing
     */
    @Test
    public void testScriptDefinedRelationshipWithExternalJar() {
        setScriptProperties();
        runner.assertValid();

        Set<Relationship> relationships = runner.getProcessor().getRelationships();
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
     */
    @Test
    public void testInvokeScriptCausesException() {
        final TestRunner runner = TestRunners.newTestRunner(new InvokeScriptedProcessor());
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "ECMAScript");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_BODY, getFileContentsAsString(
                TEST_RESOURCE_LOCATION + "javascript/testInvokeScriptCausesException.js")
        );
        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        assertThrows(AssertionError.class, () -> runner.run());
    }

    /**
     * Tests a script that routes the FlowFile to failure.
     *
     * @Any error encountered while testing
     */
    @Test
    public void testScriptRoutesToFailure() {
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

    /**
     * Tests an empty script with Nashorn (which throws an NPE if it is loaded), this test verifies an empty script is not attempted to be loaded.
     *
     * @Any error encountered while testing
     */
    @Test
    public void testEmptyScript() {
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "ECMAScript");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_BODY, "");
        runner.assertNotValid();
    }

    private void setScriptProperties() {
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "ECMAScript");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, getResource("/javascript/test_reader.js"));
        runner.setProperty(ScriptingComponentUtils.MODULES, getResource("/jar"));
    }

    private String getResource(final String resourcePath) {
        final URL resourceUrl = Objects.requireNonNull(TestInvokeJavascript.class.getResource(resourcePath), resourcePath);
        final URI resourceUri;
        try {
            resourceUri = resourceUrl.toURI();
        } catch (final URISyntaxException e) {
            throw new RuntimeException(e);
        }
        return Paths.get(resourceUri).toString();
    }
}
