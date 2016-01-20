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

import org.apache.commons.io.FileUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockProcessorInitializationContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestInvokeGroovy {

    private TestRunner runner;

    /**
     * Copies all scripts to the target directory because when they are compiled they can leave unwanted .class files.
     *
     * @throws Exception Any error encountered while testing
     */
    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        FileUtils.copyDirectory(new File("src/test/resources"), new File("target/test/resources"));
    }

    @Before
    public void setup() throws Exception {
        final InvokeScriptProcessor invokeScriptProcessor = new InvokeScriptProcessor();
        // Need to do something to initialize the properties, like retrieve the list of properties
        assertNotNull(invokeScriptProcessor.getSupportedPropertyDescriptors());
        runner = TestRunners.newTestRunner(invokeScriptProcessor);
    }

    /**
     * Tests a script that has a Groovy Processor that that reads the first line of text from the flowfiles content and stores the value in an attribute of the outgoing flowfile.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testReadFlowFileContentAndStoreInFlowFileAttribute() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(InvokeScriptProcessor.SCRIPT_ENGINE, "Groovy");
        runner.setProperty(InvokeScriptProcessor.SCRIPT_FILE, "target/test/resources/groovy/test_reader.groovy");
        runner.setProperty(InvokeScriptProcessor.MODULES, "target/test/resources/groovy");

        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred("success", 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
        result.get(0).assertAttributeEquals("from-content", "test content");
    }

    /**
     * Tests a script that has a Groovy Processor that that reads the first line of text from the flowfiles content and
     * stores the value in an attribute of the outgoing flowfile.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testScriptDefinedAttribute() throws Exception {
        InvokeScriptProcessor processor = new InvokeScriptProcessor();
        MockProcessContext context = new MockProcessContext(processor);
        MockProcessorInitializationContext initContext = new MockProcessorInitializationContext(processor, context);

        processor.initialize(initContext);

        context.setProperty(InvokeScriptProcessor.SCRIPT_ENGINE, "Groovy");
        context.setProperty(InvokeScriptProcessor.SCRIPT_FILE, "target/test/resources/groovy/test_reader.groovy");
        context.setProperty(InvokeScriptProcessor.MODULES, "target/test/resources/groovy");

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
     * Tests a script that has a Groovy Processor that that reads the first line of text from the flowfiles content and
     * stores the value in an attribute of the outgoing flowfile.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testScriptDefinedRelationship() throws Exception {
        InvokeScriptProcessor processor = new InvokeScriptProcessor();
        MockProcessContext context = new MockProcessContext(processor);
        MockProcessorInitializationContext initContext = new MockProcessorInitializationContext(processor, context);

        processor.initialize(initContext);

        context.setProperty(InvokeScriptProcessor.SCRIPT_ENGINE, "Groovy");
        context.setProperty(InvokeScriptProcessor.SCRIPT_FILE, "target/test/resources/groovy/test_reader.groovy");

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
     * Tests a script that throws a ProcessException within. The expected result is that the exception will be
     * propagated
     *
     * @throws Exception Any error encountered while testing
     */
    @Test(expected = AssertionError.class)
    public void testScriptException() throws Exception {
        String scriptBody = "class GroovyProcessor implements Processor {\n" +
                "\n" +
                "    def ProcessorLog log\n" +
                "\n" +
                "    @Override\n" +
                "    void initialize(ProcessorInitializationContext context) {\n" +
                "    }\n" +
                "\n" +
                "    @Override\n" +
                "    Set<Relationship> getRelationships() {\n" +
                "        return [] as Set\n" +
                "    }\n" +
                "\n" +
                "    @Override\n" +
                "    void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {\n" +
                "        throw new ProcessException();\n" +
                "    }\n" +
                "\n" +
                "    @Override\n" +
                "    Collection<ValidationResult> validate(ValidationContext context) {\n" +
                "        return null\n" +
                "    }\n" +
                "\n" +
                "    @Override\n" +
                "    PropertyDescriptor getPropertyDescriptor(String name) {\n" +
                "        return null\n" +
                "    }\n" +
                "\n" +
                "    @Override\n" +
                "    void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {\n" +
                "\n" +
                "    }\n" +
                "\n" +
                "    @Override\n" +
                "    List<PropertyDescriptor> getPropertyDescriptors() {\n" +
                "        return null\n" +
                "    }\n" +
                "\n" +
                "    @Override\n" +
                "    String getIdentifier() {\n" +
                "        return null\n" +
                "    }\n" +
                "}\n" +
                "\n" +
                "processor = new GroovyProcessor();";
        final TestRunner runner = TestRunners.newTestRunner(new InvokeScriptProcessor());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(InvokeScriptProcessor.SCRIPT_ENGINE, "Groovy");
        runner.setProperty(InvokeScriptProcessor.SCRIPT_BODY, scriptBody);

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
        String scriptBody = "class GroovyProcessor implements Processor {\n" +
                "\n" +
                "    def ProcessorLog log\n" +
                "\n" +
                "    @Override\n" +
                "    void initialize(ProcessorInitializationContext context) {\n" +
                "    }\n" +
                "\n" +
                "    @Override\n" +
                "    Set<Relationship> getRelationships() {\n" +
                "        return [] as Set\n" +
                "    }\n" +
                "\n" +
                "    @Override\n" +
                "    void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {\n" +
                "        def session = sessionFactory.createSession()\n" +
                "        def flowFile = session.get()\n" +
                "        if(!flowFile) return\n" +
                "        session.transfer(flowFile, InvokeScriptProcessor.REL_FAILURE)" +
                "    }\n" +
                "\n" +
                "    @Override\n" +
                "    Collection<ValidationResult> validate(ValidationContext context) {\n" +
                "        return null\n" +
                "    }\n" +
                "\n" +
                "    @Override\n" +
                "    PropertyDescriptor getPropertyDescriptor(String name) {\n" +
                "        return null\n" +
                "    }\n" +
                "\n" +
                "    @Override\n" +
                "    void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {\n" +
                "\n" +
                "    }\n" +
                "\n" +
                "    @Override\n" +
                "    List<PropertyDescriptor> getPropertyDescriptors() {\n" +
                "        return null\n" +
                "    }\n" +
                "\n" +
                "    @Override\n" +
                "    String getIdentifier() {\n" +
                "        return null\n" +
                "    }\n" +
                "}\n" +
                "\n" +
                "processor = new GroovyProcessor();";
        final TestRunner runner = TestRunners.newTestRunner(new InvokeScriptProcessor());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(InvokeScriptProcessor.SCRIPT_ENGINE, "Groovy");
        runner.setProperty(InvokeScriptProcessor.SCRIPT_BODY, scriptBody);

        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(InvokeScriptProcessor.REL_FAILURE, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(InvokeScriptProcessor.REL_FAILURE);
        assertFalse(result.isEmpty());
    }
}
