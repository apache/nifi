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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.script.ScriptingComponentUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TestInvokeJython extends BaseScriptTest {

    /**
     * Copies all scripts to the target directory because when they are compiled they can leave unwanted .class files.
     *
     * @throws Exception Any error encountered while testing
     */
    @Before
    public void setup() throws Exception {
        super.setupInvokeScriptProcessor();
    }

    /**
     * Tests a script that has a Jython processor that is always invalid.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testAlwaysInvalid() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new InvokeScriptedProcessor());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "python");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, "target/test/resources/jython/test_invalid.py");

        final Collection<ValidationResult> results = ((MockProcessContext) runner.getProcessContext()).validate();
        Assert.assertEquals(1L, results.size());
        Assert.assertEquals("Never valid.", results.iterator().next().getExplanation());
    }

    /**
     * Test a script that has a Jython processor that reads a value from a processor property and another from a flowfile attribute then stores both in the attributes of the flowfile being routed.
     * <p>
     * This may seem contrived but it verifies that the Jython processors properties are being considered and are able to be set and validated. It verifies the processor is able to access the property
     * values and flowfile attribute values during onTrigger. Lastly, it verifies the processor is able to route the flowfile to a relationship it specified.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testUpdateAttributeFromProcessorPropertyAndFlowFileAttribute() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new InvokeScriptedProcessor());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "python");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, "target/test/resources/jython/test_update_attribute.py");
        runner.setProperty("for-attributes", "value-1");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("for-attributes", "value-2");

        runner.assertValid();
        runner.enqueue(new byte[0], attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred("success", 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");

        // verify reading a property value
        result.get(0).assertAttributeEquals("from-property", "value-1");

        // verify reading an attribute value
        result.get(0).assertAttributeEquals("from-attribute", "value-2");
    }

    /**
     * Test a script that has a Jython processor that reads the system path as controlled by the Module Directory property then stores it in the attributes of the flowfile being routed.
     * <p>
     * This tests whether the JythonScriptEngineConfigurator successfully translates the "Module Directory"  property into Python system paths, even with strings that contain Python escape sequences
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testUpdateAttributeFromProcessorModulePaths() throws Exception {
        // Prepare a set of easily identified paths for the Module Directory property
        final String moduleDirectoryTestPrefix = "test";
        final String[] testModuleDirectoryValues = { "abc","\\a\\b\\c","\\123","\\d\"e" };
        final int numTestValues = testModuleDirectoryValues.length;
        // Prepend each module directory value with a simple prefix and an identifying number so we can identify it later.
        final List<String> testModuleDirectoryFullValues = IntStream.range(0,numTestValues)
                .boxed()
                .map(i -> String.format("%s#%s#%s",moduleDirectoryTestPrefix,i,testModuleDirectoryValues[i]))
                .collect(Collectors.toList());
        final String testModuleDirectoryCombined = String.join(",",testModuleDirectoryFullValues);

        // Run the script that captures the system path resulting from the Module Directory property
        final TestRunner runner = TestRunners.newTestRunner(new InvokeScriptedProcessor());

        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "python");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, "target/test/resources/jython/test_modules_path.py");
        runner.setProperty(ScriptingComponentUtils.MODULES, testModuleDirectoryCombined);

        final Map<String, String> attributes = new HashMap<>();

        runner.assertValid();
        runner.enqueue(new byte[0], attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred("success", 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");

        // verify successful processing of the module paths
        result.get(0).assertAttributeExists("from-path");
        final String[] effectivePaths = result.get(0).getAttribute("from-path").split(","); // Extract the comma-delimited paths from the script-produced attribute
        Assert.assertTrue(effectivePaths.length >= numTestValues); // we should have our test values, plus defaults
        // Isolate only the paths with our identified prefix
        final List<String> relevantPaths = Arrays.stream(effectivePaths).filter(path -> path.startsWith(moduleDirectoryTestPrefix)).collect(Collectors.toList());
        Assert.assertEquals(testModuleDirectoryFullValues.size(), relevantPaths.size());
        relevantPaths.forEach(path -> {
            final int resultIx = Integer.valueOf(StringUtils.substringBetween(path,"#")); // extract the index so we can relate it to the sources, despite potential mangling
            final String expectedValue = testModuleDirectoryFullValues.get(resultIx);
            Assert.assertEquals(expectedValue, path); // Ensure our path was passed through without mangling
        });
    }

    /**
     * Tests a script that has a Jython Processor that that reads the first line of text from the flowfiles content and stores the value in an attribute of the outgoing flowfile.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testReadFlowFileContentAndStoreInFlowFileAttribute() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new InvokeScriptedProcessor());
        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "python");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, "target/test/resources/jython/test_reader.py");
        // Use EL to populate MODULES property
        runner.setProperty(ScriptingComponentUtils.MODULES, "target/test/resources/${literal('JYTHON'):toLower()}");

        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred("success", 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
        result.get(0).assertAttributeEquals("from-content", "test content");
    }

    /**
     * Tests compression and decompression using two different InvokeScriptedProcessor processor instances. A string is compressed and decompressed and compared.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testCompressor() throws Exception {
        final TestRunner one = TestRunners.newTestRunner(new InvokeScriptedProcessor());
        one.setValidateExpressionUsage(false);
        one.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "python");
        one.setProperty(ScriptingComponentUtils.SCRIPT_FILE, "target/test/resources/jython/test_compress.py");
        one.setProperty(ScriptingComponentUtils.MODULES, "target/test/resources/jython");
        one.setProperty("mode", "compress");

        one.assertValid();
        one.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        one.run();

        one.assertAllFlowFilesTransferred("success", 1);
        final List<MockFlowFile> oneResult = one.getFlowFilesForRelationship("success");

        final TestRunner two = TestRunners.newTestRunner(new InvokeScriptedProcessor());
        two.setValidateExpressionUsage(false);
        two.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "python");

        two.setProperty(ScriptingComponentUtils.MODULES, "target/test/resources/jython");
        two.setProperty(ScriptingComponentUtils.SCRIPT_FILE, "target/test/resources/jython/test_compress.py");
        two.setProperty("mode", "decompress");

        two.assertValid();
        two.enqueue(oneResult.get(0));
        two.run();

        two.assertAllFlowFilesTransferred("success", 1);
        final List<MockFlowFile> twoResult = two.getFlowFilesForRelationship("success");
        Assert.assertEquals("test content", new String(twoResult.get(0).toByteArray(), StandardCharsets.UTF_8));
    }

    /**
     * Tests a script file that creates and transfers a new flow file.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testInvalidConfiguration() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "python");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, TEST_RESOURCE_LOCATION);
        runner.setProperty(ScriptingComponentUtils.SCRIPT_BODY, "body");

        runner.assertNotValid();
    }
}
