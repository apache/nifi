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

import org.apache.nifi.processors.script.ExecuteScript;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author unattributed
 *
 */
public class TestExecuteScript {

    static Logger LOG;

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.script.ExecuteScript", "trace");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.script.TestExecuteScript", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.AbstractProcessor", "debug");
        LOG = LoggerFactory.getLogger(TestExecuteScript.class);
    }

    private TestRunner controller;

    private final String multiline = "Lorem ipsum dolor sit amet,\n"
            + "consectetur adipisicing elit,\n"
            + "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.\n"
            + "Ut enim ad minim veniam,\n"
            + "quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.\n"
            + "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.\n"
            + "Excepteur sint occaecat cupidatat non proident,\n"
            + "sunt in culpa qui officia deserunt mollit anim id est laborum.";

    /**
     * Create a mock SingleProcessorController using our processor and pass data
     * to it via byte array. Returns the Sink that provides access to any files
     * that pass out of the processor
     */
    @Before
    public void setupEach() throws IOException {
        controller = TestRunners.newTestRunner(ExecuteScript.class);
        controller.setValidateExpressionUsage(false);

        // copy all scripts to target directory and run from there. some python
        // scripts create .class files that end up in src/test/resources.
        FileUtils.copyDirectory(new File("src/test/resources"), new File("target/test-scripts"));
    }

    // Fail if the specified relationship does not contain exactly one file
    // with the expected value
    private void assertRelationshipContents(String expected, String relationship) {
        controller.assertTransferCount(relationship, 1);
        MockFlowFile ff = controller.getFlowFilesForRelationship(relationship).get(0);
        ff.assertContentEquals(expected);
    }

    // Fail if the specified relationship does not contain specified number of files
    // with the expected value
    private void assertRelationshipContents(String expected, String relationship, int count) {
        controller.assertTransferCount(relationship, count);
        MockFlowFile ff = controller.getFlowFilesForRelationship(relationship).get(count - 1);
        ff.assertContentEquals(expected);
    }

    // ////////////////////////////////////
    // General tests
    @Test(expected = IllegalArgumentException.class)
    public void failOnBadName() {
        LOG.info("Supplying bad script file names");

        // None of these should result in actually setting the property, because they're non-existent / bad files
        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "not/really.rb");
        controller.assertNotValid();
        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "fakey/fake.js");
        controller.assertNotValid();
        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "pom.xml");
        controller.assertNotValid();
    }

    // ////////////////////////////////////
    // Ruby script tests
    @Test
    public void testSimpleReadR() {
        LOG.info("Ruby script: fail file based on reading contents");

        controller.enqueue("This stuff is fine".getBytes());
        controller.enqueue(multiline.getBytes());

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/readTest.rb");
        controller.setThreadCount(2);
        controller.run(2);

        assertRelationshipContents(multiline, "failure");
        assertRelationshipContents("This stuff is fine", "success");

        controller.getFlowFilesForRelationship("success").get(0).assertAttributeEquals("filename", "NewFileNameFromReadTest");
    }

    @Test
    public void testParamReadR() {
        LOG.info("Ruby script: Failing file based on reading contents");

        Map<String, String> attrs1 = new HashMap<>();
        attrs1.put("filename", "StuffIsFine.txt");
        Map<String, String> attrs2 = new HashMap<>();
        attrs2.put("filename", "multiline.txt");
        controller.enqueue("This stuff is fine".getBytes(), attrs1);
        controller.enqueue(multiline.getBytes(), attrs2);

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/readWithParams.rb");
        controller.setProperty("expr", "rehenderit");
        controller.run(2);

        assertRelationshipContents(multiline, "failure");
        assertRelationshipContents("This stuff is fine", "success");
    }

    @Test
    public void testWriteLastLineR() {
        LOG.info("Running Ruby script to output last line of file");

        controller.enqueue(multiline.getBytes());

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/writeTest.rb");
        controller.run();

        List<MockFlowFile> files = controller.getFlowFilesForRelationship("success");

        assertEquals("Process did not generate an output file", 1, files.size());

        byte[] blob = files.get(0).toByteArray();
        String[] lines = new String(blob).split("\n");

        assertEquals("File had more than one line", 1, lines.length);
        assertEquals("sunt in culpa qui officia deserunt mollit anim id est laborum.", lines[0]);
    }

    @Test
    public void testWriteOptionalParametersR() {
        LOG.info("Ruby script that uses optional parameters");

        controller.enqueue(multiline.getBytes());

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/paramTest.rb");
        controller.setProperty("repeat", "3");
        controller.run();

        List<MockFlowFile> files = controller.getFlowFilesForRelationship("success");

        assertEquals("Process did not generate an output file", 1, files.size());

        byte[] blob = files.get(0).toByteArray();
        String[] lines = new String(blob).split("\n");

        assertEquals("File did not have 3 lines", 3, lines.length);
        assertEquals("sunt in culpa qui officia deserunt mollit anim id est laborum.", lines[0]);
    }

    @Test
    public void testSetupOptionalValidationR() {
        LOG.info("Ruby script creating validators for optional properties");

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/optionalValidators.rb");
        controller.assertNotValid();
        controller.setProperty("int", "abc");
        controller.assertNotValid();
        controller.setProperty("url", "not@valid");
        controller.assertNotValid();
        controller.setProperty("nonEmpty", "");
        controller.assertNotValid();

        controller.setProperty("int", "123");
        controller.setProperty("url", "http://localhost");
        controller.setProperty("nonEmpty", "abc123");
        controller.assertValid();
    }

    @Test
    public void testTwoScriptsSameThreadSameClassName() {
        LOG.info("Test 2 different scripts with the same ruby class name");

        Map<String, String> attrs1 = new HashMap<>();
        attrs1.put("filename", "StuffIsFine.txt");
        Map<String, String> attrs2 = new HashMap<>();
        attrs2.put("filename", "multiline.txt");

        controller.enqueue("This stuff is fine".getBytes(), attrs1);
        controller.enqueue(multiline.getBytes(), attrs2);

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/readWithParams.rb");
        controller.setProperty("expr", "rehenderit");
        controller.run(2);

        assertRelationshipContents(multiline, "failure");
        assertRelationshipContents("This stuff is fine", "success");

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/optionalValidators.rb");
        controller.assertNotValid();
        controller.setProperty("int", "abc");
        controller.assertNotValid();
        controller.setProperty("url", "not@valid");
        controller.assertNotValid();
        controller.setProperty("nonEmpty", "");
        controller.assertNotValid();

        controller.setProperty("int", "123");
        controller.setProperty("url", "http://localhost");
        controller.setProperty("nonEmpty", "abc123");
        controller.assertValid();
    }

    @Test
    public void testUpdateScriptR() throws Exception {
        LOG.info("Test one script with updated class");

        File testFile = File.createTempFile("script", ".rb");
        File original = new File("target/test-scripts/readWithParams.rb");
        FileUtils.copyFile(original, testFile);
        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, testFile.getPath());
        controller.assertValid();
        original = new File("target/test-scripts/optionalValidators.rb");
        FileUtils.copyFile(original, testFile);
        controller.setProperty(ExecuteScript.SCRIPT_CHECK_INTERVAL, "5 secs");
        Thread.sleep(6000);

        controller.assertNotValid();
        controller.setProperty("int", "abc");
        controller.assertNotValid();
        controller.setProperty("url", "not@valid");
        controller.assertNotValid();
        controller.setProperty("nonEmpty", "");
        controller.assertNotValid();

        controller.setProperty("int", "123");
        controller.setProperty("url", "http://localhost");
        controller.setProperty("nonEmpty", "abc123");
        controller.assertValid();
        FileUtils.deleteQuietly(testFile);
    }

    @Test
    public void testMultiThreadExecR() {
        LOG.info("Ruby script 20 threads: Failing file based on reading contents");

        Map<String, String> attrs1 = new HashMap<>();
        attrs1.put("filename", "StuffIsFine.txt");
        Map<String, String> attrs2 = new HashMap<>();
        attrs2.put("filename", "multiline.txt");
        controller.setThreadCount(20);
        for (int i = 0; i < 10; i++) {
            controller.enqueue("This stuff is fine".getBytes(), attrs1);
            controller.enqueue(multiline.getBytes(), attrs2);
        }

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/readWithParams.rb");
        controller.setProperty("expr", "rehenderit");
        controller.run(20);

        controller.assertTransferCount("failure", 10);
        controller.assertTransferCount("success", 10);
        for (int i = 0; i < 10; i++) {
            MockFlowFile ff = controller.getFlowFilesForRelationship("failure").get(i);
            ff.assertContentEquals(multiline);
            assertTrue(ff.getAttribute("filename").endsWith("modified"));
            ff = controller.getFlowFilesForRelationship("success").get(i);
            ff.assertContentEquals("This stuff is fine");
            assertTrue(ff.getAttribute("filename").endsWith("modified"));
        }

    }

    @Test
    public void testManualValidationR() {
        LOG.info("Ruby script defining manual validator");

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/alwaysFail.rb");
        controller.assertNotValid();
    }

    @Test
    public void testGetRelationshipsR() {
        LOG.info("Ruby script: getRelationships");
        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/routeTest.rb");
        // at this point, the script has not been instantiated so the processor simply returns an empty set
        Set<Relationship> rels = controller.getProcessor().getRelationships();
        assertEquals(0, rels.size());
        // this will instantiate the script
        controller.assertValid();
        // this will call the script
        rels = controller.getProcessor().getRelationships();
        assertEquals(3, rels.size());
    }

    @Test
    public void testGetExceptionRouteR() {
        LOG.info("Ruby script defining route taken in event of exception");

        controller.enqueue("This stuff is fine".getBytes());
        controller.enqueue("Bad things go to 'b'.".getBytes());
        controller.enqueue(multiline.getBytes());

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/routeTest.rb");

        controller.run(3);

        assertRelationshipContents("This stuff is fine", "a");
        assertRelationshipContents("Bad things go to 'b'.", "b");
        assertRelationshipContents(multiline, "c");

    }

    @Test
    public void testSimpleConverterR() {
        LOG.info("Running Ruby converter script");

        for (int i = 0; i < 20; i++) {
            controller.enqueue(multiline.getBytes());
        }

        controller.setThreadCount(20);
        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/simpleConverter.rb");
        controller.run(20);

        List<MockFlowFile> successFiles = controller.getFlowFilesForRelationship("success");
        List<MockFlowFile> failFiles = controller.getFlowFilesForRelationship("failure");

        assertEquals("Process did not generate 20 SUCCESS files", 20, successFiles.size());
        assertEquals("Process did not generate 20 FAILURE files", 20, failFiles.size());

        MockFlowFile sFile = successFiles.get(19);
        MockFlowFile fFile = failFiles.get(19);

        byte[] blob = fFile.toByteArray();
        String[] lines = new String(blob).split("\n");

        assertEquals("File had more than one line", 1, lines.length);
        assertEquals("Lorem ipsum dolor sit amet,", lines[0]);

        blob = sFile.toByteArray();
        lines = new String(blob).split("\n");

        assertEquals("SUCCESS had wrong number of lines", 7, lines.length);
        assertEquals("consectetur adipisicing elit,", lines[0]);
    }

    @Test
    public void testLoadLocalR() {
        LOG.info("Ruby: load another script file");

        controller.enqueue("This stuff is fine".getBytes());
        controller.enqueue(multiline.getBytes());

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/loadLocal.rb");
        controller.run(2);

        assertRelationshipContents(multiline, "failure");
        assertRelationshipContents("This stuff is fine", "success");
    }

    @Test
    public void testFlowFileR() {
        LOG.info("Ruby: get FlowFile properties");

        controller.enqueue(multiline.getBytes());
        HashMap<String, String> meta = new HashMap<String, String>();
        meta.put("evict", "yup");
        controller.enqueue("This would be plenty long but it's also evicted.".getBytes(), meta);
        controller.enqueue("This is too short".getBytes());

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/ffTest.rb");
        controller.run(3);

        assertRelationshipContents(multiline, "success");
        assertRelationshipContents("This is too short", "failure");
        assertRelationshipContents("This would be plenty long but it's also evicted.", "evict");
    }

    // //////////////////////////////////// // JS tests
    @Test
    public void testSimpleReadJS() {
        LOG.info("Javascript: fail file based on reading contents");

        controller.enqueue("This stuff is fine".getBytes());
        controller.enqueue(multiline.getBytes());

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/readTest.js");
        controller.run(2);

        assertRelationshipContents(multiline, "failure");
        assertRelationshipContents("This stuff is fine", "success");
    }

    @Test
    public void testParamReadJS() {
        LOG.info("Javascript: read contents and fail based on parameter");

        controller.enqueue("This stuff is fine".getBytes());
        controller.enqueue(multiline.getBytes());

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/readWithParams.js");
        controller.setProperty("expr", "sed do");
        controller.run(2);

        assertRelationshipContents(multiline, "failure");
        assertRelationshipContents("This stuff is fine", "success");
    }

    @Test
    public void testWriteLastLineJS() {
        LOG.info("Running Javascript to output last line of file");

        controller.enqueue(multiline.getBytes());

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/writeTest.js");
        controller.run();

        List<MockFlowFile> sunkFiles = controller.getFlowFilesForRelationship("success");

        assertEquals("Process did not generate an output file", 1, sunkFiles.size());

        MockFlowFile sunkFile = sunkFiles.iterator().next();
        byte[] blob = sunkFile.toByteArray();
        String[] lines = new String(blob).split("\n");

        assertEquals("File had more than one line", 1, lines.length);
        assertEquals("sunt in culpa qui officia deserunt mollit anim id est laborum.", lines[0]);
    }

    @Test
    public void testWriteOptionalParametersJS() {
        LOG.info("Javascript processCallback that uses optional parameters");

        controller.enqueue(multiline.getBytes());

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/paramTest.js");
        controller.setProperty("repeat", "3");
        controller.run();

        List<MockFlowFile> sunkFiles = controller.getFlowFilesForRelationship("success");

        assertEquals("Process did not generate an output file", 1, sunkFiles.size());

        MockFlowFile sunkFile = sunkFiles.iterator().next();
        byte[] blob = sunkFile.toByteArray();
        String[] lines = new String(blob).split("\n");

        assertEquals("File did not have 3 lines", 3, lines.length);
        assertEquals("sunt in culpa qui officia deserunt mollit anim id est laborum.", lines[0]);
    }

    @Test
    public void testSetupOptionalValidationJS() {
        LOG.info("Javascript creating validators for optional properties");

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/optionalValidators.js");
        controller.setProperty("int", "abc");
        controller.setProperty("url", "not@valid");
        controller.setProperty("nonEmpty", "");
        assertEquals(2, controller.getProcessor().getPropertyDescriptors().size());
        controller.assertNotValid(); // due to invalid values above
        assertEquals(5, controller.getProcessor().getPropertyDescriptors().size());

        controller.setProperty("int", "123");
        controller.setProperty("url", "http://localhost");
        controller.setProperty("nonEmpty", "abc123");
        assertEquals(5, controller.getProcessor().getPropertyDescriptors().size());
        controller.assertValid();
    }

    @Test
    public void testManualValidationJS() {
        LOG.info("Javascript defining manual validator");

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/alwaysFail.js");
        controller.assertNotValid();
    }

    @Test
    public void testGetExceptionRouteJS() {
        LOG.info("Javascript defining route taken in event of exception");

        controller.enqueue("This stuff is fine".getBytes());
        controller.enqueue("Bad things go to 'b'.".getBytes());
        controller.enqueue(multiline.getBytes());

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/routeTest.js");

        controller.run(3);

        assertRelationshipContents("This stuff is fine", "a");
        assertRelationshipContents("Bad things go to 'b'.", "b");
        assertRelationshipContents(multiline, "c");

    }

    @Test
    public void testSimpleConverterJS() {
        LOG.info("Running Javascript converter script");

        for (int i = 0; i < 20; i++) {
            controller.enqueue(multiline.getBytes());
        }

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/simpleConverter.js");
        controller.run(20);

        List<MockFlowFile> successFiles = controller.getFlowFilesForRelationship("success");
        List<MockFlowFile> failFiles = controller.getFlowFilesForRelationship("failure");

        assertEquals("Process did not generate 20 SUCCESS files", 20, successFiles.size());
        assertEquals("Process did not generate 20 FAILURE file", 20, failFiles.size());

        MockFlowFile sFile = successFiles.get(19);
        MockFlowFile fFile = failFiles.get(0);

        byte[] blob = sFile.toByteArray();
        String[] lines = new String(blob).split("\n");

        assertEquals("SUCCESS had wrong number of lines", 7, lines.length);
        assertTrue(lines[0].startsWith("consectetur adipisicing elit,"));

        blob = fFile.toByteArray();
        lines = new String(blob).split("\n");

        assertEquals("File had more than one line", 1, lines.length);
        assertTrue(lines[0].startsWith("Lorem ipsum dolor sit amet,"));
    }

    @Test
    public void testLoadLocalJS() {
        LOG.info("Javascript: load another script file");

        controller.enqueue("This stuff is fine".getBytes());
        controller.enqueue(multiline.getBytes());

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/loadLocal.js");
        controller.run(2);

        assertRelationshipContents(multiline, "failure");
        assertRelationshipContents("This stuff is fine", "success");
    }

    @Test
    public void testXMLJS() {
        LOG.info("Javascript: native XML parser");

        controller.enqueue("<a><b foo='bar'>Bad</b><b good='true'>Good</b><b good='false'>Bad</b></a>".getBytes());
        controller.enqueue("<a><b>Hello</b><b>world</b></a>".getBytes());

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/parseXml.js");
        controller.run(2);

        assertRelationshipContents("Good", "success");
        assertRelationshipContents("<a><b>Hello</b><b>world</b></a>", "failure");
    }

    @Test
    public void testFlowFileJS() {
        LOG.info("JavaScript: get FlowFile properties");

        controller.enqueue("This is too short".getBytes());
        controller.enqueue(multiline.getBytes());

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/ffTest.js");
        controller.run(2);

        assertRelationshipContents(multiline, "success");
        assertRelationshipContents("This is too short", "failure");
    }

    @Test
    public void testMultiThreadExecJS() {
        LOG.info("JavaScript script 20 threads: Failing file based on reading contents");

        Map<String, String> attrs1 = new HashMap<>();
        attrs1.put("filename", "StuffIsFine.txt");
        Map<String, String> attrs2 = new HashMap<>();
        attrs2.put("filename", "multiline.txt");
        controller.setThreadCount(20);
        for (int i = 0; i < 10; i++) {
            controller.enqueue("This stuff is fine".getBytes(), attrs1);
            controller.enqueue(multiline.getBytes(), attrs2);
        }

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/readWithParams.js");
        controller.setProperty("expr", "rehenderit");
        controller.run(20);

        controller.assertTransferCount("failure", 10);
        controller.assertTransferCount("success", 10);
        for (int i = 0; i < 10; i++) {
            MockFlowFile ff = controller.getFlowFilesForRelationship("failure").get(i);
            ff.assertContentEquals(multiline);
            assertTrue(ff.getAttribute("filename").endsWith("modified"));
            ff = controller.getFlowFilesForRelationship("success").get(i);
            ff.assertContentEquals("This stuff is fine");
            assertTrue(ff.getAttribute("filename").endsWith("modified"));
        }
    }

    @Test
    public void testUpdateScriptJS() throws Exception {
        LOG.info("Test one script with updated class");

        File testFile = File.createTempFile("script", ".js");
        File original = new File("target/test-scripts/readWithParams.js");
        FileUtils.copyFile(original, testFile);
        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, testFile.getPath());
        controller.assertValid();
        original = new File("target/test-scripts/optionalValidators.js");
        FileUtils.copyFile(original, testFile);
        controller.setProperty(ExecuteScript.SCRIPT_CHECK_INTERVAL, "5 secs");
        Thread.sleep(6000);

        controller.assertNotValid();
        controller.setProperty("int", "abc");
        controller.assertNotValid();
        controller.setProperty("url", "not@valid");
        controller.assertNotValid();
        controller.setProperty("nonEmpty", "");
        controller.assertNotValid();

        controller.setProperty("int", "123");
        controller.setProperty("url", "http://localhost");
        controller.setProperty("nonEmpty", "abc123");
        controller.assertValid();
        FileUtils.deleteQuietly(testFile);
    }

    // ////////////////////////////////// // Python script tests
    @Test
    public void testSimpleReadP() {
        LOG.info("Python script: fail file based on reading contents");

        for (int i = 0; i < 20; i++) {
            Map<String, String> attr1 = new HashMap<>();
            attr1.put("filename", "FineStuff");
            attr1.put("counter", Integer.toString(i));
            Map<String, String> attr2 = new HashMap<>();
            attr2.put("filename", "MultiLine");
            attr2.put("counter", Integer.toString(i));
            controller.enqueue("This stuff is fine".getBytes(), attr1);
            controller.enqueue(multiline.getBytes(), attr2);
        }

        controller.setThreadCount(40);
        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/readTest.py");
        controller.run(40);

        assertRelationshipContents(multiline, "failure", 20);
        assertRelationshipContents("This stuff is fine", "success", 20);

        List<MockFlowFile> fails = controller.getFlowFilesForRelationship("failure");
        List<MockFlowFile> successes = controller.getFlowFilesForRelationship("success");
        for (int i = 0; i < 20; i++) {
            assertTrue(fails.get(i).getAttribute("filename").matches("^.*\\d+$"));
            assertTrue(successes.get(i).getAttribute("filename").matches("^.*\\d+$"));
        }
    }

    @Test
    public void testParamReadP() {
        LOG.info("Python script: read contents and fail based on parameter");

        controller.enqueue("This stuff is fine".getBytes());
        controller.enqueue(multiline.getBytes());

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/readWithParams.py");
        controller.setProperty("expr", "sed do");
        controller.run(2);

        assertRelationshipContents(multiline, "failure");
        assertRelationshipContents("This stuff is fine", "success");
    }

    @Test
    public void testWriteLastLineP() {
        LOG.info("Running Python script to output last line of file");

        controller.enqueue(multiline.getBytes());

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/writeTest.py");
        controller.run();

        List<MockFlowFile> sunkFiles = controller.getFlowFilesForRelationship("success");

        assertEquals("Process did not generate an output file", 1, sunkFiles.size());

        MockFlowFile sunkFile = sunkFiles.iterator().next();
        byte[] blob = sunkFile.toByteArray();
        String[] lines = new String(blob).split("\n");

        assertEquals("File had more than one line", 1, lines.length);
        assertEquals("sunt in culpa qui officia deserunt mollit anim id est laborum.", lines[0]);
    }

    @Test
    public void testWriteOptionalParametersP() {
        LOG.info("Python script processCallback that uses optional parameters");

        controller.enqueue(multiline.getBytes());

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/paramTest.py");
        controller.setProperty("repeat", "3");
        controller.run();

        List<MockFlowFile> sunkFiles = controller.getFlowFilesForRelationship("success");

        assertEquals("Process did not generate an output file", 1, sunkFiles.size());

        MockFlowFile sunkFile = sunkFiles.iterator().next();
        byte[] blob = sunkFile.toByteArray();
        String[] lines = new String(blob).split("\n");

        assertEquals("File did not have 3 lines", 3, lines.length);
        assertTrue(lines[2].startsWith("sunt in culpa qui officia deserunt mollit anim id est laborum."));
    }

    @Test
    public void testManualValidationP() {
        LOG.info("Python defining manual validator");

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/alwaysFail.py");
        controller.assertNotValid();
    }

    @Test
    public void testSetupOptionalValidationP() {
        LOG.info("Python script creating validators for optional properties");

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/optionalValidators.py");
        controller.setProperty("int", "abc");
        controller.setProperty("url", "not@valid");
        controller.setProperty("nonEmpty", "");
        assertEquals(2, controller.getProcessor().getPropertyDescriptors().size());
        controller.assertNotValid();

        controller.setProperty("int", "123");
        controller.setProperty("url", "http://localhost");
        controller.setProperty("nonEmpty", "abc123");
        assertEquals(5, controller.getProcessor().getPropertyDescriptors().size());
        controller.assertValid();
    }

    @Test
    public void testGetExceptionRouteP() {
        LOG.info("Python script defining route taken in event of exception");

        controller.enqueue("This stuff is fine".getBytes());
        controller.enqueue("Bad things go to 'b'.".getBytes());
        controller.enqueue(multiline.getBytes());

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/routeTest.py");

        // Don't put the error in the logs
        // TestableAppender ta = new TestableAppender();
        // ta.attach(Logger.getLogger(ExecuteScript.class));
        controller.run(3);
        // ta.detach();

        assertRelationshipContents("This stuff is fine", "a");
        assertRelationshipContents("Bad things go to 'b'.", "b");
        assertRelationshipContents(multiline, "c");

        // ta.assertFound("threw exception");
    }

    @Test
    public void testLoadLocalP() throws Exception {

        final Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    testGetExceptionRouteP();
                    setupEach();
                } catch (Exception e) {

                }
            }
        });

        t.start();
        t.join();

        LOG.info("Python: load another script file");

        controller.enqueue("This stuff is fine".getBytes());
        controller.enqueue(multiline.getBytes());

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/loadLocal.py");
        controller.run(2);

        assertRelationshipContents(multiline, "failure");
        assertRelationshipContents("This stuff is fine", "success");
    }

    @Test
    public void testSimpleConverterP() {
        LOG.info("Running Python converter script");

        for (int i = 0; i < 20; i++) {
            controller.enqueue(multiline.getBytes());
        }

        controller.setThreadCount(20);
        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/simpleConverter.py");
        controller.run(20);

        List<MockFlowFile> successFiles = controller.getFlowFilesForRelationship("success");
        List<MockFlowFile> failFiles = controller.getFlowFilesForRelationship("failure");

        assertEquals("Process did not generate 20 SUCCESS files", 20, successFiles.size());
        assertEquals("Process did not generate 20 FAILURE files", 20, failFiles.size());

        MockFlowFile sFile = successFiles.iterator().next();
        MockFlowFile fFile = failFiles.iterator().next();

        byte[] blob = sFile.toByteArray();
        String[] lines = new String(blob).split("\n");

        assertEquals("SUCCESS had wrong number of lines", 7, lines.length);
        assertTrue(lines[0].startsWith("consectetur adipisicing elit,"));

        blob = fFile.toByteArray();
        lines = new String(blob).split("\n");

        assertEquals("File had more than one line", 1, lines.length);
        assertTrue(lines[0].startsWith("Lorem ipsum dolor sit amet,"));
    }

    @Test
    public void testFlowFileP() {
        LOG.info("Python: get FlowFile properties");

        controller.enqueue("This is too short".getBytes());
        controller.enqueue(multiline.getBytes());

        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/ffTest.py");
        controller.run(2);

        assertRelationshipContents(multiline, "success");
        assertRelationshipContents("This is too short", "failure");
    }

    @Test
    public void testMultiThreadExecP() {
        LOG.info("Pthon script 20 threads: Failing file based on reading contents");

        Map<String, String> attrs1 = new HashMap<>();
        attrs1.put("filename", "StuffIsFine.txt");
        Map<String, String> attrs2 = new HashMap<>();
        attrs2.put("filename", "multiline.txt");
        for (int i = 0; i < 10; i++) {
            controller.enqueue("This stuff is fine".getBytes(), attrs1);
            controller.enqueue(multiline.getBytes(), attrs2);
        }

        controller.setThreadCount(20);
        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, "target/test-scripts/readWithParams.py");
        controller.setProperty("expr", "sed do");
        controller.run(20);

        controller.assertTransferCount("failure", 10);
        controller.assertTransferCount("success", 10);
        for (int i = 0; i < 10; i++) {
            MockFlowFile ff = controller.getFlowFilesForRelationship("failure").get(i);
            ff.assertContentEquals(multiline);
            assertTrue(ff.getAttribute("filename").endsWith("modified"));
            ff = controller.getFlowFilesForRelationship("success").get(i);
            ff.assertContentEquals("This stuff is fine");
            assertTrue(ff.getAttribute("filename").endsWith("modified"));
        }
    }

    @Test
    public void testUpdateScriptP() throws Exception {
        LOG.info("Test one script with updated class");

        File testFile = File.createTempFile("script", ".py");
        File original = new File("target/test-scripts/readTest.py");
        FileUtils.copyFile(original, testFile);
        controller.setProperty(ExecuteScript.SCRIPT_FILE_NAME, testFile.getPath());
        controller.assertValid();
        original = new File("target/test-scripts/readWithParams.py");
        FileUtils.copyFile(original, testFile);
        controller.setProperty(ExecuteScript.SCRIPT_CHECK_INTERVAL, "5 secs");
        Thread.sleep(6000);

        controller.assertNotValid(); // need to set 'expr'
        controller.setProperty("int", "abc");
        controller.assertNotValid();
        controller.setProperty("url", "not@valid");
        controller.assertNotValid();
        controller.setProperty("nonEmpty", "");
        controller.assertNotValid();

        controller.setProperty("expr", "sed do");
        controller.assertValid();
        assertEquals(6, controller.getProcessContext().getProperties().size());
        FileUtils.deleteQuietly(testFile);
    }

}
