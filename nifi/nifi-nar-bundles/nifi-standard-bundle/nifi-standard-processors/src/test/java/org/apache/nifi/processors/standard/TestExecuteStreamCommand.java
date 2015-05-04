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
package org.apache.nifi.processors.standard;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TestExecuteStreamCommand {

    private static Logger LOGGER;

    @BeforeClass
    public static void init() {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.ExecuteStreamCommand", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.TestExecuteStreamCommand", "debug");
        LOGGER = LoggerFactory.getLogger(TestExecuteStreamCommand.class);
    }

    @Test
    public void testExecuteJar() throws Exception {
        File exJar = new File("src/test/resources/ExecuteCommand/TestSuccess.jar");
        File dummy = new File("src/test/resources/ExecuteCommand/1000bytes.txt");
        String jarPath = exJar.getAbsolutePath();
        exJar.setExecutable(true);
        final TestRunner controller = TestRunners.newTestRunner(ExecuteStreamCommand.class);
        controller.setValidateExpressionUsage(false);
        controller.enqueue(dummy.toPath());
        controller.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, "java");
        controller.setProperty(ExecuteStreamCommand.EXECUTION_ARGUMENTS, "-jar;" + jarPath);
        controller.run(1);
        controller.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        controller.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 1);

        List<MockFlowFile> flowFiles = controller.getFlowFilesForRelationship(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP);
        MockFlowFile outputFlowFile = flowFiles.get(0);
        byte[] byteArray = outputFlowFile.toByteArray();
        String result = new String(byteArray);
        assertTrue("Test was a success\r\n".equals(result) || "Test was a success\n".equals(result));
        assertEquals("0", outputFlowFile.getAttribute("execution.status"));
        assertEquals("java", outputFlowFile.getAttribute("execution.command"));
        assertEquals("-jar;", outputFlowFile.getAttribute("execution.command.args").substring(0, 5));
        String attribute = outputFlowFile.getAttribute("execution.command.args");
        String expected = "src" + File.separator + "test" + File.separator + "resources" + File.separator + "ExecuteCommand" + File.separator + "TestSuccess.jar";
        assertEquals(expected, attribute.substring(attribute.length() - expected.length()));

        MockFlowFile originalFlowFile = controller.getFlowFilesForRelationship(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP).get(0);
        assertEquals(outputFlowFile.getAttribute("execution.status"), originalFlowFile.getAttribute("execution.status"));
        assertEquals(outputFlowFile.getAttribute("execution.command"), originalFlowFile.getAttribute("execution.command"));
        assertEquals(outputFlowFile.getAttribute("execution.command.args"), originalFlowFile.getAttribute("execution.command.args"));
    }

    @Test
    public void testExecuteJarWithBadPath() throws Exception {
        File exJar = new File("src/test/resources/ExecuteCommand/noSuchFile.jar");
        File dummy = new File("src/test/resources/ExecuteCommand/1000bytes.txt");
        String jarPath = exJar.getAbsolutePath();
        exJar.setExecutable(true);
        final TestRunner controller = TestRunners.newTestRunner(ExecuteStreamCommand.class);
        controller.setValidateExpressionUsage(false);
        controller.enqueue(dummy.toPath());
        controller.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, "java");
        controller.setProperty(ExecuteStreamCommand.EXECUTION_ARGUMENTS, "-jar;" + jarPath);
        controller.run(1);
        controller.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        controller.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 1);
        List<MockFlowFile> flowFiles = controller.getFlowFilesForRelationship(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP);
        assertEquals(0, flowFiles.get(0).getSize());
        assertEquals("Error: Unable to access jarfile", flowFiles.get(0).getAttribute("execution.error").substring(0, 31));
    }

    @Test
    public void testExecuteIngestAndUpdate() throws IOException {
        File exJar = new File("src/test/resources/ExecuteCommand/TestIngestAndUpdate.jar");
        File dummy = new File("src/test/resources/ExecuteCommand/1000bytes.txt");
        File dummy100MBytes = new File("target/100MB.txt");
        FileInputStream fis = new FileInputStream(dummy);
        FileOutputStream fos = new FileOutputStream(dummy100MBytes);
        byte[] bytes = new byte[1024];
        assertEquals(1000, fis.read(bytes));
        fis.close();
        for (int i = 0; i < 100000; i++) {
            fos.write(bytes, 0, 1000);
        }
        fos.close();
        String jarPath = exJar.getAbsolutePath();
        exJar.setExecutable(true);
        final TestRunner controller = TestRunners.newTestRunner(ExecuteStreamCommand.class);
        controller.setValidateExpressionUsage(false);
        controller.enqueue(dummy100MBytes.toPath());
        controller.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, "java");
        controller.setProperty(ExecuteStreamCommand.EXECUTION_ARGUMENTS, "-jar;" + jarPath);
        controller.run(1);
        controller.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        controller.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 1);
        List<MockFlowFile> flowFiles = controller.getFlowFilesForRelationship(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP);
        byte[] byteArray = flowFiles.get(0).toByteArray();
        String result = new String(byteArray);

        assertTrue(result.contains(File.separator + "nifi-standard-processors:ModifiedResult\r\n")
          || result.contains(File.separator + "nifi-standard-processors:ModifiedResult\n"));
    }

    @Test
    public void testExecuteIngestAndUpdateWithWorkingDir() throws IOException {
        File exJar = new File("src/test/resources/ExecuteCommand/TestIngestAndUpdate.jar");
        File dummy = new File("src/test/resources/ExecuteCommand/1000bytes.txt");
        String jarPath = exJar.getAbsolutePath();
        exJar.setExecutable(true);
        final TestRunner controller = TestRunners.newTestRunner(ExecuteStreamCommand.class);
        controller.setValidateExpressionUsage(false);
        controller.enqueue(dummy.toPath());
        controller.setProperty(ExecuteStreamCommand.WORKING_DIR, "target");
        controller.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, "java");
        controller.setProperty(ExecuteStreamCommand.EXECUTION_ARGUMENTS, "-jar;" + jarPath);
        controller.run(1);
        controller.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        controller.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 1);
        List<MockFlowFile> flowFiles = controller.getFlowFilesForRelationship(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP);
        byte[] byteArray = flowFiles.get(0).toByteArray();
        String result = new String(byteArray);
        assertTrue(result.contains(File.separator + "nifi-standard-processors" + File.separator + "target:ModifiedResult\r\n")
          || result.contains(File.separator + "nifi-standard-processors" + File.separator + "target:ModifiedResult\n"));
    }

    @Test
    public void testIgnoredStdin() throws IOException {
        File exJar = new File("src/test/resources/ExecuteCommand/TestIngestAndUpdate.jar");
        File dummy = new File("src/test/resources/ExecuteCommand/1000bytes.txt");
        String jarPath = exJar.getAbsolutePath();
        exJar.setExecutable(true);
        final TestRunner controller = TestRunners.newTestRunner(ExecuteStreamCommand.class);
        controller.setValidateExpressionUsage(false);
        controller.enqueue(dummy.toPath());
        controller.setProperty(ExecuteStreamCommand.WORKING_DIR, "target");
        controller.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, "java");
        controller.setProperty(ExecuteStreamCommand.EXECUTION_ARGUMENTS, "-jar;" + jarPath);
        controller.setProperty(ExecuteStreamCommand.IGNORE_STDIN, "true");
        controller.run(1);
        controller.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        controller.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 1);
        List<MockFlowFile> flowFiles = controller.getFlowFilesForRelationship(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP);
        byte[] byteArray = flowFiles.get(0).toByteArray();
        String result = new String(byteArray);
        assertTrue("TestIngestAndUpdate.jar should not have received anything to modify",
          result.endsWith("target:ModifiedResult\n"));
    }

    // this is dependent on window with cygwin...so it's not enabled
    @Ignore
    @Test
    public void testExecuteTouch() throws Exception {
        File testFile = new File("target/test.txt");
        testFile.delete();
        File dummy = new File("src/test/resources/ExecuteCommand/1000bytes.txt");
        final TestRunner controller = TestRunners.newTestRunner(ExecuteStreamCommand.class);
        controller.setValidateExpressionUsage(false);
        controller.enqueue(dummy.toPath());
        controller.enqueue(dummy.toPath());
        controller.enqueue(dummy.toPath());
        controller.enqueue(dummy.toPath());
        controller.enqueue(dummy.toPath());
        controller.setProperty(ExecuteStreamCommand.WORKING_DIR, "target/xx1");
        controller.setThreadCount(6);
        controller.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, "c:\\cygwin\\bin\\touch");
        controller.setProperty(ExecuteStreamCommand.EXECUTION_ARGUMENTS, "test.txt");
        controller.assertValid();
        controller.run(6);
        List<MockFlowFile> flowFiles = controller.getFlowFilesForRelationship(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP);
        assertEquals(5, flowFiles.size());
        assertEquals(0, flowFiles.get(0).getSize());

    }

    @Test
    public void testDynamicEnvironment() throws Exception {
        File exJar = new File("src/test/resources/ExecuteCommand/TestDynamicEnvironment.jar");
        File dummy = new File("src/test/resources/ExecuteCommand/1000bytes.txt");
        String jarPath = exJar.getAbsolutePath();
        exJar.setExecutable(true);
        final TestRunner controller = TestRunners.newTestRunner(ExecuteStreamCommand.class);
        controller.setProperty("NIFI_TEST_1", "testvalue1");
        controller.setProperty("NIFI_TEST_2", "testvalue2");
        controller.setValidateExpressionUsage(false);
        controller.enqueue(dummy.toPath());
        controller.setProperty(ExecuteStreamCommand.WORKING_DIR, "target");
        controller.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, "java");
        controller.setProperty(ExecuteStreamCommand.EXECUTION_ARGUMENTS, "-jar;" + jarPath);
        controller.run(1);
        controller.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        controller.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 1);
        List<MockFlowFile> flowFiles = controller.getFlowFilesForRelationship(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP);
        byte[] byteArray = flowFiles.get(0).toByteArray();
        String result = new String(byteArray);
        String[] dynamicEnvironment = result.split("\n");
        assertEquals("Should contain two environment variables starting with NIFI", 2, dynamicEnvironment.length);
        assertEquals("NIFI_TEST_2 environment variable is missing", "NIFI_TEST_2=testvalue2", dynamicEnvironment[0]);
        assertEquals("NIFI_TEST_1 environment variable is missing", "NIFI_TEST_1=testvalue1", dynamicEnvironment[1]);
    }
}
