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

import java.io.File;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.standard.util.ArgumentUtils;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestExecuteProcess {

    @Test
    public void testSplitArgs() {
        final List<String> nullArgs = ArgumentUtils.splitArgs(null, ' ');
        assertNotNull(nullArgs);
        assertTrue(nullArgs.isEmpty());

        final List<String> zeroArgs = ArgumentUtils.splitArgs("  ", ' ');
        assertNotNull(zeroArgs);
        assertEquals(3, zeroArgs.size());
        String[] expectedArray = {"", "", ""};
        assertArrayEquals(expectedArray, zeroArgs.toArray(new String[0]));

        final List<String> singleArg = ArgumentUtils.splitArgs("    hello   ", ';');
        assertEquals(1, singleArg.size());
        assertEquals("    hello   ", singleArg.get(0));

        final List<String> twoArg = ArgumentUtils.splitArgs("   hello ;   good-bye   ", ';');
        assertEquals(2, twoArg.size());
        assertEquals("   hello ", twoArg.get(0));
        assertEquals("   good-bye   ", twoArg.get(1));

        final List<String> oneUnnecessarilyQuotedArg = ArgumentUtils.splitArgs("  \"hello\" ", ';');
        assertEquals(1, oneUnnecessarilyQuotedArg.size());
        assertEquals("  hello ", oneUnnecessarilyQuotedArg.get(0));

        final List<String> twoQuotedArg = ArgumentUtils.splitArgs("\"   hello\" \"good   bye\"", ' ');
        assertEquals(2, twoQuotedArg.size());
        assertEquals("   hello", twoQuotedArg.get(0));
        assertEquals("good   bye", twoQuotedArg.get(1));

        final List<String> twoArgOneQuotedPerDelimiterArg = ArgumentUtils.splitArgs("one;two;three\";\"and\";\"half\"", ';');
        assertEquals(3, twoArgOneQuotedPerDelimiterArg.size());
        assertEquals("one", twoArgOneQuotedPerDelimiterArg.get(0));
        assertEquals("two", twoArgOneQuotedPerDelimiterArg.get(1));
        assertEquals("three;and;half", twoArgOneQuotedPerDelimiterArg.get(2));

        final List<String> twoArgOneWholeQuotedArgOneEmptyArg = ArgumentUtils.splitArgs("one;two;\"three;and;half\";", ';');
        assertEquals(4, twoArgOneWholeQuotedArgOneEmptyArg.size());
        assertEquals("one", twoArgOneWholeQuotedArgOneEmptyArg.get(0));
        assertEquals("two", twoArgOneWholeQuotedArgOneEmptyArg.get(1));
        assertEquals("three;and;half", twoArgOneWholeQuotedArgOneEmptyArg.get(2));
        assertEquals("", twoArgOneWholeQuotedArgOneEmptyArg.get(3));
    }

    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testEcho() {
        final TestRunner runner = TestRunners.newTestRunner(ExecuteProcess.class);
        runner.setProperty(ExecuteProcess.COMMAND, "echo");
        runner.setProperty(ExecuteProcess.COMMAND_ARGUMENTS, "test-args");
        runner.setProperty(ExecuteProcess.BATCH_DURATION, "500 millis");
        runner.setProperty(ExecuteProcess.MIME_TYPE, "application/json");

        runner.run();

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteProcess.REL_SUCCESS);
        for (final MockFlowFile flowFile : flowFiles) {
            flowFile.assertAttributeNotExists(CoreAttributes.MIME_TYPE.key());
        }
    }

    @Test
    public void validateProcessInterruptOnStop() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(ExecuteProcess.class);
        runner.setEnvironmentVariableValue("command", "ping");
        runner.setProperty(ExecuteProcess.COMMAND, "${command}");
        runner.setProperty(ExecuteProcess.COMMAND_ARGUMENTS, "nifi.apache.org");
        runner.setProperty(ExecuteProcess.BATCH_DURATION, "500 millis");

        runner.run();
        Thread.sleep(500);
        ExecuteProcess processor = (ExecuteProcess) runner.getProcessor();
        Field executorF = ExecuteProcess.class.getDeclaredField("executor");
        executorF.setAccessible(true);
        ExecutorService executor = (ExecutorService) executorF.get(processor);
        assertTrue(executor.isShutdown());
        assertTrue(executor.isTerminated());

        Field processF = ExecuteProcess.class.getDeclaredField("externalProcess");
        processF.setAccessible(true);
        Process process = (Process) processF.get(processor);
        assertFalse(process.isAlive());

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteProcess.REL_SUCCESS);
        if (!flowFiles.isEmpty()) {
            assertEquals("ping", flowFiles.get(0).getAttribute("command"));
        }
    }

    // @Test
    public void testBigBinaryInputData() {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi", "TRACE");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.processors.standard", "DEBUG");

        String workingDirName = "/var/test";
        String testFile = "eclipse-java-luna-SR2-win32.zip";

        final TestRunner runner = TestRunners.newTestRunner(ExecuteProcess.class);
        runner.setProperty(ExecuteProcess.COMMAND, "cmd");
        runner.setProperty(ExecuteProcess.COMMAND_ARGUMENTS, " /c type " + testFile);
        runner.setProperty(ExecuteProcess.WORKING_DIR, workingDirName);

        File inFile = new File(workingDirName, testFile);

        runner.run();

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteProcess.REL_SUCCESS);
        long totalFlowFilesSize = 0;
        for (final MockFlowFile flowFile : flowFiles) {
            totalFlowFilesSize += flowFile.getSize();
        }

        assertEquals(inFile.length(), totalFlowFilesSize);
    }

    @Test
    public void testBigInputSplit() {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi", "TRACE");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.processors.standard", "DEBUG");

        String workingDirName = "/var/test";
        String testFile = "Novo_dicionário_da_língua_portuguesa_by_Cândido_de_Figueiredo.txt";
        // String testFile = "eclipse-java-luna-SR2-win32.zip";

        final TestRunner runner = TestRunners.newTestRunner(ExecuteProcess.class);
        runner.setProperty(ExecuteProcess.COMMAND, "cmd");
        runner.setProperty(ExecuteProcess.COMMAND_ARGUMENTS, " /c type " + testFile);
        runner.setProperty(ExecuteProcess.WORKING_DIR, workingDirName);
        runner.setProperty(ExecuteProcess.BATCH_DURATION, "150 millis");

        File inFile = new File(workingDirName, testFile);

        ProcessContext processContext = runner.getProcessContext();

        ExecuteProcess processor = (ExecuteProcess) runner.getProcessor();
        processor.updateScheduledTrue();
        processor.setupExecutor(processContext);

        processor.onTrigger(processContext, runner.getProcessSessionFactory());
        processor.onTrigger(processContext, runner.getProcessSessionFactory());
        processor.onTrigger(processContext, runner.getProcessSessionFactory());
        processor.onTrigger(processContext, runner.getProcessSessionFactory());
        processor.onTrigger(processContext, runner.getProcessSessionFactory());
        processor.onTrigger(processContext, runner.getProcessSessionFactory());
        processor.onTrigger(processContext, runner.getProcessSessionFactory());
        processor.onTrigger(processContext, runner.getProcessSessionFactory());
        processor.onTrigger(processContext, runner.getProcessSessionFactory());

        // runner.run(5,true,false);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteProcess.REL_SUCCESS);
        long totalFlowFilesSize = 0;
        for (final MockFlowFile flowFile : flowFiles) {
            totalFlowFilesSize += flowFile.getSize();
        }

        assertEquals(inFile.length(), totalFlowFilesSize);
    }

    @Test
    public void testNotRedirectErrorStream() {
        final TestRunner runner = TestRunners.newTestRunner(ExecuteProcess.class);
        runner.setProperty(ExecuteProcess.COMMAND, "cd");
        runner.setProperty(ExecuteProcess.COMMAND_ARGUMENTS, "does-not-exist");

        ProcessContext processContext = runner.getProcessContext();

        ExecuteProcess processor = (ExecuteProcess) runner.getProcessor();
        processor.updateScheduledTrue();
        processor.setupExecutor(processContext);

        processor.onTrigger(processContext, runner.getProcessSessionFactory());

        if (isCommandFailed(runner)) return;

        // ExecuteProcess doesn't wait for finishing to drain error stream if it's configure NOT to redirect stream.
        // This causes test failure when draining the error stream didn't finish
        // fast enough before the thread of this test case method checks the warn msg count.
        // So, this loop wait for a while until the log msg count becomes expected number, otherwise let it fail.
        final int expectedWarningMessages = 1;
        final int maxRetry = 5;
        for (int i = 0; i < maxRetry
            && (runner.getLogger().getWarnMessages().size() < expectedWarningMessages); i++) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
            }
        }
        final List<LogMessage> warnMessages = runner.getLogger().getWarnMessages();
        final String errorMsg = "If redirect error stream is false, the output should be logged as a warning so that user can notice on bulletin.";
        assertEquals(expectedWarningMessages, warnMessages.size(), errorMsg);
        final List<MockFlowFile> succeeded = runner.getFlowFilesForRelationship(ExecuteProcess.REL_SUCCESS);
        assertEquals(0, succeeded.size());
    }


    @Test
    public void testRedirectErrorStream() {
        final TestRunner runner = TestRunners.newTestRunner(ExecuteProcess.class);
        runner.setProperty(ExecuteProcess.COMMAND, "cd");
        runner.setProperty(ExecuteProcess.COMMAND_ARGUMENTS, "does-not-exist");
        runner.setProperty(ExecuteProcess.REDIRECT_ERROR_STREAM, "true");

        ProcessContext processContext = runner.getProcessContext();

        ExecuteProcess processor = (ExecuteProcess) runner.getProcessor();
        processor.updateScheduledTrue();
        processor.setupExecutor(processContext);

        processor.onTrigger(processContext, runner.getProcessSessionFactory());

        if (isCommandFailed(runner)) return;

        final List<LogMessage> warnMessages = runner.getLogger().getWarnMessages();
        assertEquals(0, warnMessages.size(), "If redirect error stream is true " +
                "the output should be sent as a content of flow-file.");
        final List<MockFlowFile> succeeded = runner.getFlowFilesForRelationship(ExecuteProcess.REL_SUCCESS);
        assertEquals(1, succeeded.size());
    }

    @Test
    public void testRedirectErrorStreamWithExpressions() {
        final TestRunner runner = TestRunners.newTestRunner(ExecuteProcess.class);
        runner.setProperty(ExecuteProcess.COMMAND, "ls");
        runner.setProperty(ExecuteProcess.COMMAND_ARGUMENTS, "${literal('does-not-exist'):toUpper()}");
        runner.setProperty(ExecuteProcess.REDIRECT_ERROR_STREAM, "true");

        ProcessContext processContext = runner.getProcessContext();

        ExecuteProcess processor = (ExecuteProcess) runner.getProcessor();
        processor.updateScheduledTrue();
        processor.setupExecutor(processContext);

        processor.onTrigger(processContext, runner.getProcessSessionFactory());

        if (isCommandFailed(runner)) return;

        final List<LogMessage> warnMessages = runner.getLogger().getWarnMessages();
        assertEquals(0, warnMessages.size(), "If redirect error stream is true " +
                "the output should be sent as a content of flow-file.");
        final List<MockFlowFile> succeeded = runner.getFlowFilesForRelationship(ExecuteProcess.REL_SUCCESS);
        assertEquals(1, succeeded.size());
        assertTrue(new String(succeeded.get(0).toByteArray()).contains("DOES-NOT-EXIST"));
        assertEquals(succeeded.get(0).getAttribute(ExecuteProcess.ATTRIBUTE_COMMAND), "ls");
        assertEquals(succeeded.get(0).getAttribute(ExecuteProcess.ATTRIBUTE_COMMAND_ARGS), "DOES-NOT-EXIST");
    }

    /**
     * On some environment, the test command immediately fail with an IOException
     * because of the native UnixProcess.init method implementation difference.
     *
     * @return true, if the command fails
     */
    private boolean isCommandFailed(final TestRunner runner) {
        final List<LogMessage> errorMessages = runner.getLogger().getErrorMessages();
        return (errorMessages.size() > 0
                && errorMessages.stream()
                    .anyMatch(m -> m.getMsg().contains("Failed to create process")));
    }

    /**
     * On configuration of this processor to run only on primary cluster node, other nodes call
     * {@link org.apache.nifi.annotation.lifecycle.OnUnscheduled} method after an invocation (Start/Stop or RunOnce),
     * causing an NPE.  NPE guard added; test for this situation.
     */
    @Test
    public void testProcessorNotScheduled() {
        final TestRunner runner = TestRunners.newTestRunner(ExecuteProcess.class);
        runner.setProperty(ExecuteProcess.COMMAND, "ls");
        runner.run(0);
    }
}
