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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processors.standard.util.ArgumentUtils;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestExecuteStreamCommand {
    private static final Path JAVA_FILES_DIR = Paths.get("src/test/java");
    private static final Path NO_SUCH_FILE = Paths.get("NoSuchFile.java");
    private static final Path TEST_DYNAMIC_ENVIRONMENT = Paths.get("TestDynamicEnvironment.java");
    private static final Path TEST_INGEST_AND_UPDATE = Paths.get("TestIngestAndUpdate.java");
    private static final Path TEST_LOG_STDERR = Paths.get("TestLogStdErr.java");
    private static final Path TEST_SUCCESS = Paths.get("TestSuccess.java");
    private static final String JAVA_COMMAND = "java";

    @TempDir
    private File tempDir;

    private TestRunner runner;

    @BeforeEach
    void setUp() {
        runner = TestRunners.newTestRunner(ExecuteStreamCommand.class);
    }

    @Test
    public void testDynamicPropertyArgumentsStrategyValid() {
        runner.setProperty(ExecuteStreamCommand.ARGUMENTS_STRATEGY, ExecuteStreamCommand.DYNAMIC_PROPERTY_ARGUMENTS_STRATEGY.getValue());
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, JAVA_COMMAND);
        runner.setProperty("command.argument.1", "-version");

        runner.assertValid();

        final List<LogMessage> warnMessages = runner.getLogger().getWarnMessages();
        assertTrue(warnMessages.isEmpty(), "Warning Log Messages found");
    }

    @Test
    public void testCommandArgumentsPropertyStrategyValid() {
        runner.setProperty(ExecuteStreamCommand.ARGUMENTS_STRATEGY, ExecuteStreamCommand.COMMAND_ARGUMENTS_PROPERTY_STRATEGY.getValue());
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, JAVA_COMMAND);
        runner.setProperty("RUNTIME_VERSION", "version-1");

        runner.assertValid();

        final List<LogMessage> warnMessages = runner.getLogger().getWarnMessages();
        assertTrue(warnMessages.isEmpty(), "Warning Log Messages found");
    }

    @Test
    public void testExecuteJavaFile() {
        final Path javaFile = JAVA_FILES_DIR.resolve(TEST_SUCCESS);
        runner.setProperty(ExecuteStreamCommand.MIME_TYPE.getName(), "text/plain");
        runner.enqueue("");
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, JAVA_COMMAND);
        runner.setProperty(ExecuteStreamCommand.EXECUTION_ARGUMENTS, javaFile.toString());
        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        runner.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP);
        final MockFlowFile outputFlowFile = flowFiles.getFirst();
        final String result = outputFlowFile.getContent();
        assertTrue(Pattern.compile("Test was a success\r?\n").matcher(result).find());
        assertEquals("0", outputFlowFile.getAttribute("execution.status"));
        assertEquals(JAVA_COMMAND, outputFlowFile.getAttribute("execution.command"));
        assertEquals(javaFile.toString(), outputFlowFile.getAttribute("execution.command.args"));
        outputFlowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "text/plain");

        final MockFlowFile originalFlowFile = runner.getFlowFilesForRelationship(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP).getFirst();
        assertEquals(outputFlowFile.getAttribute("execution.status"), originalFlowFile.getAttribute("execution.status"));
        assertEquals(outputFlowFile.getAttribute("execution.command"), originalFlowFile.getAttribute("execution.command"));
        assertEquals(outputFlowFile.getAttribute("execution.command.args"), originalFlowFile.getAttribute("execution.command.args"));
    }

    @Test
    public void testExecuteJavaFileDynamicPropArgs() {
        final Path javaFile = JAVA_FILES_DIR.resolve(TEST_SUCCESS);
        runner.enqueue("");
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, JAVA_COMMAND);
        runner.setProperty(ExecuteStreamCommand.ARGUMENTS_STRATEGY, ExecuteStreamCommand.DYNAMIC_PROPERTY_ARGUMENTS_STRATEGY.getValue());
        final PropertyDescriptor dynamicProp1 = new PropertyDescriptor.Builder()
            .dynamic(true)
            .name("command.argument.1")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
        runner.setProperty(dynamicProp1, javaFile.toString());
        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        runner.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP);
        final MockFlowFile outputFlowFile = flowFiles.getFirst();
        final String result = outputFlowFile.getContent();
        assertTrue(Pattern.compile("Test was a success\r?\n").matcher(result).find());
        assertEquals("0", outputFlowFile.getAttribute("execution.status"));
        assertEquals(JAVA_COMMAND, outputFlowFile.getAttribute("execution.command"));
        assertEquals(javaFile.toString(), outputFlowFile.getAttribute("execution.command.args"));

        final MockFlowFile originalFlowFile = runner.getFlowFilesForRelationship(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP).getFirst();
        assertEquals(outputFlowFile.getAttribute("execution.status"), originalFlowFile.getAttribute("execution.status"));
        assertEquals(outputFlowFile.getAttribute("execution.command"), originalFlowFile.getAttribute("execution.command"));
        assertEquals(outputFlowFile.getAttribute("execution.command.args"), originalFlowFile.getAttribute("execution.command.args"));
    }

    @Test
    public void testExecuteJavaFileWithBadPath() {
        final Path javaFile = JAVA_FILES_DIR.resolve(NO_SUCH_FILE);
        runner.enqueue("blah");
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, JAVA_COMMAND);
        runner.setProperty(ExecuteStreamCommand.EXECUTION_ARGUMENTS, javaFile.toString());
        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        runner.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 0);
        runner.assertTransferCount(ExecuteStreamCommand.NONZERO_STATUS_RELATIONSHIP, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.NONZERO_STATUS_RELATIONSHIP);
        final MockFlowFile flowFile = flowFiles.getFirst();
        assertEquals(0, flowFile.getSize());
        assertTrue(flowFile.getAttribute("execution.error").contains("java.lang.ClassNotFoundException"));
        assertTrue(flowFile.isPenalized());
    }

    @Test
    public void testExecuteJavaFileWithBadPathDynamicProperties() {
        final Path javaFile = JAVA_FILES_DIR.resolve(NO_SUCH_FILE);
        runner.enqueue("blah");
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, JAVA_COMMAND);
        runner.setProperty(ExecuteStreamCommand.ARGUMENTS_STRATEGY, ExecuteStreamCommand.DYNAMIC_PROPERTY_ARGUMENTS_STRATEGY.getValue());
        final PropertyDescriptor dynamicProp1 = new PropertyDescriptor.Builder()
            .dynamic(true)
            .name("command.argument.1")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
        runner.setProperty(dynamicProp1, javaFile.toString());
        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        runner.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 0);
        runner.assertTransferCount(ExecuteStreamCommand.NONZERO_STATUS_RELATIONSHIP, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.NONZERO_STATUS_RELATIONSHIP);
        final MockFlowFile flowFile = flowFiles.getFirst();
        assertEquals(0, flowFile.getSize());
        assertTrue(flowFile.getAttribute("execution.error").contains("java.lang.ClassNotFoundException"));
        assertTrue(flowFile.isPenalized());
    }

    @Test
    public void testExecuteIngestAndUpdate() {
        final Path javaFile = JAVA_FILES_DIR.resolve(TEST_INGEST_AND_UPDATE);
        final String content = "Print Me";
        runner.enqueue(content);
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, JAVA_COMMAND);
        runner.setProperty(ExecuteStreamCommand.EXECUTION_ARGUMENTS, javaFile.toString());
        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        runner.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP);
        final String result = flowFiles.getFirst().getContent();
        final String expectedOutput = "nifi-standard-processors:ModifiedResult\r?\n%s".formatted(content);

        assertTrue(Pattern.compile(expectedOutput).matcher(result).find());
    }

    @Test
    public void testExecuteIngestAndUpdateDynamicProperties() {
        final Path javaFile = JAVA_FILES_DIR.resolve(TEST_INGEST_AND_UPDATE);
        final String content = "Print Me";
        runner.enqueue(content);
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, JAVA_COMMAND);
        runner.setProperty(ExecuteStreamCommand.ARGUMENTS_STRATEGY, ExecuteStreamCommand.DYNAMIC_PROPERTY_ARGUMENTS_STRATEGY.getValue());
        final PropertyDescriptor dynamicProp1 = new PropertyDescriptor.Builder()
            .dynamic(true)
            .name("command.argument.1")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
        runner.setProperty(dynamicProp1, javaFile.toString());
        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        runner.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP);
        final String result = flowFiles.getFirst().getContent();
        final String expectedOutput = "nifi-standard-processors:ModifiedResult\r?\n%s".formatted(content);

        assertTrue(Pattern.compile(expectedOutput).matcher(result).find());
    }

    @Test
    public void testLoggingToStdErr() {
        final Path javaFile = JAVA_FILES_DIR.resolve(TEST_LOG_STDERR);
        runner.setValidateExpressionUsage(false);
        runner.enqueue("");
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, JAVA_COMMAND);
        runner.setProperty(ExecuteStreamCommand.EXECUTION_ARGUMENTS, javaFile.toString());
        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        runner.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP);
        final MockFlowFile flowFile = flowFiles.getFirst();
        assertEquals(0, flowFile.getSize());
        assertTrue(flowFile.getAttribute("execution.error").matches("^\\W{7}f+$"),
                "Attribute 'execution.error' did not match regular expression ^\\W{7}f+$. Full value of 'execution.error' attribute is %s".formatted(flowFile.getAttribute("execution.error")));
    }

    @Test
    public void testLoggingToStdErrDynamicProperties() {
        final Path javaFile = JAVA_FILES_DIR.resolve(TEST_LOG_STDERR);
        runner.setValidateExpressionUsage(false);
        runner.enqueue("");
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, JAVA_COMMAND);
        runner.setProperty(ExecuteStreamCommand.ARGUMENTS_STRATEGY, ExecuteStreamCommand.DYNAMIC_PROPERTY_ARGUMENTS_STRATEGY.getValue());
        final PropertyDescriptor dynamicProp1 = new PropertyDescriptor.Builder()
            .dynamic(true)
            .name("command.argument.1")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
        runner.setProperty(dynamicProp1, javaFile.toString());
        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        runner.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP);
        final MockFlowFile flowFile = flowFiles.getFirst();
        assertEquals(0, flowFile.getSize());
        assertTrue(flowFile.getAttribute("execution.error").matches("^\\W{7}f+$"),
                "Attribute 'execution.error' did not match regular expression ^\\W{7}f+$. Full value of 'execution.error' attribute is %s".formatted(flowFile.getAttribute("execution.error")));
    }

    @Test
    public void testExecuteIngestAndUpdateWithWorkingDir() {
        final String content = "Print Me";
        runner.enqueue(content);
        runner.setProperty(ExecuteStreamCommand.WORKING_DIR, JAVA_FILES_DIR.toString());
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, JAVA_COMMAND);
        runner.setProperty(ExecuteStreamCommand.EXECUTION_ARGUMENTS, TEST_INGEST_AND_UPDATE.toString());
        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        runner.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP);
        final String result = flowFiles.getFirst().getContent();

        final String quotedSeparator = Pattern.quote(File.separator);
        final String expectedOutput = "%1$snifi-standard-processors%1$ssrc%1$stest%1$sjava:ModifiedResult\r?\n%2$s".formatted(quotedSeparator, content);

        assertTrue(Pattern.compile(expectedOutput).matcher(result).find());
    }

    @Test
    public void testExecuteIngestAndUpdateWithWorkingDirDynamicProperties() {
        final String content = "Print Me";
        runner.enqueue(content);
        runner.setProperty(ExecuteStreamCommand.WORKING_DIR, JAVA_FILES_DIR.toString());
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, JAVA_COMMAND);
        runner.setProperty(ExecuteStreamCommand.ARGUMENTS_STRATEGY, ExecuteStreamCommand.DYNAMIC_PROPERTY_ARGUMENTS_STRATEGY.getValue());
        final PropertyDescriptor dynamicProp1 = new PropertyDescriptor.Builder()
            .dynamic(true)
            .name("command.argument.1")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
        runner.setProperty(dynamicProp1, TEST_INGEST_AND_UPDATE.toString());
        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        runner.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP);
        final String result = flowFiles.getFirst().getContent();

        final String quotedSeparator = Pattern.quote(File.separator);
        final String expectedOutput = "%1$snifi-standard-processors%1$ssrc%1$stest%1$sjava:ModifiedResult\r?\n%2$s".formatted(quotedSeparator, content);

        assertTrue(Pattern.compile(expectedOutput).matcher(result).find());
    }

    @Test
    public void testIgnoredStdin() {
        final String content = "Print Me";
        runner.enqueue(content);
        runner.setProperty(ExecuteStreamCommand.WORKING_DIR, JAVA_FILES_DIR.toString());
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, JAVA_COMMAND);
        runner.setProperty(ExecuteStreamCommand.EXECUTION_ARGUMENTS, TEST_INGEST_AND_UPDATE.toString());
        runner.setProperty(ExecuteStreamCommand.IGNORE_STDIN, "true");
        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        runner.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP);
        final String result = flowFiles.getFirst().getContent();

        final String quotedSeparator = Pattern.quote(File.separator);
        final String expectedOutput = "src%1$stest%1$sjava:ModifiedResult\r?\n".formatted(quotedSeparator);

        assertTrue(Pattern.compile(expectedOutput).matcher(result).find());
    }

    @Test
    public void testIgnoredStdinDynamicProperties() {
        final String content = "Print Me";
        runner.enqueue(content);
        runner.setProperty(ExecuteStreamCommand.WORKING_DIR, JAVA_FILES_DIR.toString());
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, JAVA_COMMAND);
        runner.setProperty(ExecuteStreamCommand.ARGUMENTS_STRATEGY, ExecuteStreamCommand.DYNAMIC_PROPERTY_ARGUMENTS_STRATEGY.getValue());
        final PropertyDescriptor dynamicProp1 = new PropertyDescriptor.Builder()
            .dynamic(true)
            .name("command.argument.1")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
        runner.setProperty(dynamicProp1, TEST_INGEST_AND_UPDATE.toString());
        runner.setProperty(ExecuteStreamCommand.IGNORE_STDIN, "true");
        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        runner.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP);
        final String result = flowFiles.getFirst().getContent();

        final String quotedSeparator = Pattern.quote(File.separator);
        final String expectedOutput = "src%1$stest%1$sjava:ModifiedResult\r?\n".formatted(quotedSeparator);

        assertTrue(Pattern.compile(expectedOutput).matcher(result).find());
    }

    @Test
    public void testDynamicEnvironment() {
        runner.setProperty("NIFI_TEST_1", "testvalue1");
        runner.setProperty("NIFI_TEST_2", "testvalue2");
        runner.enqueue("");
        runner.setProperty(ExecuteStreamCommand.WORKING_DIR, JAVA_FILES_DIR.toString());
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, JAVA_COMMAND);
        runner.setProperty(ExecuteStreamCommand.EXECUTION_ARGUMENTS, TEST_DYNAMIC_ENVIRONMENT.toString());
        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        runner.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP);
        final String result = flowFiles.getFirst().getContent();
        final Set<String> dynamicEnvironmentVariables = new HashSet<>(Arrays.asList(result.split("\r?\n")));
        final Set<String> expectedEnvironmentVariables = Set.of("NIFI_TEST_1=testvalue1", "NIFI_TEST_2=testvalue2");
        assertTrue(dynamicEnvironmentVariables.containsAll(expectedEnvironmentVariables));
    }

    @Test
    public void testDynamicEnvironmentDynamicProperties() {
        runner.setProperty("NIFI_TEST_1", "testvalue1");
        runner.setProperty("NIFI_TEST_2", "testvalue2");
        runner.enqueue("");
        runner.setProperty(ExecuteStreamCommand.WORKING_DIR, JAVA_FILES_DIR.toString());
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, JAVA_COMMAND);
        runner.setProperty(ExecuteStreamCommand.ARGUMENTS_STRATEGY, ExecuteStreamCommand.DYNAMIC_PROPERTY_ARGUMENTS_STRATEGY.getValue());
        final PropertyDescriptor dynamicProp1 = new PropertyDescriptor.Builder()
            .dynamic(true)
            .name("command.argument.1")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
        runner.setProperty(dynamicProp1, TEST_DYNAMIC_ENVIRONMENT.toString());
        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        runner.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP);
        final String result = flowFiles.getFirst().getContent();
        final Set<String> dynamicEnvironmentVariables = new HashSet<>(Arrays.asList(result.split("\r?\n")));
        final Set<String> expectedEnvironmentVariables = Set.of("NIFI_TEST_1=testvalue1", "NIFI_TEST_2=testvalue2");
        assertTrue(dynamicEnvironmentVariables.containsAll(expectedEnvironmentVariables));
    }

    @Test
    public void testSmallEchoPutToAttribute() {
        final File dummy = new File("src/test/resources/hello.txt");
        assertTrue(dummy.exists());
        runner.setProperty(ExecuteStreamCommand.MIME_TYPE, "application/json");
        runner.enqueue("".getBytes());

        if (isWindows()) {
            runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, "cmd.exe");
            runner.setProperty(ExecuteStreamCommand.EXECUTION_ARGUMENTS, "/c;echo Hello");
            runner.setProperty(ExecuteStreamCommand.ARG_DELIMITER, ";");
        } else {
            runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, "echo");
            runner.setProperty(ExecuteStreamCommand.EXECUTION_ARGUMENTS, "Hello");
        }
        runner.setProperty(ExecuteStreamCommand.IGNORE_STDIN, "true");
        runner.setProperty(ExecuteStreamCommand.PUT_OUTPUT_IN_ATTRIBUTE, "executeStreamCommand.output");

        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        runner.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 0);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP);
        final MockFlowFile outputFlowFile = flowFiles.getFirst();
        outputFlowFile.assertContentEquals("");
        outputFlowFile.assertAttributeNotExists(CoreAttributes.MIME_TYPE.key());
        final String ouput = outputFlowFile.getAttribute("executeStreamCommand.output");
        assertTrue(ouput.startsWith("Hello"));
        assertEquals("0", outputFlowFile.getAttribute("execution.status"));
        assertEquals(isWindows() ? "cmd.exe" : "echo", outputFlowFile.getAttribute("execution.command"));
    }

    @Test
    public void testSmallEchoPutToAttributeDynamicProperties() {
        final File dummy = new File("src/test/resources/hello.txt");
        assertTrue(dummy.exists());
        runner.enqueue("".getBytes());
        runner.setProperty(ExecuteStreamCommand.ARGUMENTS_STRATEGY, ExecuteStreamCommand.DYNAMIC_PROPERTY_ARGUMENTS_STRATEGY.getValue());

        if (isWindows()) {
            runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, "cmd.exe");
            final PropertyDescriptor dynamicProp1 = new PropertyDescriptor.Builder()
                .dynamic(true)
                .name("command.argument.1")
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
            runner.setProperty(dynamicProp1, "/c");
            final PropertyDescriptor dynamicProp2 = new PropertyDescriptor.Builder()
                .dynamic(true)
                .name("command.argument.2")
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
            runner.setProperty(dynamicProp2, "echo Hello");
            runner.setProperty(ExecuteStreamCommand.EXECUTION_ARGUMENTS, "/c;echo Hello");
        } else {
            runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, "echo");
            final PropertyDescriptor dynamicProp1 = new PropertyDescriptor.Builder()
                .dynamic(true)
                .name("command.argument.1")
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
            runner.setProperty(dynamicProp1, "Hello");
        }
        runner.setProperty(ExecuteStreamCommand.IGNORE_STDIN, "true");
        runner.setProperty(ExecuteStreamCommand.PUT_OUTPUT_IN_ATTRIBUTE, "executeStreamCommand.output");

        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        runner.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 0);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP);
        final MockFlowFile outputFlowFile = flowFiles.getFirst();
        outputFlowFile.assertContentEquals("");
        final String ouput = outputFlowFile.getAttribute("executeStreamCommand.output");
        assertTrue(ouput.startsWith("Hello"));
        assertEquals("0", outputFlowFile.getAttribute("execution.status"));
        assertEquals(isWindows() ? "cmd.exe" : "echo", outputFlowFile.getAttribute("execution.command"));
    }

    @Test
    public void testArgumentsWithQuotesFromAttributeDynamicProperties() {
        final Map<String, String> attrs = new HashMap<>();
        final String exStr = "Hello World with quotes";
        attrs.put("str.attribute", exStr);
        runner.enqueue("".getBytes(), attrs);

        runner.setProperty(ExecuteStreamCommand.ARGUMENTS_STRATEGY, ExecuteStreamCommand.DYNAMIC_PROPERTY_ARGUMENTS_STRATEGY.getValue());

        if (isWindows()) {
            runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, "cmd.exe");
            final PropertyDescriptor dynamicProp1 = new PropertyDescriptor.Builder()
                .dynamic(true)
                .name("command.argument.1")
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
            runner.setProperty(dynamicProp1, "/c");
            final PropertyDescriptor dynamicProp2 = new PropertyDescriptor.Builder()
                .dynamic(true)
                .name("command.argument.2")
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
            runner.setProperty(dynamicProp2, "echo");
        } else {
            runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, "echo");
        }
        final PropertyDescriptor dynamicProp3 = new PropertyDescriptor.Builder()
            .dynamic(true)
            .name("command.argument.3")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
        runner.setProperty(dynamicProp3, "${str.attribute}");
        runner.setProperty(ExecuteStreamCommand.IGNORE_STDIN, "true");

        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        runner.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP);
        final MockFlowFile outputFlowFile = flowFiles.getFirst();
        String output = outputFlowFile.getContent().trim();
        if (isWindows()) {
            output = StringUtils.unwrap(output, '"');
        }
        assertEquals(exStr, output);
        assertEquals("0", outputFlowFile.getAttribute("execution.status"));
        assertEquals(isWindows() ? "cmd.exe" : "echo", outputFlowFile.getAttribute("execution.command"));
    }

    @Test
    public void testExecuteJavaFilePutToAttribute() {
        final Path javaFile = JAVA_FILES_DIR.resolve(TEST_SUCCESS);
        final String content = "Print Me";
        runner.enqueue(content);
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, JAVA_COMMAND);
        runner.setProperty(ExecuteStreamCommand.EXECUTION_ARGUMENTS, javaFile.toString());
        runner.setProperty(ExecuteStreamCommand.PUT_OUTPUT_IN_ATTRIBUTE, "executeStreamCommand.output");
        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        runner.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 0);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP);
        final MockFlowFile outputFlowFile = flowFiles.getFirst();
        final String result = outputFlowFile.getAttribute("executeStreamCommand.output");
        outputFlowFile.assertContentEquals(content);
        assertTrue(Pattern.compile("Test was a success\r?\n").matcher(result).find());
        assertEquals("0", outputFlowFile.getAttribute("execution.status"));
        assertEquals(JAVA_COMMAND, outputFlowFile.getAttribute("execution.command"));
        assertEquals(javaFile.toString(), outputFlowFile.getAttribute("execution.command.args"));
    }

    @Test
    public void testExecuteJavaFilePutToAttributeDynamicProperties() {
        final Path javaFile = JAVA_FILES_DIR.resolve(TEST_SUCCESS);
        final String content = "Print Me";
        runner.enqueue(content);
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, JAVA_COMMAND);
        runner.setProperty(ExecuteStreamCommand.ARGUMENTS_STRATEGY, ExecuteStreamCommand.DYNAMIC_PROPERTY_ARGUMENTS_STRATEGY.getValue());
        final PropertyDescriptor dynamicProp1 = new PropertyDescriptor.Builder()
            .dynamic(true)
            .name("command.argument.1")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
        runner.setProperty(dynamicProp1, javaFile.toString());
        runner.setProperty(ExecuteStreamCommand.PUT_OUTPUT_IN_ATTRIBUTE, "executeStreamCommand.output");
        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        runner.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 0);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP);
        final MockFlowFile outputFlowFile = flowFiles.getFirst();
        final String result = outputFlowFile.getAttribute("executeStreamCommand.output");
        outputFlowFile.assertContentEquals(content);
        assertTrue(Pattern.compile("Test was a success\r?\n").matcher(result).find());
        assertEquals("0", outputFlowFile.getAttribute("execution.status"));
        assertEquals(JAVA_COMMAND, outputFlowFile.getAttribute("execution.command"));
        assertEquals(javaFile.toString(), outputFlowFile.getAttribute("execution.command.args"));
    }

    @Test
    public void testExecuteJavaFileToAttributeConfiguration() throws Exception {
        final Path javaFile = JAVA_FILES_DIR.resolve(TEST_SUCCESS);
        runner.enqueue("small test".getBytes());
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, JAVA_COMMAND);
        runner.setProperty(ExecuteStreamCommand.EXECUTION_ARGUMENTS, javaFile.toString());
        runner.setProperty(ExecuteStreamCommand.PUT_ATTRIBUTE_MAX_LENGTH, "10");
        runner.setProperty(ExecuteStreamCommand.PUT_OUTPUT_IN_ATTRIBUTE, "outputDest");
        assertEquals(1, runner.getProcessContext().getAvailableRelationships().size());
        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        runner.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 0);
        runner.assertTransferCount(ExecuteStreamCommand.NONZERO_STATUS_RELATIONSHIP, 0);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP);
        final MockFlowFile outputFlowFile = flowFiles.getFirst();
        outputFlowFile.assertContentEquals("small test".getBytes());
        final String result = outputFlowFile.getAttribute("outputDest");
        assertTrue(Pattern.compile("Test was a").matcher(result).find());
        assertEquals("0", outputFlowFile.getAttribute("execution.status"));
        assertEquals(JAVA_COMMAND, outputFlowFile.getAttribute("execution.command"));
        assertEquals(javaFile.toString(), outputFlowFile.getAttribute("execution.command.args"));
    }

    @Test
    public void testExecuteJavaFileToAttributeConfigurationDynamicProperties() {
        final Path javaFile = JAVA_FILES_DIR.resolve(TEST_SUCCESS);
        runner.enqueue("small test".getBytes());
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, JAVA_COMMAND);
        runner.setProperty(ExecuteStreamCommand.ARGUMENTS_STRATEGY, ExecuteStreamCommand.DYNAMIC_PROPERTY_ARGUMENTS_STRATEGY.getValue());
        final PropertyDescriptor dynamicProp1 = new PropertyDescriptor.Builder()
            .dynamic(true)
            .name("command.argument.1")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
        runner.setProperty(dynamicProp1, javaFile.toString());
        runner.setProperty(ExecuteStreamCommand.PUT_ATTRIBUTE_MAX_LENGTH, "10");
        runner.setProperty(ExecuteStreamCommand.PUT_OUTPUT_IN_ATTRIBUTE, "outputDest");
        assertEquals(1, runner.getProcessContext().getAvailableRelationships().size());
        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        runner.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 0);
        runner.assertTransferCount(ExecuteStreamCommand.NONZERO_STATUS_RELATIONSHIP, 0);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP);
        final MockFlowFile outputFlowFile = flowFiles.getFirst();
        outputFlowFile.assertContentEquals("small test");
        final String result = outputFlowFile.getAttribute("outputDest");
        assertTrue(Pattern.compile("Test was a").matcher(result).find());
        assertEquals("0", outputFlowFile.getAttribute("execution.status"));
        assertEquals(JAVA_COMMAND, outputFlowFile.getAttribute("execution.command"));
        assertEquals(javaFile.toString(), outputFlowFile.getAttribute("execution.command.args"));
    }

    @Test
    public void testExecuteIngestAndUpdatePutToAttribute() {
        final Path javaFile = JAVA_FILES_DIR.resolve(TEST_INGEST_AND_UPDATE);
        runner.enqueue("");
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, JAVA_COMMAND);
        runner.setProperty(ExecuteStreamCommand.EXECUTION_ARGUMENTS, javaFile.toString());
        runner.setProperty(ExecuteStreamCommand.PUT_OUTPUT_IN_ATTRIBUTE, "outputDest");
        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        runner.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 0);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP);
        final String result = flowFiles.getFirst().getAttribute("outputDest");
        assertTrue(Pattern.compile("nifi-standard-processors:ModifiedResult\r?\n").matcher(result).find());
    }

    @Test
    public void testExecuteIngestAndUpdatePutToAttributeDynamicProperties() {
        final Path javaFile = JAVA_FILES_DIR.resolve(TEST_INGEST_AND_UPDATE);
        runner.enqueue("");
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, JAVA_COMMAND);
        runner.setProperty(ExecuteStreamCommand.ARGUMENTS_STRATEGY, ExecuteStreamCommand.DYNAMIC_PROPERTY_ARGUMENTS_STRATEGY.getValue());
        final PropertyDescriptor dynamicProp1 = new PropertyDescriptor.Builder()
            .dynamic(true)
            .name("command.argument.1")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
        runner.setProperty(dynamicProp1, javaFile.toString());
        runner.setProperty(ExecuteStreamCommand.PUT_OUTPUT_IN_ATTRIBUTE, "outputDest");
        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        runner.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 0);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP);
        final String result = flowFiles.getFirst().getAttribute("outputDest");

        assertTrue(Pattern.compile("nifi-standard-processors:ModifiedResult\r?\n").matcher(result).find());
    }

    @Test
    public void testLargePutToAttribute() throws IOException {
        final File dummy = new File("src/test/resources/ExecuteCommand/1000bytes.txt");
        final File dummy10MBytes = new File(tempDir, "10MB.txt");
        final byte[] bytes = Files.readAllBytes(dummy.toPath());
        try (FileOutputStream fos = new FileOutputStream(dummy10MBytes)) {
            for (int i = 0; i < 10000; i++) {
                fos.write(bytes, 0, 1000);
            }
        }

        runner.enqueue("".getBytes());
        if (isWindows()) {
            runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, "cmd.exe");
            runner.setProperty(ExecuteStreamCommand.EXECUTION_ARGUMENTS, "/c;type " + dummy10MBytes.getAbsolutePath());
            runner.setProperty(ExecuteStreamCommand.ARG_DELIMITER, ";");
        } else {
            runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, "cat");
            runner.setProperty(ExecuteStreamCommand.EXECUTION_ARGUMENTS, dummy10MBytes.getAbsolutePath());
        }
        runner.setProperty(ExecuteStreamCommand.IGNORE_STDIN, "true");
        runner.setProperty(ExecuteStreamCommand.PUT_OUTPUT_IN_ATTRIBUTE, "executeStreamCommand.output");
        runner.setProperty(ExecuteStreamCommand.PUT_ATTRIBUTE_MAX_LENGTH, "256");

        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        runner.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 0);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP);

        flowFiles.getFirst().assertAttributeEquals("execution.status", "0");
        final String result = flowFiles.getFirst().getAttribute("executeStreamCommand.output");
        assertTrue(Pattern.compile("a{256}").matcher(result).matches());
    }

    @Test
    public void testLargePutToAttributeDynamicProperties() throws IOException {
        final File dummy = new File("src/test/resources/ExecuteCommand/1000bytes.txt");
        final File dummy10MBytes = new File(tempDir, "10MB.txt");
        final byte[] bytes = Files.readAllBytes(dummy.toPath());
        try (FileOutputStream fos = new FileOutputStream(dummy10MBytes)) {
            for (int i = 0; i < 10000; i++) {
                fos.write(bytes, 0, 1000);
            }
        }

        runner.enqueue("".getBytes());
        runner.setProperty(ExecuteStreamCommand.ARGUMENTS_STRATEGY, ExecuteStreamCommand.DYNAMIC_PROPERTY_ARGUMENTS_STRATEGY.getValue());
        if (isWindows()) {
            runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, "cmd.exe");
            final PropertyDescriptor dynamicProp1 = new PropertyDescriptor.Builder()
                .dynamic(true)
                .name("command.argument.1")
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
            runner.setProperty(dynamicProp1, "/c");
            final PropertyDescriptor dynamicProp2 = new PropertyDescriptor.Builder()
                .dynamic(true)
                .name("command.argument.2")
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
            runner.setProperty(dynamicProp2, "type " + dummy10MBytes.getAbsolutePath());
        } else {
            runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, "cat");
            final PropertyDescriptor dynamicProp1 = new PropertyDescriptor.Builder()
                .dynamic(true)
                .name("command.argument.1")
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
            runner.setProperty(dynamicProp1, dummy10MBytes.getAbsolutePath());
        }
        runner.setProperty(ExecuteStreamCommand.IGNORE_STDIN, "true");
        runner.setProperty(ExecuteStreamCommand.PUT_OUTPUT_IN_ATTRIBUTE, "executeStreamCommand.output");
        runner.setProperty(ExecuteStreamCommand.PUT_ATTRIBUTE_MAX_LENGTH, "256");

        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        runner.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 0);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP);

        flowFiles.getFirst().assertAttributeEquals("execution.status", "0");
        final String result = flowFiles.getFirst().getAttribute("executeStreamCommand.output");
        assertTrue(Pattern.compile("a{256}").matcher(result).matches());
    }

    @Test
    public void testExecuteIngestAndUpdateWithWorkingDirPutToAttribute() {
        runner.enqueue("");
        runner.setProperty(ExecuteStreamCommand.WORKING_DIR, JAVA_FILES_DIR.toString());
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, JAVA_COMMAND);
        runner.setProperty(ExecuteStreamCommand.PUT_OUTPUT_IN_ATTRIBUTE, "streamOutput");
        runner.setProperty(ExecuteStreamCommand.EXECUTION_ARGUMENTS, TEST_INGEST_AND_UPDATE.toString());
        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP);
        final String result = flowFiles.getFirst().getAttribute("streamOutput");

        final String quotedSeparator = Pattern.quote(File.separator);
        final String expectedOutput = "%1$snifi-standard-processors%1$ssrc%1$stest%1$sjava:ModifiedResult\r?\n".formatted(quotedSeparator);
        assertTrue(Pattern.compile(expectedOutput).matcher(result).find());
    }

    @Test
    public void testExecuteIngestAndUpdateWithWorkingDirPutToAttributeDynamicProperties() {
        runner.enqueue("");
        runner.setProperty(ExecuteStreamCommand.WORKING_DIR, JAVA_FILES_DIR.toString());
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, JAVA_COMMAND);
        runner.setProperty(ExecuteStreamCommand.PUT_OUTPUT_IN_ATTRIBUTE, "streamOutput");
        runner.setProperty(ExecuteStreamCommand.ARGUMENTS_STRATEGY, ExecuteStreamCommand.DYNAMIC_PROPERTY_ARGUMENTS_STRATEGY.getValue());
        final PropertyDescriptor dynamicProp1 = new PropertyDescriptor.Builder()
            .dynamic(true)
            .name("command.argument.1")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
        runner.setProperty(dynamicProp1, TEST_INGEST_AND_UPDATE.toString());
        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP);
        final String result = flowFiles.getFirst().getAttribute("streamOutput");

        final String quotedSeparator = Pattern.quote(File.separator);
        final String expectedOutput = "%1$snifi-standard-processors%1$ssrc%1$stest%1$sjava:ModifiedResult\r?\n".formatted(quotedSeparator);
        assertTrue(Pattern.compile(expectedOutput).matcher(result).find());
    }

    @Test
    public void testIgnoredStdinPutToAttribute() {
        runner.enqueue("");
        runner.setProperty(ExecuteStreamCommand.WORKING_DIR, JAVA_FILES_DIR.toString());
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, JAVA_COMMAND);
        runner.setProperty(ExecuteStreamCommand.EXECUTION_ARGUMENTS, TEST_INGEST_AND_UPDATE.toString());
        runner.setProperty(ExecuteStreamCommand.IGNORE_STDIN, "true");
        runner.setProperty(ExecuteStreamCommand.PUT_OUTPUT_IN_ATTRIBUTE, "executeStreamCommand.output");
        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP);
        final String result = flowFiles.getFirst().getAttribute("executeStreamCommand.output");

        final String quotedSeparator = Pattern.quote(File.separator);
        final String expectedOutput = "%1$ssrc%1$stest%1$sjava:ModifiedResult\r?\n".formatted(quotedSeparator);

        assertTrue(Pattern.compile(expectedOutput).matcher(result).find(),
                "TestIngestAndUpdate.java should not have received anything to modify");
    }

    @Test
    public void testIgnoredStdinPutToAttributeDynamicProperties() {
        runner.enqueue("");
        runner.setProperty(ExecuteStreamCommand.WORKING_DIR, JAVA_FILES_DIR.toString());
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, JAVA_COMMAND);
        runner.setProperty(ExecuteStreamCommand.ARGUMENTS_STRATEGY, ExecuteStreamCommand.DYNAMIC_PROPERTY_ARGUMENTS_STRATEGY.getValue());
        final PropertyDescriptor dynamicProp1 = new PropertyDescriptor.Builder()
            .dynamic(true)
            .name("command.argument.1")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
        runner.setProperty(dynamicProp1, TEST_INGEST_AND_UPDATE.toString());
        runner.setProperty(ExecuteStreamCommand.IGNORE_STDIN, "true");
        runner.setProperty(ExecuteStreamCommand.PUT_OUTPUT_IN_ATTRIBUTE, "executeStreamCommand.output");
        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP);
        final String result = flowFiles.getFirst().getAttribute("executeStreamCommand.output");
        final String quotedSeparator = Pattern.quote(File.separator);
        final String expectedOutput = "%1$ssrc%1$stest%1$sjava:ModifiedResult\r?\n".formatted(quotedSeparator);

        assertTrue(Pattern.compile(expectedOutput).matcher(result).find(),
                "TestIngestAndUpdate.java should not have received anything to modify");
    }

    @Test
    public void testDynamicEnvironmentPutToAttribute() {
        runner.setProperty("NIFI_TEST_1", "testvalue1");
        runner.setProperty("NIFI_TEST_2", "testvalue2");
        runner.enqueue("");
        runner.setProperty(ExecuteStreamCommand.WORKING_DIR, JAVA_FILES_DIR.toString());
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, JAVA_COMMAND);
        runner.setProperty(ExecuteStreamCommand.EXECUTION_ARGUMENTS, TEST_DYNAMIC_ENVIRONMENT.toString());
        runner.setProperty(ExecuteStreamCommand.PUT_OUTPUT_IN_ATTRIBUTE, "executeStreamCommand.output");
        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP);
        final String result = flowFiles.getFirst().getAttribute("executeStreamCommand.output");
        final Set<String> dynamicEnvironmentVariables = new HashSet<>(Arrays.asList(result.split("\r?\n")));
        final Set<String> expectedEnvironmentVariables = Set.of("NIFI_TEST_1=testvalue1", "NIFI_TEST_2=testvalue2");
        assertTrue(dynamicEnvironmentVariables.containsAll(expectedEnvironmentVariables));
    }

    @Test
    public void testDynamicEnvironmentPutToAttributeDynamicProperties() {
        runner.setProperty("NIFI_TEST_1", "testvalue1");
        runner.setProperty("NIFI_TEST_2", "testvalue2");
        runner.enqueue("");
        runner.setProperty(ExecuteStreamCommand.WORKING_DIR, JAVA_FILES_DIR.toString());
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, JAVA_COMMAND);
        runner.setProperty(ExecuteStreamCommand.ARGUMENTS_STRATEGY, ExecuteStreamCommand.DYNAMIC_PROPERTY_ARGUMENTS_STRATEGY.getValue());
        final PropertyDescriptor dynamicProp1 = new PropertyDescriptor.Builder()
            .dynamic(true)
            .name("command.argument.1")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
        runner.setProperty(dynamicProp1, TEST_DYNAMIC_ENVIRONMENT.toString());
        runner.setProperty(ExecuteStreamCommand.PUT_OUTPUT_IN_ATTRIBUTE, "executeStreamCommand.output");
        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP);
        final String result = flowFiles.getFirst().getAttribute("executeStreamCommand.output");
        final Set<String> dynamicEnvironmentVariables = new HashSet<>(Arrays.asList(result.split("\r?\n")));
        final Set<String> expectedEnvironmentVariables = Set.of("NIFI_TEST_1=testvalue1", "NIFI_TEST_2=testvalue2");
        assertTrue(dynamicEnvironmentVariables.containsAll(expectedEnvironmentVariables));
    }

    @Test
    public void testQuotedArguments() {
        List<String> args = ArgumentUtils.splitArgs("echo -n \"arg1 arg2 arg3\"", ' ');
        assertEquals(3, args.size());
        args = ArgumentUtils.splitArgs("echo;-n;\"arg1 arg2 arg3\"", ';');
        assertEquals(3, args.size());
    }

    @Test
    public void testInvalidDelimiter() {
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, "echo");
        runner.assertValid();
        runner.setProperty(ExecuteStreamCommand.ARG_DELIMITER, "foo");
        runner.assertNotValid();
        runner.setProperty(ExecuteStreamCommand.ARG_DELIMITER, "f");
        runner.assertValid();
    }

    @Test
    public void testExecuteJavaFilePutToAttributeBadPath() {
        final Path javaFile = JAVA_FILES_DIR.resolve(NO_SUCH_FILE);
        final String content = "Print Me";
        runner.enqueue(content);
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, JAVA_COMMAND);
        runner.setProperty(ExecuteStreamCommand.EXECUTION_ARGUMENTS, javaFile.toString());
        runner.setProperty(ExecuteStreamCommand.PUT_OUTPUT_IN_ATTRIBUTE, "executeStreamCommand.output");
        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 0);
        runner.assertTransferCount(ExecuteStreamCommand.NONZERO_STATUS_RELATIONSHIP, 0);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP);
        final MockFlowFile outputFlowFile = flowFiles.getFirst();
        final String result = outputFlowFile.getAttribute("executeStreamCommand.output");
        outputFlowFile.assertContentEquals(content);
        assertTrue(result.isEmpty()); // java with bad path only prints to standard error not standard out
        assertEquals("1", outputFlowFile.getAttribute("execution.status")); // java -jar with bad path exits with code 1
        assertEquals(JAVA_COMMAND, outputFlowFile.getAttribute("execution.command"));
        assertEquals(javaFile.toString(), outputFlowFile.getAttribute("execution.command.args"));
    }

    @Test
    public void testExecuteJavaFilePutToAttributeBadPathDynamicProperties() {
        final Path javaFile = JAVA_FILES_DIR.resolve(NO_SUCH_FILE);
        final String content = "Print Me";
        runner.enqueue(content);
        runner.setProperty(ExecuteStreamCommand.EXECUTION_COMMAND, JAVA_COMMAND);
        runner.setProperty(ExecuteStreamCommand.ARGUMENTS_STRATEGY, ExecuteStreamCommand.DYNAMIC_PROPERTY_ARGUMENTS_STRATEGY.getValue());
        final PropertyDescriptor dynamicProp1 = new PropertyDescriptor.Builder()
            .dynamic(true)
            .name("command.argument.1")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
        runner.setProperty(dynamicProp1, javaFile.toString());
        runner.setProperty(ExecuteStreamCommand.PUT_OUTPUT_IN_ATTRIBUTE, "executeStreamCommand.output");
        runner.run(1);
        runner.assertTransferCount(ExecuteStreamCommand.OUTPUT_STREAM_RELATIONSHIP, 0);
        runner.assertTransferCount(ExecuteStreamCommand.NONZERO_STATUS_RELATIONSHIP, 0);
        runner.assertTransferCount(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP, 1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStreamCommand.ORIGINAL_RELATIONSHIP);
        final MockFlowFile outputFlowFile = flowFiles.getFirst();
        final String result = outputFlowFile.getAttribute("executeStreamCommand.output");
        outputFlowFile.assertContentEquals(content);
        assertTrue(result.isEmpty()); // java with bad path only prints to standard error not standard out
        assertEquals("1", outputFlowFile.getAttribute("execution.status")); // java -jar with bad path exits with code 1
        assertEquals(JAVA_COMMAND, outputFlowFile.getAttribute("execution.command"));
        assertEquals(javaFile.toString(), outputFlowFile.getAttribute("execution.command.args"));
        final String attribute = outputFlowFile.getAttribute("execution.command.args");
        assertEquals(javaFile.toString(), attribute);
    }

    @Test
    void testMigrateProperties() {
        final Map<String, String> expectedRenamed =
                Map.of("argumentsStrategy", ExecuteStreamCommand.ARGUMENTS_STRATEGY.getName());

        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        assertEquals(expectedRenamed, propertyMigrationResult.getPropertiesRenamed());
    }

    private static boolean isWindows() {
        return System.getProperty("os.name").toLowerCase().startsWith("windows");
    }
}
