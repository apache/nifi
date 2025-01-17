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

package org.apache.nifi.py4j;

import org.apache.nifi.components.AsyncLoadedProcessor;
import org.apache.nifi.components.AsyncLoadedProcessor.LoadState;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.python.ControllerServiceTypeLookup;
import org.apache.nifi.python.PythonBridge;
import org.apache.nifi.python.PythonBridgeInitializationContext;
import org.apache.nifi.python.PythonProcessConfig;
import org.apache.nifi.python.PythonProcessorDetails;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.DisabledOnOs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PythonControllerInteractionIT {
    private static PythonBridge bridge;
    private static final String PRETTY_PRINT_JSON = "PrettyPrintJson";
    private static final String VERSION = "0.0.1-SNAPSHOT";

    private static final Map<String, Class<? extends ControllerService>> controllerServiceMap = new HashMap<>();

    @BeforeAll
    public static void launchPython() throws IOException {
        final File target = new File("target");
        final File pythonDir = new File(target, "python");
        final File frameworkDir = new File(pythonDir, "framework");
        final File extensionsDir = new File(pythonDir, "extensions");
        final File logsDir = new File(pythonDir, "logs");

        final PythonProcessConfig config = new PythonProcessConfig.Builder()
            .pythonFrameworkDirectory(frameworkDir)
            .pythonExtensionsDirectories(Collections.singleton(extensionsDir))
            .pythonWorkingDirectory(new File("./target/python/work"))
            .commsTimeout(Duration.ofSeconds(0))
            .maxPythonProcessesPerType(25)
            .maxPythonProcesses(100)
            .build();

        Files.createDirectories(logsDir.toPath());
        final File logFile = new File(logsDir, "python.log");
        if (logFile.exists()) {
            logFile.delete();
        }

        final PythonBridgeInitializationContext initializationContext = new PythonBridgeInitializationContext() {
            @Override
            public PythonProcessConfig getPythonProcessConfig() {
                return config;
            }

            @Override
            public ControllerServiceTypeLookup getControllerServiceTypeLookup() {
                return controllerServiceMap::get;
            }

            @Override
            public Supplier<Set<File>> getNarDirectoryLookup() {
                return Collections::emptySet;
            }
        };

        bridge = new StandardPythonBridge();
        bridge.initialize(initializationContext);
        bridge.start();
    }

    @AfterEach
    public void cleanup() {
        controllerServiceMap.clear();
    }

    @AfterAll
    public static void shutdownPython() {
        if (bridge != null) {
            bridge.shutdown();
        }
    }


    @Test
    public void testStartAndStop() {
        // Do nothing. Just use the @BeforeAll / @AfterAll to handle the start & stop.
    }

    @Test
    public void testPing() throws IOException {
        bridge.ping();
    }


    @Test
    public void testGetProcessorDetails() {
        bridge.discoverExtensions(true);

        final List<PythonProcessorDetails> extensionDetails = bridge.getProcessorTypes();
        final List<String> types = extensionDetails.stream()
            .map(PythonProcessorDetails::getProcessorType)
            .toList();

        assertTrue(types.contains(PRETTY_PRINT_JSON));
        assertTrue(types.contains("ConvertCsvToExcel"));

        final PythonProcessorDetails convertCsvToExcel = extensionDetails.stream()
            .filter(details -> details.getProcessorType().equals("ConvertCsvToExcel"))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("Could not find ConvertCsvToExcel"));

        assertEquals("0.0.1-SNAPSHOT", convertCsvToExcel.getProcessorVersion());
        assertEquals(List.of("csv", "excel"), convertCsvToExcel.getTags());
        assertEquals(new File("target/python/extensions/ConvertCsvToExcel.py").getAbsolutePath(),
            new File(convertCsvToExcel.getSourceLocation()).getAbsolutePath());
    }

    @Test
    public void testMultipleProcesses() throws IOException {
        // Create a PrettyPrintJson Processor
        final byte[] jsonContent = Files.readAllBytes(Paths.get("src/test/resources/json/input/simple-person.json"));
        for (int i = 0; i < 3; i++) {
            final TestRunner runner = createFlowFileTransform(PRETTY_PRINT_JSON);

            runner.enqueue(jsonContent);
            runner.run();
            runner.assertTransferCount("original", 1);
            runner.assertTransferCount("success", 1);
        }
    }

    @Test
    @Disabled("Just for manual testing...")
    public void runPrettyPrintJsonManyThreads() throws IOException {
        // Create a PrettyPrintJson Processor
        final TestRunner runner = createFlowFileTransform(PRETTY_PRINT_JSON);

        final int flowFileCount = 100_000;
        final int threadCount = 12;

        final byte[] jsonContent = Files.readAllBytes(Paths.get("src/test/resources/json/input/simple-person.json"));
        for (int i = 0; i < flowFileCount; i++) {
            runner.enqueue(jsonContent);
        }

        runner.setThreadCount(threadCount);
        runner.run(flowFileCount);
        runner.assertAllFlowFilesTransferred("success", flowFileCount);
    }


    @Test
    public void testSimplePrettyPrint() throws IOException {
        // Setup
        final TestRunner runner = createFlowFileTransform(PRETTY_PRINT_JSON);
        runner.enqueue(Paths.get("src/test/resources/json/input/simple-person.json"));
        runner.setProperty("Indentation", "2");

        // Trigger the processor
        runner.run();
        runner.assertTransferCount("original", 1);
        runner.assertTransferCount("success", 1);
        final MockFlowFile indent2Output = runner.getFlowFilesForRelationship("success").get(0);

        // Validate its output
        assertNotNull(indent2Output.getAttribute("uuid"));
        assertNotNull(indent2Output.getAttribute("filename"));
        indent2Output.assertContentEquals(Paths.get("src/test/resources/json/output/simple-person-pretty-2.json"));
    }

    @Test
    public void testValidator() {
        final TestRunner runner = createFlowFileTransform(PRETTY_PRINT_JSON);

        runner.setProperty("Indentation", "-1");
        runner.assertNotValid();

        runner.setProperty("Indentation", "Hello");
        runner.assertNotValid();

        runner.setProperty("Indentation", "");
        runner.assertNotValid();

        runner.setProperty("Indentation", String.valueOf( ((long) Integer.MAX_VALUE) + 1 ));
        runner.assertNotValid();

        runner.setProperty("Indentation", "4");
        runner.assertValid();
    }

    @Test
    public void testCsvToExcel() {
        // Create a PrettyPrintJson Processor
        final TestRunner runner = createFlowFileTransform("ConvertCsvToExcel");
        runner.enqueue("name, number\nJohn Doe, 500");

        // Trigger the processor
        waitForValid(runner);
        runner.run();
        runner.assertTransferCount("original", 1);
        runner.assertTransferCount("success", 1);
    }

    @Test
    public void testExpressionLanguageWithAttributes() {
        // Setup
        final TestRunner runner = createFlowFileTransform("WritePropertyToFlowFile");
        runner.setProperty("Message", "Hola Mundo");
        runner.enqueue("Hello World");

        // Trigger the processor
        runner.run();
        runner.assertTransferCount("original", 1);
        runner.assertTransferCount("success", 1);
        runner.getFlowFilesForRelationship("success").get(0).assertContentEquals("Hola Mundo");
    }

    @Test
    public void testPythonPackage() {
        // Create a WriteNumber Processor
        final TestRunner runner = createFlowFileTransform("WriteNumber");
        runner.enqueue("");

        // Trigger the processor
        waitForValid(runner);
        runner.run();
        runner.assertTransferCount("original", 1);
        runner.assertTransferCount("success", 1);
        final String content = runner.getFlowFilesForRelationship("success").get(0).getContent();
        final int resultNum = Integer.parseInt(content);
        assertTrue(resultNum >= 0);
        assertTrue(resultNum <= 1000);
    }

    private TestRunner createFlowFileTransform(final String type) {
        return createProcessor(type);
    }

    @Test
    @Disabled("requires specific local env config...")
    public void testImportRequirements() {
        // Discover extensions so that they can be created
        bridge.discoverExtensions(true);

        final PythonProcessorDetails writeNumpyVersionDetails = bridge.getProcessorTypes().stream()
            .filter(details -> details.getProcessorType().equals("WriteNumpyVersion"))
            .findAny()
            .orElseThrow(() -> new RuntimeException("Could not find WriteNumpyVersion"));

        final List<String> dependencies = writeNumpyVersionDetails.getDependencies();
        assertEquals(1, dependencies.size());
        assertEquals("numpy==1.25.0", dependencies.get(0));

        // Setup
        final TestRunner runner = createFlowFileTransform("WriteNumpyVersion");
        runner.enqueue("Hello World");

        // Trigger the processor
        runner.run();
        runner.assertTransferCount("original", 1);
        runner.assertTransferCount("success", 1);
        runner.getFlowFilesForRelationship("success").get(0).assertContentEquals("1.25.0");
    }


    @Test
    public void testControllerService() throws InitializationException {
        // Setup
        controllerServiceMap.put("StringLookupService", TestLookupService.class);
        final TestRunner runner = createFlowFileTransform("LookupAddress");
        final StringLookupService lookupService = new TestLookupService((Collections.singletonMap("John Doe", "123 My Street")));
        runner.addControllerService("lookup", lookupService);
        runner.enableControllerService(lookupService);

        runner.setProperty("Lookup", "lookup");
        runner.enqueue("{\"name\":\"John Doe\"}");

        runner.run();
        runner.assertTransferCount("original", 1);
        runner.assertTransferCount("success", 1);

        final MockFlowFile output = runner.getFlowFilesForRelationship("success").get(0);
        assertTrue(output.getContent().contains("123 My Street"));
    }

    @Test
    public void testReload() throws IOException, InterruptedException {
        final File sourceFile = new File("target/python/extensions/WriteMessage.py");

        final String originalMessage = "Hello, World";
        final String replacement = "Hola, Mundo";

        // Ensure that we started with "Hello, World" because if the test is run multiple times, we may already be starting with the modified version
        replaceFileText(sourceFile, replacement, originalMessage);

        // Setup
        final TestRunner runner = createFlowFileTransform("WriteMessage");
        runner.enqueue("");

        // Trigger the processor
        waitForValid(runner);
        runner.run();
        runner.assertTransferCount("original", 1);
        runner.assertTransferCount("success", 1);
        runner.getFlowFilesForRelationship("success").get(0).assertContentEquals(originalMessage);

        // Wait a bit because some file systems only have second-precision timestamps so wait a little more than 1 second
        // (to account for imprecision of the Thread.sleep method) to ensure
        // that when we write to the file that the lastModified timestamp will be different.
        Thread.sleep(1300L);

        // Change the source code of the WriteMessage.py class to write a different message.
        replaceFileText(sourceFile, originalMessage, replacement);

        // Reload the processor and run again
        runner.enqueue("");
        runner.clearTransferState();
        runner.run();

        // Ensure that the output is correct
        runner.assertTransferCount("original", 1);
        runner.assertTransferCount("success", 1);
        runner.getFlowFilesForRelationship("success").get(0).assertContentEquals(replacement);
    }

    private void replaceFileText(final File file, final String text, final String replacement) throws IOException {
        final byte[] sourceBytes = Files.readAllBytes(file.toPath());
        final String source = new String(sourceBytes, StandardCharsets.UTF_8);
        final String modifiedSource = source.replace(text, replacement);

        // We have to use a FileOutputStream rather than Files.write() because we need to get the FileChannel and call force() to fsync.
        // Otherwise, we have a threading issue in which the file may be reloaded before the Operating System flushes the contents to disk.
        try (final FileOutputStream fos = new FileOutputStream(file)) {
            fos.write(modifiedSource.getBytes(StandardCharsets.UTF_8));
            fos.getChannel().force(false);
        }
    }

    @Test
    public void testMultipleVersions() throws IOException {
        // If the testReload() test runs first, the contents of the WriteMessage.py file may have changed to write "Hola, Mundo" instead of "Hello, World".
        // So we need to ensure that it's updated appropriately before beginning.
        final File sourceFile = new File("target/python/extensions/WriteMessage.py");
        replaceFileText(sourceFile, "Hola, Mundo", "Hello, World");

        // Discover extensions so that they can be created
        bridge.discoverExtensions(true);

        // Ensure that we find 2 different versions of the WriteMessage Processor.
        final List<PythonProcessorDetails> processorTypes = bridge.getProcessorTypes();
        final long v1Count = processorTypes.stream()
            .filter(details -> details.getProcessorType().equals("WriteMessage"))
            .filter(details -> details.getProcessorVersion().equals(VERSION))
            .count();
        assertEquals(1, v1Count);

        final long v2Count = processorTypes.stream()
            .filter(details -> details.getProcessorType().equals("WriteMessage"))
            .filter(details -> details.getProcessorVersion().equals("0.0.2-SNAPSHOT"))
            .count();
        assertEquals(1, v2Count);

        // Create a WriteMessage Processor, version 0.0.1-SNAPSHOT
        final TestRunner runnerV1 = createFlowFileTransform("WriteMessage");
        runnerV1.enqueue("");

        // Trigger the processor
        waitForValid(runnerV1);
        runnerV1.run();
        runnerV1.assertTransferCount("success", 1);
        runnerV1.assertTransferCount("original", 1);
        runnerV1.getFlowFilesForRelationship("success").get(0).assertContentEquals("Hello, World");

        // Create an instance of WriteMessage V2
        final TestRunner runnerV2 = createProcessor("WriteMessage", "0.0.2-SNAPSHOT");
        runnerV2.enqueue("");

        // Trigger the processor
        waitForValid(runnerV2);
        runnerV2.run();
        runnerV2.assertTransferCount("success", 1);
        runnerV2.assertTransferCount("original", 1);
        runnerV2.getFlowFilesForRelationship("success").get(0).assertContentEquals("Hello, World 2");
    }

    private void waitForValid(final TestRunner runner) {
        final long maxTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(60L);
        while (System.currentTimeMillis() < maxTime) {
            if (runner.isValid()) {
                return;
            }

            try {
                Thread.sleep(10L);
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for processor to be valid");
            }
        }
    }

    @Test
    public void testRecordTransformWithDynamicProperties() throws InitializationException {
        // Create a SetRecordField Processor
        final TestRunner runner = createRecordTransformRunner("SetRecordField");
        runner.setProperty("name", "Jane Doe");
        runner.setProperty("number", "8");

        // Create a Record to transform and transform it
        final String json = "[{ \"name\": \"John Doe\" }]";
        runner.enqueue(json);
        runner.run();
        runner.assertTransferCount("original", 1);
        runner.assertTransferCount("success", 1);

        // Verify the results
        final MockFlowFile out = runner.getFlowFilesForRelationship("success").get(0);
        out.assertContentEquals("""
            [{"name":"Jane Doe","number":"8"}]""");
    }


    private TestRunner createRecordTransformRunner(final String type) throws InitializationException {
        final TestRunner runner = createProcessor("SetRecordField");
        runner.setValidateExpressionUsage(false);

        final JsonTreeReader reader = new JsonTreeReader();
        final JsonRecordSetWriter writer = new JsonRecordSetWriter();

        runner.addControllerService("reader", reader);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(reader);
        runner.enableControllerService(writer);
        runner.setProperty("Record Reader", "reader");
        runner.setProperty("Record Writer", "writer");

        return runner;
    }

    @Test
    public void testRecordTransformWithInnerRecord() throws InitializationException {
        // Create a SetRecordField Processor
        final TestRunner runner = createRecordTransformRunner("SetRecordField");
        runner.setProperty("name", "Jane Doe");

        // Create a Record to transform and transform it
        final String json = "[{\"name\": \"Jake Doe\", \"father\": { \"name\": \"John Doe\" }}]";
        runner.enqueue(json);
        runner.run();

        // Verify the results
        runner.assertTransferCount("success", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship("success").get(0);
        out.assertContentEquals("""
            [{"name":"Jane Doe","father":{"name":"John Doe"}}]""");
    }


    @Test
    public void testCustomRelationships() {
        final TestRunner runner = createFlowFileTransform("RouteFlowFile");

        final Set<Relationship> relationships = runner.getProcessor().getRelationships();
        assertEquals(4, relationships.size());
        assertTrue(relationships.stream().anyMatch(rel -> rel.getName().equals("small")));
        assertTrue(relationships.stream().anyMatch(rel -> rel.getName().equals("large")));
        assertTrue(relationships.stream().anyMatch(rel -> rel.getName().equals("original")));
        assertTrue(relationships.stream().anyMatch(rel -> rel.getName().equals("failure")));

        final MockFlowFile smallInputFlowFile = runner.enqueue(new byte[25]);
        final MockFlowFile largeInputFlowFile = runner.enqueue(new byte[75 * 1024]);
        runner.run(2);

        runner.assertTransferCount("original", 2);
        runner.assertTransferCount("small", 1);
        runner.assertTransferCount("large", 1);
        runner.assertTransferCount("failure", 0);
        final FlowFile largeOutputFlowFile = runner.getFlowFilesForRelationship("large").getFirst();
        assertEquals(largeInputFlowFile.getId(), largeOutputFlowFile.getId(), "Large Transformed Flow File should be the same as inbound");
        final FlowFile smallOutputFlowFile = runner.getFlowFilesForRelationship("small").getFirst();
        assertEquals(smallInputFlowFile.getId(), smallOutputFlowFile.getId(), "Small Transformed Flow File should be the same as inbound");
    }

    @Test
    @Timeout(45)
    @DisabledOnOs(org.junit.jupiter.api.condition.OS.WINDOWS) // Cannot run on windows because ExitAfterFourInvocations uses `kill -9` command
    public void testProcessRestarted() {
        final TestRunner runner = createFlowFileTransform("ExitAfterFourInvocations");

        for (int i = 0; i < 10; i++) {
            runner.enqueue(Integer.toString(i));
        }

        runner.run(4);
        assertThrows(Throwable.class, runner::run);

        // Run 2 additional times. Because the Python Process will have to be restarted, it may take a bit,
        // so we keep trying until we succeed, relying on the 15 second timeout for the test to fail us if
        // the Process doesn't get restarted in time.
        for (int i = 0; i < 2; i++) {
            while (true) {
                try {
                    runner.run(1, false, i == 0);
                    break;
                } catch (final Throwable t) {
                    try {
                        Thread.sleep(1000L);
                    } catch (final InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        // Trigger stop on finish
        runner.run(1, true, false);

        runner.assertTransferCount("success", 7);
    }


    @Test
    public void testRouteToFailureWithAttributes() {
        final TestRunner runner = createFlowFileTransform("FailWithAttributes");
        runner.enqueue("Hello, World");
        runner.run();

        runner.assertAllFlowFilesTransferred("failure", 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship("failure").getFirst();
        out.assertAttributeEquals("number", "1");
        out.assertAttributeEquals("failureReason", "Intentional failure of unit test");
    }

    @Test
    public void testCreateFlowFile() throws IOException {
        final String processorName = "CreateFlowFile";
        final String propertyName = "FlowFile Contents";
        final String relationshipSuccess = "success";
        final String relationshipMultiline = "multiline";

        final String singleLineContent = "Hello World!";
        testSourceProcessor(processorName,
                Map.of(propertyName, singleLineContent),
                Map.of(relationshipSuccess, 1, relationshipMultiline, 0),
                relationshipSuccess,
                singleLineContent.getBytes(StandardCharsets.UTF_8));

        final String multiLineContent = "Hello\nWorld!";
        testSourceProcessor(processorName,
                Map.of(propertyName, multiLineContent),
                Map.of(relationshipSuccess, 0, relationshipMultiline, 1),
                relationshipMultiline,
                multiLineContent.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testCreateNothing() {
        // Test the use-case where the source processor returns with None.
        final TestRunner runner = createProcessor("CreateNothing");
        waitForValid(runner);
        runner.run();

        runner.assertTransferCount("success", 0);
    }

    @Test
    public void testMultipleProcessorsInAPackage() {
        // This processor is in a package with another processor, which has additional dependency requirements
        final TestRunner runner = createProcessor("CreateHttpRequest");
        waitForValid(runner);
        runner.run();

        runner.assertTransferCount("success", 1);
    }

    @Test
    public void testStateManagerSetState() {
        final TestRunner runner = createStateManagerTesterProcessor("setState");

        final MockStateManager stateManager = runner.getStateManager();
        stateManager.assertStateNotSet();

        runner.run();

        stateManager.assertStateSet(Scope.CLUSTER);
        stateManager.assertStateEquals(Map.of("state_key_1", "state_value_1"), Scope.CLUSTER);
        runner.assertTransferCount("success", 1);
    }

    @Test
    public void testStateManagerGetState() throws IOException {
        final TestRunner runner = createStateManagerTesterProcessor("getState");

        final MockStateManager stateManager = initializeStateManager(runner);

        runner.run();

        // The processor reads the state and adds the key-value pairs
        // to the FlowFile as attributes.
        stateManager.assertStateEquals(Map.of("state_key_1", "state_value_1"), Scope.CLUSTER);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship("success").get(0);
        flowFile.assertAttributeEquals("state_key_1", "state_value_1");
    }

    @Test
    public void testStateManagerReplace() throws IOException {
        final TestRunner runner = createStateManagerTesterProcessor("replace");

        final MockStateManager stateManager = initializeStateManager(runner);

        runner.run();

        final Map finalState = Map.of("state_key_2", "state_value_2");
        stateManager.assertStateEquals(finalState, Scope.CLUSTER);
        runner.assertTransferCount("success", 1);
    }

    @Test
    public void testStateManagerClear() throws IOException {
        final TestRunner runner = createStateManagerTesterProcessor("clear");

        final MockStateManager stateManager = initializeStateManager(runner);

        runner.run();

        stateManager.assertStateEquals(Map.of(), Scope.CLUSTER);
        runner.assertTransferCount("success", 1);
    }

    @Test
    public void testStateManagerExceptionHandling() {
        // Use-case tested: StateManager's exception can be caught
        // in the Python code and execution continued without producing errors.
        final TestRunner runner = createProcessor("TestStateManagerException");
        waitForValid(runner);

        final MockStateManager stateManager = runner.getStateManager();
        stateManager.setFailOnStateSet(Scope.CLUSTER, true);

        runner.run();

        runner.assertTransferCount("success", 1);
        runner.getFlowFilesForRelationship("success").get(0).assertAttributeEquals("exception_msg", "Set state failed");
    }

    public interface StringLookupService extends ControllerService {
        Optional<String> lookup(Map<String, String> coordinates);
    }

    public static class TestLookupService extends AbstractControllerService implements StringLookupService {
        private final Map<String, String> mapping;

        public TestLookupService(final Map<String, String> mapping) {
            this.mapping = mapping;
        }

        @Override
        public Optional<String> lookup(final Map<String, String> coordinates) {
            final String lookupValue = coordinates.get("key");
            return Optional.ofNullable(mapping.get(lookupValue));
        }
    }

    private static String createId() {
        return UUID.randomUUID().toString();
    }

    private TestRunner createProcessor(final String type) {
        return createProcessor(type, VERSION);
    }

    private TestRunner createProcessor(final String type, final String version) {
        bridge.discoverExtensions(true);
        final AsyncLoadedProcessor processor = bridge.createProcessor(createId(), type, version, true, true);

        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setValidateExpressionUsage(false);

        final long maxInitTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30L);
        while (true) {
            final LoadState state = processor.getState();
            if (state == LoadState.FINISHED_LOADING) {
                break;
            }
            if (state == LoadState.DEPENDENCY_DOWNLOAD_FAILED || state == LoadState.LOADING_PROCESSOR_CODE_FAILED) {
                throw new RuntimeException("Failed to initialize processor of type %s version %s".formatted(type, version));
            }

            if (System.currentTimeMillis() > maxInitTime) {
                throw new RuntimeException("Timed out waiting for processor of type %s version %s to initialize".formatted(type, version));
            }

            try {
                Thread.sleep(10L);
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while initializing processor of type %s version %s".formatted(type, version));
            }
        }

        return runner;
    }

    private void testSourceProcessor(final String processorName,
                                     final Map<String, String> propertiesWithValues,
                                     final Map<String, Integer> relationshipsWithFlowFileCounts,
                                     final String expectedOuputRelationship,
                                     final byte[] expectedContent) throws IOException {

        final TestRunner runner = createProcessor(processorName);

        propertiesWithValues.forEach((propertyName, propertyValue) -> {
            runner.setProperty(propertyName, propertyValue);
        });

        waitForValid(runner);
        runner.run();

        relationshipsWithFlowFileCounts.forEach((relationship, count) -> {
            runner.assertTransferCount(relationship, count);
        });

        final MockFlowFile output = runner.getFlowFilesForRelationship(expectedOuputRelationship).get(0);
        output.assertContentEquals(expectedContent);
    }

    private TestRunner createStateManagerTesterProcessor(String methodToTest) {
        final TestRunner runner = createProcessor("TestStateManager");
        runner.setProperty("StateManager Method To Test", methodToTest);
        waitForValid(runner);
        return runner;
    }

    private MockStateManager initializeStateManager(TestRunner runner) throws IOException {
        final MockStateManager stateManager = runner.getStateManager();
        final Map initialState = Map.of("state_key_1", "state_value_1");
        stateManager.setState(initialState, Scope.CLUSTER);
        return stateManager;
    }

}
