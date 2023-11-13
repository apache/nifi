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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.mock.MockProcessorInitializationContext;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.python.ControllerServiceTypeLookup;
import org.apache.nifi.python.PythonBridge;
import org.apache.nifi.python.PythonBridgeInitializationContext;
import org.apache.nifi.python.PythonProcessConfig;
import org.apache.nifi.python.PythonProcessorDetails;
import org.apache.nifi.python.processor.FlowFileTransformProxy;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockPropertyValue;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

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
            .pythonLogsDirectory(logsDir)
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
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.py4j", "DEBUG");

        bridge.discoverExtensions();

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
        assertEquals(new File("target/python/extensions/ConvertCsvToExcel.py").getAbsolutePath(),
            new File(convertCsvToExcel.getSourceLocation()).getAbsolutePath());
    }

    @Test
    public void testMultipleProcesses() throws IOException {
        // Create a PrettyPrintJson Processor
        final byte[] jsonContent = Files.readAllBytes(Paths.get("src/test/resources/json/input/simple-person.json"));
        for (int i=0; i < 3; i++) {
            final FlowFileTransformProxy wrapper = createFlowFileTransform(PRETTY_PRINT_JSON);
            final TestRunner runner = TestRunners.newTestRunner(wrapper);

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
        final FlowFileTransformProxy wrapper = createFlowFileTransform(PRETTY_PRINT_JSON);
        final TestRunner runner = TestRunners.newTestRunner(wrapper);

        final int flowFileCount = 100_000;
        final int threadCount = 12;

        final byte[] jsonContent = Files.readAllBytes(Paths.get("src/test/resources/json/input/simple-person.json"));
        for (int i=0; i < flowFileCount; i++) {
            runner.enqueue(jsonContent);
        }

        runner.setThreadCount(threadCount);
        runner.run(flowFileCount);
        runner.assertAllFlowFilesTransferred("success", flowFileCount);
    }


    @Test
    public void testSimplePrettyPrint() throws IOException {
        // Setup
        final FlowFileTransformProxy wrapper = createFlowFileTransform(PRETTY_PRINT_JSON);
        final TestRunner runner = TestRunners.newTestRunner(wrapper);
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
        final FlowFileTransformProxy wrapper = createFlowFileTransform(PRETTY_PRINT_JSON);
        final TestRunner runner = TestRunners.newTestRunner(wrapper);

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
        final FlowFileTransformProxy wrapper = createFlowFileTransform("ConvertCsvToExcel");
        final TestRunner runner = TestRunners.newTestRunner(wrapper);
        runner.enqueue("name, number\nJohn Doe, 500");

        // Trigger the processor
        runner.run();
        runner.assertTransferCount("original", 1);
        runner.assertTransferCount("success", 1);
    }

    @Test
    public void testExpressionLanguageWithAttributes() {
        // Setup
        final FlowFileTransformProxy wrapper = createFlowFileTransform("WritePropertyToFlowFile");
        final TestRunner runner = TestRunners.newTestRunner(wrapper);
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
        final FlowFileTransformProxy wrapper = createFlowFileTransform("WriteNumber");
        final TestRunner runner = TestRunners.newTestRunner(wrapper);
        runner.enqueue("");

        // Trigger the processor
        runner.run();
        runner.assertTransferCount("original", 1);
        runner.assertTransferCount("success", 1);
        final String content = runner.getFlowFilesForRelationship("success").get(0).getContent();
        final int resultNum = Integer.parseInt(content);
        assertTrue(resultNum >= 0);
        assertTrue(resultNum <= 1000);
    }

    private FlowFileTransformProxy createFlowFileTransform(final String type) {
        final Processor processor = createProcessor(type);
        assertNotNull(processor);

        processor.initialize(new MockProcessorInitializationContext());
        return (FlowFileTransformProxy) processor;
    }

    @Test
    public void testImportRequirements() {
        // Discover extensions so that they can be created
        bridge.discoverExtensions();

        final PythonProcessorDetails writeNumpyVersionDetails = bridge.getProcessorTypes().stream()
            .filter(details -> details.getProcessorType().equals("WriteNumpyVersion"))
            .findAny()
            .orElseThrow(() -> new RuntimeException("Could not find WriteNumpyVersion"));

        final List<String> dependencies = writeNumpyVersionDetails.getDependencies();
        assertEquals(1, dependencies.size());
        assertEquals("numpy==1.25.0", dependencies.get(0));

        // Setup
        final FlowFileTransformProxy wrapper = createFlowFileTransform("WriteNumpyVersion");
        final TestRunner runner = TestRunners.newTestRunner(wrapper);
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
        final FlowFileTransformProxy wrapper = createFlowFileTransform("LookupAddress");
        final TestRunner runner = TestRunners.newTestRunner(wrapper);
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
        final FlowFileTransformProxy wrapper = createFlowFileTransform("WriteMessage");
        final TestRunner runner = TestRunners.newTestRunner(wrapper);
        runner.enqueue("");

        // Trigger the processor
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
        bridge.discoverExtensions();

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
        final FlowFileTransformProxy wrapperV1 = createFlowFileTransform("WriteMessage");
        final TestRunner runnerV1 = TestRunners.newTestRunner(wrapperV1);
        runnerV1.enqueue("");

        // Trigger the processor
        runnerV1.run();
        runnerV1.assertTransferCount("success", 1);
        runnerV1.assertTransferCount("original", 1);
        runnerV1.getFlowFilesForRelationship("success").get(0).assertContentEquals("Hello, World");

        // Create an instance of WriteMessage V2
        final Processor procV2 = createProcessor("WriteMessage", "0.0.2-SNAPSHOT");
        final TestRunner runnerV2 = TestRunners.newTestRunner(procV2);
        runnerV2.enqueue("");

        // Trigger the processor
        runnerV2.run();
        runnerV2.assertTransferCount("success", 1);
        runnerV2.assertTransferCount("original", 1);
        runnerV2.getFlowFilesForRelationship("success").get(0).assertContentEquals("Hello, World 2");
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

    private ProcessContext createContext(final Map<PropertyDescriptor, String> propertyValues) {
        final ProcessContext context = Mockito.mock(ProcessContext.class);

        when(context.getProperties()).thenReturn(propertyValues);
        when(context.getProperty(any(String.class))).thenAnswer(new Answer<>() {
            @Override
            public Object answer(final InvocationOnMock invocationOnMock) {
                final String name = invocationOnMock.getArgument(0, String.class);
                final PropertyDescriptor descriptor = new PropertyDescriptor.Builder().name(name).build();
                final String stringValue = propertyValues.get(descriptor);
                return new MockPropertyValue(stringValue);
            }
        });

        return context;
    }

    private TestRunner createRecordTransformRunner(final String type) throws InitializationException {
        final Processor processor = createProcessor("SetRecordField");
        assertNotNull(processor);

        final JsonTreeReader reader = new JsonTreeReader();
        final JsonRecordSetWriter writer = new JsonRecordSetWriter();

        final TestRunner runner = TestRunners.newTestRunner(processor);
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


    private RecordSchema createSimpleRecordSchema(final String... fieldNames) {
        return createSimpleRecordSchema(Arrays.asList(fieldNames));
    }

    private RecordSchema createSimpleRecordSchema(final List<String> fieldNames) {
        final List<RecordField> recordFields = new ArrayList<>();
        for (final String fieldName : fieldNames) {
            recordFields.add(new RecordField(fieldName, RecordFieldType.STRING.getDataType(), true));
        }

        final RecordSchema schema = new SimpleRecordSchema(recordFields);
        return schema;
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

    private Processor createProcessor(final String type) {
        return createProcessor(type, VERSION);
    }

    private Processor createProcessor(final String type, final String version) {
        bridge.discoverExtensions();
        final AsyncLoadedProcessor processor = bridge.createProcessor(createId(), type, version, true);

        final ProcessorInitializationContext initContext = new MockProcessorInitializationContext();
        processor.initialize(initContext);

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
        processor.initialize(new MockProcessorInitializationContext());
        return processor;
    }

}
