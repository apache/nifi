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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.mock.MockComponentLogger;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.python.ControllerServiceTypeLookup;
import org.apache.nifi.python.PythonBridge;
import org.apache.nifi.python.PythonBridgeInitializationContext;
import org.apache.nifi.python.PythonProcessConfig;
import org.apache.nifi.python.PythonProcessorDetails;
import org.apache.nifi.python.processor.EmptyAttributeMap;
import org.apache.nifi.python.processor.FlowFileTransformProxy;
import org.apache.nifi.python.processor.PythonProcessorBridge;
import org.apache.nifi.python.processor.PythonProcessorInitializationContext;
import org.apache.nifi.python.processor.RecordTransform;
import org.apache.nifi.python.processor.RecordTransformResult;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockPropertyValue;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
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
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
            .collect(Collectors.toList());

        assertTrue(types.contains(PRETTY_PRINT_JSON));
        assertTrue(types.contains("ConvertCsvToExcel"));

        final PythonProcessorDetails convertCsvToExcel = extensionDetails.stream()
            .filter(details -> details.getProcessorType().equals("ConvertCsvToExcel"))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("Could not find ConvertCsvToExcel"));

        assertEquals("0.0.1-SNAPSHOT", convertCsvToExcel.getProcessorVersion());
        assertNull(convertCsvToExcel.getPyPiPackageName());
        assertEquals(new File("target/python/extensions/ConvertCsvToExcel.py").getAbsolutePath(),
            new File(convertCsvToExcel.getSourceLocation()).getAbsolutePath());
    }

    @Test
    public void testMultipleProcesses() throws IOException {
        // Create a PrettyPrintJson Processor
        final byte[] jsonContent = Files.readAllBytes(Paths.get("src/test/resources/json/input/simple-person.json"));
        for (int i=0; i < 3; i++) {
            final PythonProcessorBridge prettyPrintJson = createProcessor(PRETTY_PRINT_JSON);
            assertNotNull(prettyPrintJson);

            final FlowFileTransformProxy wrapper = new FlowFileTransformProxy(prettyPrintJson);
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
        final PythonProcessorBridge prettyPrintJson = createProcessor(PRETTY_PRINT_JSON);
        assertNotNull(prettyPrintJson);

        // Setup
        final FlowFileTransformProxy wrapper = new FlowFileTransformProxy(prettyPrintJson);
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
        // Discover extensions so that they can be created
        final PythonProcessorBridge prettyPrintJson = createProcessor(PRETTY_PRINT_JSON);
        assertNotNull(prettyPrintJson);

        // Setup
        final FlowFileTransformProxy wrapper = new FlowFileTransformProxy(prettyPrintJson);
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
        final PythonProcessorBridge prettyPrintJson = createProcessor(PRETTY_PRINT_JSON);
        final FlowFileTransformProxy wrapper = new FlowFileTransformProxy(prettyPrintJson);
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
        final PythonProcessorBridge csvToExcel = createProcessor("ConvertCsvToExcel");
        assertNotNull(csvToExcel);

        // Setup
        final FlowFileTransformProxy wrapper = new FlowFileTransformProxy(csvToExcel);
        final TestRunner runner = TestRunners.newTestRunner(wrapper);
        runner.enqueue("name, number\nJohn Doe, 500");

        // Trigger the processor
        runner.run();
        runner.assertTransferCount("original", 1);
        runner.assertTransferCount("success", 1);
    }

    @Test
    public void testExpressionLanguageWithAttributes() {
        final PythonProcessorBridge writeProperty = createProcessor("WritePropertyToFlowFile");
        assertNotNull(writeProperty);

        // Setup
        final FlowFileTransformProxy wrapper = new FlowFileTransformProxy(writeProperty);
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
        final PythonProcessorBridge procBridge = createProcessor("WriteNumber");
        assertNotNull(procBridge);

        // Setup
        final FlowFileTransformProxy wrapper = new FlowFileTransformProxy(procBridge);
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
        assertEquals("numpy==1.20.0", dependencies.get(0));

        // Create a PrettyPrintJson Processor
        final PythonProcessorBridge writeNumPyVersion = createProcessor("WriteNumpyVersion");
        assertNotNull(writeNumPyVersion);

        // Setup
        final FlowFileTransformProxy wrapper = new FlowFileTransformProxy(writeNumPyVersion);
        final TestRunner runner = TestRunners.newTestRunner(wrapper);
        runner.enqueue("Hello World");

        // Trigger the processor
        runner.run();
        runner.assertTransferCount("original", 1);
        runner.assertTransferCount("success", 1);
        runner.getFlowFilesForRelationship("success").get(0).assertContentEquals("1.20.0");
    }


    @Test
    public void testControllerService() throws InitializationException {
        final PythonProcessorBridge processor = createProcessor("LookupAddress");
        assertNotNull(processor);

        controllerServiceMap.put("StringLookupService", TestLookupService.class);

        // Setup
        final FlowFileTransformProxy wrapper = new FlowFileTransformProxy(processor);
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

        // Create a PrettyPrintJson Processor
        final PythonProcessorBridge processor = createProcessor("WriteMessage");
        processor.initialize(createInitContext());
        assertNotNull(processor);

        // Setup
        final FlowFileTransformProxy wrapper = new FlowFileTransformProxy(processor);
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
        final PythonProcessorBridge procV1 = createProcessor("WriteMessage");
        final FlowFileTransformProxy wrapperV1 = new FlowFileTransformProxy(procV1);
        final TestRunner runnerV1 = TestRunners.newTestRunner(wrapperV1);
        runnerV1.enqueue("");

        // Trigger the processor
        runnerV1.run();
        runnerV1.assertTransferCount("success", 1);
        runnerV1.assertTransferCount("original", 1);
        runnerV1.getFlowFilesForRelationship("success").get(0).assertContentEquals("Hello, World");

        // Create an instance of WriteMessage V2
        final PythonProcessorBridge procV2 = bridge.createProcessor(createId(), "WriteMessage", "0.0.2-SNAPSHOT", true);
        final FlowFileTransformProxy wrapperV2 = new FlowFileTransformProxy(procV2);
        final TestRunner runnerV2 = TestRunners.newTestRunner(wrapperV2);
        runnerV2.enqueue("");

        // Trigger the processor
        runnerV2.run();
        runnerV2.assertTransferCount("success", 1);
        runnerV2.assertTransferCount("original", 1);
        runnerV2.getFlowFilesForRelationship("success").get(0).assertContentEquals("Hello, World 2");
    }

    @Test
    public void testRecordTransformWithDynamicProperties() {
        // Create a PrettyPrintJson Processor
        final PythonProcessorBridge processor = createProcessor("SetRecordField");
        assertNotNull(processor);

        // Mock out ProcessContext to reflect that the processor should set the 'name' field to 'Jane Doe'
        final PropertyDescriptor nameDescriptor = new PropertyDescriptor.Builder()
            .name("name")
            .dynamic(true)
            .addValidator(Validator.VALID)
            .build();
        final PropertyDescriptor numberDescriptor = new PropertyDescriptor.Builder()
            .name("number")
            .dynamic(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

        final Map<PropertyDescriptor, String> propertyMap = new HashMap<>();
        propertyMap.put(nameDescriptor, "Jane Doe");
        propertyMap.put(numberDescriptor, "8");

        final ProcessContext context = createContext(propertyMap);

        // Create a Record to transform and transform it
        final String json = "[{ \"name\": \"John Doe\" }]";
        final RecordSchema schema = createSimpleRecordSchema("name");
        final RecordTransform recordTransform = (RecordTransform) processor.getProcessorAdapter().getProcessor();
        recordTransform.setContext(context);
        final RecordTransformResult result = recordTransform.transformRecord(json, schema, new EmptyAttributeMap()).get(0);

        // Verify the results
        assertEquals("success", result.getRelationship());
        assertNull(result.getSchema());
        assertEquals("{\"name\": \"Jane Doe\", \"number\": \"8\"}", result.getRecordJson());
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

    @Test
    public void testRecordTransformWithInnerRecord() {
        // Create a PrettyPrintJson Processor
        final PythonProcessorBridge processor = createProcessor("SetRecordField");
        assertNotNull(processor);

        // Mock out ProcessContext to reflect that the processor should set the 'name' field to 'Jane Doe'
        final PropertyDescriptor nameDescriptor = new PropertyDescriptor.Builder()
            .name("name")
            .dynamic(true)
            .addValidator(Validator.VALID)
            .build();

        final Map<PropertyDescriptor, String> propertyMap = new HashMap<>();
        propertyMap.put(nameDescriptor, "Jane Doe");
        final ProcessContext context = createContext(propertyMap);

        // Create a Record to transform and transform it
        final String json = "[{\"name\": \"Jake Doe\", \"father\": { \"name\": \"John Doe\" }}]";
        final RecordSchema recordSchema = createTwoLevelRecord().getSchema();
        final RecordTransform recordTransform = (RecordTransform) processor.getProcessorAdapter().getProcessor();
        recordTransform.setContext(context);
        final RecordTransformResult result = recordTransform.transformRecord(json, recordSchema, new EmptyAttributeMap()).get(0);

        // Verify the results
        assertEquals("success", result.getRelationship());

        assertEquals("{\"name\": \"Jane Doe\", \"father\": {\"name\": \"John Doe\"}}", result.getRecordJson());
    }


    @Test
    public void testLogger() {
        bridge.discoverExtensions();

        final String procId = createId();
        final PythonProcessorBridge logContentsBridge = bridge.createProcessor(procId, "LogContents", VERSION, true);

        final ComponentLog logger = Mockito.mock(ComponentLog.class);
        final PythonProcessorInitializationContext initContext = new PythonProcessorInitializationContext() {
            @Override
            public String getIdentifier() {
                return procId;
            }

            @Override
            public ComponentLog getLogger() {
                return logger;
            }
        };

        logContentsBridge.initialize(initContext);

        final TestRunner runner = TestRunners.newTestRunner(logContentsBridge.getProcessorProxy());
        runner.enqueue("Hello World");
        runner.run();

        runner.assertTransferCount("original", 1);
        runner.assertTransferCount("success", 1);
        final ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(logger).info(argumentCaptor.capture());
        assertEquals("Hello World", argumentCaptor.getValue());
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

    private Record createSimpleRecord(final Map<String, Object> values) {
        final List<RecordField> recordFields = new ArrayList<>();
        for (final Map.Entry<String, Object> entry : values.entrySet()) {
            final DataType dataType = DataTypeUtils.inferDataType(entry.getValue(), RecordFieldType.STRING.getDataType());
            recordFields.add(new RecordField(entry.getKey(), dataType, true));
        }

        final RecordSchema schema = new SimpleRecordSchema(recordFields);
        return new MapRecord(schema, values);
    }

    private Record createTwoLevelRecord() {
        final Map<String, Object> innerPersonValues = new HashMap<>();
        innerPersonValues.put("name", "Jake Doe");
        final Record innerPersonRecord = createSimpleRecord(innerPersonValues);

        final Map<String, Object> outerPersonValues = new HashMap<>();
        outerPersonValues.put("name", "John Doe");
        outerPersonValues.put("father", innerPersonRecord);
        final Record outerPersonRecord = createSimpleRecord(outerPersonValues);

        return outerPersonRecord;
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

    private PythonProcessorBridge createProcessor(final String type) {
        bridge.discoverExtensions();
        final PythonProcessorBridge processor = bridge.createProcessor(createId(), type, VERSION, true);
        processor.initialize(createInitContext());
        return processor;
    }

    private PythonProcessorInitializationContext createInitContext() {
        return new PythonProcessorInitializationContext() {
            @Override
            public String getIdentifier() {
                return "unit-test-id";
            }

            @Override
            public ComponentLog getLogger() {
                return new MockComponentLogger();
            }
        };
    }
}
