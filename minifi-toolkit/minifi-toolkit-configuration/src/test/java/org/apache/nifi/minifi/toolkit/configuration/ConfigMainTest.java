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

package org.apache.nifi.minifi.toolkit.configuration;

import org.apache.commons.io.Charsets;
import org.apache.nifi.controller.repository.io.LimitedInputStream;
import org.apache.nifi.minifi.commons.schema.ConfigSchema;
import org.apache.nifi.minifi.commons.schema.ConnectionSchema;
import org.apache.nifi.minifi.commons.schema.ProcessorSchema;
import org.apache.nifi.minifi.commons.schema.RemoteInputPortSchema;
import org.apache.nifi.minifi.commons.schema.common.ConvertableSchema;
import org.apache.nifi.minifi.commons.schema.serialization.SchemaLoader;
import org.apache.nifi.minifi.commons.schema.exception.SchemaLoaderException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import javax.xml.bind.JAXBException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.minifi.toolkit.configuration.ConfigMain.SUCCESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ConfigMainTest {
    @Mock
    PathInputStreamFactory pathInputStreamFactory;

    @Mock
    PathOutputStreamFactory pathOutputStreamFactory;

    ConfigMain configMain;

    String testInput;

    String testOutput;

    @Before
    public void setup() {
        configMain = new ConfigMain(pathInputStreamFactory, pathOutputStreamFactory);
        testInput = "testInput";
        testOutput = "testOutput";
    }

    @Test
    public void testExecuteNoArgs() {
        assertEquals(ConfigMain.ERR_INVALID_ARGS, configMain.execute(new String[0]));
    }

    @Test
    public void testExecuteInvalidCommand() {
        assertEquals(ConfigMain.ERR_INVALID_ARGS, configMain.execute(new String[]{"badCommand"}));
    }

    @Test
    public void testValidateInvalidCommand() {
        assertEquals(ConfigMain.ERR_INVALID_ARGS, configMain.execute(new String[]{ConfigMain.VALIDATE}));
    }

    @Test
    public void testValidateErrorOpeningInput() throws FileNotFoundException {
        when(pathInputStreamFactory.create(testInput)).thenThrow(new FileNotFoundException());
        assertEquals(ConfigMain.ERR_UNABLE_TO_OPEN_INPUT, configMain.execute(new String[]{ConfigMain.VALIDATE, testInput}));
    }

    @Test
    public void testValidateUnableToParseConfig() throws FileNotFoundException {
        when(pathInputStreamFactory.create(testInput)).thenReturn(new ByteArrayInputStream("!@#$%^&".getBytes(Charsets.UTF_8)));
        assertEquals(ConfigMain.ERR_UNABLE_TO_PARSE_CONFIG, configMain.execute(new String[]{ConfigMain.VALIDATE, testInput}));
    }

    @Test
    public void testValidateInvalidConfig() throws FileNotFoundException {
        when(pathInputStreamFactory.create(testInput)).thenAnswer(invocation ->
                ConfigMainTest.class.getClassLoader().getResourceAsStream("config-malformed-field.yml"));
        assertEquals(ConfigMain.ERR_INVALID_CONFIG, configMain.execute(new String[]{ConfigMain.VALIDATE, testInput}));
    }

    @Test
    public void testTransformInvalidCommand() {
        assertEquals(ConfigMain.ERR_INVALID_ARGS, configMain.execute(new String[]{ConfigMain.TRANSFORM}));
    }

    @Test
    public void testValidateSuccess() throws FileNotFoundException {
        when(pathInputStreamFactory.create(testInput)).thenAnswer(invocation ->
                ConfigMainTest.class.getClassLoader().getResourceAsStream("config.yml"));
        assertEquals(SUCCESS, configMain.execute(new String[]{ConfigMain.VALIDATE, testInput}));
    }

    @Test
    public void testValidateV1Success() throws FileNotFoundException {
        when(pathInputStreamFactory.create(testInput)).thenAnswer(invocation ->
                ConfigMainTest.class.getClassLoader().getResourceAsStream("config-v1.yml"));
        assertEquals(SUCCESS, configMain.execute(new String[]{ConfigMain.VALIDATE, testInput}));
    }

    @Test
    public void testTransformErrorOpeningInput() throws FileNotFoundException {
        when(pathInputStreamFactory.create(testInput)).thenThrow(new FileNotFoundException());
        assertEquals(ConfigMain.ERR_UNABLE_TO_OPEN_INPUT, configMain.execute(new String[]{ConfigMain.TRANSFORM, testInput, testOutput}));
    }

    @Test
    public void testTransformErrorOpeningOutput() throws FileNotFoundException {
        when(pathInputStreamFactory.create(testInput)).thenAnswer(invocation ->
                ConfigMainTest.class.getClassLoader().getResourceAsStream("CsvToJson.xml"));
        when(pathOutputStreamFactory.create(testOutput)).thenThrow(new FileNotFoundException());
        assertEquals(ConfigMain.ERR_UNABLE_TO_OPEN_OUTPUT, configMain.execute(new String[]{ConfigMain.TRANSFORM, testInput, testOutput}));
    }

    @Test
    public void testTransformErrorReadingTemplate() throws FileNotFoundException {
        when(pathInputStreamFactory.create(testInput)).thenAnswer(invocation -> new ByteArrayInputStream("malformed xml".getBytes(Charsets.UTF_8)));
        assertEquals(ConfigMain.ERR_UNABLE_TO_READ_TEMPLATE, configMain.execute(new String[]{ConfigMain.TRANSFORM, testInput, testOutput}));
    }

    @Test
    public void testTransformErrorTransformingTemplate() throws FileNotFoundException {
        when(pathInputStreamFactory.create(testInput)).thenAnswer(invocation ->
                new LimitedInputStream(ConfigMainTest.class.getClassLoader().getResourceAsStream("TemplateWithFunnel.xml"), 25));
        assertEquals(ConfigMain.ERR_UNABLE_TO_READ_TEMPLATE, configMain.execute(new String[]{ConfigMain.TRANSFORM, testInput, testOutput}));
    }

    @Test
    public void testTransformSuccess() throws FileNotFoundException {
        when(pathInputStreamFactory.create(testInput)).thenAnswer(invocation ->
                ConfigMainTest.class.getClassLoader().getResourceAsStream("CsvToJson.xml"));
        when(pathOutputStreamFactory.create(testOutput)).thenAnswer(invocation -> new ByteArrayOutputStream());
        assertEquals(SUCCESS, configMain.execute(new String[]{ConfigMain.TRANSFORM, testInput, testOutput}));
    }

    @Test
    public void testTransformRoundTripCsvToJson() throws IOException, JAXBException, SchemaLoaderException {
        transformRoundTrip("CsvToJson");
    }

    @Test
    public void testTransformRoundTripDecompression() throws IOException, JAXBException, SchemaLoaderException {
        transformRoundTrip("DecompressionCircularFlow");
    }

    @Test
    public void testTransformRoundTripInvokeHttp() throws IOException, JAXBException, SchemaLoaderException {
        transformRoundTrip("InvokeHttpMiNiFiTemplateTest");
    }

    @Test
    public void testTransformRoundTripReplaceText() throws IOException, JAXBException, SchemaLoaderException {
        transformRoundTrip("ReplaceTextExpressionLanguageCSVReformatting");
    }

    @Test
    public void testTransformRoundTripStressTestFramework() throws IOException, JAXBException, SchemaLoaderException {
        transformRoundTrip("StressTestFramework");
    }

    @Test
    public void testTransformRoundTripStressTestFrameworkFunnel() throws IOException, JAXBException, SchemaLoaderException {
        transformRoundTrip("StressTestFrameworkFunnel");
    }

    @Test
    public void testTransformRoundTripMultipleRelationships() throws IOException, JAXBException, SchemaLoaderException {
        transformRoundTrip("MultipleRelationships");
    }

    @Test
    public void testTransformRoundTripProcessGroupsAndRemoteProcessGroups() throws IOException, JAXBException, SchemaLoaderException {
        transformRoundTrip("ProcessGroupsAndRemoteProcessGroups");
    }

    @Test
    public void testSuccessTransformProcessGroup() throws IOException, JAXBException, SchemaLoaderException {
        ConfigMain.transformTemplateToSchema(getClass().getClassLoader().getResourceAsStream("TemplateWithProcessGroup.xml")).toMap();
    }

    @Test
    public void testSuccessTransformInputPort() throws IOException, JAXBException, SchemaLoaderException {
        ConfigMain.transformTemplateToSchema(getClass().getClassLoader().getResourceAsStream("TemplateWithOutputPort.xml")).toMap();
    }

    @Test
    public void testSuccessTransformOutputPort() throws IOException, JAXBException, SchemaLoaderException {
        ConfigMain.transformTemplateToSchema(getClass().getClassLoader().getResourceAsStream("TemplateWithInputPort.xml")).toMap();
    }

    @Test
    public void testSuccessTransformFunnel() throws IOException, JAXBException, SchemaLoaderException {
        ConfigMain.transformTemplateToSchema(getClass().getClassLoader().getResourceAsStream("TemplateWithFunnel.xml")).toMap();
    }

    @Test
    public void testUpgradeInputFileNotFoundException() throws FileNotFoundException {
        when(pathInputStreamFactory.create(testInput)).thenThrow(new FileNotFoundException());
        assertEquals(ConfigMain.ERR_UNABLE_TO_OPEN_INPUT, configMain.execute(new String[]{ConfigMain.UPGRADE, testInput, testOutput}));
    }

    @Test
    public void testUpgradeCantLoadSchema() throws FileNotFoundException {
        when(pathInputStreamFactory.create(testInput)).thenReturn(new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException();
            }
        });
        assertEquals(ConfigMain.ERR_UNABLE_TO_PARSE_CONFIG, configMain.execute(new String[]{ConfigMain.UPGRADE, testInput, testOutput}));
    }

    @Test
    public void testUpgradeOutputFileNotFoundException() throws FileNotFoundException {
        when(pathInputStreamFactory.create(testInput)).thenReturn(getClass().getClassLoader().getResourceAsStream("CsvToJson-v1.yml"));
        when(pathOutputStreamFactory.create(testOutput)).thenThrow(new FileNotFoundException());
        assertEquals(ConfigMain.ERR_UNABLE_TO_OPEN_OUTPUT, configMain.execute(new String[]{ConfigMain.UPGRADE, testInput, testOutput}));
    }

    @Test
    public void testUpgradeCantSaveSchema() throws FileNotFoundException {
        when(pathInputStreamFactory.create(testInput)).thenReturn(getClass().getClassLoader().getResourceAsStream("CsvToJson-v1.yml"));
        when(pathOutputStreamFactory.create(testOutput)).thenReturn(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                throw new IOException();
            }
        });
        assertEquals(ConfigMain.ERR_UNABLE_TO_SAVE_CONFIG, configMain.execute(new String[]{ConfigMain.UPGRADE, testInput, testOutput}));
    }

    @Test
    public void testUpgradeInvalidArgs() {
        assertEquals(ConfigMain.ERR_INVALID_ARGS, configMain.execute(new String[]{ConfigMain.UPGRADE}));
    }

    private void transformRoundTrip(String name) throws JAXBException, IOException, SchemaLoaderException {
        Map<String, Object> templateMap = ConfigMain.transformTemplateToSchema(getClass().getClassLoader().getResourceAsStream(name + ".xml")).toMap();
        Map<String, Object> yamlMap = SchemaLoader.loadYamlAsMap(getClass().getClassLoader().getResourceAsStream(name + ".yml"));
        assertNoMapDifferences(templateMap, yamlMap);
        testV1YmlIfPresent(name, yamlMap);
    }

    private InputStream upgradeAndReturn(String name) throws FileNotFoundException {
        InputStream yamlV1Stream = getClass().getClassLoader().getResourceAsStream(name + "-v1.yml");
        if (yamlV1Stream == null) {
            return null;
        }
        when(pathInputStreamFactory.create(testInput)).thenReturn(yamlV1Stream);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        when(pathOutputStreamFactory.create(testOutput)).thenReturn(outputStream);
        assertEquals(SUCCESS, configMain.execute(new String[]{"upgrade", testInput, testOutput}));
        return new ByteArrayInputStream(outputStream.toByteArray());
    }

    private void testV1YmlIfPresent(String name, Map<String, Object> yamlMap) throws IOException, SchemaLoaderException {
        InputStream upgradedInputStream = upgradeAndReturn(name);
        if (upgradedInputStream != null) {
            ConvertableSchema<ConfigSchema> configSchemaConvertableSchema = SchemaLoader.loadConvertableSchemaFromYaml(upgradedInputStream);
            ConfigSchema configSchemaUpgradedFromV1 = configSchemaConvertableSchema.convert();
            assertTrue(configSchemaUpgradedFromV1.isValid());
            assertEquals(configSchemaConvertableSchema, configSchemaUpgradedFromV1);
            ConfigSchema configSchemaFromCurrent = new ConfigSchema(yamlMap);
            List<ProcessorSchema> currentProcessors = configSchemaFromCurrent.getProcessGroupSchema().getProcessors();
            List<ProcessorSchema> v1Processors = configSchemaUpgradedFromV1.getProcessGroupSchema().getProcessors();
            assertEquals(currentProcessors.size(), v1Processors.size());

            // V1 doesn't have ids so we need to map the autogenerated ones to the ones from the template
            Map<String, String> v1IdToCurrentIdMap = new HashMap<>();
            for (int i = 0; i < currentProcessors.size(); i++) {
                ProcessorSchema currentProcessor = currentProcessors.get(i);
                ProcessorSchema v1Processor = v1Processors.get(i);
                assertEquals(currentProcessor.getName(), v1Processor.getName());
                v1IdToCurrentIdMap.put(v1Processor.getId(), currentProcessor.getId());
                v1Processor.setId(currentProcessor.getId());
            }

            configSchemaUpgradedFromV1.getProcessGroupSchema().getRemoteProcessingGroups().stream().flatMap(g -> g.getInputPorts().stream()).map(RemoteInputPortSchema::getId).sequential()
                    .forEach(id -> v1IdToCurrentIdMap.put(id, id));

            List<ConnectionSchema> currentConnections = configSchemaFromCurrent.getProcessGroupSchema().getConnections();
            List<ConnectionSchema> v1Connections = configSchemaUpgradedFromV1.getProcessGroupSchema().getConnections();

            // Update source and dest ids, can set connection id equal because it isn't referenced elsewhere
            assertEquals(currentConnections.size(), v1Connections.size());
            for (int i = 0; i < currentConnections.size(); i++) {
                ConnectionSchema currentConnection = currentConnections.get(i);
                ConnectionSchema v1Connection = v1Connections.get(i);
                assertEquals(currentConnection.getName(), v1Connection.getName());
                v1Connection.setId(currentConnection.getId());
                v1Connection.setSourceId(v1IdToCurrentIdMap.get(v1Connection.getSourceId()));
                v1Connection.setDestinationId(v1IdToCurrentIdMap.get(v1Connection.getDestinationId()));
            }
            Map<String, Object> v1YamlMap = configSchemaUpgradedFromV1.toMap();
            assertNoMapDifferences(v1YamlMap, configSchemaFromCurrent.toMap());
        }
    }

    private void assertNoMapDifferences(Map<String, Object> templateMap, Map<String, Object> yamlMap) {
        List<String> differences = new ArrayList<>();
        getMapDifferences("", differences, yamlMap, templateMap);
        if (differences.size() > 0) {
            fail(String.join("\n", differences.toArray(new String[differences.size()])));
        }
    }

    private void getMapDifferences(String path, List<String> differences, Map<String, Object> expected, Map<String, Object> actual) {
        for (Map.Entry<String, Object> stringObjectEntry : expected.entrySet()) {
            String key = stringObjectEntry.getKey();
            String newPath = path.isEmpty() ? key : path + "." + key;
            if (!actual.containsKey(key)) {
                differences.add("Missing key: " + newPath);
            } else {
                getObjectDifferences(newPath, differences, stringObjectEntry.getValue(), actual.get(key));
            }
        }

        Set<String> extraKeys = new HashSet<>(actual.keySet());
        extraKeys.removeAll(expected.keySet());
        for (String extraKey : extraKeys) {
            differences.add("Extra key: " + path + extraKey);
        }
    }

    private void getListDifferences(String path, List<String> differences, List<Object> expected, List<Object> actual) {
        if (expected.size() == actual.size()) {
            for (int i = 0; i < expected.size(); i++) {
                getObjectDifferences(path + "[" + i + "]", differences, expected.get(i), actual.get(i));
            }
        } else {
            differences.add("Expect size of " + expected.size() + " for list at " + path + " but got " + actual.size());
        }
    }

    private void getObjectDifferences(String path, List<String> differences, Object expectedValue, Object actualValue) {
        if (expectedValue instanceof Map) {
            if (actualValue instanceof Map) {
                getMapDifferences(path, differences, (Map) expectedValue, (Map) actualValue);
            } else {
                differences.add("Expected map at " + path + " but got " + actualValue);
            }
        } else if (expectedValue instanceof List) {
            if (actualValue instanceof List) {
                getListDifferences(path, differences, (List) expectedValue, (List) actualValue);
            } else {
                differences.add("Expected map at " + path + " but got " + actualValue);
            }
        } else if (expectedValue == null) {
            if (actualValue != null) {
                differences.add("Expected null at " + path + " but got " + actualValue);
            }
        } else if (expectedValue instanceof Number) {
            if (actualValue instanceof Number) {
                if (!expectedValue.toString().equals(actualValue.toString())) {
                    differences.add("Expected value of " + expectedValue + " at " + path + " but got " + actualValue);
                }
            } else {
                differences.add("Expected Number at " + path + " but got " + actualValue);
            }
        } else if (!expectedValue.equals(actualValue)) {
            differences.add("Expected " + expectedValue + " at " + path + " but got " + actualValue);
        }
    }
}
