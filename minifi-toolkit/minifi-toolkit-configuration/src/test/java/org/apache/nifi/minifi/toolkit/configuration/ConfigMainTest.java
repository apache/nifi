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
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
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
        assertEquals(ConfigMain.SUCCESS, configMain.execute(new String[]{ConfigMain.VALIDATE, testInput}));
    }

    @Test
    public void testTransformErrorOpeningInput() throws FileNotFoundException {
        when(pathInputStreamFactory.create(testInput)).thenThrow(new FileNotFoundException());
        assertEquals(ConfigMain.ERR_UNABLE_TO_OPEN_INPUT, configMain.execute(new String[]{ConfigMain.TRANSFORM, testInput, testOutput}));
    }

    @Test
    public void testTransformErrorOpeningOutput() throws FileNotFoundException {
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
                ConfigMainTest.class.getClassLoader().getResourceAsStream("CsvToJson.xml"));
        when(pathOutputStreamFactory.create(testOutput)).thenAnswer(invocation -> new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                throw new IOException();
            }
        });
        assertEquals(ConfigMain.ERR_UNABLE_TO_TRANFORM_TEMPLATE, configMain.execute(new String[]{ConfigMain.TRANSFORM, testInput, testOutput}));
    }

    @Test
    public void testTransformSuccess() throws FileNotFoundException {
        when(pathInputStreamFactory.create(testInput)).thenAnswer(invocation ->
                ConfigMainTest.class.getClassLoader().getResourceAsStream("CsvToJson.xml"));
        when(pathOutputStreamFactory.create(testOutput)).thenAnswer(invocation -> new ByteArrayOutputStream());
        assertEquals(ConfigMain.SUCCESS, configMain.execute(new String[]{ConfigMain.TRANSFORM, testInput, testOutput}));
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

    private void transformRoundTrip(String name) throws JAXBException, IOException, SchemaLoaderException {
        Map<String, Object> templateMap = ConfigMain.transformTemplateToSchema(getClass().getClassLoader().getResourceAsStream(name + ".xml")).toMap();
        Map<String, Object> yamlMap = SchemaLoader.loadYamlAsMap(getClass().getClassLoader().getResourceAsStream(name + ".yml"));
        assertNoMapDifferences(templateMap, yamlMap);
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
