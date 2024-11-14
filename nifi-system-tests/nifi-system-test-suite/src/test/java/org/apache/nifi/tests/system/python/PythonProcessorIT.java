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

package org.apache.nifi.tests.system.python;

import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PythonProcessorIT extends NiFiSystemIT {

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createPythonicInstanceFactory();
    }

    @Test
    public void testFlowFileTransformWithEL() throws NiFiClientException, IOException, InterruptedException {
        final String messageContents = "Hello World";

        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity writeProperty = getClientUtil().createPythonProcessor("WritePropertyToFlowFile");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");

        // Config GenerateFlowFile to add a "greeting" attribute with a value of "Hello World"
        final ProcessorConfigDTO generateConfig = generate.getComponent().getConfig();
        generateConfig.setProperties(Collections.singletonMap("greeting", messageContents));
        getClientUtil().updateProcessorConfig(generate, generateConfig);

        // Configure the WritePropertyToFlowFile processor to write the "greeting" attribute's value to the FlowFile content
        final ProcessorConfigDTO writePropertyConfig = writeProperty.getComponent().getConfig();
        writePropertyConfig.setProperties(Collections.singletonMap("Message", "${greeting}"));
        getClientUtil().updateProcessorConfig(writeProperty, writePropertyConfig);

        // Connect flow
        getClientUtil().createConnection(generate, writeProperty, "success");
        getClientUtil().setAutoTerminatedRelationships(writeProperty, "failure");
        final ConnectionEntity outputConnection = getClientUtil().createConnection(writeProperty, terminate, "success");

        // Wait for processor validation to complete
        getClientUtil().waitForValidProcessor(generate.getId());
        getClientUtil().waitForValidProcessor(writeProperty.getId());

        // Run the flow
        getClientUtil().startProcessor(generate);
        getClientUtil().startProcessor(writeProperty);

        // Wait for output to be queued up
        waitForQueueCount(outputConnection.getId(), 1);

        // Validate the output
        final String contents = getClientUtil().getFlowFileContentAsUtf8(outputConnection.getId(), 0);
        assertEquals(messageContents, contents);
    }

    @Test
    public void testRecordTransform() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity setRecordField = getClientUtil().createPythonProcessor("SetRecordField");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("sport", "Ball");
        attributes.put("greeting", "hello");
        getClientUtil().updateProcessorProperties(generate, attributes);

        // Add Reader and Writer
        final ControllerServiceEntity csvReader = getClientUtil().createControllerService("MockCSVReader");
        final ControllerServiceEntity csvWriter = getClientUtil().createControllerService("MockCSVWriter");

        getClientUtil().enableControllerService(csvReader);
        getClientUtil().enableControllerService(csvWriter);

        // Configure the SetRecordField property
        final Map<String, String> fieldMap = new HashMap<>();
        fieldMap.put("Record Reader", csvReader.getId());
        fieldMap.put("Record Writer", csvWriter.getId());
        fieldMap.put("age", "3");
        fieldMap.put("color", "yellow");
        fieldMap.put("sport", "${sport}");  // test EL for plain attribute
        fieldMap.put("greeting", "${greeting:toUpper()}");      // test EL function
        getClientUtil().updateProcessorProperties(setRecordField, fieldMap);
        getClientUtil().setAutoTerminatedRelationships(setRecordField, new HashSet<>(Arrays.asList("original", "failure")));

        // Set contents of GenerateFlowFile
        getClientUtil().updateProcessorProperties(generate, Collections.singletonMap("Text", "name, age, color, sport, greeting\nJane Doe, 7, red,,\nJake Doe, 14, blue,,"));

        // Connect flow
        getClientUtil().createConnection(generate, setRecordField, "success");
        final ConnectionEntity outputConnection = getClientUtil().createConnection(setRecordField, terminate, "success");

        // Wait for processor validation to complete
        getClientUtil().waitForValidProcessor(generate.getId());
        getClientUtil().waitForValidProcessor(setRecordField.getId());

        // Run the flow
        getClientUtil().startProcessor(generate);
        getClientUtil().startProcessor(setRecordField);

        // Wait for output data
        waitForQueueCount(outputConnection.getId(), 1);

        // Verify output contents. We don't know the order that the fields will be in, but we know that we should get back 3 fields per record: name, age, color.
        final String contents = getClientUtil().getFlowFileContentAsUtf8(outputConnection.getId(), 0);
        final String[] lines = contents.split("\n");
        final String headerLine = lines[0];
        final List<String> headers = Stream.of(headerLine.split(","))
            .map(String::trim)
            .toList();
        assertTrue(headers.contains("name"));
        assertTrue(headers.contains("age"));
        assertTrue(headers.contains("color"));
        assertTrue(headers.contains("sport"));
        assertTrue(headers.contains("greeting"));

        final Map<String, Integer> headerIndices = new HashMap<>();
        int index = 0;
        for (final String header : headers) {
            headerIndices.put(header, index++);
        }

        final String firstRecordLine = lines[1];
        final List<String> firstRecordValues = Stream.of(firstRecordLine.split(","))
            .map(String::trim)
            .toList();
        assertEquals("Jane Doe", firstRecordValues.get( headerIndices.get("name") ));
        assertEquals("yellow", firstRecordValues.get( headerIndices.get("color") ));
        assertEquals("3", firstRecordValues.get( headerIndices.get("age") ));
        assertEquals("Ball", firstRecordValues.get( headerIndices.get("sport") ));
        assertEquals("HELLO", firstRecordValues.get( headerIndices.get("greeting") ));

        final String secondRecordLine = lines[2];
        final List<String> secondRecordValues = Stream.of(secondRecordLine.split(","))
            .map(String::trim)
            .toList();
        assertEquals("Jake Doe", secondRecordValues.get( headerIndices.get("name") ));
        assertEquals("yellow", secondRecordValues.get( headerIndices.get("color") ));
        assertEquals("3", secondRecordValues.get( headerIndices.get("age") ));
        assertEquals("Ball", secondRecordValues.get( headerIndices.get("sport") ));
        assertEquals("HELLO", secondRecordValues.get( headerIndices.get("greeting") ));
    }

    @Test
    public void testRecordTransformPartitioning() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity setRecordField = getClientUtil().createPythonProcessor("SetRecordField");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");

        // Add Reader and Writer
        final ControllerServiceEntity csvReader = getClientUtil().createControllerService("MockCSVReader");
        final ControllerServiceEntity csvWriter = getClientUtil().createControllerService("MockCSVWriter");

        getClientUtil().enableControllerService(csvReader);
        getClientUtil().enableControllerService(csvWriter);

        // Configure the SetRecordField property
        final Map<String, String> fieldMap = new HashMap<>();
        fieldMap.put("Record Reader", csvReader.getId());
        fieldMap.put("Record Writer", csvWriter.getId());
        getClientUtil().updateProcessorProperties(setRecordField, fieldMap);
        getClientUtil().setAutoTerminatedRelationships(setRecordField, new HashSet<>(Arrays.asList("original", "failure")));

        // Set contents of GenerateFlowFile
        getClientUtil().updateProcessorProperties(generate,
            Collections.singletonMap("Text", "name, group\nJane Doe, default\nJake Doe, other"));

        // Connect flow
        getClientUtil().createConnection(generate, setRecordField, "success");
        final ConnectionEntity outputConnection = getClientUtil().createConnection(setRecordField, terminate, "success");

        // Wait for processor validation to complete
        getClientUtil().waitForValidProcessor(generate.getId());
        getClientUtil().waitForValidProcessor(setRecordField.getId());

        // Run the flow
        getClientUtil().startProcessor(generate);
        getClientUtil().startProcessor(setRecordField);

        // Wait for output data
        waitForQueueCount(outputConnection.getId(), 2);

        // Verify output contents. We don't know the order that the fields will be in, but we know that we should get back 2 fields per record: name, group.
        final String ff1Contents = getClientUtil().getFlowFileContentAsUtf8(outputConnection.getId(), 0);
        final String[] ff1Lines = ff1Contents.split("\n");
        final String ff1HeaderLine = ff1Lines[0];
        final List<String> ff1Headers = Stream.of(ff1HeaderLine.split(","))
            .map(String::trim)
            .toList();
        assertTrue(ff1Headers.contains("name"));
        assertTrue(ff1Headers.contains("group"));

        final Map<String, Integer> ff1HeaderIndices = new HashMap<>();
        int index = 0;
        for (final String header : ff1Headers) {
            ff1HeaderIndices.put(header, index++);
        }

        final String firstRecordLine = ff1Lines[1];
        final List<String> firstRecordValues = Stream.of(firstRecordLine.split(","))
            .map(String::trim)
            .toList();
        assertEquals("Jane Doe", firstRecordValues.get( ff1HeaderIndices.get("name") ));
        assertEquals("default", firstRecordValues.get( ff1HeaderIndices.get("group") ));

        final String ff2Contents = getClientUtil().getFlowFileContentAsUtf8(outputConnection.getId(), 1);
        final String[] ff2Lines = ff2Contents.split("\n");
        final String ff2HeaderLine = ff2Lines[0];
        final List<String> ff2Headers = Stream.of(ff2HeaderLine.split(","))
            .map(String::trim)
            .toList();
        assertTrue(ff2Headers.contains("name"));
        assertTrue(ff2Headers.contains("group"));

        final Map<String, Integer> headerIndices = new HashMap<>();
        index = 0;
        for (final String header : ff2Headers) {
            headerIndices.put(header, index++);
        }

        final String secondRecordLine = ff2Lines[1];
        final List<String> secondRecordValues = Stream.of(secondRecordLine.split(","))
            .map(String::trim)
            .toList();
        assertEquals("Jake Doe", secondRecordValues.get( headerIndices.get("name") ));
        assertEquals("other", secondRecordValues.get( headerIndices.get("group") ));
    }

    @Test
    public void testFlowFileSource() throws NiFiClientException, IOException, InterruptedException {
        final String messageContents = "Hello World";

        final ProcessorEntity createFlowFilePython = getClientUtil().createPythonProcessor("CreateFlowFile");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");

        // Config CreateFlowFile with "Hello World" as the value of "FlowFile Contents" attribute
        final ProcessorConfigDTO generateConfig = createFlowFilePython.getComponent().getConfig();
        generateConfig.setProperties(Collections.singletonMap("FlowFile Contents", messageContents));
        getClientUtil().updateProcessorConfig(createFlowFilePython, generateConfig);

        // Connect the processors
        final ConnectionEntity outputConnection = getClientUtil().createConnection(createFlowFilePython, terminate, "success");
        getClientUtil().setAutoTerminatedRelationships(createFlowFilePython, "multiline");

        // Wait for processor validation to complete
        getClientUtil().waitForValidProcessor(createFlowFilePython.getId());

        // Run the flow
        runProcessorOnce(createFlowFilePython);

        // Wait for output to be queued up
        waitForQueueCount(outputConnection.getId(), 1);

        // Validate the output
        final String contents = getClientUtil().getFlowFileContentAsUtf8(outputConnection.getId(), 0);
        assertEquals(messageContents, contents);
    }

    private void runProcessorOnce(final ProcessorEntity processorEntity) throws NiFiClientException, IOException, InterruptedException {
        getNifiClient().getProcessorClient().runProcessorOnce(processorEntity);
        getClientUtil().waitForStoppedProcessor(processorEntity.getId());
    }
}
