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
package org.apache.nifi.processors.box;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.MetadataTemplate;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class CreateBoxMetadataTemplateTest extends AbstractBoxFileTest {

    private static final String TEMPLATE_NAME = "Test Template";
    private static final String TEMPLATE_KEY = "test_template";
    private static final String HIDDEN_VALUE = "false";

    private List<MetadataTemplate.Field> capturedFields;
    private String capturedTemplateKey;
    private String capturedTemplateName;
    private Boolean capturedHidden;

    private class TestCreateBoxMetadataTemplate extends CreateBoxMetadataTemplate {
        @Override
        protected BoxAPIConnection getBoxAPIConnection(ProcessContext context) {
            return mockBoxAPIConnection;
        }

        @Override
        protected void createBoxMetadataTemplate(final BoxAPIConnection boxAPIConnection,
                                                 final String templateKey,
                                                 final String templateName,
                                                 final boolean isHidden,
                                                 final List<MetadataTemplate.Field> fields) {
            capturedFields = fields;
            capturedTemplateKey = templateKey;
            capturedTemplateName = templateName;
            capturedHidden = isHidden;
        }
    }

    @Override
    @BeforeEach
    void setUp() throws Exception {
        final TestCreateBoxMetadataTemplate processor = new TestCreateBoxMetadataTemplate();
        testRunner = TestRunners.newTestRunner(processor);
        super.setUp();

        configureJsonRecordReader(testRunner);
        testRunner.setProperty(CreateBoxMetadataTemplate.TEMPLATE_NAME, TEMPLATE_NAME);
        testRunner.setProperty(CreateBoxMetadataTemplate.TEMPLATE_KEY, TEMPLATE_KEY);
        testRunner.setProperty(CreateBoxMetadataTemplate.HIDDEN, HIDDEN_VALUE);
        testRunner.setProperty(CreateBoxMetadataTemplate.RECORD_READER, "json-reader");
    }

    private void configureJsonRecordReader(TestRunner runner) throws InitializationException {
        final JsonTreeReader readerService = new JsonTreeReader();
        runner.addControllerService("json-reader", readerService);
        runner.enableControllerService(readerService);
    }

    @Test
    void testSuccessfulTemplateCreation() {
        final String inputJson = """
                [
                    {"key": "field1", "type": "string", "displayName": "Field One"},
                    {"key": "field2", "type": "float"}
                ]
                """;

        testRunner.enqueue(inputJson);
        testRunner.run();
        assertEquals(2, capturedFields.size());

        final MetadataTemplate.Field field1 = capturedFields.getFirst();
        assertEquals("field1", field1.getKey());
        assertEquals("string", field1.getType());
        assertEquals("Field One", field1.getDisplayName());

        final MetadataTemplate.Field field2 = capturedFields.get(1);
        assertEquals("field2", field2.getKey());
        assertEquals("float", field2.getType());

        testRunner.assertAllFlowFilesTransferred(CreateBoxMetadataTemplate.REL_SUCCESS, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(CreateBoxMetadataTemplate.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("box.template.name", TEMPLATE_NAME);
        flowFile.assertAttributeEquals("box.template.key", TEMPLATE_KEY);
        flowFile.assertAttributeEquals("box.template.scope", CreateBoxMetadataTemplate.SCOPE_ENTERPRISE);
        flowFile.assertAttributeEquals("box.template.fields.count", "2");
    }

    @Test
    void testEmptyInput() {
        final String inputJson = "[]";

        testRunner.enqueue(inputJson);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(CreateBoxMetadataTemplate.REL_FAILURE, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(CreateBoxMetadataTemplate.REL_FAILURE).getFirst();
        flowFile.assertAttributeEquals("error.message", "No valid metadata field specifications found in the input");
    }

    @Test
    void testInvalidRecords() {
        // First record missing the key; second record has an invalid type.
        final String inputJson = """
                [
                    {"type": "string", "displayName": "No Key"},
                    {"key": "field2", "type": "invalid"}
                ]
                """;

        testRunner.enqueue(inputJson);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(CreateBoxMetadataTemplate.REL_FAILURE, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(CreateBoxMetadataTemplate.REL_FAILURE).getFirst();
        final String errorMessage = flowFile.getAttribute("error.message");
        assertTrue(errorMessage.contains("missing a key field"));
        assertTrue(errorMessage.contains("has an invalid type"));
    }

    @Test
    void testExpressionLanguage() {
        testRunner.setProperty(CreateBoxMetadataTemplate.TEMPLATE_NAME, "${template.name}");
        testRunner.setProperty(CreateBoxMetadataTemplate.TEMPLATE_KEY, "${template.key}");
        testRunner.setProperty(CreateBoxMetadataTemplate.HIDDEN, "true");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("template.name", "Template Name");
        attributes.put("template.key", "templateKey");

        final String inputJson = """
                [
                    {"key": "field1", "type": "date", "displayName": "Date Field"}
                ]
                """;

        testRunner.enqueue(inputJson, attributes);
        testRunner.run();

        assertEquals("templateKey", capturedTemplateKey);
        assertEquals("Template Name", capturedTemplateName);
        assertEquals(true, capturedHidden);
        assertEquals(1, capturedFields.size());

        final MetadataTemplate.Field field = capturedFields.getFirst();
        assertEquals("field1", field.getKey());
        assertEquals("date", field.getType());
        assertEquals("Date Field", field.getDisplayName());

        testRunner.assertAllFlowFilesTransferred(CreateBoxMetadataTemplate.REL_SUCCESS, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(CreateBoxMetadataTemplate.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals("box.template.name", "Template Name");
        flowFile.assertAttributeEquals("box.template.key", "templateKey");
        flowFile.assertAttributeEquals("box.template.scope", CreateBoxMetadataTemplate.SCOPE_ENTERPRISE);
        flowFile.assertAttributeEquals("box.template.fields.count", "1");
    }

    @Test
    void testAllFieldTypes() {
        final String inputJson = """
                [
                    {"key": "strField", "type": "string", "displayName": "String Field", "description": "A string field", "hidden": false},
                    {"key": "numField", "type": "float", "displayName": "Number Field", "description": "A float field", "hidden": true},
                    {"key": "dateField", "type": "date", "displayName": "Date Field", "description": "A date field"}
                ]
                """;

        testRunner.enqueue(inputJson);
        testRunner.run();

        assertEquals(3, capturedFields.size());
        assertEquals("string", capturedFields.get(0).getType());
        assertEquals("float", capturedFields.get(1).getType());
        assertEquals("date", capturedFields.get(2).getType());
        assertEquals("String Field", capturedFields.get(0).getDisplayName());
        assertEquals("Number Field", capturedFields.get(1).getDisplayName());
        assertEquals("Date Field", capturedFields.get(2).getDisplayName());
        assertEquals("A string field", capturedFields.get(0).getDescription());
        assertEquals("A float field", capturedFields.get(1).getDescription());
        assertEquals("A date field", capturedFields.get(2).getDescription());
        assertEquals(false, capturedFields.get(0).getIsHidden());
        assertEquals(true, capturedFields.get(1).getIsHidden());

        testRunner.assertAllFlowFilesTransferred(CreateBoxMetadataTemplate.REL_SUCCESS, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(CreateBoxMetadataTemplate.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals("box.template.fields.count", "3");
    }
}
