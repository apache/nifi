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

package org.apache.nifi.xml;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.inference.SchemaInferenceUtil;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestXMLReader {

    private static final String CONTENT_NAME = "content_field";
    private static final String EVALUATE_IS_ARRAY = "xml.stream.is.array";
    private static String testSchemaText;
    private static Path personRecord;
    private static Path fieldWithSubElement;
    private static Path people;

    private TestRunner runner;
    private XMLReader reader;

    @BeforeAll
    static void setUpBeforeAll() throws Exception {
        testSchemaText = getSchemaText("src/test/resources/xml/testschema");
        personRecord = Paths.get("src/test/resources/xml/person_record.xml");
        fieldWithSubElement = Paths.get("src/test/resources/xml/field_with_sub-element.xml");
        people = Paths.get("src/test/resources/xml/people.xml");
    }

    @BeforeEach
    void setUp() throws Exception {
        runner = TestRunners.newTestRunner(TestXMLReaderProcessor.class);
        reader = new XMLReader();

        runner.addControllerService("xml_reader", reader);
        runner.setProperty(TestXMLReaderProcessor.XML_READER, "xml_reader");
    }

    @Test
    void testRecordFormatDeterminedBasedOnAttribute() throws Exception {
        final Map<PropertyDescriptor, String> xmlReaderProperties = new HashMap<>();
        xmlReaderProperties.put(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY.getValue());
        xmlReaderProperties.put(SchemaAccessUtils.SCHEMA_TEXT, testSchemaText);
        xmlReaderProperties.put(XMLReader.RECORD_FORMAT, XMLReader.RECORD_EVALUATE.getValue());
        configureAndEnableXmlReader(xmlReaderProperties);

        runner.enqueue(people, Collections.singletonMap(EVALUATE_IS_ARRAY, "true"));
        runner.run();

        runner.assertTransferCount(TestXMLReaderProcessor.SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(TestXMLReaderProcessor.SUCCESS).getFirst();
        final List<String> records = getRecords(flowFile);

        assertEquals(4, records.size());
    }

    @Test
    void testRecordFormatArray() throws Exception {
        final Map<PropertyDescriptor, String> xmlReaderProperties = new HashMap<>();
        xmlReaderProperties.put(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY.getValue());
        xmlReaderProperties.put(SchemaAccessUtils.SCHEMA_TEXT, testSchemaText);
        xmlReaderProperties.put(XMLReader.RECORD_FORMAT, XMLReader.RECORD_ARRAY.getValue());
        configureAndEnableXmlReader(xmlReaderProperties);

        runner.enqueue(people, Collections.singletonMap(EVALUATE_IS_ARRAY, "true"));
        runner.run();

        runner.assertTransferCount(TestXMLReaderProcessor.SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(TestXMLReaderProcessor.SUCCESS).getFirst();
        final List<String> records = getRecords(flowFile);

        assertEquals(4, records.size());
    }

    @Test
    void testRecordFormatNotArray() throws Exception {
        final Map<PropertyDescriptor, String> xmlReaderProperties = new HashMap<>();
        xmlReaderProperties.put(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY.getValue());
        xmlReaderProperties.put(SchemaAccessUtils.SCHEMA_TEXT, testSchemaText);
        xmlReaderProperties.put(XMLReader.RECORD_FORMAT, XMLReader.RECORD_SINGLE.getValue());
        configureAndEnableXmlReader(xmlReaderProperties);

        final Path data = Paths.get("src/test/resources/xml/person.xml");
        runner.enqueue(data, Collections.singletonMap(EVALUATE_IS_ARRAY, "true"));
        runner.run();

        runner.assertTransferCount(TestXMLReaderProcessor.SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(TestXMLReaderProcessor.SUCCESS).getFirst();
        final List<String> records = getRecords(flowFile);

        assertEquals(1, records.size());
    }

    @Test
    void testAttributePrefix() throws Exception {
        final Map<PropertyDescriptor, String> xmlReaderProperties = new HashMap<>();
        xmlReaderProperties.put(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY.getValue());
        xmlReaderProperties.put(SchemaAccessUtils.SCHEMA_TEXT, testSchemaText);
        final String attributePrefix = "attribute_prefix";
        xmlReaderProperties.put(XMLReader.ATTRIBUTE_PREFIX, "${" + attributePrefix + "}");
        xmlReaderProperties.put(XMLReader.RECORD_FORMAT, XMLReader.RECORD_ARRAY.getValue());
        configureAndEnableXmlReader(xmlReaderProperties);

        runner.enqueue(people, Collections.singletonMap(attributePrefix, "ATTR_"));
        runner.run();

        runner.assertTransferCount(TestXMLReaderProcessor.SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(TestXMLReaderProcessor.SUCCESS).getFirst();
        final List<String> records = getRecords(flowFile);

        assertEquals(4, records.size());
        assertEquals("MapRecord[{ATTR_ID=P1, NAME=Cleve Butler, AGE=42, COUNTRY=USA}]", records.get(0));
        assertEquals("MapRecord[{ATTR_ID=P2, NAME=Ainslie Fletcher, AGE=33, COUNTRY=UK}]", records.get(1));
        assertEquals("MapRecord[{ATTR_ID=P3, NAME=Amélie Bonfils, AGE=74, COUNTRY=FR}]", records.get(2));
        assertEquals("MapRecord[{ATTR_ID=P4, NAME=Elenora Scrivens, AGE=16, COUNTRY=USA}]", records.get(3));
    }

    @Test
    void testContentField() throws Exception {
        final String outputSchemaText = getSchemaText("src/test/resources/xml/testschema2");
        final Map<PropertyDescriptor, String> xmlReaderProperties = new HashMap<>();
        xmlReaderProperties.put(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY.getValue());
        xmlReaderProperties.put(SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        xmlReaderProperties.put(XMLReader.CONTENT_FIELD_NAME, "${" + CONTENT_NAME + "}");
        xmlReaderProperties.put(XMLReader.RECORD_FORMAT, XMLReader.RECORD_ARRAY.getValue());
        configureAndEnableXmlReader(xmlReaderProperties);

        final Path data = Paths.get("src/test/resources/xml/people_tag_in_characters.xml");
        runner.enqueue(data, Collections.singletonMap(CONTENT_NAME, "CONTENT"));
        runner.run();

        runner.assertTransferCount(TestXMLReaderProcessor.SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(TestXMLReaderProcessor.SUCCESS).getFirst();
        final List<String> records = getRecords(flowFile);

        assertEquals(5, records.size());
        assertEquals("MapRecord[{ID=P1, NAME=MapRecord[{ATTR=attr content, INNER=inner content, CONTENT=Cleve Butler}], AGE=42}]", records.get(0));
        assertEquals("MapRecord[{ID=P2, NAME=MapRecord[{ATTR=attr content, INNER=inner content, CONTENT=Ainslie Fletcher}], AGE=33}]", records.get(1));
        assertEquals("MapRecord[{ID=P3, NAME=MapRecord[{ATTR=attr content, INNER=inner content, CONTENT=Amélie Bonfils}], AGE=74}]", records.get(2));
        assertEquals("MapRecord[{ID=P4, NAME=MapRecord[{ATTR=attr content, INNER=inner content, CONTENT=Elenora Scrivens}], AGE=16}]", records.get(3));
        assertEquals("MapRecord[{ID=P5, NAME=MapRecord[{INNER=inner content}]}]", records.get(4));
    }

    @Test
    void testInferSchema() throws Exception {
        final String expectedContent = "MapRecord[{num=123, name=John Doe, software=MapRecord[{favorite=true, " + CONTENT_NAME + "=Apache NiFi}]}]";
        final Map<PropertyDescriptor, String> xmlReaderProperties = new HashMap<>();
        xmlReaderProperties.put(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA.getValue());
        xmlReaderProperties.put(XMLReader.RECORD_FORMAT, XMLReader.RECORD_SINGLE.getValue());
        xmlReaderProperties.put(XMLReader.CONTENT_FIELD_NAME, CONTENT_NAME);
        configureAndEnableXmlReader(xmlReaderProperties);

        runner.enqueue(personRecord);
        runner.run();

        runner.assertTransferCount(TestXMLReaderProcessor.SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(TestXMLReaderProcessor.SUCCESS).getFirst();
        final String actualContent = out.getContent();
        assertEquals(expectedContent, actualContent);
    }

    @Test
    void testInferSchemaContentFieldNameNotSet() throws Exception {
        final String expectedContent = "MapRecord[{num=123, name=John Doe, software=MapRecord[{favorite=true}]}]";
        final Map<PropertyDescriptor, String> xmlReaderProperties = new HashMap<>();
        xmlReaderProperties.put(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA.getValue());
        xmlReaderProperties.put(XMLReader.RECORD_FORMAT, XMLReader.RECORD_SINGLE.getValue());
        configureAndEnableXmlReader(xmlReaderProperties);

        runner.enqueue(personRecord);
        runner.run();

        runner.assertTransferCount(TestXMLReaderProcessor.SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(TestXMLReaderProcessor.SUCCESS).getFirst();
        final String actualContent = out.getContent();
        assertEquals(expectedContent, actualContent);
    }

    @Test
    void testInferSchemaContentFieldNameNotSetSubElementExists() throws Exception {
        final String expectedContent = "MapRecord[{field_with_attribute=MapRecord[{attr=attr_content, value=123}]}]";
        final Map<PropertyDescriptor, String> xmlReaderProperties = new HashMap<>();
        xmlReaderProperties.put(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA.getValue());
        xmlReaderProperties.put(XMLReader.RECORD_FORMAT, XMLReader.RECORD_SINGLE.getValue());
        configureAndEnableXmlReader(xmlReaderProperties);

        runner.enqueue(fieldWithSubElement);
        runner.run();

        runner.assertTransferCount(TestXMLReaderProcessor.SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(TestXMLReaderProcessor.SUCCESS).getFirst();
        final String actualContent = out.getContent();
        assertEquals(expectedContent, actualContent);
    }

    @Test
    void testInferSchemaContentFieldNameSetSubElementExistsNameClash() throws Exception {
        final String expectedContent = "MapRecord[{field_with_attribute=MapRecord[{attr=attr_content, value=content of field}]}]";
        final Map<PropertyDescriptor, String> xmlReaderProperties = new HashMap<>();
        xmlReaderProperties.put(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA.getValue());
        xmlReaderProperties.put(XMLReader.RECORD_FORMAT, XMLReader.RECORD_SINGLE.getValue());
        xmlReaderProperties.put(XMLReader.CONTENT_FIELD_NAME, "value");
        configureAndEnableXmlReader(xmlReaderProperties);

        runner.enqueue(fieldWithSubElement);
        runner.run();

        runner.assertTransferCount(TestXMLReaderProcessor.SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(TestXMLReaderProcessor.SUCCESS).getFirst();
        final String actualContent = out.getContent();
        assertEquals(expectedContent, actualContent);
    }

    @Test
    void testInferSchemaContentFieldNameSetSubElementExistsNoNameClash() throws Exception {
        final String expectedContent = String.format("MapRecord[{field_with_attribute=MapRecord[{attr=attr_content, value=123, %s=content of field}]}]", CONTENT_NAME);
        final Map<PropertyDescriptor, String> xmlReaderProperties = new HashMap<>();
        xmlReaderProperties.put(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA.getValue());
        xmlReaderProperties.put(XMLReader.RECORD_FORMAT, XMLReader.RECORD_SINGLE.getValue());
        xmlReaderProperties.put(XMLReader.CONTENT_FIELD_NAME, CONTENT_NAME);
        configureAndEnableXmlReader(xmlReaderProperties);

        runner.enqueue(fieldWithSubElement);
        runner.run();

        runner.assertTransferCount(TestXMLReaderProcessor.SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(TestXMLReaderProcessor.SUCCESS).getFirst();
        final String actualContent = out.getContent();
        assertEquals(expectedContent, actualContent);
    }

    @Test
    void testInferSchemaIgnoreAttributes() throws Exception {
        final String expectedContent = "MapRecord[{num=123, name=John Doe, software=Apache NiFi}]";
        final Map<PropertyDescriptor, String> xmlReaderProperties = new HashMap<>();
        xmlReaderProperties.put(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA.getValue());
        xmlReaderProperties.put(XMLReader.RECORD_FORMAT, XMLReader.RECORD_SINGLE.getValue());
        xmlReaderProperties.put(XMLReader.PARSE_XML_ATTRIBUTES, "false");
        configureAndEnableXmlReader(xmlReaderProperties);

        runner.enqueue(personRecord);
        runner.run();

        runner.assertTransferCount(TestXMLReaderProcessor.SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(TestXMLReaderProcessor.SUCCESS).getFirst();
        final String actualContent = out.getContent();
        assertEquals(expectedContent, actualContent);
    }

    @Test
    void testInferSchemaWhereNameValuesHasMixedTypes() throws Exception {
        final Map<PropertyDescriptor, String> xmlReaderProperties = new HashMap<>();
        xmlReaderProperties.put(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA.getValue());
        xmlReaderProperties.put(XMLReader.RECORD_FORMAT, XMLReader.RECORD_SINGLE.getValue());
        xmlReaderProperties.put(XMLReader.PARSE_XML_ATTRIBUTES, "true");
        xmlReaderProperties.put(XMLReader.CONTENT_FIELD_NAME, "Value");
        configureAndEnableXmlReader(xmlReaderProperties);
        runner.setProperty(TestXMLReaderProcessor.RECORD_FIELD_TO_GET_AS_STRING, "Data");

        final Path data = Paths.get("src/test/resources/xml/dataWithArrayOfDifferentTypes.xml");
        runner.enqueue(data);
        runner.run();

        final MockFlowFile out = runner.getFlowFilesForRelationship(TestXMLReaderProcessor.SUCCESS).getFirst();
        final String expectedContent = "[MapRecord[{Name=Param1, Value=String1}], MapRecord[{Name=Param2, Value=2}], MapRecord[{Name=Param3, Value=String3}]]";
        final String actualContent = out.getContent();
        assertEquals(expectedContent, actualContent);
    }

    private void configureAndEnableXmlReader(Map<PropertyDescriptor, String> xmlReaderProperties) {
        for (Map.Entry<PropertyDescriptor, String> entry : xmlReaderProperties.entrySet()) {
            runner.setProperty(reader, entry.getKey(), entry.getValue());
        }
        runner.enableControllerService(reader);
    }

    private List<String> getRecords(MockFlowFile flowFile) {
        return Arrays.asList(flowFile.getContent().split("\n"));
    }

    private static String getSchemaText(String schemaPath) throws Exception {
        return Files.readString(Paths.get(schemaPath));
    }
}
