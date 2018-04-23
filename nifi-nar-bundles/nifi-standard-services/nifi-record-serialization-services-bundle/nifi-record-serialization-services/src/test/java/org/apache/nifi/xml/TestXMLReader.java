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

import static junit.framework.TestCase.assertEquals;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class TestXMLReader {

    private XMLReader reader;

    private final String ATTRIBUTE_PREFIX = "attribute_prefix";
    private final String CONTENT_NAME = "content_field";
    private final String EVALUATE_IS_ARRAY = "xml.stream.is.array";

    public TestRunner setup(String filePath) throws InitializationException, IOException {

        TestRunner runner = TestRunners.newTestRunner(TestXMLReaderProcessor.class);
        reader = new XMLReader();
        runner.addControllerService("xml_reader", reader);
        runner.setProperty(TestXMLReaderProcessor.XML_READER, "xml_reader");

        final String outputSchemaText = new String(Files.readAllBytes(Paths.get(filePath)));
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);

        return runner;
    }

    @Test
    public void testRecordFormat() throws IOException, InitializationException {
        TestRunner runner = setup("src/test/resources/xml/testschema");

        runner.setProperty(reader, XMLReader.RECORD_FORMAT, XMLReader.RECORD_EVALUATE);

        runner.enableControllerService(reader);

        InputStream is = new FileInputStream("src/test/resources/xml/people.xml");
        runner.enqueue(is, Collections.singletonMap(EVALUATE_IS_ARRAY, "true"));
        runner.run();

        List<MockFlowFile> flowFile = runner.getFlowFilesForRelationship(TestXMLReaderProcessor.SUCCESS);
        List<String> records = Arrays.asList((new String(runner.getContentAsByteArray(flowFile.get(0)))).split("\n"));

        assertEquals(4, records.size());
    }

    @Test
    public void testRecordFormat2() throws IOException, InitializationException {
        TestRunner runner = setup("src/test/resources/xml/testschema");

        runner.setProperty(reader, XMLReader.RECORD_FORMAT, XMLReader.RECORD_ARRAY);

        runner.enableControllerService(reader);

        InputStream is = new FileInputStream("src/test/resources/xml/people.xml");
        runner.enqueue(is, Collections.singletonMap(EVALUATE_IS_ARRAY, "true"));
        runner.run();

        List<MockFlowFile> flowFile = runner.getFlowFilesForRelationship(TestXMLReaderProcessor.SUCCESS);
        List<String> records = Arrays.asList((new String(runner.getContentAsByteArray(flowFile.get(0)))).split("\n"));

        assertEquals(4, records.size());
    }

    @Test
    public void testRecordFormat3() throws IOException, InitializationException {
        TestRunner runner = setup("src/test/resources/xml/testschema");

        runner.setProperty(reader, XMLReader.RECORD_FORMAT, XMLReader.RECORD_SINGLE);

        runner.enableControllerService(reader);

        InputStream is = new FileInputStream("src/test/resources/xml/person.xml");
        runner.enqueue(is, Collections.singletonMap(EVALUATE_IS_ARRAY, "true"));
        runner.run();

        List<MockFlowFile> flowFile = runner.getFlowFilesForRelationship(TestXMLReaderProcessor.SUCCESS);
        List<String> records = Arrays.asList(new String(runner.getContentAsByteArray(flowFile.get(0))).split("\n"));

        assertEquals(1, records.size());
    }

    @Test
    public void testAttributePrefix() throws IOException, InitializationException {
        TestRunner runner = setup("src/test/resources/xml/testschema");

        runner.setProperty(reader, XMLReader.ATTRIBUTE_PREFIX, "${" + ATTRIBUTE_PREFIX + "}");
        runner.setProperty(reader, XMLReader.RECORD_FORMAT, XMLReader.RECORD_ARRAY);

        runner.enableControllerService(reader);

        InputStream is = new FileInputStream("src/test/resources/xml/people.xml");
        runner.enqueue(is, Collections.singletonMap(ATTRIBUTE_PREFIX, "ATTR_"));
        runner.run();

        List<MockFlowFile> flowFile = runner.getFlowFilesForRelationship(TestXMLReaderProcessor.SUCCESS);
        List<String> records = Arrays.asList(new String(runner.getContentAsByteArray(flowFile.get(0))).split("\n"));

        assertEquals(4, records.size());
        assertEquals("MapRecord[{COUNTRY=USA, ATTR_ID=P1, NAME=Cleve Butler, AGE=42}]", records.get(0));
        assertEquals("MapRecord[{COUNTRY=UK, ATTR_ID=P2, NAME=Ainslie Fletcher, AGE=33}]", records.get(1));
        assertEquals("MapRecord[{COUNTRY=FR, ATTR_ID=P3, NAME=Amélie Bonfils, AGE=74}]", records.get(2));
        assertEquals("MapRecord[{COUNTRY=USA, ATTR_ID=P4, NAME=Elenora Scrivens, AGE=16}]", records.get(3));
    }

    @Test
    public void testContentField() throws IOException, InitializationException {
        TestRunner runner = setup("src/test/resources/xml/testschema2");

        runner.setProperty(reader, XMLReader.CONTENT_FIELD_NAME, "${" + CONTENT_NAME + "}");
        runner.setProperty(reader, XMLReader.RECORD_FORMAT, XMLReader.RECORD_ARRAY);

        runner.enableControllerService(reader);

        InputStream is = new FileInputStream("src/test/resources/xml/people_tag_in_characters.xml");
        runner.enqueue(is, Collections.singletonMap(CONTENT_NAME, "CONTENT"));
        runner.run();

        List<MockFlowFile> flowFile = runner.getFlowFilesForRelationship(TestXMLReaderProcessor.SUCCESS);
        List<String> records = Arrays.asList(new String(runner.getContentAsByteArray(flowFile.get(0))).split("\n"));

        assertEquals(5, records.size());
        assertEquals("MapRecord[{ID=P1, NAME=MapRecord[{CONTENT=Cleve Butler, ATTR=attr content, INNER=inner content}], AGE=42}]", records.get(0));
        assertEquals("MapRecord[{ID=P2, NAME=MapRecord[{CONTENT=Ainslie Fletcher, ATTR=attr content, INNER=inner content}], AGE=33}]", records.get(1));
        assertEquals("MapRecord[{ID=P3, NAME=MapRecord[{CONTENT=Amélie Bonfils, ATTR=attr content, INNER=inner content}], AGE=74}]", records.get(2));
        assertEquals("MapRecord[{ID=P4, NAME=MapRecord[{CONTENT=Elenora Scrivens, ATTR=attr content, INNER=inner content}], AGE=16}]", records.get(3));
        assertEquals("MapRecord[{ID=P5, NAME=MapRecord[{INNER=inner content}]}]", records.get(4));
    }
}
