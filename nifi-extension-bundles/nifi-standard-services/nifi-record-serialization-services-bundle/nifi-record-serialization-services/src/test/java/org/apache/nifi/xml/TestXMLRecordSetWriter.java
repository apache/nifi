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

import org.apache.avro.Schema;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.SchemaRegistryRecordSetWriter;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.xmlunit.diff.DefaultNodeMatcher;
import org.xmlunit.diff.ElementSelectors;
import org.xmlunit.matchers.CompareMatcher;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_BRANCH_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REFERENCE_READER;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REGISTRY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_VERSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestXMLRecordSetWriter {

    private TestRunner setup(XMLRecordSetWriter writer) throws InitializationException, IOException {
        TestRunner runner = TestRunners.newTestRunner(TestXMLRecordSetWriterProcessor.class);

        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/xml/testschema3")));

        runner.addControllerService("xml_writer", writer);
        runner.setProperty(TestXMLRecordSetWriterProcessor.XML_WRITER, "xml_writer");

        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, XMLRecordSetWriter.PRETTY_PRINT_XML, new AllowableValue("true"));

        runner.setProperty(writer, "Schema Write Strategy", "no-schema");

        return runner;
    }

    @Test
    public void testDefault() throws IOException, InitializationException {
        XMLRecordSetWriter writer = new XMLRecordSetWriter();
        TestRunner runner = setup(writer);

        runner.setProperty(writer, XMLRecordSetWriter.ROOT_TAG_NAME, "root");

        runner.enableControllerService(writer);
        runner.enqueue("");
        runner.run();
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(TestXMLRecordSetWriterProcessor.SUCCESS, 1);

        String expected = "<root><array_record><array_field>1</array_field><array_field></array_field><array_field>3</array_field>" +
                "<name1>val1</name1><name2></name2></array_record>" +
                "<array_record><array_field>1</array_field><array_field></array_field><array_field>3</array_field>" +
                "<name1>val1</name1><name2></name2></array_record></root>";
        String actual = new String(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(TestXMLRecordSetWriterProcessor.SUCCESS).getFirst()));
        assertThat(expected, CompareMatcher.isSimilarTo(actual).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testDefaultSingleRecord() throws IOException, InitializationException {
        XMLRecordSetWriter writer = new XMLRecordSetWriter();
        TestRunner runner = setup(writer);

        runner.setProperty(TestXMLRecordSetWriterProcessor.MULTIPLE_RECORDS, "false");

        runner.enableControllerService(writer);
        runner.enqueue("");
        runner.run();
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(TestXMLRecordSetWriterProcessor.SUCCESS, 1);

        String expected = "<array_record><array_field>1</array_field><array_field></array_field><array_field>3</array_field>" +
                "<name1>val1</name1><name2></name2></array_record>";

        String actual = new String(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(TestXMLRecordSetWriterProcessor.SUCCESS).getFirst()));
        assertThat(expected, CompareMatcher.isSimilarTo(actual).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testRootAndRecordNaming() throws IOException, InitializationException {
        XMLRecordSetWriter writer = new XMLRecordSetWriter();
        TestRunner runner = setup(writer);

        runner.setProperty(writer, XMLRecordSetWriter.ROOT_TAG_NAME, "ROOT_NODE");
        runner.setProperty(writer, XMLRecordSetWriter.RECORD_TAG_NAME, "RECORD_NODE");

        runner.enableControllerService(writer);
        runner.enqueue("");
        runner.run();
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(TestXMLRecordSetWriterProcessor.SUCCESS, 1);

        String expected = "<ROOT_NODE><RECORD_NODE><array_field>1</array_field><array_field></array_field><array_field>3</array_field>" +
                "<name1>val1</name1><name2></name2></RECORD_NODE>" +
                "<RECORD_NODE><array_field>1</array_field><array_field></array_field><array_field>3</array_field>" +
                "<name1>val1</name1><name2></name2></RECORD_NODE></ROOT_NODE>";
        String actual = new String(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(TestXMLRecordSetWriterProcessor.SUCCESS).getFirst()));
        assertThat(expected, CompareMatcher.isSimilarTo(actual).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testSchemaRootRecordNaming() throws IOException, InitializationException {
        String avroSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/xml/testschema3")));
        Schema avroSchema = new Schema.Parser().parse(avroSchemaText);

        SchemaIdentifier schemaId = SchemaIdentifier.builder().name("schemaName").build();
        RecordSchema recordSchema = AvroTypeUtil.createSchema(avroSchema, avroSchemaText, schemaId);

        XMLRecordSetWriter writer = new _XMLRecordSetWriter(recordSchema);
        TestRunner runner = setup(writer);

        runner.setProperty(writer, XMLRecordSetWriter.ROOT_TAG_NAME, "ROOT_NODE");

        runner.enableControllerService(writer);
        runner.enqueue("");
        runner.run();
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(TestXMLRecordSetWriterProcessor.SUCCESS, 1);

        String expected = "<ROOT_NODE><array_record><array_field>1</array_field><array_field></array_field><array_field>3</array_field>" +
                "<name1>val1</name1><name2></name2></array_record>" +
                "<array_record><array_field>1</array_field><array_field></array_field><array_field>3</array_field>" +
                "<name1>val1</name1><name2></name2></array_record></ROOT_NODE>";
        String actual = new String(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(TestXMLRecordSetWriterProcessor.SUCCESS).getFirst()));
        assertThat(expected, CompareMatcher.isSimilarTo(actual).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testNullSuppression() throws IOException, InitializationException {
        XMLRecordSetWriter writer = new XMLRecordSetWriter();
        TestRunner runner = setup(writer);

        runner.setProperty(writer, XMLRecordSetWriter.ROOT_TAG_NAME, "root");
        runner.setProperty(writer, XMLRecordSetWriter.RECORD_TAG_NAME, "record");

        runner.setProperty(writer, XMLRecordSetWriter.SUPPRESS_NULLS, XMLRecordSetWriter.ALWAYS_SUPPRESS);

        runner.enableControllerService(writer);
        runner.enqueue("");
        runner.run();
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(TestXMLRecordSetWriterProcessor.SUCCESS, 1);

        String expected = "<root><record><array_field>1</array_field><array_field>3</array_field>" +
                "<name1>val1</name1></record>" +
                "<record><array_field>1</array_field><array_field>3</array_field>" +
                "<name1>val1</name1></record></root>";
        String actual = new String(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(TestXMLRecordSetWriterProcessor.SUCCESS).getFirst()));
        assertThat(expected, CompareMatcher.isSimilarTo(actual).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testArrayWrapping() throws IOException, InitializationException {
        XMLRecordSetWriter writer = new XMLRecordSetWriter();
        TestRunner runner = setup(writer);

        runner.setProperty(writer, XMLRecordSetWriter.ROOT_TAG_NAME, "root");
        runner.setProperty(writer, XMLRecordSetWriter.RECORD_TAG_NAME, "record");

        runner.setProperty(writer, XMLRecordSetWriter.ARRAY_WRAPPING, XMLRecordSetWriter.USE_PROPERTY_AS_WRAPPER);
        runner.setProperty(writer, XMLRecordSetWriter.ARRAY_TAG_NAME, "wrap");

        runner.enableControllerService(writer);
        runner.enqueue("");
        runner.run();
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(TestXMLRecordSetWriterProcessor.SUCCESS, 1);

        String expected = "<root><record><wrap><array_field>1</array_field><array_field></array_field><array_field>3</array_field></wrap>" +
                "<name1>val1</name1><name2></name2></record>" +
                "<record><wrap><array_field>1</array_field><array_field></array_field><array_field>3</array_field></wrap>" +
                "<name1>val1</name1><name2></name2></record></root>";
        String actual = new String(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(TestXMLRecordSetWriterProcessor.SUCCESS).getFirst()));
        assertThat(expected, CompareMatcher.isSimilarTo(actual).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testValidation() throws IOException, InitializationException {
        XMLRecordSetWriter writer = new XMLRecordSetWriter();
        TestRunner runner = setup(writer);

        runner.setProperty(writer, XMLRecordSetWriter.ROOT_TAG_NAME, "root");
        runner.setProperty(writer, XMLRecordSetWriter.RECORD_TAG_NAME, "record");

        runner.setProperty(writer, XMLRecordSetWriter.ARRAY_WRAPPING, XMLRecordSetWriter.USE_PROPERTY_AS_WRAPPER);
        runner.assertNotValid(writer);

        runner.setProperty(writer, XMLRecordSetWriter.ARRAY_TAG_NAME, "array-tag-name");
        runner.assertValid(writer);

        runner.enableControllerService(writer);
        runner.enqueue("");

        // +
        runner.disableControllerService(writer);
        runner.removeProperty(writer, XMLRecordSetWriter.ARRAY_TAG_NAME);
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> runner.enableControllerService(writer));
        assertTrue(e.getMessage().contains(XMLRecordSetWriter.ARRAY_TAG_NAME.getName())
            && e.getMessage().contains(XMLRecordSetWriter.ARRAY_WRAPPING.getName())
            && e.getMessage().endsWith("has to be set."));
    }

    @Test
    void testMigrateProperties() {
        final XMLRecordSetWriter service = new XMLRecordSetWriter();
        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry("suppress_nulls", XMLRecordSetWriter.SUPPRESS_NULLS.getName()),
                Map.entry("pretty_print_xml", XMLRecordSetWriter.PRETTY_PRINT_XML.getName()),
                Map.entry("omit_xml_declaration", XMLRecordSetWriter.OMIT_XML_DECLARATION.getName()),
                Map.entry("root_tag_name", XMLRecordSetWriter.ROOT_TAG_NAME.getName()),
                Map.entry("record_tag_name", XMLRecordSetWriter.RECORD_TAG_NAME.getName()),
                Map.entry("array_wrapping", XMLRecordSetWriter.ARRAY_WRAPPING.getName()),
                Map.entry("array_tag_name", XMLRecordSetWriter.ARRAY_TAG_NAME.getName()),
                Map.entry("schema-cache", SchemaRegistryRecordSetWriter.SCHEMA_CACHE.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_ACCESS_STRATEGY_PROPERTY_NAME, SCHEMA_ACCESS_STRATEGY.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_REGISTRY_PROPERTY_NAME, SCHEMA_REGISTRY.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_NAME_PROPERTY_NAME, SCHEMA_NAME.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_BRANCH_NAME_PROPERTY_NAME, SCHEMA_BRANCH_NAME.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_VERSION_PROPERTY_NAME, SCHEMA_VERSION.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_TEXT_PROPERTY_NAME, SCHEMA_TEXT.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_REFERENCE_READER_PROPERTY_NAME, SCHEMA_REFERENCE_READER.getName())
        );

        final Map<String, String> propertyValues = Map.of();
        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(propertyValues);
        service.migrateProperties(configuration);

        final PropertyMigrationResult result = configuration.toPropertyMigrationResult();
        final Map<String, String> propertiesRenamed = result.getPropertiesRenamed();

        assertEquals(expectedRenamed, propertiesRenamed);

        final Set<String> expectedRemoved = Set.of("schema-protocol-version");
        assertEquals(expectedRemoved, result.getPropertiesRemoved());
    }

    static class _XMLRecordSetWriter extends XMLRecordSetWriter {

        RecordSchema recordSchema;

        _XMLRecordSetWriter(RecordSchema recordSchema) {
            this.recordSchema = recordSchema;
        }

        @Override
        public RecordSetWriter createWriter(ComponentLog logger, RecordSchema schema, OutputStream out, Map<String, String> attributes)
                throws SchemaNotFoundException, IOException {
            return super.createWriter(logger, this.recordSchema, out, attributes);
        }
    }
}
