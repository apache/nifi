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
package org.apache.nifi.cef;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.inference.SchemaInferenceUtil;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MockSchemaRegistry;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestCEFReader {
    private TestRunner runner;
    private TestCEFProcessor processor;
    private CEFReader reader;

    @BeforeEach
    public void setUp() {
        runner = null;
        processor = null;
        reader = null;
    }

    @Test
    public void testValidatingReaderWhenRawFieldValueIsInvalid() throws Exception {
        setUpReader();
        setRawField("dst"); // Invalid because there is an extension with the same name
        setSchemaIsInferred(CEFReader.HEADERS_ONLY);

        assertReaderIsInvalid();
    }

    @Test
    public void testReadingSingleRowWithHeaderFields() throws Exception {
        setUpReader();
        setSchemaIsInferred(CEFReader.HEADERS_ONLY);
        enableReader();

        triggerProcessor(TestCEFUtil.INPUT_SINGLE_ROW_HEADER_FIELDS_ONLY);

        assertNumberOfResults(1);
        assertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES);
    }

    @Test
    public void testReadingSingleRowWithHeaderFieldsAndRaw() throws Exception {
        setUpReader();
        setRawField(TestCEFUtil.RAW_FIELD);
        setSchemaIsInferred(CEFReader.HEADERS_ONLY);
        enableReader();

        triggerProcessor(TestCEFUtil.INPUT_SINGLE_ROW_HEADER_FIELDS_ONLY);

        assertNumberOfResults(1);
        assertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, Collections.singletonMap(TestCEFUtil.RAW_FIELD, TestCEFUtil.RAW_VALUE));
    }

    @Test
    public void testReadingSingleRowWithExtensionFieldsWhenSchemaIsHeadersOnly() throws Exception {
        setUpReader();
        setSchemaIsInferred(CEFReader.HEADERS_ONLY);
        enableReader();

        triggerProcessor(TestCEFUtil.INPUT_SINGLE_ROW_WITH_EXTENSIONS);

        assertNumberOfResults(1);
        assertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES);
    }

    @Test
    public void testReadingSingleRowWithCustomExtensionFieldsAsStrings() throws Exception {
        setUpReader();
        setSchemaIsInferred(CEFReader.CUSTOM_EXTENSIONS_AS_STRINGS);
        enableReader();

        triggerProcessor(TestCEFUtil.INPUT_SINGLE_ROW_WITH_CUSTOM_EXTENSIONS);

        assertNumberOfResults(1);
        assertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, TestCEFUtil.EXPECTED_EXTENSION_VALUES, Collections.singletonMap(TestCEFUtil.CUSTOM_EXTENSION_FIELD_NAME, "123"));
    }

    @Test
    public void testMisformattedRowsWithoutInvalidFieldIsBeingSet() throws Exception {
        setUpReader();
        setSchemaIsInferred(CEFReader.CUSTOM_EXTENSIONS_INFERRED);
        enableReader();

        triggerProcessorWithError(TestCEFUtil.INPUT_MISFORMATTED_ROW);
    }

    @Test
    public void testMisformattedRowsWithInvalidFieldIsSet() throws Exception {
        setUpReader();
        setInvalidField();
        setSchemaIsInferred(CEFReader.CUSTOM_EXTENSIONS_INFERRED);
        enableReader();

        triggerProcessor(TestCEFUtil.INPUT_MISFORMATTED_ROW);

        assertNumberOfResults(3);
        assertFieldIsSet(1, "invalid", "Oct 12 04:16:11 localhost CEF:0|nxlog.org|nxlog|2.7.1243|");
    }

    @Test
    public void testReadingSingleRowWithCustomExtensionFields() throws Exception {
        setUpReader();
        setSchemaIsInferred(CEFReader.CUSTOM_EXTENSIONS_INFERRED);
        enableReader();

        triggerProcessor(TestCEFUtil.INPUT_SINGLE_ROW_WITH_CUSTOM_EXTENSIONS);

        assertNumberOfResults(1);
        assertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, TestCEFUtil.EXPECTED_EXTENSION_VALUES, Collections.singletonMap(TestCEFUtil.CUSTOM_EXTENSION_FIELD_NAME, 123));
    }

    @Test
    public void testReadingMultipleRows() throws Exception {
        setUpReader();
        setSchemaIsInferred(CEFReader.HEADERS_ONLY);
        enableReader();

        triggerProcessor(TestCEFUtil.INPUT_MULTIPLE_IDENTICAL_ROWS);

        assertNumberOfResults(3);
        assertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES);
    }

    @Test
    public void testReadingMultipleRowsWithEmptyRows() throws Exception {
        setUpReader();
        setSchemaIsInferred(CEFReader.HEADERS_ONLY);
        enableReader();

        triggerProcessor(TestCEFUtil.INPUT_MULTIPLE_ROWS_WITH_EMPTY_ROWS);

        assertNumberOfResults(3);
        assertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES);
    }

    @Test
    public void testReadingMultipleRowsStartingWithEmptyRow() throws Exception {
        setUpReader();
        setSchemaIsInferred(CEFReader.HEADERS_ONLY);
        enableReader();

        triggerProcessor(TestCEFUtil.INPUT_MULTIPLE_ROWS_STARTING_WITH_EMPTY_ROW);

        assertNumberOfResults(3);
        assertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES);
    }

    @Test
    public void testWithPredefinedSchema() throws Exception {
        setUpReader();
        setSchema(CEFSchemaUtil.getHeaderFields());
        enableReader();

        triggerProcessor(TestCEFUtil.INPUT_SINGLE_ROW_WITH_EXTENSIONS);

        assertNumberOfResults(1);
        assertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES);
    }

    @Test
    public void testReadingSingleRowWithEmptyExtensionFields() throws Exception {
        setUpReader();
        setAcceptEmptyExtensions();
        setSchemaIsInferred(CEFReader.CUSTOM_EXTENSIONS_INFERRED);
        enableReader();

        triggerProcessor(TestCEFUtil.INPUT_SINGLE_ROW_WITH_EMPTY_CUSTOM_EXTENSIONS);

        assertNumberOfResults(1);
        assertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, TestCEFUtil.EXPECTED_EXTENSION_VALUES, Collections.singletonMap(TestCEFUtil.CUSTOM_EXTENSION_FIELD_NAME, ""));
    }

    private void setUpReader() throws InitializationException {
        processor = new TestCEFProcessor();
        runner = TestRunners.newTestRunner(processor);
        reader = new CEFReader();
        runner.addControllerService("reader", reader);
        runner.setProperty(TestCEFProcessor.READER, "reader");
    }

    private void setAcceptEmptyExtensions() {
        runner.setProperty(reader, CEFReader.ACCEPT_EMPTY_EXTENSIONS, "true");
    }

    private void setSchemaIsInferred(final AllowableValue value) {
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA);
        runner.setProperty(reader, CEFReader.INFERENCE_STRATEGY, value);
    }

    private void setInvalidField() {
        runner.setProperty(reader, CEFReader.INVALID_FIELD, "invalid");
    }

    private void setSchema(List<RecordField> fields) throws InitializationException {
        final MockSchemaRegistry registry = new MockSchemaRegistry();
        registry.addSchema("predefinedSchema", new SimpleRecordSchema(fields));
        runner.addControllerService("registry", registry);
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_NAME_PROPERTY);
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_NAME, "predefinedSchema");
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_REGISTRY, "registry");
        runner.enableControllerService(registry);
    }

    private void setRawField(final String value) {
        runner.setProperty(reader, CEFReader.RAW_FIELD, value);
    }

    private void enableReader() {
        runner.assertValid(reader);
        runner.enableControllerService(reader);
    }

    private void triggerProcessor(final String input) throws FileNotFoundException {
        runner.enqueue(new FileInputStream(input));
        runner.run();
        runner.assertAllFlowFilesTransferred(TestCEFProcessor.SUCCESS);
    }

    private void triggerProcessorWithError(final String input) throws FileNotFoundException {
        runner.enqueue(new FileInputStream(input));
        final AssertionError exception = assertThrows(AssertionError.class, () -> runner.run());
        assertInstanceOf(RuntimeException.class, exception.getCause());
        assertInstanceOf(IOException.class, exception.getCause().getCause());
    }

    private void assertNumberOfResults(final int numberOfResults) {
        assertEquals(numberOfResults, processor.getRecords().size());
    }

    private void assertReaderIsInvalid() {
        runner.assertNotValid(reader);
    }

    private void assertFieldIsSet(final int number, final String name, final String value) {
        assertEquals(value, processor.getRecords().get(number).getValue(name));
    }

    private void assertFieldsAre(final Map<String, Object>... fieldGroups) {
        final Map<String, Object> expectedFields = new HashMap<>();
        Arrays.stream(fieldGroups).forEach(fieldGroup -> expectedFields.putAll(fieldGroup));
        processor.getRecords().forEach(r -> TestCEFUtil.assertFieldsAre(r, expectedFields));
    }

    private static class TestCEFProcessor extends AbstractProcessor {
        private final List<Record> records = new ArrayList<>();

        static final Relationship SUCCESS = new Relationship.Builder().name("success").build();

        static final PropertyDescriptor READER = new PropertyDescriptor.Builder()
                .name("reader")
                .identifiesControllerService(CEFReader.class)
                .build();

        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
            final FlowFile flowFile = session.get();
            final RecordReaderFactory readerFactory = context.getProperty(READER).asControllerService(RecordReaderFactory.class);

            try (
                final InputStream in = session.read(flowFile);
                final RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger())
            ) {
                Record record;
                while ((record = reader.nextRecord()) != null) {
                    records.add(record);
                }
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }

            session.transfer(flowFile, SUCCESS);
        }

        @Override
        public Set<Relationship> getRelationships() {
            return Collections.singleton(SUCCESS);
        }

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return Collections.singletonList(READER);
        }

        public List<Record> getRecords() {
            return records;
        }
    }
}
