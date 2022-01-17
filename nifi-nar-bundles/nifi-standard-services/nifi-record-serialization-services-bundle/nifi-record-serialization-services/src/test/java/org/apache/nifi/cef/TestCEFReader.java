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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

public class TestCEFReader {
    private TestRunner runner;
    private TestCEFProcessor processor;
    private CEFReader reader;

    @Before
    public void setUp() {
        runner = null;
        processor = null;
        reader = null;
    }

    @Test
    public void testValidatingReaderWhenRawFieldValueIsInvalid() throws Exception {
        // given
        givenReaderSetUp();
        givenRawFieldIs("dst"); // Invalid because there is an extension with the same name
        givenSchemaIsInferred(CEFReader.HEADERS_ONLY);

        // when & then
        thenAssertInvalid();
    }

    @Test
    public void testReadingSingleRowWithHeaderFields() throws Exception {
        // given
        givenReaderSetUp();
        givenSchemaIsInferred(CEFReader.HEADERS_ONLY);
        givenReaderIsEnabled();

        // when
        whenProcessorRuns(TestCEFUtil.INPUT_SINGLE_ROW_HEADER_FIELDS_ONLY);

        // then
        thenAssertNumberOfResults(1);
        thenAssertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES);
    }

    @Test
    public void testReadingSingleRowWithHeaderFieldsAndRaw() throws Exception {
        // given
        givenReaderSetUp();
        givenRawFieldIs(TestCEFUtil.RAW_FIELD);
        givenSchemaIsInferred(CEFReader.HEADERS_ONLY);
        givenReaderIsEnabled();

        // when
        whenProcessorRuns(TestCEFUtil.INPUT_SINGLE_ROW_HEADER_FIELDS_ONLY);

        // then
        thenAssertNumberOfResults(1);
        thenAssertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, Collections.singletonMap(TestCEFUtil.RAW_FIELD, TestCEFUtil.RAW_VALUE));
    }

    @Test
    public void testReadingSingleRowWithExtensionFieldsWhenSchemaIsHeadersOnly() throws Exception {
        // given
        givenReaderSetUp();
        givenSchemaIsInferred(CEFReader.HEADERS_ONLY);
        givenReaderIsEnabled();

        // when
        whenProcessorRuns(TestCEFUtil.INPUT_SINGLE_ROW_WITH_EXTENSIONS);

        // then
        thenAssertNumberOfResults(1);
        thenAssertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES);
    }

    @Test
    public void testReadingSingleRowWithCustomExtensionFieldsAsStrings() throws Exception {
        // given
        givenReaderSetUp();
        givenSchemaIsInferred(CEFReader.CUSTOM_EXTENSIONS_AS_STRINGS);
        givenReaderIsEnabled();

        // when
        whenProcessorRuns(TestCEFUtil.INPUT_SINGLE_ROW_WITH_CUSTOM_EXTENSIONS);

        // then
        thenAssertNumberOfResults(1);
        thenAssertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, TestCEFUtil.EXPECTED_EXTENSION_VALUES, Collections.singletonMap(TestCEFUtil.CUSTOM_EXTENSION_FIELD_NAME, "123"));
    }

    @Test
    public void testMisformattedRowsWithoutInvalidFieldIsBeingSet() throws Exception {
        // given
        givenReaderSetUp();
        givenSchemaIsInferred(CEFReader.CUSTOM_EXTENSIONS_INFERRED);
        givenReaderIsEnabled();

        // when
        whenProcessorRunsWithError(TestCEFUtil.INPUT_MISFORMATTED_ROW);
    }

    @Test
    public void testMisformattedRowsWithInvalidFieldIsSet() throws Exception {
        // given
        givenReaderSetUp();
        givenInvalidFieldIsSet();
        givenSchemaIsInferred(CEFReader.CUSTOM_EXTENSIONS_INFERRED);
        givenReaderIsEnabled();

        // when
        whenProcessorRuns(TestCEFUtil.INPUT_MISFORMATTED_ROW);

        // then
        thenAssertNumberOfResults(3);
        thenAssertFieldIsSet(1, "invalid", "Oct 12 04:16:11 localhost CEF:0|nxlog.org|nxlog|2.7.1243|");
    }

    @Test
    public void testReadingSingleRowWithCustomExtensionFields() throws Exception {
        // given
        givenReaderSetUp();
        givenSchemaIsInferred(CEFReader.CUSTOM_EXTENSIONS_INFERRED);
        givenReaderIsEnabled();

        // when
        whenProcessorRuns(TestCEFUtil.INPUT_SINGLE_ROW_WITH_CUSTOM_EXTENSIONS);

        // then
        thenAssertNumberOfResults(1);
        thenAssertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, TestCEFUtil.EXPECTED_EXTENSION_VALUES, Collections.singletonMap(TestCEFUtil.CUSTOM_EXTENSION_FIELD_NAME, 123));
    }

    @Test
    public void testReadingMultipleRows() throws Exception {
        // given
        givenReaderSetUp();
        givenSchemaIsInferred(CEFReader.HEADERS_ONLY);
        givenReaderIsEnabled();

        // when
        whenProcessorRuns(TestCEFUtil.INPUT_MULTIPLE_IDENTICAL_ROWS);

        // then
        thenAssertNumberOfResults(3);
        thenAssertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES);
    }

    @Test
    public void testReadingMultipleRowsWithEmptyRows() throws Exception {
        // given
        givenReaderSetUp();
        givenSchemaIsInferred(CEFReader.HEADERS_ONLY);
        givenReaderIsEnabled();

        // when
        whenProcessorRuns(TestCEFUtil.INPUT_MULTIPLE_ROWS_WITH_EMPTY_ROWS);

        // then
        thenAssertNumberOfResults(3);
        thenAssertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES);
    }

    @Test
    public void testReadingMultipleRowsStartingWithEmptyRow() throws Exception {
        // given
        givenReaderSetUp();
        givenSchemaIsInferred(CEFReader.HEADERS_ONLY);
        givenReaderIsEnabled();

        // when
        whenProcessorRuns(TestCEFUtil.INPUT_MULTIPLE_ROWS_STARTING_WITH_EMPTY_ROW);

        // then
        thenAssertNumberOfResults(3);
        thenAssertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES);
    }

    @Test
    public void testWithPredefinedSchema() throws Exception {
        // given
        givenReaderSetUp();
        givenSchema(CEFSchemaUtil.getHeaderFields());
        givenReaderIsEnabled();

        // when
        whenProcessorRuns(TestCEFUtil.INPUT_SINGLE_ROW_WITH_EXTENSIONS);

        // then
        thenAssertNumberOfResults(1);
        thenAssertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES);
    }

    @Test
    public void testReadingSingleRowWithEmptyExtensionFields() throws Exception {
        // given
        givenReaderSetUp();
        givenAcceptEmptyExtensions();
        givenSchemaIsInferred(CEFReader.CUSTOM_EXTENSIONS_INFERRED);
        givenReaderIsEnabled();

        // when
        whenProcessorRuns(TestCEFUtil.INPUT_SINGLE_ROW_WITH_EMPTY_CUSTOM_EXTENSIONS);

        // then
        thenAssertNumberOfResults(1);
        thenAssertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, TestCEFUtil.EXPECTED_EXTENSION_VALUES, Collections.singletonMap(TestCEFUtil.CUSTOM_EXTENSION_FIELD_NAME, ""));
    }

    private void givenReaderSetUp() throws InitializationException {
        processor = new TestCEFProcessor();
        runner = TestRunners.newTestRunner(processor);
        reader = new CEFReader();
        runner.addControllerService("reader", reader);
        runner.setProperty(TestCEFProcessor.READER, "reader");
    }

    private void givenAcceptEmptyExtensions() {
        runner.setProperty(reader, CEFReader.ACCEPT_EMPTY_EXTENSIONS, "true");
    }

    private void givenSchemaIsInferred(final AllowableValue value) {
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA);
        runner.setProperty(reader, CEFReader.INFERENCE_STRATEGY, value);
    }

    private void givenInvalidFieldIsSet() {
        runner.setProperty(reader, CEFReader.INVALID_FIELD, "invalid");
    }

    private void givenSchema(List<RecordField> fields) throws InitializationException {
        final MockSchemaRegistry registry = new MockSchemaRegistry();
        registry.addSchema("predefinedSchema", new SimpleRecordSchema(fields));
        runner.addControllerService("registry", registry);
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_NAME_PROPERTY);
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_NAME, "predefinedSchema");
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_REGISTRY, "registry");
        runner.enableControllerService(registry);
    }

    private void givenRawFieldIs(final String value) {
        runner.setProperty(reader, CEFReader.RAW_FIELD, value);
    }

    private void givenReaderIsEnabled() {
        runner.assertValid(reader);
        runner.enableControllerService(reader);
    }

    private void whenProcessorRuns(final String input) throws FileNotFoundException {
        runner.enqueue(new FileInputStream(input));
        runner.run();
        runner.assertAllFlowFilesTransferred(TestCEFProcessor.SUCCESS);
    }

    private void whenProcessorRunsWithError(final String input) {
        try {
            runner.enqueue(new FileInputStream(input));
            runner.run();
            Assert.fail();
        } catch (final Throwable e) {
            // the TestCEFProcessor wraps the original exception into a RuntimeException
            Assert.assertTrue(e.getCause() instanceof RuntimeException);
            Assert.assertTrue(e.getCause().getCause() instanceof IOException);
        }
    }

    private void thenAssertNumberOfResults(final int numberOfResults) {
        Assert.assertEquals(numberOfResults, processor.getRecords().size());
    }

    private void thenAssertInvalid() {
        runner.assertNotValid(reader);
    }

    private void thenAssertFieldIsSet(final int number, final String name, final String value) {
        Assert.assertEquals(value, processor.getRecords().get(number).getValue(name));
    }

    private void thenAssertFieldsAre(final Map<String, Object>... fieldGroups) {
        final Map<String, Object> expectedFields = new HashMap<>();
        Arrays.stream(fieldGroups).forEach(fieldGroup -> expectedFields.putAll(fieldGroup));
        processor.getRecords().forEach(r -> TestCEFUtil.thenAssertFieldsAre(r, expectedFields));
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
            return Arrays.asList(READER);
        }

        public List<Record> getRecords() {
            return records;
        }
    }
}
