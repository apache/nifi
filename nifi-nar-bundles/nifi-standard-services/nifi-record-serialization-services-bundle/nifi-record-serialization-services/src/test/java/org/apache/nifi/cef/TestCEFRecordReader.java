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

import com.fluenda.parcefone.parser.CEFParser;
import org.apache.bval.jsr.ApacheValidationProvider;
import org.apache.nifi.mock.MockComponentLogger;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.validation.Validation;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class TestCEFRecordReader {
    private final javax.validation.Validator validator = Validation.byProvider(ApacheValidationProvider.class).configure().buildValidatorFactory().getValidator();
    private final CEFParser parser = new CEFParser(validator);

    private RecordSchema schema;
    private InputStream inputStream;
    private CEFRecordReader testSubject;
    private List<Record> results;
    private boolean includeCustomExtensions;
    private boolean acceptEmptyExtensions;
    private boolean dropUnknownFields;
    private String rawField;
    private String invalidField;

    @Before
    public void setUp() {
        schema = null;
        inputStream = null;
        testSubject = null;
        results = null;
        includeCustomExtensions = false;
        acceptEmptyExtensions = false;
        dropUnknownFields = true;
        rawField = null;
        invalidField = null;
    }

    @After
    public void tearDown() throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }
    }

    @Test
    public void testReadingSingleLineWithHeaderFieldsUsingSchemaWithHeaderFields() throws Exception {
        // given
        givenSchema(CEFSchemaUtil.getHeaderFields());
        givenReader(TestCEFUtil.INPUT_SINGLE_ROW_HEADER_FIELDS_ONLY);

        // when
        whenReadingRecords();

        // then
        thenAssertNumberOfResults(1);
        thenAssertSchemaIsSet();
        thenAssertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES);
    }

    @Test
    public void testReadingSingleLineWithExtensionFieldsUsingSchemaWithHeaderFields() throws Exception {
        // given
        givenSchema(CEFSchemaUtil.getHeaderFields());
        givenReader(TestCEFUtil.INPUT_SINGLE_ROW_WITH_EXTENSIONS);

        // when
        whenReadingRecords();

        // then
        thenAssertNumberOfResults(1);
        thenAssertSchemaIsSet();
        thenAssertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES);
    }

    @Test
    public void testReadingSingleLineWithCustomExtensionFieldsUsingSchemaWithHeaderFields() throws Exception {
        // given
        givenSchema(CEFSchemaUtil.getHeaderFields());
        givenReader(TestCEFUtil.INPUT_SINGLE_ROW_WITH_CUSTOM_EXTENSIONS);

        // when
        whenReadingRecords();

        // then
        thenAssertNumberOfResults(1);
        thenAssertSchemaIsSet();
        thenAssertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES);
    }

    @Test
    public void testReadingSingleLineWithHeaderFieldsUsingSchemaWithExtensionFields() throws Exception {
        // given
        givenSchema(TestCEFUtil.givenFieldWithExtensions());
        givenReader(TestCEFUtil.INPUT_SINGLE_ROW_HEADER_FIELDS_ONLY);

        // when
        whenReadingRecords();

        // then
        thenAssertNumberOfResults(1);
        thenAssertSchemaIsSet();
        thenAssertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, withNulls(TestCEFUtil.EXPECTED_EXTENSION_VALUES));
    }

    @Test
    public void testReadingSingleLineWithExtensionFieldsUsingSchemaWithExtensionFields() throws Exception {
        // given
        givenSchema(TestCEFUtil.givenFieldWithExtensions());
        givenReader(TestCEFUtil.INPUT_SINGLE_ROW_WITH_EXTENSIONS);

        // when
        whenReadingRecords();

        // then
        thenAssertNumberOfResults(1);
        thenAssertSchemaIsSet();
        thenAssertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, TestCEFUtil.EXPECTED_EXTENSION_VALUES);
    }

    @Test
    public void testReadingSingleLineWithCustomExtensionFieldsUsingSchemaWithExtensionFields() throws Exception {
        // given
        givenSchema(TestCEFUtil.givenFieldWithExtensions());
        givenReader(TestCEFUtil.INPUT_SINGLE_ROW_WITH_CUSTOM_EXTENSIONS);

        // when
        whenReadingRecords();

        // then
        thenAssertNumberOfResults(1);
        thenAssertSchemaIsSet();
        thenAssertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, TestCEFUtil.EXPECTED_EXTENSION_VALUES);
    }

    @Test
    public void testReadingSingleLineWithHeaderFieldsUsingSchemaWithCustomExtensionFieldsAsStrings() throws Exception {
        // given
        givenSchema(TestCEFUtil.givenFieldsWithCustomExtensions(TestCEFUtil.CUSTOM_EXTENSION_FIELD_AS_STRING));
        givenReader(TestCEFUtil.INPUT_SINGLE_ROW_HEADER_FIELDS_ONLY);

        // when
        whenReadingRecords();

        // then
        thenAssertNumberOfResults(1);
        thenAssertSchemaIsSet();
        thenAssertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, withNulls(TestCEFUtil.EXPECTED_EXTENSION_VALUES), Collections.singletonMap(TestCEFUtil.CUSTOM_EXTENSION_FIELD_NAME, null));
    }

    @Test
    public void testReadingSingleLineWithExtensionFieldsUsingSchemaWithCustomExtensionFieldsAsStrings() throws Exception {
        // given
        givenSchema(TestCEFUtil.givenFieldsWithCustomExtensions(TestCEFUtil.CUSTOM_EXTENSION_FIELD_AS_STRING));
        givenIncludeCustomExtensions();
        givenReader(TestCEFUtil.INPUT_SINGLE_ROW_WITH_EXTENSIONS);

        // when
        whenReadingRecords();

        // then
        thenAssertNumberOfResults(1);
        thenAssertSchemaIsSet();
        thenAssertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, TestCEFUtil.EXPECTED_EXTENSION_VALUES, Collections.singletonMap(TestCEFUtil.CUSTOM_EXTENSION_FIELD_NAME, null));
    }

    @Test
    public void testReadingSingleLineWithCustomExtensionFieldsUsingSchemaWithCustomExtensionFieldsAsStrings() throws Exception {
        // given
        givenSchema(TestCEFUtil.givenFieldsWithCustomExtensions(TestCEFUtil.CUSTOM_EXTENSION_FIELD_AS_STRING));
        givenIncludeCustomExtensions();
        givenReader(TestCEFUtil.INPUT_SINGLE_ROW_WITH_CUSTOM_EXTENSIONS);

        // when
        whenReadingRecords();

        // then
        thenAssertNumberOfResults(1);
        thenAssertSchemaIsSet();
        thenAssertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, TestCEFUtil.EXPECTED_EXTENSION_VALUES, Collections.singletonMap(TestCEFUtil.CUSTOM_EXTENSION_FIELD_NAME, "123"));
    }

    @Test
    public void testReadingSingleLineWithHeaderFieldsUsingSchemaWithCustomExtensionFields() throws Exception {
        // given
        givenSchema(TestCEFUtil.givenFieldsWithCustomExtensions(TestCEFUtil.CUSTOM_EXTENSION_FIELD));
        givenIncludeCustomExtensions();
        givenReader(TestCEFUtil.INPUT_SINGLE_ROW_HEADER_FIELDS_ONLY);

        // when
        whenReadingRecords();

        // then
        thenAssertNumberOfResults(1);
        thenAssertSchemaIsSet();
        thenAssertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, withNulls(TestCEFUtil.EXPECTED_EXTENSION_VALUES), Collections.singletonMap(TestCEFUtil.CUSTOM_EXTENSION_FIELD_NAME, null));
    }

    @Test
    public void testReadingSingleLineWithExtensionFieldsUsingSchemaWithCustomExtensionFields() throws Exception {
        // given
        givenSchema(TestCEFUtil.givenFieldsWithCustomExtensions(TestCEFUtil.CUSTOM_EXTENSION_FIELD));
        givenIncludeCustomExtensions();
        givenReader(TestCEFUtil.INPUT_SINGLE_ROW_WITH_EXTENSIONS);

        // when
        whenReadingRecords();

        // then
        thenAssertNumberOfResults(1);
        thenAssertSchemaIsSet();
        thenAssertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, TestCEFUtil.EXPECTED_EXTENSION_VALUES, Collections.singletonMap(TestCEFUtil.CUSTOM_EXTENSION_FIELD_NAME, null));
    }

    @Test
    public void testReadingSingleLineWithCustomExtensionFieldsUsingSchemaWithCustomExtensionFields() throws Exception {
        // given
        givenSchema(TestCEFUtil.givenFieldsWithCustomExtensions(TestCEFUtil.CUSTOM_EXTENSION_FIELD));
        givenIncludeCustomExtensions();
        givenReader(TestCEFUtil.INPUT_SINGLE_ROW_WITH_CUSTOM_EXTENSIONS);

        // when
        whenReadingRecords();

        // then
        thenAssertNumberOfResults(1);
        thenAssertSchemaIsSet();
        thenAssertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, TestCEFUtil.EXPECTED_EXTENSION_VALUES, Collections.singletonMap(TestCEFUtil.CUSTOM_EXTENSION_FIELD_NAME, 123));
    }

    @Test
    public void testReadingContainingRawEvent() throws Exception {
        // given
        givenSchema(givenFieldsWithHeaderAndRaw());
        givenRawMessageField(TestCEFUtil.RAW_FIELD);
        givenReader(TestCEFUtil.INPUT_SINGLE_ROW_HEADER_FIELDS_ONLY);

        // when
        whenReadingRecords();

        // then
        thenAssertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, Collections.singletonMap(TestCEFUtil.RAW_FIELD, TestCEFUtil.RAW_VALUE));
    }

    @Test
    public void testSingleLineWithHeaderFieldsAndNotDroppingUnknownFields() throws Exception {
        // given
        givenKeepUnknownFields();
        givenSchema(givenFieldsWithExtensionsExpect("c6a1", "dmac"));
        givenReader(TestCEFUtil.INPUT_SINGLE_ROW_WITH_EXTENSIONS);

        // when
        whenReadingRecords();

        // then
        thenAssertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, without(TestCEFUtil.EXPECTED_EXTENSION_VALUES, "c6a1", "dmac"));
        thenAssertFieldIs("c6a1", TestCEFUtil.EXPECTED_EXTENSION_VALUES.get("c6a1"));
        thenAssertFieldIs("dmac", TestCEFUtil.EXPECTED_EXTENSION_VALUES.get("dmac"));
    }

    @Test
    public void testReadingEmptyRow()  throws Exception {
        // given
        givenSchema(CEFSchemaUtil.getHeaderFields());
        givenReader(TestCEFUtil.INPUT_EMPTY_ROW);

        // when
        whenReadingRecords();

        // then
        thenAssertNumberOfResults(0);
    }

    @Test(expected = MalformedRecordException.class)
    public void testReadingMisformattedRow()  throws Exception {
        // given
        givenSchema(CEFSchemaUtil.getHeaderFields());
        givenReader(TestCEFUtil.INPUT_MISFORMATTED_ROW);

        // when
        whenReadingRecords();
    }

    @Test
    public void testReadingMisformattedRowWhenInvalidFieldIsSet()  throws Exception {
        // given
        givenInvalidFieldIsSet();
        givenSchema(CEFSchemaUtil.getHeaderFields());
        givenReader(TestCEFUtil.INPUT_MISFORMATTED_ROW);

        // when
        whenReadingRecords();

        //
        thenAssertNumberOfResults(3);
    }

    @Test
    public void testReadingMultipleIdenticalRows() throws Exception {
        // given
        givenSchema(CEFSchemaUtil.getHeaderFields());
        givenReader(TestCEFUtil.INPUT_MULTIPLE_IDENTICAL_ROWS);

        // when
        whenReadingRecords();

        // then
        thenAssertNumberOfResults(3);
        thenAssertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES);
    }

    @Test
    public void testReadingMultipleIdenticalRowsWithEmptyOnes() throws Exception {
        // given
        givenSchema(CEFSchemaUtil.getHeaderFields());
        givenReader(TestCEFUtil.INPUT_MULTIPLE_ROWS_WITH_EMPTY_ROWS);

        // when
        whenReadingRecords(5);

        // then
        thenAssertNumberOfResults(3);
        thenAssertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES);
    }

    @Test
    public void testReadingMultipleRowsWithDecreasingNumberOfExtensionFields() throws Exception {
        // given
        givenSchema(TestCEFUtil.givenFieldWithExtensions());
        givenReader(TestCEFUtil.INPUT_MULTIPLE_ROWS_WITH_DECREASING_NUMBER_OF_EXTENSIONS);

        // when
        whenReadingRecords();

        // then
        thenAssertNumberOfResults(2);
        thenAssertFieldsAre(0, TestCEFUtil.EXPECTED_HEADER_VALUES, TestCEFUtil.EXPECTED_EXTENSION_VALUES);
        thenAssertFieldsAre(1, TestCEFUtil.EXPECTED_HEADER_VALUES, withNulls(TestCEFUtil.EXPECTED_EXTENSION_VALUES, "c6a1", "dmac"));
    }

    @Test
    public void testReadingMultipleRowsWithIncreasingNumberOfExtensionFields() throws Exception {
        // given
        givenSchema(givenFieldsWithExtensionsExpect("c6a1", "dmac"));
        givenReader(TestCEFUtil.INPUT_MULTIPLE_ROWS_WITH_INCREASING_NUMBER_OF_EXTENSIONS);

        // when
        whenReadingRecords();

        // then
        thenAssertNumberOfResults(2);
        thenAssertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, without(TestCEFUtil.EXPECTED_EXTENSION_VALUES, "c6a1", "dmac"));
    }

    @Test(expected = NumberFormatException.class)
    public void testReadingIncorrectHeaderField() throws Exception {
        // given
        givenSchema(CEFSchemaUtil.getHeaderFields());
        givenReader(TestCEFUtil.INPUT_SINGLE_ROW_WITH_INCORRECT_HEADER_FIELD);

        // when
        whenReadingRecords();
    }

    @Test(expected = NumberFormatException.class)
    public void testReadingIncorrectCustomVariableFormat() throws Exception {
        // given
        givenSchema(TestCEFUtil.givenFieldsWithCustomExtensions(TestCEFUtil.CUSTOM_EXTENSION_FIELD));
        givenIncludeCustomExtensions();
        givenReader(TestCEFUtil.INPUT_SINGLE_ROW_WITH_INCORRECT_CUSTOM_EXTENSIONS);

        // when
        whenReadingRecords();
    }

    @Test
    public void testReadingEmptyValuesWhenAcceptingEmptyExtensions() throws Exception {
        // given
        givenSchema(TestCEFUtil.givenFieldWithExtensions());
        givenAcceptingEmptyExtensions();
        givenReader(TestCEFUtil.INPUT_SINGLE_ROW_WITH_EMPTY_EXTENSION);

        // when
        whenReadingRecords();

        // then
        thenAssertNumberOfResults(1);
        thenAssertSchemaIsSet();

        final Map<String, Object> expectedExtensionValues = new HashMap<>();
        expectedExtensionValues.putAll(TestCEFUtil.EXPECTED_EXTENSION_VALUES);
        expectedExtensionValues.put("cn1", null);
        expectedExtensionValues.put("cn1Label", "");

        thenAssertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, expectedExtensionValues);
    }

    @Test(expected = NumberFormatException.class)
    public void testReadingEmptyValuesWhenNotAcceptingEmptyExtensions() throws Exception {
        // given
        givenSchema(TestCEFUtil.givenFieldWithExtensions());
        givenReader(TestCEFUtil.INPUT_SINGLE_ROW_WITH_EMPTY_EXTENSION);

        // when
        whenReadingRecords();

        // then - as empty is not accepted, number type fields will not be parsed properly
    }

    private static List<RecordField> givenFieldsWithHeaderAndRaw() {
        final List<RecordField> result = new ArrayList<>(CEFSchemaUtil.getHeaderFields());
        result.add(new RecordField(TestCEFUtil.RAW_FIELD, RecordFieldType.STRING.getDataType()));
        return result;
    }

    private List<RecordField> givenFieldsWithExtensionsExpect(final String... fieldsToExclude) {
        final List<RecordField> result = new ArrayList<>();
        final Set<String> excludedFields = new HashSet<>();
        Arrays.stream(fieldsToExclude).forEach(f -> excludedFields.add(f));

        for (final RecordField record : TestCEFUtil.givenFieldWithExtensions()) {
            if (!excludedFields.contains(record.getFieldName())) {
                result.add(record);
            }
        }

        return result;
    }

    private void givenSchema(final List<RecordField> fields) {
        this.schema = new SimpleRecordSchema(fields);
    }

    private void givenIncludeCustomExtensions() {
        includeCustomExtensions = true;
    }

    private void givenAcceptingEmptyExtensions() {
        acceptEmptyExtensions = true;
    }

    private void givenRawMessageField(final String rawField) {
        this.rawField = rawField;
    }

    private void givenKeepUnknownFields() {
        dropUnknownFields = false;
    }

    private void givenInvalidFieldIsSet() {
        invalidField = "invalid";
    }

    private void givenReader(final String file) throws FileNotFoundException {
        inputStream = new FileInputStream(file);
        testSubject = new CEFRecordReader(inputStream, schema, parser, new MockComponentLogger(), Locale.US, rawField, invalidField, includeCustomExtensions, acceptEmptyExtensions);
    }

    private void whenReadingRecords() throws IOException, MalformedRecordException {
        final List<Record> results = new ArrayList<>();
        Record result;

        while ((result = testSubject.nextRecord(true, dropUnknownFields)) != null) {
            results.add(result);
        }

        this.results = results;
    }

    private void whenReadingRecords(final int count) throws IOException, MalformedRecordException {
        final List<Record> results = new ArrayList<>();

        for (int i=1; i<=count; i++) {
            final Record record = testSubject.nextRecord(true, dropUnknownFields);

            if (record != null) {
                results.add(record);
            }
        }

        this.results = results;
    }

    private void thenAssertFieldsAre(final Map<String, Object>... fieldGroups) {
        final Map<String, Object> expectedFields = new HashMap<>();
        Arrays.stream(fieldGroups).forEach(fieldGroup -> expectedFields.putAll(fieldGroup));
        results.forEach(r -> TestCEFUtil.thenAssertFieldsAre(r, expectedFields));
    }

    private void thenAssertFieldsAre(final int number, final Map<String, Object>... fieldGroups) {
        final Map<String, Object> expectedFields = new HashMap<>();
        Arrays.stream(fieldGroups).forEach(fieldGroup -> expectedFields.putAll(fieldGroup));
        TestCEFUtil.thenAssertFieldsAre(results.get(number), expectedFields);
    }

    private void thenAssertFieldIs(final String field, final Object expectedValue) {
        results.forEach(r -> Assert.assertEquals(expectedValue, r.getValue(field)));
    }

    private void thenAssertNumberOfResults(final int numberOfResults) {
        Assert.assertEquals(numberOfResults, results.size());
    }

    private void thenAssertSchemaIsSet() {
        results.forEach(r -> Assert.assertEquals(schema, r.getSchema()));
    }

    private static Map<String, Object> withNulls(final Map<String, Object> expectedFields) {
        final Map<String, Object> result = new HashMap<>();
        expectedFields.keySet().forEach(f -> result.put(f, null));
        return result;
    }

    private static Map<String, Object> withNulls(final Map<String, Object> expectedFields, final String... fieldsWithNullValue) {
        final Map<String, Object> result = new HashMap<>(expectedFields);
        Arrays.stream(fieldsWithNullValue).forEach(field -> result.put(field, null));
        return result;
    }

    private static Map<String, Object> without(final Map<String, Object> expectedFields, final String... fieldsToRemove) {
        final Map<String, Object> result = new HashMap<>();
        final Set<String> fieldsToNotInclude = new HashSet<>(Arrays.asList(fieldsToRemove));

        for (final Map.Entry<String, Object> field : expectedFields.entrySet()) {
            if (!fieldsToNotInclude.contains(field.getKey())) {
                result.put(field.getKey(), field.getValue());
            }
        }

        return result;
    }
}
