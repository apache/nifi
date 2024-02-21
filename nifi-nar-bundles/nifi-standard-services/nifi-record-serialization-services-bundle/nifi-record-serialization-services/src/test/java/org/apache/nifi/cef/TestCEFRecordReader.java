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
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

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

    private final CEFParser parser = new CEFParser();

    private RecordSchema schema;
    private InputStream inputStream;
    private CEFRecordReader testSubject;
    private List<Record> results;
    private boolean includeCustomExtensions;
    private boolean acceptEmptyExtensions;
    private boolean dropUnknownFields;
    private String rawField;
    private String invalidField;

    @BeforeEach
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

    @AfterEach
    public void tearDown() throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }
    }

    @Test
    public void testReadingSingleLineWithHeaderFieldsUsingSchemaWithHeaderFields() throws Exception {
        setSchema(CEFSchemaUtil.getHeaderFields());
        setReader(TestCEFUtil.INPUT_SINGLE_ROW_HEADER_FIELDS_ONLY);

        readRecords();

        assertNumberOfResults(1);
        assertSchemaIsSet();
        assertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES);
    }

    @Test
    public void testReadingSingleLineWithExtensionFieldsUsingSchemaWithHeaderFields() throws Exception {
        setSchema(CEFSchemaUtil.getHeaderFields());
        setReader(TestCEFUtil.INPUT_SINGLE_ROW_WITH_EXTENSIONS);

        readRecords();

        assertNumberOfResults(1);
        assertSchemaIsSet();
        assertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES);
    }

    @Test
    public void testReadingSingleLineWithCustomExtensionFieldsUsingSchemaWithHeaderFields() throws Exception {
        setSchema(CEFSchemaUtil.getHeaderFields());
        setReader(TestCEFUtil.INPUT_SINGLE_ROW_WITH_CUSTOM_EXTENSIONS);

        readRecords();

        assertNumberOfResults(1);
        assertSchemaIsSet();
        assertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES);
    }

    @Test
    public void testReadingSingleLineWithHeaderFieldsUsingSchemaWithExtensionFields() throws Exception {
        setSchema(TestCEFUtil.getFieldWithExtensions());
        setReader(TestCEFUtil.INPUT_SINGLE_ROW_HEADER_FIELDS_ONLY);

        readRecords();

        assertNumberOfResults(1);
        assertSchemaIsSet();
        assertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, withNulls(TestCEFUtil.EXPECTED_EXTENSION_VALUES));
    }

    @Test
    public void testReadingSingleLineWithExtensionFieldsUsingSchemaWithExtensionFields() throws Exception {
        setSchema(TestCEFUtil.getFieldWithExtensions());
        setReader(TestCEFUtil.INPUT_SINGLE_ROW_WITH_EXTENSIONS);

        readRecords();

        assertNumberOfResults(1);
        assertSchemaIsSet();
        assertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, TestCEFUtil.EXPECTED_EXTENSION_VALUES);
    }

    @Test
    public void testReadingSingleLineWithCustomExtensionFieldsUsingSchemaWithExtensionFields() throws Exception {
        setSchema(TestCEFUtil.getFieldWithExtensions());
        setReader(TestCEFUtil.INPUT_SINGLE_ROW_WITH_CUSTOM_EXTENSIONS);

        readRecords();

        assertNumberOfResults(1);
        assertSchemaIsSet();
        assertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, TestCEFUtil.EXPECTED_EXTENSION_VALUES);
    }

    @Test
    public void testReadingSingleLineWithHeaderFieldsUsingSchemaWithCustomExtensionFieldsAsStrings() throws Exception {
        setSchema(TestCEFUtil.getFieldsWithCustomExtensions(TestCEFUtil.CUSTOM_EXTENSION_FIELD_AS_STRING));
        setReader(TestCEFUtil.INPUT_SINGLE_ROW_HEADER_FIELDS_ONLY);

        readRecords();

        assertNumberOfResults(1);
        assertSchemaIsSet();
        assertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, withNulls(TestCEFUtil.EXPECTED_EXTENSION_VALUES), Collections.singletonMap(TestCEFUtil.CUSTOM_EXTENSION_FIELD_NAME, null));
    }

    @Test
    public void testReadingSingleLineWithExtensionFieldsUsingSchemaWithCustomExtensionFieldsAsStrings() throws Exception {
        setSchema(TestCEFUtil.getFieldsWithCustomExtensions(TestCEFUtil.CUSTOM_EXTENSION_FIELD_AS_STRING));
        setIncludeCustomExtensions();
        setReader(TestCEFUtil.INPUT_SINGLE_ROW_WITH_EXTENSIONS);

        readRecords();

        assertNumberOfResults(1);
        assertSchemaIsSet();
        assertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, TestCEFUtil.EXPECTED_EXTENSION_VALUES, Collections.singletonMap(TestCEFUtil.CUSTOM_EXTENSION_FIELD_NAME, null));
    }

    @Test
    public void testReadingSingleLineWithCustomExtensionFieldsUsingSchemaWithCustomExtensionFieldsAsStrings() throws Exception {
        setSchema(TestCEFUtil.getFieldsWithCustomExtensions(TestCEFUtil.CUSTOM_EXTENSION_FIELD_AS_STRING));
        setIncludeCustomExtensions();
        setReader(TestCEFUtil.INPUT_SINGLE_ROW_WITH_CUSTOM_EXTENSIONS);

        readRecords();

        assertNumberOfResults(1);
        assertSchemaIsSet();
        assertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, TestCEFUtil.EXPECTED_EXTENSION_VALUES, Collections.singletonMap(TestCEFUtil.CUSTOM_EXTENSION_FIELD_NAME, "123"));
    }

    @Test
    public void testReadingSingleLineWithHeaderFieldsUsingSchemaWithCustomExtensionFields() throws Exception {
        setSchema(TestCEFUtil.getFieldsWithCustomExtensions(TestCEFUtil.CUSTOM_EXTENSION_FIELD));
        setIncludeCustomExtensions();
        setReader(TestCEFUtil.INPUT_SINGLE_ROW_HEADER_FIELDS_ONLY);

        readRecords();

        assertNumberOfResults(1);
        assertSchemaIsSet();
        assertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, withNulls(TestCEFUtil.EXPECTED_EXTENSION_VALUES), Collections.singletonMap(TestCEFUtil.CUSTOM_EXTENSION_FIELD_NAME, null));
    }

    @Test
    public void testReadingSingleLineWithExtensionFieldsUsingSchemaWithCustomExtensionFields() throws Exception {
        setSchema(TestCEFUtil.getFieldsWithCustomExtensions(TestCEFUtil.CUSTOM_EXTENSION_FIELD));
        setIncludeCustomExtensions();
        setReader(TestCEFUtil.INPUT_SINGLE_ROW_WITH_EXTENSIONS);

        readRecords();

        assertNumberOfResults(1);
        assertSchemaIsSet();
        assertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, TestCEFUtil.EXPECTED_EXTENSION_VALUES, Collections.singletonMap(TestCEFUtil.CUSTOM_EXTENSION_FIELD_NAME, null));
    }

    @Test
    public void testReadingSingleLineWithCustomExtensionFieldsUsingSchemaWithCustomExtensionFields() throws Exception {
        setSchema(TestCEFUtil.getFieldsWithCustomExtensions(TestCEFUtil.CUSTOM_EXTENSION_FIELD));
        setIncludeCustomExtensions();
        setReader(TestCEFUtil.INPUT_SINGLE_ROW_WITH_CUSTOM_EXTENSIONS);

        readRecords();

        assertNumberOfResults(1);
        assertSchemaIsSet();
        assertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, TestCEFUtil.EXPECTED_EXTENSION_VALUES, Collections.singletonMap(TestCEFUtil.CUSTOM_EXTENSION_FIELD_NAME, 123));
    }

    @Test
    public void testReadingContainingRawEvent() throws Exception {
        setSchema(getFieldsWithHeaderAndRaw());
        setRawMessageField(TestCEFUtil.RAW_FIELD);
        setReader(TestCEFUtil.INPUT_SINGLE_ROW_HEADER_FIELDS_ONLY);

        readRecords();

        assertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, Collections.singletonMap(TestCEFUtil.RAW_FIELD, TestCEFUtil.RAW_VALUE));
    }

    @Test
    public void testSingleLineWithHeaderFieldsAndNotDroppingUnknownFields() throws Exception {
        setKeepUnknownFields();
        setSchema(getFieldsWithExtensionsExpect("c6a1", "dmac"));
        setReader(TestCEFUtil.INPUT_SINGLE_ROW_WITH_EXTENSIONS);

        readRecords();

        assertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, without(TestCEFUtil.EXPECTED_EXTENSION_VALUES, "c6a1", "dmac"));
        assertFieldIs("c6a1", TestCEFUtil.EXPECTED_EXTENSION_VALUES.get("c6a1"));
        assertFieldIs("dmac", TestCEFUtil.EXPECTED_EXTENSION_VALUES.get("dmac"));
    }

    @Test
    public void testReadingEmptyRow()  throws Exception {
        setSchema(CEFSchemaUtil.getHeaderFields());
        setReader(TestCEFUtil.INPUT_EMPTY_ROW);

        readRecords();

        assertNumberOfResults(0);
    }

    @Test
    public void testReadingMisformattedRow()  throws Exception {
        setSchema(CEFSchemaUtil.getHeaderFields());
        setReader(TestCEFUtil.INPUT_MISFORMATTED_ROW);

        Assertions.assertThrows(MalformedRecordException.class, this::readRecords);
    }

    @Test
    public void testReadingMisformattedRowWhenInvalidFieldIsSet()  throws Exception {
        setInvalidField();
        setSchema(CEFSchemaUtil.getHeaderFields());
        setReader(TestCEFUtil.INPUT_MISFORMATTED_ROW);

        readRecords();

        assertNumberOfResults(3);
    }

    @Test
    public void testReadingMultipleIdenticalRows() throws Exception {
        setSchema(CEFSchemaUtil.getHeaderFields());
        setReader(TestCEFUtil.INPUT_MULTIPLE_IDENTICAL_ROWS);

        readRecords();

        assertNumberOfResults(3);
        assertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES);
    }

    @Test
    public void testReadingMultipleIdenticalRowsWithEmptyOnes() throws Exception {
        setSchema(CEFSchemaUtil.getHeaderFields());
        setReader(TestCEFUtil.INPUT_MULTIPLE_ROWS_WITH_EMPTY_ROWS);

        readRecords(5);

        assertNumberOfResults(3);
        assertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES);
    }

    @Test
    public void testReadingMultipleRowsWithDecreasingNumberOfExtensionFields() throws Exception {
        setSchema(TestCEFUtil.getFieldWithExtensions());
        setReader(TestCEFUtil.INPUT_MULTIPLE_ROWS_WITH_DECREASING_NUMBER_OF_EXTENSIONS);

        readRecords();

        assertNumberOfResults(2);
        assertFieldsAre(0, TestCEFUtil.EXPECTED_HEADER_VALUES, TestCEFUtil.EXPECTED_EXTENSION_VALUES);
        assertFieldsAre(1, TestCEFUtil.EXPECTED_HEADER_VALUES, withNulls(TestCEFUtil.EXPECTED_EXTENSION_VALUES, "c6a1", "dmac"));
    }

    @Test
    public void testReadingMultipleRowsWithIncreasingNumberOfExtensionFields() throws Exception {
        setSchema(getFieldsWithExtensionsExpect("c6a1", "dmac"));
        setReader(TestCEFUtil.INPUT_MULTIPLE_ROWS_WITH_INCREASING_NUMBER_OF_EXTENSIONS);

        readRecords();

        assertNumberOfResults(2);
        assertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, without(TestCEFUtil.EXPECTED_EXTENSION_VALUES, "c6a1", "dmac"));
    }

    @Test
    public void testReadingIncorrectHeaderField() throws Exception {
        setSchema(CEFSchemaUtil.getHeaderFields());
        setReader(TestCEFUtil.INPUT_SINGLE_ROW_WITH_INCORRECT_HEADER_FIELD);

        Assertions.assertThrows(NumberFormatException.class, () -> readRecords());
    }

    @Test
    public void testReadingIncorrectCustomVariableFormat() throws Exception {
        setSchema(TestCEFUtil.getFieldsWithCustomExtensions(TestCEFUtil.CUSTOM_EXTENSION_FIELD));
        setIncludeCustomExtensions();
        setReader(TestCEFUtil.INPUT_SINGLE_ROW_WITH_INCORRECT_CUSTOM_EXTENSIONS);

        Assertions.assertThrows(NumberFormatException.class, () -> readRecords());
    }

    @Test
    public void testReadingEmptyValuesWhenAcceptingEmptyExtensions() throws Exception {
        setSchema(TestCEFUtil.getFieldWithExtensions());
        setAcceptEmptyExtensions();
        setReader(TestCEFUtil.INPUT_SINGLE_ROW_WITH_EMPTY_EXTENSION);

        readRecords();

        assertNumberOfResults(1);
        assertSchemaIsSet();

        final Map<String, Object> expectedExtensionValues = new HashMap<>();
        expectedExtensionValues.putAll(TestCEFUtil.EXPECTED_EXTENSION_VALUES);
        expectedExtensionValues.put("cn1", null);
        expectedExtensionValues.put("cn1Label", "");

        assertFieldsAre(TestCEFUtil.EXPECTED_HEADER_VALUES, expectedExtensionValues);
    }

    @Test
    public void testReadingEmptyValuesWhenNotAcceptingEmptyExtensions() throws Exception {
        setSchema(TestCEFUtil.getFieldWithExtensions());
        setReader(TestCEFUtil.INPUT_SINGLE_ROW_WITH_EMPTY_EXTENSION);

        // As empty is not accepted, number type fields will not be parsed properly
        Assertions.assertThrows(NumberFormatException.class, () -> readRecords());
    }

    private static List<RecordField> getFieldsWithHeaderAndRaw() {
        final List<RecordField> result = new ArrayList<>(CEFSchemaUtil.getHeaderFields());
        result.add(new RecordField(TestCEFUtil.RAW_FIELD, RecordFieldType.STRING.getDataType()));
        return result;
    }

    private List<RecordField> getFieldsWithExtensionsExpect(final String... fieldsToExclude) {
        final List<RecordField> result = new ArrayList<>();
        final Set<String> excludedFields = new HashSet<>();
        Arrays.stream(fieldsToExclude).forEach(f -> excludedFields.add(f));

        for (final RecordField record : TestCEFUtil.getFieldWithExtensions()) {
            if (!excludedFields.contains(record.getFieldName())) {
                result.add(record);
            }
        }

        return result;
    }

    private void setSchema(final List<RecordField> fields) {
        this.schema = new SimpleRecordSchema(fields);
    }

    private void setIncludeCustomExtensions() {
        includeCustomExtensions = true;
    }

    private void setAcceptEmptyExtensions() {
        acceptEmptyExtensions = true;
    }

    private void setRawMessageField(final String rawField) {
        this.rawField = rawField;
    }

    private void setKeepUnknownFields() {
        dropUnknownFields = false;
    }

    private void setInvalidField() {
        invalidField = "invalid";
    }

    private void setReader(final String file) throws FileNotFoundException {
        inputStream = new FileInputStream(file);
        testSubject = new CEFRecordReader(inputStream, schema, parser, new MockComponentLog("id", "TestLogger"), Locale.US, rawField, invalidField, includeCustomExtensions, acceptEmptyExtensions);
    }

    private void readRecords() throws IOException, MalformedRecordException {
        final List<Record> results = new ArrayList<>();
        Record result;

        while ((result = testSubject.nextRecord(true, dropUnknownFields)) != null) {
            results.add(result);
        }

        this.results = results;
    }

    private void readRecords(final int count) throws IOException, MalformedRecordException {
        final List<Record> results = new ArrayList<>();

        for (int i=1; i<=count; i++) {
            final Record record = testSubject.nextRecord(true, dropUnknownFields);

            if (record != null) {
                results.add(record);
            }
        }

        this.results = results;
    }

    private void assertFieldsAre(final Map<String, Object>... fieldGroups) {
        final Map<String, Object> expectedFields = new HashMap<>();
        Arrays.stream(fieldGroups).forEach(fieldGroup -> expectedFields.putAll(fieldGroup));
        results.forEach(r -> TestCEFUtil.assertFieldsAre(r, expectedFields));
    }

    private void assertFieldsAre(final int number, final Map<String, Object>... fieldGroups) {
        final Map<String, Object> expectedFields = new HashMap<>();
        Arrays.stream(fieldGroups).forEach(fieldGroup -> expectedFields.putAll(fieldGroup));
        TestCEFUtil.assertFieldsAre(results.get(number), expectedFields);
    }

    private void assertFieldIs(final String field, final Object expectedValue) {
        results.forEach(r -> Assertions.assertEquals(expectedValue, r.getValue(field)));
    }

    private void assertNumberOfResults(final int numberOfResults) {
        Assertions.assertEquals(numberOfResults, results.size());
    }

    private void assertSchemaIsSet() {
        results.forEach(r -> Assertions.assertEquals(schema, r.getSchema()));
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
