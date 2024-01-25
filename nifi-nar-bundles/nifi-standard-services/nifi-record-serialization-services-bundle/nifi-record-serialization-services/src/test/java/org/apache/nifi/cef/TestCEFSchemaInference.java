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

import com.fluenda.parcefone.event.CommonEvent;
import com.fluenda.parcefone.parser.CEFParser;
import org.apache.nifi.schema.inference.RecordSource;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

public class TestCEFSchemaInference {

    private final CEFParser parser = new CEFParser();

    private boolean includeExtensions;
    private boolean includeCustomExtensions;
    private String rawMessageField;
    private CEFSchemaInference testSubject;
    private RecordSource<CommonEvent> recordSource;
    private CEFCustomExtensionTypeResolver typeResolver;
    private RecordSchema result;

    @BeforeEach
    public void setUp() {
        includeExtensions = false;
        includeCustomExtensions = false;
        rawMessageField = null;
        testSubject = null;
        recordSource = null;
        result = null;
    }

    @Test
    public void testInferBasedOnHeaderFields() throws Exception {
        setRecordSource(TestCEFUtil.INPUT_SINGLE_ROW_HEADER_FIELDS_ONLY);
        setUpTestSubject();

        inferSchema();

        assertSchemaConsistsOf(CEFSchemaUtil.getHeaderFields());
    }

    @Test
    public void testInferBasedOnHeaderFieldsWhenIncludingExtensions() throws Exception {
        setIncludeExtensions();
        setRecordSource(TestCEFUtil.INPUT_SINGLE_ROW_HEADER_FIELDS_ONLY);
        setUpTestSubject();

        inferSchema();

        assertSchemaConsistsOf(CEFSchemaUtil.getHeaderFields());
    }

    @Test
    public void testInferBasedOnHeaderFieldsWithRaw() throws Exception {
        setRawMessageField("raw");
        setRecordSource(TestCEFUtil.INPUT_SINGLE_ROW_HEADER_FIELDS_ONLY);
        setUpTestSubject();

        inferSchema();

        assertSchemaConsistsOf(CEFSchemaUtil.getHeaderFields(), Collections.singletonList(new RecordField("raw", RecordFieldType.STRING.getDataType())));
    }

    @Test
    public void testInferBasedOnHeaderAndExtensionFieldsWhenNotIncludingExtensions() throws Exception {
        setRecordSource(TestCEFUtil.INPUT_SINGLE_ROW_WITH_EXTENSIONS);
        setUpTestSubject();

        inferSchema();

        assertSchemaConsistsOf(CEFSchemaUtil.getHeaderFields());
    }

    @Test
    public void testInferBasedOnHeaderAndExtensionFieldsWhenIncludingExtensions() throws Exception {
        setIncludeExtensions();
        setRecordSource(TestCEFUtil.INPUT_SINGLE_ROW_WITH_EXTENSIONS);
        setUpTestSubject();

        inferSchema();

        assertSchemaConsistsOf(TestCEFUtil.getFieldWithExtensions());
    }

    @Test
    public void testInferBasedOnRowWithCustomExtensionsButSkipping() throws Exception {
        setIncludeExtensions();
        setRecordSource(TestCEFUtil.INPUT_SINGLE_ROW_WITH_CUSTOM_EXTENSIONS);
        setUpTestSubject();

        inferSchema();

        assertSchemaConsistsOf(TestCEFUtil.getFieldWithExtensions());
    }

    @Test
    public void testInferBasedOnRowWithCustomExtensionsAsString() throws Exception {
        setIncludeExtensions();
        setIncludeCustomExtensions(CEFCustomExtensionTypeResolver.STRING_RESOLVER);
        setRecordSource(TestCEFUtil.INPUT_SINGLE_ROW_WITH_CUSTOM_EXTENSIONS);
        setUpTestSubject();

        inferSchema();

        assertSchemaConsistsOf(TestCEFUtil.getFieldsWithCustomExtensions(TestCEFUtil.CUSTOM_EXTENSION_FIELD_AS_STRING));
    }

    @Test
    public void testInferBasedOnRowWithCustomExtensions() throws Exception {
        setIncludeExtensions();
        setIncludeCustomExtensions(CEFCustomExtensionTypeResolver.SIMPLE_RESOLVER);
        setRecordSource(TestCEFUtil.INPUT_SINGLE_ROW_WITH_CUSTOM_EXTENSIONS);
        setUpTestSubject();

        inferSchema();

        assertSchemaConsistsOf(TestCEFUtil.getFieldsWithCustomExtensions(TestCEFUtil.CUSTOM_EXTENSION_FIELD));
    }

    @Test
    public void testInferBasedOnRowWithEmptyExtensions() throws Exception {
        setIncludeExtensions();
        setRecordSource(TestCEFUtil.INPUT_SINGLE_ROW_WITH_EMPTY_EXTENSION);
        setUpTestSubject();

        inferSchema();

        // As extension types are defined by the extension dictionary, even with empty value the data type can be inferred
        assertSchemaConsistsOf(TestCEFUtil.getFieldWithExtensions());
    }

    @Test
    public void testInferBasedOnRowWithEmptyCustomExtensions() throws Exception {
        setIncludeExtensions();
        setIncludeCustomExtensions(CEFCustomExtensionTypeResolver.SIMPLE_RESOLVER);
        setRecordSource(TestCEFUtil.INPUT_SINGLE_ROW_WITH_EMPTY_CUSTOM_EXTENSIONS);
        setUpTestSubject();

        inferSchema();

        // As there is no value provided to infer based on, the empty custom field will be considered as string
        assertSchemaConsistsOf(TestCEFUtil.getFieldsWithCustomExtensions(TestCEFUtil.CUSTOM_EXTENSION_FIELD_AS_STRING));
    }

    @Test
    public void testInferWhenStartingWithEmptyRow() throws Exception {
        setRecordSource(TestCEFUtil.INPUT_MULTIPLE_ROWS_STARTING_WITH_EMPTY_ROW);
        setUpTestSubject();

        inferSchema();

        assertSchemaConsistsOf(CEFSchemaUtil.getHeaderFields());
    }

    @Test
    public void testInferWithDifferentCustomFieldTypes() throws Exception {
        setIncludeExtensions();
        setIncludeCustomExtensions(CEFCustomExtensionTypeResolver.SIMPLE_RESOLVER);
        setRecordSource(TestCEFUtil.INPUT_MULTIPLE_ROWS_WITH_DIFFERENT_CUSTOM_TYPES);
        setUpTestSubject();

        inferSchema();

        assertSchemaConsistsOf(TestCEFUtil.getFieldsWithCustomExtensions(TestCEFUtil.CUSTOM_EXTENSION_FIELD_AS_CHOICE));
    }

    @Test
    public void testInferBasedOnEmptyRow() throws Exception{
        setRecordSource(TestCEFUtil.INPUT_EMPTY_ROW);
        setUpTestSubject();

        inferSchema();

        assertSchemaConsistsOf(CEFSchemaUtil.getHeaderFields()); // For CEF it is always expected to have a header
    }

    @Test
    public void testInferBasedOnMisformattedRow() throws Exception{
        setRecordSource(TestCEFUtil.INPUT_MISFORMATTED_ROW);
        setUpTestSubject();

        Assertions.assertThrows(IOException.class, this::inferSchema);
    }

    @Test
    public void testInferBasedOnMisformattedRowWhenNonFailFast() throws Exception{
        setRecordSourceWhenNonFailFast(TestCEFUtil.INPUT_MISFORMATTED_ROW);
        setUpTestSubject();

        inferSchema();

        assertSchemaConsistsOf(CEFSchemaUtil.getHeaderFields());
    }

    @Test
    public void testInferBasedOnHeaderFieldsWithInvalid() throws Exception {
        setRecordSource(TestCEFUtil.INPUT_SINGLE_ROW_HEADER_FIELDS_ONLY);
        setUpTestSubjectWithInvalidField();

        inferSchema();

        assertSchemaConsistsOf(CEFSchemaUtil.getHeaderFields(), Collections.singletonList(new RecordField("invalid", RecordFieldType.STRING.getDataType())));
    }

    private void setIncludeExtensions() {
        this.includeExtensions = true;
    }

    private void setIncludeCustomExtensions(final CEFCustomExtensionTypeResolver typeResolver) {
        this.includeCustomExtensions = true;
        this.typeResolver = typeResolver;
    }

    private void setUpTestSubject() {
        this.testSubject = new CEFSchemaInference(includeExtensions, includeCustomExtensions, typeResolver, rawMessageField, null);
    }

    private void setUpTestSubjectWithInvalidField() {
        this.testSubject = new CEFSchemaInference(includeExtensions, includeCustomExtensions, typeResolver, rawMessageField, "invalid");
    }

    private void setRawMessageField(final String rawMessageField) {
        this.rawMessageField = rawMessageField;
    }

    private void setRecordSource(final String inputFile) throws FileNotFoundException {
        setRecordSource(inputFile, true);
    }

    private void setRecordSource(final String inputFile, final boolean failFast) throws FileNotFoundException {
        final FileInputStream inputStream = new FileInputStream(inputFile);
        this.recordSource = new CEFRecordSource(inputStream, parser, Locale.US, true, failFast);
    }

    private void setRecordSourceWhenNonFailFast(final String inputFile) throws FileNotFoundException {
        setRecordSource(inputFile, false);
    }

    private void inferSchema() throws IOException {
        result = testSubject.inferSchema(recordSource);
    }

    private void assertSchemaConsistsOf(final List<RecordField>... expectedFieldGroups) {
        final List<RecordField> expectedFields = Arrays.stream(expectedFieldGroups).flatMap(group -> group.stream()).collect(Collectors.toList());
        Assertions.assertEquals(expectedFields.size(), result.getFieldCount());

        for (final RecordField expectedField : expectedFields) {
            final Optional<RecordField> field = result.getField(expectedField.getFieldName());
            Assertions.assertTrue(field.isPresent());
            Assertions.assertEquals(expectedField, field.get());
        }
    }
}
