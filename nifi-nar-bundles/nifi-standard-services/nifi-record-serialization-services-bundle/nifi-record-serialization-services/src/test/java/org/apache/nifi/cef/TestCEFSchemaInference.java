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
import org.apache.bval.jsr.ApacheValidationProvider;
import org.apache.nifi.schema.inference.RecordSource;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.validation.Validation;
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

    private final javax.validation.Validator validator = Validation.byProvider(ApacheValidationProvider.class).configure().buildValidatorFactory().getValidator();
    private final CEFParser parser = new CEFParser(validator);

    private boolean includeExtensions;
    private boolean includeCustomExtensions;
    private String rawMessageField;
    private CEFSchemaInference testSubject;
    private RecordSource<CommonEvent> recordSource;
    private CEFCustomExtensionTypeResolver typeResolver;
    private RecordSchema result;

    @Before
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
        // given
        givenRecordSource(TestCEFUtil.INPUT_SINGLE_ROW_HEADER_FIELDS_ONLY);
        givenTestSubject();

        // when
        whenInferSchema();

        // then
        thenSchemaConsistsOf(CEFSchemaUtil.getHeaderFields());
    }

    @Test
    public void testInferBasedOnHeaderFieldsWhenIncludingExtensions() throws Exception {
        // given
        givenIncludeExtensions();
        givenRecordSource(TestCEFUtil.INPUT_SINGLE_ROW_HEADER_FIELDS_ONLY);
        givenTestSubject();

        // when
        whenInferSchema();

        // then
        thenSchemaConsistsOf(CEFSchemaUtil.getHeaderFields());
    }

    @Test
    public void testInferBasedOnHeaderFieldsWithRaw() throws Exception {
        // given
        givenRawMessageField("raw");
        givenRecordSource(TestCEFUtil.INPUT_SINGLE_ROW_HEADER_FIELDS_ONLY);
        givenTestSubject();

        // when
        whenInferSchema();

        // then
        thenSchemaConsistsOf(CEFSchemaUtil.getHeaderFields(), Collections.singletonList(new RecordField("raw", RecordFieldType.STRING.getDataType())));
    }

    @Test
    public void testInferBasedOnHeaderAndExtensionFieldsWhenNotIncludingExtensions() throws Exception {
        // given
        givenRecordSource(TestCEFUtil.INPUT_SINGLE_ROW_WITH_EXTENSIONS);
        givenTestSubject();

        // when
        whenInferSchema();

        // then
        thenSchemaConsistsOf(CEFSchemaUtil.getHeaderFields());
    }

    @Test
    public void testInferBasedOnHeaderAndExtensionFieldsWhenIncludingExtensions() throws Exception {
        // given
        givenIncludeExtensions();
        givenRecordSource(TestCEFUtil.INPUT_SINGLE_ROW_WITH_EXTENSIONS);
        givenTestSubject();

        // when
        whenInferSchema();

        // then
        thenSchemaConsistsOf(TestCEFUtil.givenFieldWithExtensions());
    }

    @Test
    public void testInferBasedOnRowWithCustomExtensionsButSkipping() throws Exception {
        // given
        givenIncludeExtensions();
        givenRecordSource(TestCEFUtil.INPUT_SINGLE_ROW_WITH_CUSTOM_EXTENSIONS);
        givenTestSubject();

        // when
        whenInferSchema();

        // then
        thenSchemaConsistsOf(TestCEFUtil.givenFieldWithExtensions());
    }

    @Test
    public void testInferBasedOnRowWithCustomExtensionsAsString() throws Exception {
        // given
        givenIncludeExtensions();
        givenIncludeCustomExtensions(CEFCustomExtensionTypeResolver.STRING_RESOLVER);
        givenRecordSource(TestCEFUtil.INPUT_SINGLE_ROW_WITH_CUSTOM_EXTENSIONS);
        givenTestSubject();

        // when
        whenInferSchema();

        // then
        thenSchemaConsistsOf(TestCEFUtil.givenFieldsWithCustomExtensions(TestCEFUtil.CUSTOM_EXTENSION_FIELD_AS_STRING));
    }

    @Test
    public void testInferBasedOnRowWithCustomExtensions() throws Exception {
        // given
        givenIncludeExtensions();
        givenIncludeCustomExtensions(CEFCustomExtensionTypeResolver.SIMPLE_RESOLVER);
        givenRecordSource(TestCEFUtil.INPUT_SINGLE_ROW_WITH_CUSTOM_EXTENSIONS);
        givenTestSubject();

        // when
        whenInferSchema();

        // then
        thenSchemaConsistsOf(TestCEFUtil.givenFieldsWithCustomExtensions(TestCEFUtil.CUSTOM_EXTENSION_FIELD));
    }

    @Test
    public void testInferBasedOnRowWithEmptyExtensions() throws Exception {
        // given
        givenIncludeExtensions();
        givenRecordSource(TestCEFUtil.INPUT_SINGLE_ROW_WITH_EMPTY_EXTENSION);
        givenTestSubject();

        // when
        whenInferSchema();

        // then - as extension types are defined by the extension dictionary, even with empty value the data type can be inferred
        thenSchemaConsistsOf(TestCEFUtil.givenFieldWithExtensions());
    }

    @Test
    public void testInferBasedOnRowWithEmptyCustomExtensions() throws Exception {
        // given
        givenIncludeExtensions();
        givenIncludeCustomExtensions(CEFCustomExtensionTypeResolver.SIMPLE_RESOLVER);
        givenRecordSource(TestCEFUtil.INPUT_SINGLE_ROW_WITH_EMPTY_CUSTOM_EXTENSIONS);
        givenTestSubject();

        // when
        whenInferSchema();

        // then - as there is no value provided to infer based on, the empty custom field will be considered as string
        thenSchemaConsistsOf(TestCEFUtil.givenFieldsWithCustomExtensions(TestCEFUtil.CUSTOM_EXTENSION_FIELD_AS_STRING));
    }

    @Test
    public void testInferWhenStartingWithEmptyRow() throws Exception {
        // given
        givenRecordSource(TestCEFUtil.INPUT_MULTIPLE_ROWS_STARTING_WITH_EMPTY_ROW);
        givenTestSubject();

        // when
        whenInferSchema();

        // then
        thenSchemaConsistsOf(CEFSchemaUtil.getHeaderFields());
    }

    @Test
    public void testInferWithDifferentCustomFieldTypes() throws Exception {
        // given
        givenIncludeExtensions();
        givenIncludeCustomExtensions(CEFCustomExtensionTypeResolver.SIMPLE_RESOLVER);
        givenRecordSource(TestCEFUtil.INPUT_MULTIPLE_ROWS_WITH_DIFFERENT_CUSTOM_TYPES);
        givenTestSubject();

        // when
        whenInferSchema();

        // then
        thenSchemaConsistsOf(TestCEFUtil.givenFieldsWithCustomExtensions(TestCEFUtil.CUSTOM_EXTENSION_FIELD_AS_CHOICE));
    }

    @Test
    public void testInferBasedOnEmptyRow() throws Exception{
        // given
        givenRecordSource(TestCEFUtil.INPUT_EMPTY_ROW);
        givenTestSubject();

        // when
        whenInferSchema();

        // then
        thenSchemaConsistsOf(CEFSchemaUtil.getHeaderFields()); // For CEF it is always expected to have a header
    }

    @Test(expected = IOException.class)
    public void testInferBasedOnMisformattedRow() throws Exception{
        // given
        givenRecordSource(TestCEFUtil.INPUT_MISFORMATTED_ROW);
        givenTestSubject();

        // when
        whenInferSchema();
    }

    private void givenIncludeExtensions() {
        this.includeExtensions = true;
    }

    private void givenIncludeCustomExtensions(final CEFCustomExtensionTypeResolver typeResolver) {
        this.includeCustomExtensions = true;
        this.typeResolver = typeResolver;
    }

    private void givenTestSubject() {
        this.testSubject = new CEFSchemaInference(includeExtensions, includeCustomExtensions, typeResolver, rawMessageField);
    }

    private void givenRawMessageField(final String rawMessageField) {
        this.rawMessageField = rawMessageField;
    }

    private void givenRecordSource(final String inputFile) throws FileNotFoundException {
        final FileInputStream inputStream = new FileInputStream(inputFile);
        this.recordSource = new CEFRecordSource(inputStream, parser, Locale.US, true);
    }

    private void whenInferSchema() throws IOException {
        result = testSubject.inferSchema(recordSource);
    }

    private void thenSchemaConsistsOf(final List<RecordField>... expectedFieldGroups) {
        final List<RecordField> expectedFields = Arrays.stream(expectedFieldGroups).flatMap(group -> group.stream()).collect(Collectors.toList());
        Assert.assertEquals(expectedFields.size(), result.getFieldCount());

        for (final RecordField expectedField : expectedFields) {
            final Optional<RecordField> field = result.getField(expectedField.getFieldName());
            Assert.assertTrue(field.isPresent());
            Assert.assertEquals(expectedField, field.get());
        }
    }
}
