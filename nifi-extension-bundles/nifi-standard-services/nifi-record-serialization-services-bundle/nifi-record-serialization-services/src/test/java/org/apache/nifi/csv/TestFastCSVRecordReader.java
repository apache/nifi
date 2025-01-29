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

package org.apache.nifi.csv;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public class TestFastCSVRecordReader {
    private final DataType doubleDataType = RecordFieldType.DOUBLE.getDataType();
    private CSVFormat format;
    private static final CSVFormat TRIMMED_RFC4180 = CSVFormat.RFC4180.builder().setTrim(true).get();

    @BeforeEach
    public void setUp() {
        format = CSVFormat.RFC4180;
    }

    private List<RecordField> getDefaultFields() {
        final List<RecordField> fields = new ArrayList<>();
        for (final String fieldName : new String[]{"id", "name", "balance", "address", "city", "state", "zipCode", "country"}) {
            fields.add(new RecordField(fieldName, RecordFieldType.STRING.getDataType()));
        }
        return fields;
    }

    private FastCSVRecordReader createReader(final InputStream in, final RecordSchema schema, CSVFormat format) throws IOException {
        return new FastCSVRecordReader(in, Mockito.mock(ComponentLog.class), schema, format, true, false,
                RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), "ASCII", false);
    }

    private FastCSVRecordReader createReader(final InputStream in, final RecordSchema schema, CSVFormat format, final boolean trimDoubleQuote) throws IOException {
        return new FastCSVRecordReader(in, Mockito.mock(ComponentLog.class), schema, format, true, false,
                RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), "ASCII", trimDoubleQuote);
    }

    @Test
    public void testUTF8() throws IOException, MalformedRecordException {
        final String text = "name\n黃凱揚";

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream bais = new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8));
             final FastCSVRecordReader reader = new FastCSVRecordReader(bais, Mockito.mock(ComponentLog.class), schema, format, true, false,
                     RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), "UTF-8", false)) {

            final Record record = reader.nextRecord();
            final String name = (String) record.getValue("name");

            assertEquals("黃凱揚", name);
        }
    }

    @Test
    public void testISO8859() throws IOException, MalformedRecordException {
        final String text = "name\nÄËÖÜ";
        final byte[] bytesUTF = text.getBytes(StandardCharsets.UTF_8);
        final byte[] bytes8859 = text.getBytes(StandardCharsets.ISO_8859_1);
        assertEquals(13, bytesUTF.length, "expected size=13 for UTF-8 representation of test data");
        assertEquals(9, bytes8859.length, "expected size=9 for ISO-8859-1 representation of test data");

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream bais = new ByteArrayInputStream(text.getBytes(StandardCharsets.ISO_8859_1));
             final FastCSVRecordReader reader = new FastCSVRecordReader(bais, Mockito.mock(ComponentLog.class), schema, format, true, false,
                     RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(),
                     StandardCharsets.ISO_8859_1.name(), true)) {

            final Record record = reader.nextRecord();
            final String name = (String) record.getValue("name");

            assertEquals("ÄËÖÜ", name);
        }
    }

    @Test
    public void testDate() throws IOException, MalformedRecordException {
        final String dateValue = "1983-11-30";
        final String text = "date\n11/30/1983";

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("date", RecordFieldType.DATE.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream bais = new ByteArrayInputStream(text.getBytes());
             final FastCSVRecordReader reader = new FastCSVRecordReader(bais, Mockito.mock(ComponentLog.class), schema, format, true, false,
                     "MM/dd/yyyy", RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), "UTF-8", true)) {

            final Record record = reader.nextRecord();
            final Object date = record.getValue("date");
            assertEquals(java.sql.Date.valueOf(dateValue), date);
        }
    }

    @Test
    public void testSimpleParse() throws IOException, MalformedRecordException {
        final List<RecordField> fields = getDefaultFields();
        fields.replaceAll(f -> f.getFieldName().equals("balance") ? new RecordField("balance", doubleDataType) : f);

        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream fis = new FileInputStream("src/test/resources/csv/single-bank-account_RFC4180.csv");
             final FastCSVRecordReader reader = createReader(fis, schema, format)) {

            final Object[] record = reader.nextRecord().getValues();
            final Object[] expectedValues = new Object[]{"1", "John Doe", 4750.89D, "123 My Street", "My City", "MS", "11111", "USA"};
            assertArrayEquals(expectedValues, record);

            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testExcelFormat() throws IOException, MalformedRecordException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("fieldA", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("fieldB", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final String headerLine = "fieldA,fieldB";
        final String inputRecord = "valueA,valueB";
        final String csvData = headerLine + "\n" + inputRecord;
        final byte[] inputData = csvData.getBytes();

        try (final InputStream bais = new ByteArrayInputStream(inputData);
             final FastCSVRecordReader reader = createReader(bais, schema, CSVFormat.EXCEL)) {

            final Object[] record = reader.nextRecord().getValues();
            final Object[] expectedValues = new Object[]{"valueA", "valueB"};
            assertArrayEquals(expectedValues, record);

            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testMultipleRecords() throws IOException, MalformedRecordException {
        final List<RecordField> fields = getDefaultFields();
        fields.replaceAll(f -> f.getFieldName().equals("balance") ? new RecordField("balance", doubleDataType) : f);

        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream fis = new FileInputStream("src/test/resources/csv/multi-bank-account_RFC4180.csv");
             final FastCSVRecordReader reader = createReader(fis, schema, format)) {

            final Object[] firstRecord = reader.nextRecord().getValues();
            final Object[] firstExpectedValues = new Object[]{"1", "John Doe", 4750.89D, "123 My Street", "My City", "MS", "11111", "USA"};
            assertArrayEquals(firstExpectedValues, firstRecord);

            final Object[] secondRecord = reader.nextRecord().getValues();
            final Object[] secondExpectedValues = new Object[]{"2", "Jane Doe", 4820.09D, "321 Your Street", "Your City", "NY", "33333", "USA"};
            assertArrayEquals(secondExpectedValues, secondRecord);

            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testMissingField() throws IOException {
        final List<RecordField> fields = getDefaultFields();
        fields.replaceAll(f -> f.getFieldName().equals("balance") ? new RecordField("balance", doubleDataType) : f);

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final String headerLine = "id, name, balance, address, city, state, zipCode, country";
        final String inputRecord = "1, John, 40.80, \"\"\"123 My Street\"\"\", My City, MS, 11111";
        final String csvData = headerLine + "\n" + inputRecord;
        final byte[] inputData = csvData.getBytes();

        try (final InputStream bais = new ByteArrayInputStream(inputData);
             final FastCSVRecordReader reader = createReader(bais, schema, format)) {

            // RFC-4180 does not allow missing column names
            assertThrows(MalformedRecordException.class, reader::nextRecord);
        }
    }

    @Test
    public void testMissingField_withoutDoubleQuoteTrimming() throws IOException, MalformedRecordException {
        final List<RecordField> fields = getDefaultFields();
        fields.replaceAll(f -> f.getFieldName().equals("balance") ? new RecordField("balance", doubleDataType) : f);

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final String headerLine = "id, name, balance, address, city, state, zipCode, country";
        final String inputRecord = "1, John, 40.80, \"123 My Street\", My City, MS, 11111";
        final String csvData = headerLine + "\n" + inputRecord;
        final byte[] inputData = csvData.getBytes();

        try (final InputStream bais = new ByteArrayInputStream(inputData);
             final FastCSVRecordReader reader = createReader(bais, schema, TRIMMED_RFC4180.builder().setAllowMissingColumnNames(true).get(), false)) {

            final Record record = reader.nextRecord();
            assertNotNull(record);

            assertEquals("1", record.getValue("id"));
            assertEquals("John", record.getValue("name"));
            assertEquals(40.8D, record.getValue("balance"));
            assertEquals("\"123 My Street\"", record.getValue("address"));
            assertEquals("My City", record.getValue("city"));
            assertEquals("MS", record.getValue("state"));
            assertEquals("11111", record.getValue("zipCode"));
            assertNull(record.getValue("country"));

            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testReadRawWithDifferentFieldName() throws IOException, MalformedRecordException {
        final List<RecordField> fields = getDefaultFields();
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final String headerLine = "id,name,balance,address,city,state,zipCode,continent";
        final String inputRecord = "1,John,40.80,\"123 My Street\",My City,MS,11111,North America";
        final String csvData = headerLine + "\n" + inputRecord;
        final byte[] inputData = csvData.getBytes();

        // test nextRecord does not contain a 'continent' field
        try (final InputStream bais = new ByteArrayInputStream(inputData);
             final FastCSVRecordReader reader = createReader(bais, schema, format)) {

            final Record record = reader.nextRecord(true, true);
            assertNotNull(record);

            assertEquals("1", record.getValue("id"));
            assertEquals("John", record.getValue("name"));
            assertEquals("40.80", record.getValue("balance"));
            assertEquals("123 My Street", record.getValue("address"));
            assertEquals("My City", record.getValue("city"));
            assertEquals("MS", record.getValue("state"));
            assertEquals("11111", record.getValue("zipCode"));
            assertNull(record.getValue("country"));
            assertNull(record.getValue("continent"));

            assertNull(reader.nextRecord());
        }

        // test nextRawRecord does contain 'continent' field
        try (final InputStream bais = new ByteArrayInputStream(inputData);
             final FastCSVRecordReader reader = createReader(bais, schema, format)) {

            final Record record = reader.nextRecord(false, false);
            assertNotNull(record);

            assertEquals("1", record.getValue("id"));
            assertEquals("John", record.getValue("name"));
            assertEquals("40.80", record.getValue("balance"));
            assertEquals("123 My Street", record.getValue("address"));
            assertEquals("My City", record.getValue("city"));
            assertEquals("MS", record.getValue("state"));
            assertEquals("11111", record.getValue("zipCode"));
            assertNull(record.getValue("country"));
            assertEquals("North America", record.getValue("continent"));

            assertNull(reader.nextRecord(false, false));
        }
    }

    @Test
    public void testReadRawWithDifferentFieldName_withoutDoubleQuoteTrimming() throws IOException, MalformedRecordException {
        final List<RecordField> fields = getDefaultFields();
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final String headerLine = "id, name, balance, address, city, state, zipCode, continent";
        final String inputRecord = "1, John, 40.80, \"123 My Street\", My City, MS, 11111, North America";
        final String csvData = headerLine + "\n" + inputRecord;
        final byte[] inputData = csvData.getBytes();

        // test nextRecord does not contain a 'continent' field
        try (final InputStream bais = new ByteArrayInputStream(inputData);
             final FastCSVRecordReader reader = createReader(bais, schema, TRIMMED_RFC4180, false)) {

            final Record record = reader.nextRecord(true, true);
            assertNotNull(record);

            assertEquals("1", record.getValue("id"));
            assertEquals("John", record.getValue("name"));
            assertEquals("40.80", record.getValue("balance"));
            assertEquals("\"123 My Street\"", record.getValue("address"));
            assertEquals("My City", record.getValue("city"));
            assertEquals("MS", record.getValue("state"));
            assertEquals("11111", record.getValue("zipCode"));
            assertNull(record.getValue("country"));
            assertNull(record.getValue("continent"));

            assertNull(reader.nextRecord());
        }

        // test nextRawRecord does contain 'continent' field
        try (final InputStream bais = new ByteArrayInputStream(inputData);
             final FastCSVRecordReader reader = createReader(bais, schema, TRIMMED_RFC4180, false)) {

            final Record record = reader.nextRecord(false, false);
            assertNotNull(record);

            assertEquals("1", record.getValue("id"));
            assertEquals("John", record.getValue("name"));
            assertEquals("40.80", record.getValue("balance"));
            assertEquals("\"123 My Street\"", record.getValue("address"));
            assertEquals("My City", record.getValue("city"));
            assertEquals("MS", record.getValue("state"));
            assertEquals("11111", record.getValue("zipCode"));
            assertNull(record.getValue("country"));
            assertEquals("North America", record.getValue("continent"));

            assertNull(reader.nextRecord(false, false));
        }
    }


    @Test
    public void testFieldInSchemaButNotHeader() throws IOException, MalformedRecordException {
        final List<RecordField> fields = getDefaultFields();
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final String headerLine = "id, name, balance, address, city, state, zipCode";
        final String inputRecord = "1, John, 40.80, \"\"123 My Street\"\", My City, MS, 11111, USA";
        final String csvData = headerLine + "\n" + inputRecord;
        final byte[] inputData = csvData.getBytes();

        // Use a CUSTOM format as FastCSV sometimes handles non-compliant data such as trimming spaces from values
        format = CSVFormat.RFC4180.builder()
                .setTrim(true)
                .setIgnoreSurroundingSpaces(true)
                .setAllowMissingColumnNames(true)
                .get();

        // Create another Record Reader that indicates that the header line is present but should be ignored. This should cause
        // our schema to be the definitive list of what fields exist.
        try (final InputStream bais = new ByteArrayInputStream(inputData);
             final FastCSVRecordReader reader = new FastCSVRecordReader(bais, Mockito.mock(ComponentLog.class), schema, format, true, true,
                     RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), "UTF-8", true)) {

            final Record record = reader.nextRecord();
            assertNotNull(record);

            assertEquals("1", record.getValue("id"));
            assertEquals("John", record.getValue("name"));
            assertEquals("40.80", record.getValue("balance"));
            assertEquals("123 My Street", record.getValue("address"));
            assertEquals("My City", record.getValue("city"));
            assertEquals("MS", record.getValue("state"));
            assertEquals("11111", record.getValue("zipCode"));

            // If schema says that there are fields a, b, c
            // and the CSV has a header line that says field names are a, b
            // and then the data has values 1,2,3
            // then a=1, b=2, c=null
            assertNull(record.getValue("country"));

            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testFieldInSchemaButNotHeader_withoutDoubleQuoteTrimming() throws IOException {
        final List<RecordField> fields = getDefaultFields();
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final String headerLine = "id, name, balance, address, city, state, zipCode";
        final String inputRecord = "1, John, 40.80, \"\"\"123 My Street\"\"\", My City, MS, 11111, USA";
        final String csvData = headerLine + "\n" + inputRecord;
        final byte[] inputData = csvData.getBytes();

        try (final InputStream bais = new ByteArrayInputStream(inputData);
             final FastCSVRecordReader reader = createReader(bais, schema, TRIMMED_RFC4180, false)) {

            try {
                reader.nextRecord();
                fail("Should have thrown MalformedRecordException");
            } catch (MalformedRecordException ignored) {
                // Expected behavior
            }
        }

        // Create another Record Reader that indicates that the header line is present but should be ignored. This should cause
        // our schema to be the definitive list of what fields exist.
        try (final InputStream bais = new ByteArrayInputStream(inputData);
             final FastCSVRecordReader reader = new FastCSVRecordReader(bais, Mockito.mock(ComponentLog.class), schema, TRIMMED_RFC4180, true, true,
                     RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), "UTF-8", false)) {

            // RFC-4180 does not allow missing column names
            assertThrows(MalformedRecordException.class, reader::nextRecord);
        }
    }

    @Test
    public void testExtraFieldNotInHeader() throws IOException {
        final List<RecordField> fields = getDefaultFields();
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final String headerLine = "id, name, balance, address, city, state, zipCode, country";
        final String inputRecord = "1, John, 40.80, \"\"\"123 My Street\"\"\", My City, MS, 11111, USA, North America";
        final String csvData = headerLine + "\n" + inputRecord;
        final byte[] inputData = csvData.getBytes();

        // test nextRecord does not contain a 'continent' field
        try (final InputStream bais = new ByteArrayInputStream(inputData);
             final FastCSVRecordReader reader = createReader(bais, schema, format)) {

            // RFC-4180 does not allow missing column names
            assertThrows(MalformedRecordException.class, () -> reader.nextRecord(false, false));
        }
    }

    @Test
    public void testMultipleRecordsDelimitedWithSpecialChar() throws IOException, MalformedRecordException {

        char delimiter = StringEscapeUtils.unescapeJava("\u0001").charAt(0);

        final CSVFormat format = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).setTrim(true).setQuote('"').setDelimiter(delimiter).get();
        final List<RecordField> fields = getDefaultFields();
        fields.replaceAll(f -> f.getFieldName().equals("balance") ? new RecordField("balance", doubleDataType) : f);

        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream fis = new FileInputStream("src/test/resources/csv/multi-bank-account_spec_delimiter.csv");
             final FastCSVRecordReader reader = createReader(fis, schema, format)) {

            final Object[] firstRecord = reader.nextRecord().getValues();
            final Object[] firstExpectedValues = new Object[]{"1", "John Doe", 4750.89D, "123 My Street", "My City", "MS", "11111", "USA"};
            assertArrayEquals(firstExpectedValues, firstRecord);

            final Object[] secondRecord = reader.nextRecord().getValues();
            final Object[] secondExpectedValues = new Object[]{"2", "Jane Doe", 4820.09D, "321 Your Street", "Your City", "NY", "33333", "USA"};
            assertArrayEquals(secondExpectedValues, secondRecord);

            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testMultipleRecordsEscapedWithNull() throws IOException, MalformedRecordException {

        final CSVFormat format = CSVFormat.DEFAULT.builder()
                .setHeader()
                .setSkipHeaderRecord(true)
                .setTrim(true)
                .setQuote('"')
                .setDelimiter(",")
                .setEscape(null)
                .get();
        final List<RecordField> fields = getDefaultFields();
        fields.replaceAll(f -> f.getFieldName().equals("balance") ? new RecordField("balance", doubleDataType) : f);

        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream fis = new FileInputStream("src/test/resources/csv/multi-bank-account_escapechar.csv");
             final FastCSVRecordReader reader = createReader(fis, schema, format, true)) {

            final Object[] firstRecord = reader.nextRecord().getValues();
            final Object[] firstExpectedValues = new Object[]{"1", "John Doe\\", 4750.89D, "123 My Street", "My City", "MS", "11111", "USA"};
            assertArrayEquals(firstExpectedValues, firstRecord);

            final Object[] secondRecord = reader.nextRecord().getValues();
            final Object[] secondExpectedValues = new Object[]{"2", "Jane Doe", 4820.09D, "321 Your Street", "Your City", "NY", "33333", "USA"};
            assertArrayEquals(secondExpectedValues, secondRecord);

            assertNull(reader.nextRecord());
        }
    }
}
