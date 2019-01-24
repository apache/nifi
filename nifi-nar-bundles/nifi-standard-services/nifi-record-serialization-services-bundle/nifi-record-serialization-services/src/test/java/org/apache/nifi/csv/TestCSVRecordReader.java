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
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestCSVRecordReader {
    private final DataType doubleDataType = RecordFieldType.DOUBLE.getDataType();
    private final CSVFormat format = CSVFormat.DEFAULT.withFirstRecordAsHeader().withTrim().withQuote('"');

    private List<RecordField> getDefaultFields() {
        final List<RecordField> fields = new ArrayList<>();
        for (final String fieldName : new String[] {"id", "name", "balance", "address", "city", "state", "zipCode", "country"}) {
            fields.add(new RecordField(fieldName, RecordFieldType.STRING.getDataType()));
        }
        return fields;
    }

    private CSVRecordReader createReader(final InputStream in, final RecordSchema schema, CSVFormat format) throws IOException {
        return new CSVRecordReader(in, Mockito.mock(ComponentLog.class), schema, format, true, false,
            RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), "ASCII");
    }

    private CSVRecordReader createReader(final InputStream in, final RecordSchema schema, CSVFormat format,
                                         final String dateFormat, final String timeFormat, final String timestampFormat) throws IOException {
        return new CSVRecordReader(in, Mockito.mock(ComponentLog.class), schema, format, true, false,
                dateFormat, timeFormat, timestampFormat, "ASCII");
    }

    @Test
    public void testUTF8() throws IOException, MalformedRecordException {
        final String text = "name\n黃凱揚";

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream bais = new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8));
             final CSVRecordReader reader = new CSVRecordReader(bais, Mockito.mock(ComponentLog.class), schema, format, true, false,
                     RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), StandardCharsets.UTF_8.name())) {

            final Record record = reader.nextRecord();
            final String name = (String)record.getValue("name");

            assertEquals("黃凱揚", name);
        }
    }

    @Test
    public void testDate() throws IOException, MalformedRecordException {
        final String text = "date\n11/30/1983";

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("date", RecordFieldType.DATE.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream bais = new ByteArrayInputStream(text.getBytes());
             final CSVRecordReader reader = new CSVRecordReader(bais, Mockito.mock(ComponentLog.class), schema, format, true, false,
                     "MM/dd/yyyy", RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), "UTF-8")) {

            final Record record = reader.nextRecord();
            final java.sql.Date date = (Date) record.getValue("date");
            final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("gmt"));
            calendar.setTimeInMillis(date.getTime());

            assertEquals(1983, calendar.get(Calendar.YEAR));
            assertEquals(10, calendar.get(Calendar.MONTH));
            assertEquals(30, calendar.get(Calendar.DAY_OF_MONTH));
        }
    }

    @Test
    public void testDateNoCoersionExpectedFormat() throws IOException, MalformedRecordException {
        final String text = "date\n11/30/1983";

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("date", RecordFieldType.DATE.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream bais = new ByteArrayInputStream(text.getBytes());
             final CSVRecordReader reader = new CSVRecordReader(bais, Mockito.mock(ComponentLog.class), schema, format, true, false,
                     "MM/dd/yyyy", RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), "UTF-8")) {

            final Record record = reader.nextRecord(false, false);
            final java.sql.Date date = (Date) record.getValue("date");
            final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("gmt"));
            calendar.setTimeInMillis(date.getTime());

            assertEquals(1983, calendar.get(Calendar.YEAR));
            assertEquals(10, calendar.get(Calendar.MONTH));
            assertEquals(30, calendar.get(Calendar.DAY_OF_MONTH));
        }
    }

    @Test
    public void testDateNoCoersionUnexpectedFormat() throws IOException, MalformedRecordException {
        final String text = "date\n11/30/1983";

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("date", RecordFieldType.DATE.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream bais = new ByteArrayInputStream(text.getBytes());
             final CSVRecordReader reader = new CSVRecordReader(bais, Mockito.mock(ComponentLog.class), schema, format, true, false,
                     "MM-dd-yyyy", RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), "UTF-8")) {

            final Record record = reader.nextRecord(false, false);
            // When the values are not in the expected format, a String is returned unmodified
            assertEquals("11/30/1983", (String)record.getValue("date"));
        }
    }

    @Test
    public void testDateNullFormat() throws IOException, MalformedRecordException {
        final String text = "date\n1983-01-01";

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("date", RecordFieldType.DATE.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream bais = new ByteArrayInputStream(text.getBytes());
             final CSVRecordReader reader = new CSVRecordReader(bais, Mockito.mock(ComponentLog.class), schema, format, true, false,
                     null, RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), "UTF-8")) {

            final Record record = reader.nextRecord(false, false);
            assertEquals("1983-01-01", (String)record.getValue("date"));
        }
    }

    @Test
    public void testDateEmptyFormat() throws IOException, MalformedRecordException {
        final String text = "date\n1983-01-01";

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("date", RecordFieldType.DATE.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream bais = new ByteArrayInputStream(text.getBytes());
             final CSVRecordReader reader = new CSVRecordReader(bais, Mockito.mock(ComponentLog.class), schema, format, true, false,
                     "", RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), "UTF-8")) {

            final Record record = reader.nextRecord(false, false);
            assertEquals("1983-01-01", (String)record.getValue("date"));
        }
    }

    @Test
    public void testTimeNoCoersionExpectedFormat() throws IOException, MalformedRecordException, ParseException {
        final String timeFormat = "HH!mm!ss";
        DateFormat dateFmt = new SimpleDateFormat(timeFormat);
        dateFmt.setTimeZone(TimeZone.getTimeZone("gmt"));
        final String timeVal = "19!02!03";
        final String text = "time\n" + timeVal;

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("time", RecordFieldType.TIME.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream bais = new ByteArrayInputStream(text.getBytes());
             final CSVRecordReader reader = new CSVRecordReader(bais, Mockito.mock(ComponentLog.class), schema, format, true, false,
                     RecordFieldType.DATE.getDefaultFormat(), timeFormat, RecordFieldType.TIMESTAMP.getDefaultFormat(), "UTF-8")) {

            final Record record = reader.nextRecord(false, false);
            final java.sql.Time time = (Time) record.getValue("time");

            assertEquals(new Time(dateFmt.parse(timeVal).getTime()), time);
        }
    }

    @Test
    public void testTimeNoCoersionUnexpectedFormat() throws IOException, MalformedRecordException {
        final String text = "time\n01:02:03";

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("time", RecordFieldType.TIME.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream bais = new ByteArrayInputStream(text.getBytes());
             final CSVRecordReader reader = new CSVRecordReader(bais, Mockito.mock(ComponentLog.class), schema, format, true, false,
                     RecordFieldType.DATE.getDefaultFormat(), "HH-MM-SS", RecordFieldType.TIMESTAMP.getDefaultFormat(), "UTF-8")) {

            final Record record = reader.nextRecord(false, false);
            assertEquals("01:02:03", (String)record.getValue("time"));
        }
    }

    @Test
    public void testTimeNullFormat() throws IOException, MalformedRecordException {
        final String text = "time\n01:02:03";

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("time", RecordFieldType.TIME.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream bais = new ByteArrayInputStream(text.getBytes());
             final CSVRecordReader reader = new CSVRecordReader(bais, Mockito.mock(ComponentLog.class), schema, format, true, false,
                     RecordFieldType.DATE.getDefaultFormat(), null, RecordFieldType.TIMESTAMP.getDefaultFormat(), "UTF-8")) {

            final Record record = reader.nextRecord(false, false);
            assertEquals("01:02:03", (String)record.getValue("time"));
        }
    }

    @Test
    public void testTimeEmptyFormat() throws IOException, MalformedRecordException {
        final String text = "time\n01:02:03";

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("time", RecordFieldType.TIME.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream bais = new ByteArrayInputStream(text.getBytes());
             final CSVRecordReader reader = new CSVRecordReader(bais, Mockito.mock(ComponentLog.class), schema, format, true, false,
                     RecordFieldType.DATE.getDefaultFormat(), "", RecordFieldType.TIMESTAMP.getDefaultFormat(), "UTF-8")) {

            final Record record = reader.nextRecord(false, false);
            assertEquals("01:02:03", (String)record.getValue("time"));
        }
    }

    @Test
    public void testTimestampNoCoersionExpectedFormat() throws IOException, MalformedRecordException, ParseException {
        final String timeFormat = "HH!mm!ss";
        DateFormat dateFmt = new SimpleDateFormat(timeFormat);
        dateFmt.setTimeZone(TimeZone.getTimeZone("gmt"));
        final String timeVal = "19!02!03";
        final String text = "timestamp\n" + timeVal;

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("timestamp", RecordFieldType.TIMESTAMP.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream bais = new ByteArrayInputStream(text.getBytes());
             final CSVRecordReader reader = new CSVRecordReader(bais, Mockito.mock(ComponentLog.class), schema, format, true, false,
                     RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), timeFormat,"UTF-8")) {

            final Record record = reader.nextRecord(false, false);
            final java.sql.Timestamp time = (Timestamp) record.getValue("timestamp");

            assertEquals(new Timestamp(dateFmt.parse(timeVal).getTime()), time);
        }
    }

    @Test
    public void testTimestampNoCoersionUnexpectedFormat() throws IOException, MalformedRecordException {
        final String text = "timestamp\n01:02:03";

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("timestamp", RecordFieldType.TIMESTAMP.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream bais = new ByteArrayInputStream(text.getBytes());
             final CSVRecordReader reader = new CSVRecordReader(bais, Mockito.mock(ComponentLog.class), schema, format, true, false,
                     RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), "HH-MM-SS", "UTF-8")) {

            final Record record = reader.nextRecord(false, false);
            assertEquals("01:02:03", (String)record.getValue("timestamp"));
        }
    }


    @Test
    public void testSimpleParse() throws IOException, MalformedRecordException {
        final List<RecordField> fields = getDefaultFields();
        fields.replaceAll(f -> f.getFieldName().equals("balance") ? new RecordField("balance", doubleDataType) : f);

        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream fis = new FileInputStream(new File("src/test/resources/csv/single-bank-account.csv"));
            final CSVRecordReader reader = createReader(fis, schema, format)) {

            final Object[] record = reader.nextRecord().getValues();
            final Object[] expectedValues = new Object[] {"1", "John Doe", 4750.89D, "123 My Street", "My City", "MS", "11111", "USA"};
            Assert.assertArrayEquals(expectedValues, record);

            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testExcelFormat() throws IOException, MalformedRecordException {
        final List<RecordField> fields = new ArrayList<RecordField>();
        fields.add(new RecordField("fieldA", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("fieldB", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final String headerLine = "fieldA,fieldB";
        final String inputRecord = "valueA,valueB";
        final String csvData = headerLine + "\n" + inputRecord;
        final byte[] inputData = csvData.getBytes();

        try (final InputStream bais = new ByteArrayInputStream(inputData);
            final CSVRecordReader reader = createReader(bais, schema, CSVFormat.EXCEL)) {

            final Object[] record = reader.nextRecord().getValues();
            final Object[] expectedValues = new Object[] {"valueA", "valueB"};
            Assert.assertArrayEquals(expectedValues, record);

            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testMultipleRecords() throws IOException, MalformedRecordException {
        final List<RecordField> fields = getDefaultFields();
        fields.replaceAll(f -> f.getFieldName().equals("balance") ? new RecordField("balance", doubleDataType) : f);

        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream fis = new FileInputStream(new File("src/test/resources/csv/multi-bank-account.csv"));
            final CSVRecordReader reader = createReader(fis, schema, format)) {

            final Object[] firstRecord = reader.nextRecord().getValues();
            final Object[] firstExpectedValues = new Object[] {"1", "John Doe", 4750.89D, "123 My Street", "My City", "MS", "11111", "USA"};
            Assert.assertArrayEquals(firstExpectedValues, firstRecord);

            final Object[] secondRecord = reader.nextRecord().getValues();
            final Object[] secondExpectedValues = new Object[] {"2", "Jane Doe", 4820.09D, "321 Your Street", "Your City", "NY", "33333", "USA"};
            Assert.assertArrayEquals(secondExpectedValues, secondRecord);

            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testExtraWhiteSpace() throws IOException, MalformedRecordException {
        final List<RecordField> fields = getDefaultFields();
        fields.replaceAll(f -> f.getFieldName().equals("balance") ? new RecordField("balance", doubleDataType) : f);

        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream fis = new FileInputStream(new File("src/test/resources/csv/extra-white-space.csv"));
            final CSVRecordReader reader = createReader(fis, schema, format)) {

            final Object[] firstRecord = reader.nextRecord().getValues();
            final Object[] firstExpectedValues = new Object[] {"1", "John Doe", 4750.89D, "123 My Street", "My City", "MS", "11111", "USA"};
            Assert.assertArrayEquals(firstExpectedValues, firstRecord);

            final Object[] secondRecord = reader.nextRecord().getValues();
            final Object[] secondExpectedValues = new Object[] {"2", "Jane Doe", 4820.09D, "321 Your Street", "Your City", "NY", "33333", "USA"};
            Assert.assertArrayEquals(secondExpectedValues, secondRecord);

            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testMissingField() throws IOException, MalformedRecordException {
        final List<RecordField> fields = getDefaultFields();
        fields.replaceAll(f -> f.getFieldName().equals("balance") ? new RecordField("balance", doubleDataType) : f);

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final String headerLine = "id, name, balance, address, city, state, zipCode, country";
        final String inputRecord = "1, John, 40.80, 123 My Street, My City, MS, 11111";
        final String csvData = headerLine + "\n" + inputRecord;
        final byte[] inputData = csvData.getBytes();

        try (final InputStream bais = new ByteArrayInputStream(inputData);
            final CSVRecordReader reader = createReader(bais, schema, format)) {

            final Record record = reader.nextRecord();
            assertNotNull(record);

            assertEquals("1", record.getValue("id"));
            assertEquals("John", record.getValue("name"));
            assertEquals(40.8D, record.getValue("balance"));
            assertEquals("123 My Street", record.getValue("address"));
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

        final String headerLine = "id, name, balance, address, city, state, zipCode, continent";
        final String inputRecord = "1, John, 40.80, 123 My Street, My City, MS, 11111, North America";
        final String csvData = headerLine + "\n" + inputRecord;
        final byte[] inputData = csvData.getBytes();

        // test nextRecord does not contain a 'continent' field
        try (final InputStream bais = new ByteArrayInputStream(inputData);
            final CSVRecordReader reader = createReader(bais, schema, format)) {

            final Record record = reader.nextRecord();
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
            final CSVRecordReader reader = createReader(bais, schema, format)) {

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
    public void testFieldInSchemaButNotHeader() throws IOException, MalformedRecordException {
        final List<RecordField> fields = getDefaultFields();
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final String headerLine = "id, name, balance, address, city, state, zipCode";
        final String inputRecord = "1, John, 40.80, 123 My Street, My City, MS, 11111, USA";
        final String csvData = headerLine + "\n" + inputRecord;
        final byte[] inputData = csvData.getBytes();

        try (final InputStream bais = new ByteArrayInputStream(inputData);
            final CSVRecordReader reader = createReader(bais, schema, format)) {

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

        // Create another Record Reader that indicates that the header line is present but should be ignored. This should cause
        // our schema to be the definitive list of what fields exist.
        try (final InputStream bais = new ByteArrayInputStream(inputData);
            final CSVRecordReader reader = new CSVRecordReader(bais, Mockito.mock(ComponentLog.class), schema, format, true, true,
                RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), "UTF-8")) {

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
            // But if we configure the reader to Ignore the header, then this will not occur!
            assertEquals("USA", record.getValue("country"));

            assertNull(reader.nextRecord());
        }

    }

    @Test
    public void testExtraFieldNotInHeader() throws IOException, MalformedRecordException {
        final List<RecordField> fields = getDefaultFields();
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final String headerLine = "id, name, balance, address, city, state, zipCode, country";
        final String inputRecord = "1, John, 40.80, 123 My Street, My City, MS, 11111, USA, North America";
        final String csvData = headerLine + "\n" + inputRecord;
        final byte[] inputData = csvData.getBytes();

        // test nextRecord does not contain a 'continent' field
        try (final InputStream bais = new ByteArrayInputStream(inputData);
            final CSVRecordReader reader = createReader(bais, schema, format)) {

            final Record record = reader.nextRecord(false, false);
            assertNotNull(record);

            assertEquals("1", record.getValue("id"));
            assertEquals("John", record.getValue("name"));
            assertEquals("40.80", record.getValue("balance"));
            assertEquals("123 My Street", record.getValue("address"));
            assertEquals("My City", record.getValue("city"));
            assertEquals("MS", record.getValue("state"));
            assertEquals("11111", record.getValue("zipCode"));
            assertEquals("USA", record.getValue("country"));
            assertEquals("North America", record.getValue("unknown_field_index_8"));

            assertNull(reader.nextRecord(false, false));
        }
    }

    @Test
    public void testMultipleRecordsEscapedWithSpecialChar() throws IOException, MalformedRecordException {

        char delimiter = StringEscapeUtils.unescapeJava("\u0001").charAt(0);

        final CSVFormat format = CSVFormat.DEFAULT.withFirstRecordAsHeader().withTrim().withQuote('"').withDelimiter(delimiter);
        final List<RecordField> fields = getDefaultFields();
        fields.replaceAll(f -> f.getFieldName().equals("balance") ? new RecordField("balance", doubleDataType) : f);

        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream fis = new FileInputStream(new File("src/test/resources/csv/multi-bank-account_escapedchar.csv"));
            final CSVRecordReader reader = createReader(fis, schema, format)) {

            final Object[] firstRecord = reader.nextRecord().getValues();
            final Object[] firstExpectedValues = new Object[] {"1", "John Doe", 4750.89D, "123 My Street", "My City", "MS", "11111", "USA"};
            Assert.assertArrayEquals(firstExpectedValues, firstRecord);

            final Object[] secondRecord = reader.nextRecord().getValues();
            final Object[] secondExpectedValues = new Object[] {"2", "Jane Doe", 4820.09D, "321 Your Street", "Your City", "NY", "33333", "USA"};
            Assert.assertArrayEquals(secondExpectedValues, secondRecord);

            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testQuote() throws IOException, MalformedRecordException {
        final CSVFormat format = CSVFormat.RFC4180.withFirstRecordAsHeader().withTrim().withQuote('"');
        final String text = "\"name\"\n\"\"\"\"\n\"\"\"\"";

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream bais = new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8));
             final CSVRecordReader reader = new CSVRecordReader(bais, Mockito.mock(ComponentLog.class), schema, format, true, false,
                     RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), StandardCharsets.UTF_8.name())) {

            Record record = reader.nextRecord();
            String name = (String)record.getValue("name");
            assertEquals("\"", name);

            record = reader.nextRecord(false, false);
            name = (String)record.getValue("name");
            assertEquals("\"", name);
        }
    }

}
