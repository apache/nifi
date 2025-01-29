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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.apache.nifi.schema.access.SchemaNameAsAttribute;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class TestWriteCSVResult {

    private final CSVFormat newLine = CSVFormat.DEFAULT.builder()
        .setRecordSeparator("\n")
        .get();
    private final CSVFormat noQuote = newLine.builder()
        .setEscape('\\')
        .setQuoteMode(QuoteMode.NONE)
        .get();
    private final CSVFormat doubleQuote = CSVFormat.DEFAULT.builder()
        .setEscape('\\')
        .setQuote('"')
        .setRecordSeparator("\n")
        .get();
    private final CSVFormat doubleQuoteNoEscape = doubleQuote.builder()
        .setEscape(null)
        .get();

    @Test
    public void testNumbersNotQuoted() throws IOException {
        final Map<String, Object> values = new HashMap<>();
        values.put("name", "John Doe");
        values.put("age", 30);

        final List<RecordField> schemaFields = new ArrayList<>();
        schemaFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        schemaFields.add(new RecordField("age", RecordFieldType.INT.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(schemaFields);
        final Record record = new MapRecord(schema, values);

        // Test with Non-Numeric Quote Mode
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CSVFormat csvFormat = newLine.builder().setQuoteMode(QuoteMode.NON_NUMERIC).get();
        try (final WriteCSVResult result = new WriteCSVResult(csvFormat, schema, new SchemaNameAsAttribute(), baos,
            RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), true, "UTF-8")) {
            result.writeRecord(record);
        }

        String output = baos.toString();
        assertEquals("\"name\",\"age\"\n\"John Doe\",30\n", output);

        baos.reset();

        // Test with MINIMAL Quote Mode
        csvFormat = newLine.builder().setQuoteMode(QuoteMode.MINIMAL).get();
        try (final WriteCSVResult result = new WriteCSVResult(csvFormat, schema, new SchemaNameAsAttribute(), baos,
            RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), true, "UTF-8")) {
            result.writeRecord(record);
        }

        output = baos.toString();
        assertEquals("name,age\nJohn Doe,30\n", output);
    }

    @Test
    public void testDataTypes() throws IOException {
        final CSVFormat csvFormat = newLine.builder().setQuoteMode(QuoteMode.ALL).get();

        final StringBuilder headerBuilder = new StringBuilder();
        final List<RecordField> fields = new ArrayList<>();
        for (final RecordFieldType fieldType : RecordFieldType.values()) {
            if (fieldType == RecordFieldType.CHOICE) {
                final List<DataType> possibleTypes = new ArrayList<>();
                possibleTypes.add(RecordFieldType.INT.getDataType());
                possibleTypes.add(RecordFieldType.LONG.getDataType());

                fields.add(new RecordField(fieldType.name().toLowerCase(), fieldType.getChoiceDataType(possibleTypes)));
            } else {
                fields.add(new RecordField(fieldType.name().toLowerCase(), fieldType.getDataType()));
            }

            headerBuilder.append('"').append(fieldType.name().toLowerCase()).append('"').append(",");
        }
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final OffsetDateTime now = OffsetDateTime.now();

        try (final WriteCSVResult result = new WriteCSVResult(csvFormat, schema, new SchemaNameAsAttribute(), baos,
            RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), true, "UTF-8")) {

            final Map<String, Object> valueMap = new HashMap<>();
            valueMap.put("string", "a孟bc李12儒3");
            valueMap.put("boolean", true);
            valueMap.put("byte", (byte) 1);
            valueMap.put("char", 'c');
            valueMap.put("short", (short) 8);
            valueMap.put("int", 9);
            valueMap.put("bigint", BigInteger.valueOf(8L));
            valueMap.put("long", 8L);
            valueMap.put("float", 8.0F);
            valueMap.put("double", 8.0D);
            valueMap.put("decimal", BigDecimal.valueOf(8.1D));
            valueMap.put("date", new Date(now.toInstant().toEpochMilli()));
            valueMap.put("time", new Time(now.toInstant().toEpochMilli()));
            valueMap.put("timestamp", new Timestamp(now.toInstant().toEpochMilli()));
            valueMap.put("record", null);
            valueMap.put("choice", 48L);
            valueMap.put("array", null);
            valueMap.put("enum", null);
            valueMap.put("uuid", "8bb20bf2-ec41-4b94-80a4-922f4dba009c");

            final Record record = new MapRecord(schema, valueMap);
            final RecordSet rs = RecordSet.of(schema, record);

            result.write(rs);
        }

        final String output = baos.toString(StandardCharsets.UTF_8);

        headerBuilder.deleteCharAt(headerBuilder.length() - 1);
        final String headerLine = headerBuilder.toString();

        final String[] splits = output.split("\n");
        assertEquals(2, splits.length);
        assertEquals(headerLine, splits[0]);

        final String dateValue = getFormatter(RecordFieldType.DATE.getDefaultFormat()).format(now);
        final String timeValue = getFormatter(RecordFieldType.TIME.getDefaultFormat()).format(now);
        final String timestampValue = getFormatter(RecordFieldType.TIMESTAMP.getDefaultFormat()).format(now);

        final String values = splits[1];

        final String expectedValues = "\"true\",\"1\",\"8\",\"9\",\"8\",\"8\",\"8.0\",\"8.0\",\"8.1\",\"" + timestampValue +
            "\",\"" + dateValue + "\",\"" + timeValue + "\",\"8bb20bf2-ec41-4b94-80a4-922f4dba009c\",\"c\",,\"a孟bc李12儒3\",,\"48\",,";

        assertEquals(expectedValues, values);
    }

    @Test
    public void testExtraFieldInWriteRecord() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", "1");
        values.put("name", "John");
        final Record record = new MapRecord(schema, values);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final String output;
        try (final WriteCSVResult writer = new WriteCSVResult(noQuote, schema, new SchemaNameAsAttribute(), baos,
            RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), true, "ASCII")) {

            writer.beginRecordSet();
            writer.write(record);
            writer.finishRecordSet();
            writer.flush();
            output = baos.toString();
        }

        assertEquals("id\n1\n", output);
    }

    @Test
    public void testExtraFieldInWriteRawRecord() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        // The fields defined in the schema should be written first followed by extra ones.
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("name", "John");
        values.put("id", "1");
        final Record record = new MapRecord(schema, values);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final String output;
        try (final WriteCSVResult writer = new WriteCSVResult(noQuote, schema, new SchemaNameAsAttribute(), baos,
            RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), true, "ASCII")) {

            writer.beginRecordSet();
            writer.writeRawRecord(record);
            writer.finishRecordSet();
            writer.flush();
            output = baos.toString();
        }

        assertEquals("id,name\n1,John\n", output);
    }

    @Test
    public void testMissingFieldWriteRecord() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", "1");
        final Record record = new MapRecord(schema, values);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final String output;
        try (final WriteCSVResult writer = new WriteCSVResult(noQuote, schema, new SchemaNameAsAttribute(), baos,
            RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), true, "ASCII")) {

            writer.beginRecordSet();
            writer.writeRecord(record);
            writer.finishRecordSet();
            writer.flush();
            output = baos.toString();
        }

        assertEquals("id,name\n1,\n", output);
    }

    @Test
    public void testMissingFieldWriteRawRecord() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", "1");
        final Record record = new MapRecord(schema, values);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final String output;
        try (final WriteCSVResult writer = new WriteCSVResult(noQuote, schema, new SchemaNameAsAttribute(), baos,
            RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), true, "ASCII")) {

            writer.beginRecordSet();
            writer.writeRawRecord(record);
            writer.finishRecordSet();
            writer.flush();
            output = baos.toString();
        }

        assertEquals("id,name\n1,\n", output);
    }


    @Test
    public void testMissingAndExtraFieldWriteRecord() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", "1");
        values.put("dob", "1/1/1970");
        final Record record = new MapRecord(schema, values);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final String output;
        try (final WriteCSVResult writer = new WriteCSVResult(noQuote, schema, new SchemaNameAsAttribute(), baos,
            RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), true, "ASCII")) {

            writer.beginRecordSet();
            writer.writeRecord(record);
            writer.finishRecordSet();
            writer.flush();
            output = baos.toString();
        }

        assertEquals("id,name\n1,\n", output);
    }

    @Test
    public void testMissingAndExtraFieldWriteRawRecord() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", "1");
        values.put("dob", "1/1/1970");
        final Record record = new MapRecord(schema, values);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final String output;
        try (final WriteCSVResult writer = new WriteCSVResult(noQuote, schema, new SchemaNameAsAttribute(), baos,
            RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), true, "ASCII")) {

            writer.beginRecordSet();
            writer.writeRawRecord(record);
            writer.finishRecordSet();
            writer.flush();
            output = baos.toString();
        }

        assertEquals("id,name,dob\n1,,1/1/1970\n", output);
    }

    @Test
    public void testEscapeCharInValueWriteRecord() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", "1\\");
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final String output;
        try (final WriteCSVResult writer = new WriteCSVResult(doubleQuote, schema, new SchemaNameAsAttribute(), baos,
                RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), true, "ASCII")) {

            writer.beginRecordSet();
            writer.write(record);
            writer.finishRecordSet();
            writer.flush();
            output = baos.toString();
        }

        assertEquals("id,name\n\"1\\\\\",John Doe\n", output);
    }

    @Test
    public void testEmptyEscapeCharWriteRecord() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", "1\\");
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final String output;
        try (final WriteCSVResult writer = new WriteCSVResult(doubleQuoteNoEscape, schema, new SchemaNameAsAttribute(), baos,
                RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), true, "ASCII")) {

            writer.beginRecordSet();
            writer.write(record);
            writer.finishRecordSet();
            writer.flush();
            output = baos.toString();
        }

        assertEquals("id,name\n1\\,John Doe\n", output);
    }

    @Test
    public void testWriteHeaderWithNoRecords() throws IOException {
        final CSVFormat csvFormat = CSVFormat.DEFAULT.builder().setEscape('\\').setQuoteMode(QuoteMode.NONE).setRecordSeparator(",").get();
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final String output;
        try (final WriteCSVResult writer = new WriteCSVResult(csvFormat, schema, new SchemaNameAsAttribute(), baos,
                RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), true, "ASCII")) {

            writer.beginRecordSet();
            writer.finishRecordSet();
            writer.flush();
            output = baos.toString();
        }

        assertEquals("id,name,", output);
    }


    private DateTimeFormatter getFormatter(final String format) {
        return DateTimeFormatter.ofPattern(format);
    }
}
