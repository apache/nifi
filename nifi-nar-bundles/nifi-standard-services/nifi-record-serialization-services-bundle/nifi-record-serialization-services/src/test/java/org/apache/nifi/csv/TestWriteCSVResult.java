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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

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
import org.junit.Test;


public class TestWriteCSVResult {

    @Test
    public void testDataTypes() throws IOException {
        final CSVFormat csvFormat = CSVFormat.DEFAULT.withQuoteMode(QuoteMode.ALL).withRecordSeparator("\n");

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
        final long now = System.currentTimeMillis();

        try (final WriteCSVResult result = new WriteCSVResult(csvFormat, schema, new SchemaNameAsAttribute(), baos,
            RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), true)) {

            final Map<String, Object> valueMap = new HashMap<>();
            valueMap.put("string", "string");
            valueMap.put("boolean", true);
            valueMap.put("byte", (byte) 1);
            valueMap.put("char", 'c');
            valueMap.put("short", (short) 8);
            valueMap.put("int", 9);
            valueMap.put("bigint", BigInteger.valueOf(8L));
            valueMap.put("long", 8L);
            valueMap.put("float", 8.0F);
            valueMap.put("double", 8.0D);
            valueMap.put("date", new Date(now));
            valueMap.put("time", new Time(now));
            valueMap.put("timestamp", new Timestamp(now));
            valueMap.put("record", null);
            valueMap.put("choice", 48L);
            valueMap.put("array", null);

            final Record record = new MapRecord(schema, valueMap);
            final RecordSet rs = RecordSet.of(schema, record);

            result.write(rs);
        }

        final String output = new String(baos.toByteArray(), StandardCharsets.UTF_8);

        headerBuilder.deleteCharAt(headerBuilder.length() - 1);
        final String headerLine = headerBuilder.toString();

        final String[] splits = output.split("\n");
        assertEquals(2, splits.length);
        assertEquals(headerLine, splits[0]);

        final String values = splits[1];
        final StringBuilder expectedBuilder = new StringBuilder();
        expectedBuilder.append("\"string\",\"true\",\"1\",\"c\",\"8\",\"9\",\"8\",\"8\",\"8.0\",\"8.0\",");

        final String dateValue = getDateFormat(RecordFieldType.DATE.getDefaultFormat()).format(now);
        final String timeValue = getDateFormat(RecordFieldType.TIME.getDefaultFormat()).format(now);
        final String timestampValue = getDateFormat(RecordFieldType.TIMESTAMP.getDefaultFormat()).format(now);

        expectedBuilder.append('"').append(dateValue).append('"').append(',');
        expectedBuilder.append('"').append(timeValue).append('"').append(',');
        expectedBuilder.append('"').append(timestampValue).append('"').append(',');
        expectedBuilder.append(",\"48\",,");
        final String expectedValues = expectedBuilder.toString();

        assertEquals(expectedValues, values);
    }

    @Test
    public void testExtraFieldInWriteRecord() throws IOException {
        final CSVFormat csvFormat = CSVFormat.DEFAULT.withEscape('\\').withQuoteMode(QuoteMode.NONE).withRecordSeparator("\n");
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", "1");
        values.put("name", "John");
        final Record record = new MapRecord(schema, values);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final String output;
        try (final WriteCSVResult writer = new WriteCSVResult(csvFormat, schema, new SchemaNameAsAttribute(), baos,
            RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), true)) {

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
        final CSVFormat csvFormat = CSVFormat.DEFAULT.withEscape('\\').withQuoteMode(QuoteMode.NONE).withRecordSeparator("\n");
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", "1");
        values.put("name", "John");
        final Record record = new MapRecord(schema, values);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final String output;
        try (final WriteCSVResult writer = new WriteCSVResult(csvFormat, schema, new SchemaNameAsAttribute(), baos,
            RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), true)) {

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
        final CSVFormat csvFormat = CSVFormat.DEFAULT.withEscape('\\').withQuoteMode(QuoteMode.NONE).withRecordSeparator("\n");
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", "1");
        final Record record = new MapRecord(schema, values);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final String output;
        try (final WriteCSVResult writer = new WriteCSVResult(csvFormat, schema, new SchemaNameAsAttribute(), baos,
            RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), true)) {

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
        final CSVFormat csvFormat = CSVFormat.DEFAULT.withEscape('\\').withQuoteMode(QuoteMode.NONE).withRecordSeparator("\n");
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", "1");
        final Record record = new MapRecord(schema, values);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final String output;
        try (final WriteCSVResult writer = new WriteCSVResult(csvFormat, schema, new SchemaNameAsAttribute(), baos,
            RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), true)) {

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
        final CSVFormat csvFormat = CSVFormat.DEFAULT.withEscape('\\').withQuoteMode(QuoteMode.NONE).withRecordSeparator("\n");
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
        try (final WriteCSVResult writer = new WriteCSVResult(csvFormat, schema, new SchemaNameAsAttribute(), baos,
            RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), true)) {

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
        final CSVFormat csvFormat = CSVFormat.DEFAULT.withEscape('\\').withQuoteMode(QuoteMode.NONE).withRecordSeparator("\n");
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
        try (final WriteCSVResult writer = new WriteCSVResult(csvFormat, schema, new SchemaNameAsAttribute(), baos,
            RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), true)) {

            writer.beginRecordSet();
            writer.writeRawRecord(record);
            writer.finishRecordSet();
            writer.flush();
            output = baos.toString();
        }

        assertEquals("id,dob,name\n1,1/1/1970,\n", output);
    }


    private DateFormat getDateFormat(final String format) {
        final DateFormat df = new SimpleDateFormat(format);
        df.setTimeZone(TimeZone.getTimeZone("gmt"));
        return df;
    }

}
