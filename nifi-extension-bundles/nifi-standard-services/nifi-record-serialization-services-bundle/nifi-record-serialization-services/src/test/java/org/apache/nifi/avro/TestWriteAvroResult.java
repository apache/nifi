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

package org.apache.nifi.avro;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.ListRecordSet;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public abstract class TestWriteAvroResult {

    protected abstract RecordSetWriter createWriter(Schema schema, OutputStream out) throws IOException;

    protected abstract GenericRecord readRecord(InputStream in, Schema schema) throws IOException;

    protected abstract List<GenericRecord> readRecords(InputStream in, Schema schema, int recordCount) throws IOException;

    protected void verify(final WriteResult writeResult) {
    }

    @Test
    public void testWriteRecursiveRecord() throws IOException {
        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/avro/recursive.avsc"));
        final RecordSchema recordSchema = AvroTypeUtil.createSchema(schema);
        final FileInputStream in = new FileInputStream("src/test/resources/avro/recursive.avro");

        try (final AvroRecordReader reader = new AvroReaderWithExplicitSchema(in, recordSchema, schema);
                final RecordSetWriter writer = createWriter(schema, new ByteArrayOutputStream())) {

            final GenericRecord avroRecord = reader.nextAvroRecord();
            final Map<String, Object> recordMap = AvroTypeUtil.convertAvroRecordToMap(avroRecord, recordSchema);
            final Record record = new MapRecord(recordSchema, recordMap);
            assertDoesNotThrow(() -> writer.write(record));
        }
    }

    @Test
    public void testWriteRecord() throws IOException {
        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/avro/simple.avsc"));

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("msg", RecordFieldType.STRING.getDataType()));
        final RecordSchema recordSchema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("msg", "nifi");
        final Record record = new MapRecord(recordSchema, values);

        try (final RecordSetWriter writer = createWriter(schema, baos)) {
            writer.write(record);
        }

        final byte[] data = baos.toByteArray();

        try (final InputStream in = new ByteArrayInputStream(data)) {
            final GenericRecord avroRecord = readRecord(in, schema);

            assertNotNull(avroRecord);
            assertNotNull(avroRecord.get("msg"));
            assertEquals("nifi", avroRecord.get("msg").toString());
        }
    }

    @Test
    public void testWriteRecordSet() throws IOException {
        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/avro/simple.avsc"));

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("msg", RecordFieldType.STRING.getDataType()));
        final RecordSchema recordSchema = new SimpleRecordSchema(fields);

        final int recordCount = 3;
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < recordCount; i++) {
            final Map<String, Object> values = new HashMap<>();
            values.put("msg", "nifi" + i);
            final Record record = new MapRecord(recordSchema, values);
            records.add(record);
        }

        try (final RecordSetWriter writer = createWriter(schema, baos)) {
            writer.write(new ListRecordSet(recordSchema, records));
        }

        final byte[] data = baos.toByteArray();

        try (final InputStream in = new ByteArrayInputStream(data)) {
            final List<GenericRecord> avroRecords = readRecords(in, schema, recordCount);
            for (int i = 0; i < recordCount; i++) {
                final GenericRecord avroRecord = avroRecords.get(i);

                assertNotNull(avroRecord);
                assertNotNull(avroRecord.get("msg"));
                assertEquals("nifi" + i, avroRecord.get("msg").toString());
            }
        }
    }

    @Test
    public void testDecimalType() throws IOException {
        final Object[][] decimals = new Object[][] {
                // id, record field, value, expected value

                // Uses the whole precision and scale
                {1, RecordFieldType.DECIMAL.getDecimalDataType(10, 2),  new BigDecimal("12345678.12"),  new BigDecimal("12345678.12")},

                // Uses less precision and scale than allowed
                {2, RecordFieldType.DECIMAL.getDecimalDataType(10, 2),  new BigDecimal("123456.1"),  new BigDecimal("123456.10")},

                // Record schema uses smaller precision and scale than allowed
                {3, RecordFieldType.DECIMAL.getDecimalDataType(8, 1),  new BigDecimal("123456.1"),  new BigDecimal("123456.10")},

                // Record schema uses bigger precision and scale than allowed
                {4, RecordFieldType.DECIMAL.getDecimalDataType(16, 4),  new BigDecimal("123456.1"),  new BigDecimal("123456.10")},
        };

        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/avro/decimals.avsc"));
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final List<RecordField> fields = new ArrayList<>();
        final Map<String, Object> values = new HashMap<>();

        for (final Object[] decimal : decimals) {
            fields.add(new RecordField("decimal" + decimal[0], (DataType) decimal[1]));
            values.put("decimal" + decimal[0], decimal[2]);
        }

        final Record record = new MapRecord(new SimpleRecordSchema(fields), values);

        try (final RecordSetWriter writer = createWriter(schema, baos)) {
            writer.write(RecordSet.of(record.getSchema(), record));
        }

        final byte[] data = baos.toByteArray();

        try (final InputStream in = new ByteArrayInputStream(data)) {
            final GenericRecord avroRecord = readRecord(in, schema);

            for (final Object[] decimal : decimals) {
                final Schema decimalSchema = schema.getField("decimal" + decimal[0]).schema();
                final LogicalType logicalType = decimalSchema.getLogicalType();
                assertEquals(decimal[3], new Conversions.DecimalConversion().fromBytes((ByteBuffer) avroRecord.get("decimal" + decimal[0]), decimalSchema, logicalType));
            }
        }
    }

    @Test
    public void testLogicalTypes() throws IOException {
        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/avro/logical-types.avsc"));
        testLogicalTypes(schema);
    }

    @Test
    public void testNullableLogicalTypes() throws IOException {
        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/avro/logical-types-nullable.avsc"));
        testLogicalTypes(schema);
    }

    private void testLogicalTypes(Schema schema) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("timeMillis", RecordFieldType.TIME.getDataType()));
        fields.add(new RecordField("timeMicros", RecordFieldType.TIME.getDataType()));
        fields.add(new RecordField("timestampMillis", RecordFieldType.TIMESTAMP.getDataType()));
        fields.add(new RecordField("timestampMicros", RecordFieldType.TIMESTAMP.getDataType()));
        fields.add(new RecordField("date", RecordFieldType.DATE.getDataType()));
        fields.add(new RecordField("decimal", RecordFieldType.DECIMAL.getDecimalDataType(5, 2)));
        final RecordSchema recordSchema = new SimpleRecordSchema(fields);

        final String expectedTime = "2017-04-04 14:20:33.789";
        final DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault());
        final long timeLong = Instant.from(df.parse(expectedTime)).toEpochMilli();

        final Map<String, Object> values = new HashMap<>();
        values.put("timeMillis", new Time(timeLong));
        values.put("timeMicros", new Time(timeLong));
        values.put("timestampMillis", new Timestamp(timeLong));
        values.put("timestampMicros", new Timestamp(timeLong));
        values.put("date", new Date(timeLong));
        values.put("decimal", new BigDecimal("123.45"));
        final Record record = new MapRecord(recordSchema, values);

        try (final RecordSetWriter writer = createWriter(schema, baos)) {
            writer.write(RecordSet.of(record.getSchema(), record));
        }

        final byte[] data = baos.toByteArray();

        try (final InputStream in = new ByteArrayInputStream(data)) {
            final GenericRecord avroRecord = readRecord(in, schema);
            final long secondsSinceMidnight = 33 + (20 * 60) + (14 * 60 * 60);
            final long millisSinceMidnight = (secondsSinceMidnight * 1000L) + 789;

            assertEquals((int) millisSinceMidnight, avroRecord.get("timeMillis"));
            assertEquals(millisSinceMidnight * 1000L, avroRecord.get("timeMicros"));
            assertEquals(timeLong, avroRecord.get("timestampMillis"));
            assertEquals(timeLong * 1000L, avroRecord.get("timestampMicros"));
            // Double value will be converted into logical decimal if Avro schema is defined as logical decimal.
            final Schema decimalSchema = schema.getField("decimal").schema();
            final LogicalType logicalType = decimalSchema.getLogicalType() != null
                    ? decimalSchema.getLogicalType()
                    // Union type doesn't return logical type. Find the first logical type defined within the union.
                    : decimalSchema.getTypes().stream().map(s -> s.getLogicalType()).filter(Objects::nonNull).findFirst().get();
            final BigDecimal decimal = new Conversions.DecimalConversion().fromBytes((ByteBuffer) avroRecord.get("decimal"), decimalSchema, logicalType);
            assertEquals(new BigDecimal("123.45"), decimal);
        }
    }

    @Test
    public void testDataTypes() throws IOException {
        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/avro/datatypes.avsc"));
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        final List<RecordField> subRecordFields = Collections.singletonList(new RecordField("field1", RecordFieldType.STRING.getDataType()));
        final RecordSchema subRecordSchema = new SimpleRecordSchema(subRecordFields);
        final DataType subRecordDataType = RecordFieldType.RECORD.getRecordDataType(subRecordSchema);

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("string", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("int", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("long", RecordFieldType.LONG.getDataType()));
        fields.add(new RecordField("double", RecordFieldType.DOUBLE.getDataType()));
        fields.add(new RecordField("float", RecordFieldType.FLOAT.getDataType()));
        fields.add(new RecordField("boolean", RecordFieldType.BOOLEAN.getDataType()));
        fields.add(new RecordField("bytes", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType())));
        fields.add(new RecordField("nullOrLong", RecordFieldType.LONG.getDataType()));
        fields.add(new RecordField("array", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType())));
        fields.add(new RecordField("record", subRecordDataType));
        fields.add(new RecordField("map", RecordFieldType.MAP.getMapDataType(subRecordDataType)));
        final RecordSchema recordSchema = new SimpleRecordSchema(fields);

        final Record innerRecord = new MapRecord(subRecordSchema, Collections.singletonMap("field1", "hello"));

        final Map<String, Object> innerMap = new HashMap<>();
        innerMap.put("key1", innerRecord);

        final Map<String, Object> values = new HashMap<>();
        values.put("string", "hello");
        values.put("int", 8);
        values.put("long", 42L);
        values.put("double", 3.14159D);
        values.put("float", 1.23456F);
        values.put("boolean", true);
        values.put("bytes", AvroTypeUtil.convertByteArray("hello".getBytes()));
        values.put("nullOrLong", null);
        values.put("array", new Integer[] {1, 2, 3});
        values.put("record", innerRecord);
        values.put("map", innerMap);

        final Record record = new MapRecord(recordSchema, values);

        final WriteResult writeResult;
        try (final RecordSetWriter writer = createWriter(schema, baos)) {
            writeResult = writer.write(RecordSet.of(record.getSchema(), record));
        }

        verify(writeResult);
        final byte[] data = baos.toByteArray();

        try (final InputStream in = new ByteArrayInputStream(data)) {
            final GenericRecord avroRecord = readRecord(in, schema);
            assertMatch(record, avroRecord);
        }
    }

    @Test
    public void testFixedFieldWritesAsGenericFixed() throws IOException {
        // Avro schema with a single fixed(16) field
        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/avro/fixed16.avsc"));

        // NiFi record schema: represent FIXED as an array of BYTEs
        final List<RecordField> fields = Collections.singletonList(
                new RecordField("fixed", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType())));

        final RecordSchema recordSchema = new SimpleRecordSchema(fields);

        // 16-byte value (e.g., UUID bytes). Use NiFi's typical byte[] -> Object[]
        // conversion path
        final byte[] bytes = new byte[16];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) i;
        }
        final Map<String, Object> values = new HashMap<>();
        values.put("fixed", AvroTypeUtil.convertByteArray(bytes));
        final Record record = new MapRecord(recordSchema, values);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (final RecordSetWriter writer = new WriteAvroResultWithSchema(schema, out, CodecFactory.nullCodec())) {
            writer.write(RecordSet.of(record.getSchema(), record));
        }

        // Read back and verify Fixed value is written as GenericFixed with matching
        // bytes
        final byte[] written = out.toByteArray();
        try (final DataFileStream<GenericRecord> dfs = new DataFileStream<>(new ByteArrayInputStream(written), new GenericDatumReader<>())) {
            final GenericRecord rec = dfs.next();
            assertNotNull(rec);
            final Object fixedObj = rec.get("fixed");
            assertInstanceOf(GenericFixed.class, fixedObj);
            final byte[] actual = ((GenericFixed) fixedObj).bytes();
            assertArrayEquals(bytes, actual);
        }
    }

    @Test
    public void testFixedFieldWrongLength() throws IOException {
        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/avro/fixed16.avsc"));

        final List<RecordField> fields = Collections.singletonList(
                new RecordField("fixed", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType())));
        final RecordSchema recordSchema = new SimpleRecordSchema(fields);

        // Wrong length: 15 bytes instead of required 16
        final byte[] bytes = new byte[15];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) i;
        }
        final Map<String, Object> values = new HashMap<>();
        values.put("fixed", AvroTypeUtil.convertByteArray(bytes));
        final Record record = new MapRecord(recordSchema, values);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (final RecordSetWriter writer = new WriteAvroResultWithSchema(schema, out, CodecFactory.nullCodec())) {
            assertThrows(IllegalTypeConversionException.class, () -> writer.write(RecordSet.of(record.getSchema(), record)));
        }
    }

    @Test
    public void testDecimalBytesAndFixedRoundTrip() throws IOException {
        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/avro/decimals-bytes-fixed.avsc"));

        // Both fields are DECIMAL(9,2) in NiFi's record schema
        final List<RecordField> fields = Arrays.asList(
                new RecordField("dec_bytes", RecordFieldType.DECIMAL.getDecimalDataType(9, 2)),
                new RecordField("dec_fixed", RecordFieldType.DECIMAL.getDecimalDataType(9, 2)));
        final RecordSchema recordSchema = new SimpleRecordSchema(fields);

        final BigDecimal expected = new BigDecimal("12345.67");
        final Map<String, Object> values = new HashMap<>();
        values.put("dec_bytes", expected);
        values.put("dec_fixed", expected);
        final Record record = new MapRecord(recordSchema, values);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (final RecordSetWriter writer = new WriteAvroResultWithSchema(schema, out, CodecFactory.nullCodec())) {
            writer.write(RecordSet.of(record.getSchema(), record));
        }

        // Read back the values and convert from logical decimal representations
        final byte[] written = out.toByteArray();
        try (final DataFileStream<GenericRecord> dfs = new DataFileStream<>(new ByteArrayInputStream(written), new GenericDatumReader<>())) {
            final GenericRecord rec = dfs.next();
            assertNotNull(rec);

            // BYTES logical decimal → ByteBuffer
            final Schema bytesFieldSchema = schema.getField("dec_bytes").schema();
            final LogicalType bytesLogicalType = bytesFieldSchema.getLogicalType();
            final Object bytesObj = rec.get("dec_bytes");
            assertInstanceOf(ByteBuffer.class, bytesObj);
            final BigDecimal fromBytes = new Conversions.DecimalConversion().fromBytes((ByteBuffer) bytesObj, bytesFieldSchema, bytesLogicalType);

            // FIXED logical decimal → GenericFixed
            final Schema fixedFieldSchema = schema.getField("dec_fixed").schema();
            final LogicalType fixedLogicalType = fixedFieldSchema.getLogicalType();
            final Object fixedObj = rec.get("dec_fixed");
            assertInstanceOf(GenericFixed.class, fixedObj);
            final BigDecimal fromFixed = new Conversions.DecimalConversion().fromFixed((GenericFixed) fixedObj, fixedFieldSchema, fixedLogicalType);

            assertEquals(expected, fromBytes);
            assertEquals(expected, fromFixed);
        }
    }

    protected void assertMatch(final Record record, final GenericRecord avroRecord) {
        for (final String fieldName : record.getSchema().getFieldNames()) {
            Object avroValue = avroRecord.get(fieldName);
            final Object recordValue = record.getValue(fieldName);

            if (recordValue instanceof String) {
                assertNotNull(avroValue, fieldName + " should not have been null");
                avroValue = avroValue.toString();
            }

            if (recordValue instanceof Object[] && avroValue instanceof ByteBuffer) {
                final ByteBuffer bb = (ByteBuffer) avroValue;
                final Object[] objectArray = (Object[]) recordValue;
                assertEquals(objectArray.length, bb.remaining(),
                        "For field " + fieldName + ", byte buffer remaining should have been " + objectArray.length + " but was " + bb.remaining());

                for (Object o : objectArray) {
                    assertEquals(o, bb.get());
                }
            } else if (recordValue instanceof Object[]) {
                assertInstanceOf(GenericArray.class, avroValue, fieldName + " should have been instanceof GenericArray");
                final GenericArray<?> avroArray = (GenericArray<?>) avroValue;
                final Object[] recordArray = (Object[]) recordValue;
                assertEquals(recordArray.length, avroArray.size(),
                        fieldName + " not equal");
                for (int i = 0; i < recordArray.length; i++) {
                    assertEquals(recordArray[i], avroArray.get(i),
                            fieldName + "[" + i + "] not equal");
                }
            } else if (recordValue instanceof byte[]) {
                final ByteBuffer bb = ByteBuffer.wrap((byte[]) recordValue);
                assertEquals(bb, avroValue,
                        fieldName + " not equal");
            } else if (recordValue instanceof Map) {
                assertInstanceOf(Map.class, avroValue, fieldName + " should have been instanceof Map");
               final Map<?, ?> avroMap = (Map<?, ?>) avroValue;
               final Map<?, ?> recordMap = (Map<?, ?>) recordValue;
               assertEquals(recordMap.size(), avroMap.size(),
                       fieldName + " not equal");
               for (Object s : avroMap.keySet()) {
                  assertMatch((Record) recordMap.get(s.toString()), (GenericRecord) avroMap.get(s));
               }
            } else if (recordValue instanceof Record) {
                assertMatch((Record) recordValue, (GenericRecord) avroValue);
            } else {
                assertEquals(recordValue, avroValue,
                        fieldName + " not equal");
            }
        }
    }
}
