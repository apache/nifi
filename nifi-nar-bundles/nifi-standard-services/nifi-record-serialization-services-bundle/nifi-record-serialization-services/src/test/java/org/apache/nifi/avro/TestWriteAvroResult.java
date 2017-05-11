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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.junit.Test;

public abstract class TestWriteAvroResult {

    protected abstract WriteAvroResult createWriter(Schema schema);

    protected abstract GenericRecord readRecord(InputStream in, Schema schema) throws IOException;

    protected void verify(final WriteResult writeResult) {
    }

    @Test
    public void testLogicalTypes() throws IOException, ParseException {
        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/avro/logical-types.avsc"));
        final WriteAvroResult writer = createWriter(schema);

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("timeMillis", RecordFieldType.TIME.getDataType()));
        fields.add(new RecordField("timeMicros", RecordFieldType.TIME.getDataType()));
        fields.add(new RecordField("timestampMillis", RecordFieldType.TIMESTAMP.getDataType()));
        fields.add(new RecordField("timestampMicros", RecordFieldType.TIMESTAMP.getDataType()));
        fields.add(new RecordField("date", RecordFieldType.DATE.getDataType()));
        final RecordSchema recordSchema = new SimpleRecordSchema(fields);

        final String expectedTime = "2017-04-04 14:20:33.000";
        final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        df.setTimeZone(TimeZone.getTimeZone("gmt"));
        final long timeLong = df.parse(expectedTime).getTime();

        final Map<String, Object> values = new HashMap<>();
        values.put("timeMillis", new Time(timeLong));
        values.put("timeMicros", new Time(timeLong));
        values.put("timestampMillis", new Timestamp(timeLong));
        values.put("timestampMicros", new Timestamp(timeLong));
        values.put("date", new Date(timeLong));
        final Record record = new MapRecord(recordSchema, values);

        final byte[] data;
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            writer.write(RecordSet.of(record.getSchema(), record), baos);
            data = baos.toByteArray();
        }

        try (final InputStream in = new ByteArrayInputStream(data)) {
            final GenericRecord avroRecord = readRecord(in, schema);
            final long secondsSinceMidnight = 33 + (20 * 60) + (14 * 60 * 60);
            final long millisSinceMidnight = secondsSinceMidnight * 1000L;

            assertEquals((int) millisSinceMidnight, avroRecord.get("timeMillis"));
            assertEquals(millisSinceMidnight * 1000L, avroRecord.get("timeMicros"));
            assertEquals(timeLong, avroRecord.get("timestampMillis"));
            assertEquals(timeLong * 1000L, avroRecord.get("timestampMicros"));
            assertEquals(17260, avroRecord.get("date"));
        }
    }


    @Test
    public void testDataTypes() throws IOException {
        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/avro/datatypes.avsc"));
        final WriteAvroResult writer = createWriter(schema);

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

        final byte[] data;
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            final WriteResult writeResult = writer.write(RecordSet.of(record.getSchema(), record), baos);
            verify(writeResult);
            data = baos.toByteArray();
        }

        try (final InputStream in = new ByteArrayInputStream(data)) {
            final GenericRecord avroRecord = readRecord(in, schema);
            assertMatch(record, avroRecord);
        }
    }

    protected void assertMatch(final Record record, final GenericRecord avroRecord) {
        for (final String fieldName : record.getSchema().getFieldNames()) {
            Object avroValue = avroRecord.get(fieldName);
            final Object recordValue = record.getValue(fieldName);

            if (recordValue instanceof String) {
                assertNotNull(fieldName + " should not have been null", avroValue);
                avroValue = avroValue.toString();
            }

            if (recordValue instanceof Object[] && avroValue instanceof ByteBuffer) {
                final ByteBuffer bb = (ByteBuffer) avroValue;
                final Object[] objectArray = (Object[]) recordValue;
                assertEquals("For field " + fieldName + ", byte buffer remaining should have been " + objectArray.length + " but was " + bb.remaining(),
                    objectArray.length, bb.remaining());

                for (int i = 0; i < objectArray.length; i++) {
                    assertEquals(objectArray[i], bb.get());
                }
            } else if (recordValue instanceof Object[]) {
                assertTrue(fieldName + " should have been instanceof Array", avroValue instanceof Array);
                final Array<?> avroArray = (Array<?>) avroValue;
                final Object[] recordArray = (Object[]) recordValue;
                assertEquals(fieldName + " not equal", recordArray.length, avroArray.size());
                for (int i = 0; i < recordArray.length; i++) {
                    assertEquals(fieldName + "[" + i + "] not equal", recordArray[i], avroArray.get(i));
                }
            } else if (recordValue instanceof byte[]) {
                final ByteBuffer bb = ByteBuffer.wrap((byte[]) recordValue);
                assertEquals(fieldName + " not equal", bb, avroValue);
            } else if (recordValue instanceof Map) {
               assertTrue(fieldName + " should have been instanceof Map", avroValue instanceof Map);
               final Map<?, ?> avroMap = (Map<?, ?>) avroValue;
               final Map<?, ?> recordMap = (Map<?, ?>) recordValue;
               assertEquals(fieldName + " not equal", recordMap.size(), avroMap.size());
               for (Object s : avroMap.keySet()) {
                  assertMatch((Record) recordMap.get(s.toString()), (GenericRecord) avroMap.get(s));
               }
            } else if (recordValue instanceof Record) {
                assertMatch((Record) recordValue, (GenericRecord) avroValue);
            } else {
                assertEquals(fieldName + " not equal", recordValue, avroValue);
            }
        }
    }
}
