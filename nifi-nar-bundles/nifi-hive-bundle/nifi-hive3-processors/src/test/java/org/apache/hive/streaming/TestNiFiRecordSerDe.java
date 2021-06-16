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
package org.apache.hive.streaming;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockComponentLog;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestNiFiRecordSerDe {

    @Test
    public void testSimpleFields() throws SerDeException {
        NiFiRecordSerDe serDe = createSerDe(
                "bytec,shortc,intc,longc,boolc,floatc,doublec,stringc,varcharc,charc,datec,timestampc,decimalc",
                "tinyint:smallint:int:bigint:boolean:float:double:string:varchar(50):char(1):date:timestamp:decimal"
        );
        RecordSchema schema = new SimpleRecordSchema(
                Arrays.asList(
                        new RecordField("bytec", RecordFieldType.BYTE.getDataType()),
                        new RecordField("shortc", RecordFieldType.SHORT.getDataType()),
                        new RecordField("intc", RecordFieldType.INT.getDataType()),
                        new RecordField("longc", RecordFieldType.LONG.getDataType()),
                        new RecordField("boolc", RecordFieldType.BOOLEAN.getDataType()),
                        new RecordField("floatc", RecordFieldType.FLOAT.getDataType()),
                        new RecordField("doublec", RecordFieldType.DOUBLE.getDataType()),
                        new RecordField("stringc", RecordFieldType.STRING.getDataType()),
                        new RecordField("varcharc", RecordFieldType.STRING.getDataType()),
                        new RecordField("charc", RecordFieldType.CHAR.getDataType()),
                        new RecordField("datec", RecordFieldType.DATE.getDataType("yyyy-MM-dd")),
                        new RecordField("timestampc", RecordFieldType.TIMESTAMP.getDataType("yyyy-MM-dd HH:mm:ss")),
                        new RecordField("decimalc", RecordFieldType.DOUBLE.getDataType())
                )
        );

        long currentTimeMillis = System.currentTimeMillis();

        HashMap<String, Object> input = new HashMap<String, Object>() {{
            put("bytec", (byte) 2);
            put("shortc", (short) 45);
            put("intc", 95);
            put("longc", 876L);
            put("boolc", Boolean.TRUE);
            put("floatc", 4.56f);
            put("doublec", 2.3445);
            put("stringc", "test");
            put("varcharc", "test2");
            put("charc", 'c');
            put("datec", new java.sql.Date(currentTimeMillis));
            put("timestampc", new java.sql.Timestamp(currentTimeMillis));
            put("decimalc", 0.45);
        }};

        Date date = new Date();
        date.setTimeInMillis(currentTimeMillis);
        Timestamp ts = new Timestamp();
        ts.setTimeInMillis(currentTimeMillis);

        List<Object> expected = Arrays.asList(
                Byte.valueOf("2"),
                Short.valueOf("45"),
                95,
                876L,
                Boolean.TRUE,
                4.56f,
                2.3445,
                "test",
                "test2",
                "c",
                date,
                ts,
                HiveDecimal.create("0.45")
        );

        Object deserialized = serDe.deserialize(new ObjectWritable(new MapRecord(schema, input)));

        assertEquals(expected, deserialized);
    }

    @Test
    public void testArrays() throws SerDeException {
        NiFiRecordSerDe serDe = createSerDe(
                "binaryc,binaryc2",
                "binary:binary"
        );
        RecordSchema schema = new SimpleRecordSchema(
                Arrays.asList(
                        new RecordField("binaryc", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType())),
                        new RecordField("binaryc2", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType()))
                )
        );

        HashMap<String, Object> input = new HashMap<String, Object>() {{
            put("binaryc", new byte[]{1, 2});
            put("binaryc2", "Hello");
        }};


        Object[] expected = new Object[]{
                new byte[]{1, 2},
                "Hello".getBytes(StandardCharsets.UTF_8)
        };

        Object deserialized = serDe.deserialize(new ObjectWritable(new MapRecord(schema, input)));

        assertArrayEquals(expected, ((List)deserialized).toArray());
    }

    @Test
    public void testStructField() throws SerDeException{
        NiFiRecordSerDe serDe = createSerDe("structc",
                "struct<age:int,name:string>"
        );
        RecordSchema innerSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("age", RecordFieldType.INT.getDataType()),
                new RecordField("name", RecordFieldType.STRING.getDataType())
        ));
        RecordSchema schema = new SimpleRecordSchema(Collections.singletonList(
                new RecordField("structc", RecordFieldType.RECORD.getRecordDataType(innerSchema))
        ));

        HashMap<String, Object> value = new HashMap<String, Object>() {{
            put("structc", new MapRecord(innerSchema, new HashMap<String, Object>() {{
                put("age", 15);
                put("name", "gideon");
            }}));
        }};

        List<Object> expected = Collections.singletonList(
                Arrays.asList(15, "gideon")
        );

        Object deserialized = serDe.deserialize(new ObjectWritable(new MapRecord(schema, value)));

        assertEquals(expected, deserialized);
    }

    @Test
    public void testSimpleArray() throws SerDeException{
        long now = System.currentTimeMillis();
        Date hiveDate = new Date();
        hiveDate.setTimeInMillis(now);
        Timestamp hiveTs = new Timestamp();
        hiveTs.setTimeInMillis(now);

        testSimpleArray("tinyint", RecordFieldType.BYTE.getDataType(), new Byte[] { 5, 29 },
                new Byte[] { 5, 29 });
        testSimpleArray("smallint", RecordFieldType.SHORT.getDataType(), new Short[] { 5, 29 },
                new Short[] { 5, 29 });
        testSimpleArray("int", RecordFieldType.INT.getDataType(), new Object[] { 1, 2, 3 ,4, 5 },
                new Object[] { 1, 2, 3, 4, 5 });
        testSimpleArray("bigint", RecordFieldType.LONG.getDataType(), new Object[] { 298767L, 89876L },
                new Object[] { 298767L, 89876L });
        testSimpleArray("boolean", RecordFieldType.BOOLEAN.getDataType(), new Object[] { true, false },
                new Object[] { true, false });
        testSimpleArray("float", RecordFieldType.FLOAT.getDataType(), new Object[] { 1.23f, 3.14f },
                new Object[] { 1.23f, 3.14f });
        testSimpleArray("double", RecordFieldType.DOUBLE.getDataType(), new Object[] { 1.235, 3.142, 1.0 },
                new Object[] { 1.235, 3.142, 1.0 });
        testSimpleArray("string", RecordFieldType.STRING.getDataType(), new Object[] { "sasa", "wewe" },
                new Object[] { "sasa", "wewe" });
        testSimpleArray("varchar(20)", RecordFieldType.STRING.getDataType(), new Object[] { "niko", "fiti", "sema"},
                new Object[]  { "niko", "fiti", "sema" });
        testSimpleArray("char(1)", RecordFieldType.CHAR.getDataType(), new Object[] { 'a', 'b', 'c' },
                new Object[] { "a", "b", "c"});
        testSimpleArray("date", RecordFieldType.DATE.getDataType(), new Object[] { new java.sql.Date(now)},
                new Object[] { hiveDate });
        testSimpleArray("timestamp", RecordFieldType.TIMESTAMP.getDataType(), new Object[] { new java.sql.Timestamp(now)},
                new Object[] { hiveTs });
        testSimpleArray("decimal(10,2)", RecordFieldType.DOUBLE.getDataType(), new Object[] { 3.45, 1.25 },
                new Object[] { HiveDecimal.create(3.45), HiveDecimal.create(1.25)});
    }

    public void testSimpleArray(String typeName, DataType elementDataType, Object[] values, Object[] expected) throws SerDeException {
        NiFiRecordSerDe serDe = createSerDe("listc",
                "array<" + typeName + ">"
        );

        RecordSchema schema = new SimpleRecordSchema(Collections.singletonList(
                new RecordField("listc", RecordFieldType.ARRAY.getArrayDataType(elementDataType))
        ));

        Object deserialized = serDe.deserialize(new ObjectWritable(new MapRecord(schema, new HashMap<String, Object>() {{
            put("listc", values);
        }})));

        List<Object> fields = (List<Object>)deserialized;
        assertEquals(1, fields.size());
        List<Object> nested = (List<Object>) fields.get(0);

        for(int i=0; i<expected.length; i++){
            assertEquals(expected[i], nested.get(i));
        }
    }

    @Test
    public void testStructArray() throws SerDeException{
        NiFiRecordSerDe serDe = createSerDe("listc",
                "array<struct<age:int,name:string>>"
        );
        RecordSchema innerSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("age", RecordFieldType.INT.getDataType()),
                new RecordField("name", RecordFieldType.STRING.getDataType())
        ));
        RecordSchema schema = new SimpleRecordSchema(Collections.singletonList(
                new RecordField("listc", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(innerSchema)))
        ));
        HashMap<String, Object> input = new HashMap<String, Object>() {{
            put("listc", new Record[]{new MapRecord(innerSchema, new HashMap<String, Object>() {{
                put("age", 15);
                put("name", "gideon");
            }}),
                    new MapRecord(innerSchema, new HashMap<String, Object>() {{
                        put("age", 87);
                        put("name", "cucu");
                    }})
            });
        }};

        Object expected = Collections.singletonList(
                Arrays.asList(
                        Arrays.asList(15, "gideon"),
                        Arrays.asList(87, "cucu")
                )
        );

        Object deserialized = serDe.deserialize(new ObjectWritable(new MapRecord(schema, input)));

        assertEquals(expected, deserialized);
    }

    @Test
    public void testSimpleMap() throws SerDeException{
        testSimpleMap("string", "tinyint", RecordFieldType.BYTE.getDataType(), createMap((byte)89, (byte)2), objectMap(createMap((byte)89, (byte)2)));
        testSimpleMap("string", "smallint", RecordFieldType.SHORT.getDataType(), createMap((short)89, (short)209), objectMap(createMap((short)89, (short)209)));
        testSimpleMap("string", "int", RecordFieldType.INT.getDataType(), createMap(90, 87), objectMap(createMap(90, 87)));
        testSimpleMap("string", "bigint", RecordFieldType.BIGINT.getDataType(), createMap(87888L, 876L, 123L), objectMap(createMap(87888L, 876L, 123L)));
        testSimpleMap("string", "boolean", RecordFieldType.BOOLEAN.getDataType(), createMap(false, true, true, false), objectMap(createMap(false, true, true, false)));
        testSimpleMap("string", "float", RecordFieldType.FLOAT.getDataType(), createMap(1.2f, 8.6f, 0.125f), objectMap(createMap(1.2f, 8.6f, 0.125f)));
        testSimpleMap("string", "double", RecordFieldType.DOUBLE.getDataType(), createMap(3.142, 8.93), objectMap(createMap(3.142, 8.93)));
        testSimpleMap("string", "string", RecordFieldType.STRING.getDataType(), createMap("form", "ni", "aje"), objectMap(createMap("form", "ni", "aje")));
        testSimpleMap("string", "varchar(20)", RecordFieldType.STRING.getDataType(), createMap("niko", "kiza"), objectMap(createMap("niko", "kiza")));
        testSimpleMap("string", "char(1)", RecordFieldType.CHAR.getDataType(), createMap('a', 'b', 'c'), objectMap(createMap("a", "b", "c")));
        long now = System.currentTimeMillis();
        Date hiveDate = new Date();
        hiveDate.setTimeInMillis(now);
        Timestamp hiveTs = new Timestamp();
        hiveTs.setTimeInMillis(now);

        testSimpleMap("string", "date", RecordFieldType.DATE.getDataType(), createMap(new java.sql.Date(now)), objectMap(createMap(hiveDate)));
        testSimpleMap("string", "timestamp", RecordFieldType.TIMESTAMP.getDataType(), createMap(new java.sql.Timestamp(now)), objectMap(createMap(hiveTs)));
        testSimpleMap("string", "decimal(10,2)", RecordFieldType.DOUBLE.getDataType(), createMap(45.6, 2345.5), objectMap(createMap(
                HiveDecimal.create(45.6), HiveDecimal.create(2345.5)
        )));
    }

    private Map<String,Object> createMap(Object... keyValues){
        Map<String,Object> map = new HashMap<>(keyValues.length);
        for(int i=0; i<keyValues.length; i++){
            map.put("key." + i, keyValues[i]);
        }
        return  map;
    }

    Map<Object,Object> objectMap(Map<String,Object> input){
        return new HashMap<>(input);
    }

    private void testSimpleMap(String keyType, String valueType, DataType fieldType, Map<String, Object> fields, Map<Object, Object> expected) throws SerDeException{
        NiFiRecordSerDe serDe = createSerDe("mapc",
                "map<" + keyType + "," + valueType + ">"
        );
        RecordSchema schema = new SimpleRecordSchema(Collections.singletonList(
                new RecordField("mapc", RecordFieldType.MAP.getMapDataType(fieldType))
        ));

        Object deserialized = serDe.deserialize(new ObjectWritable(new MapRecord(schema, new HashMap<String, Object>(){
            {
                put("mapc", fields);
            }
        })));
        List<Object> desFields = (List<Object>)deserialized;
        assertEquals(1, desFields.size());
        Map<Object,Object> map = (Map<Object, Object>)desFields.get(0);
        for(Map.Entry<Object, Object> entry: expected.entrySet()){
            assertEquals(entry.getValue(), map.get(entry.getKey()));
        }
    }

    @Test
    public void testStructMap() throws SerDeException{
        NiFiRecordSerDe serDe = createSerDe(
                "mapc",
                "map<string,struct<id:int,balance:decimal(18,2)>>"
        );
        RecordSchema recordSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("balance", RecordFieldType.DOUBLE.getDataType())
        ));
        RecordSchema schema = new SimpleRecordSchema(Collections.singletonList(
                new RecordField("mapc", RecordFieldType.MAP.getMapDataType(RecordFieldType.RECORD.getRecordDataType(recordSchema)))
        ));

        HashMap<String, Object> input = new HashMap<String, Object>() {{
            put("mapc", new HashMap<String, Object>() {{
                put("current", new MapRecord(recordSchema, new HashMap<String, Object>() {{
                    put("id", 1);
                    put("balance", 56.9);
                }}));
                put("savings", new MapRecord(recordSchema, new HashMap<String, Object>() {{
                    put("id", 2);
                    put("balance", 104.65);
                }}));
            }});
        }};

        Object expected = Collections.singletonList(
                new HashMap<String, Object>() {{
                    put("current", Arrays.asList(1, HiveDecimal.create(56.9)));
                    put("savings", Arrays.asList(2, HiveDecimal.create(104.65)));
                }}
        );

        Object deserialized = serDe.deserialize(new ObjectWritable(new MapRecord(schema, input)));

        assertEquals(expected, deserialized);
    }

    NiFiRecordSerDe createSerDe(String columnNames, String typeInfo) throws SerDeException{
        Properties props = new Properties();
        props.setProperty(serdeConstants.LIST_COLUMNS, columnNames);
        props.setProperty(serdeConstants.LIST_COLUMN_TYPES, typeInfo);
        NiFiRecordSerDe serDe = new NiFiRecordSerDe(null, new MockComponentLog("logger", new Object())); //reader isn't used
        serDe.initialize(null, props); //conf isn't used
        return  serDe;
    }
}