package org.apache.nifi.serialization.record;

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

import com.mockrunner.mock.jdbc.MockResultSet;
import com.mockrunner.mock.jdbc.MockResultSetMetaData;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class TestResultSetRecordSet {

    @Test
    public void testCreateSchemaWithoutReaderSchema() throws Exception {
        List<BigDecimal> array = new ArrayList<>();
        array.add(BigDecimal.valueOf(1.1d));
        array.add(BigDecimal.valueOf(2.2d));

        MockResultSet resultSet = new MockResultSet("TestRecord");

        resultSet.addColumn("A");
        resultSet.addColumn("B");
        resultSet.addColumn("C");
        resultSet.addColumn("D");

        HashMap<String, Object> row = new HashMap<>();

        row.put("A", "Hello World!");
        row.put("B", array.toArray());
        row.put("C", Timestamp.valueOf("2020-01-01 01:02:03.123456"));
        row.put("D", 0.1f);

        resultSet.addRow(row);

        MockResultSetMetaData metadata = new MockResultSetMetaData();

        metadata.setColumnCount(4);

        metadata.setColumnType(1, Types.VARCHAR);
        metadata.setColumnName(1, "A");
        metadata.setNullable(1, 1);

        metadata.setColumnType(2, Types.ARRAY);
        metadata.setColumnName(2, "B");
        metadata.setNullable(2, 1);

        metadata.setColumnType(3, Types.TIMESTAMP);
        metadata.setColumnName(3, "C");
        metadata.setNullable(3, 1);

        metadata.setColumnType(4, Types.FLOAT);
        metadata.setColumnName(4, "D");
        metadata.setNullable(4, 1);

        resultSet.setResultSetMetaData(metadata);

        ResultSetRecordSet resultSetRecordSet = new ResultSetRecordSet(resultSet, null);

        Record record = resultSetRecordSet.next();

        RecordSchema recordSchema = record.getSchema();

        assertEquals(Optional.of(RecordFieldType.STRING.getDataType()), recordSchema.getDataType("A"));
        assertEquals(Optional.ofNullable(RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType())), recordSchema.getDataType("B"));
        assertEquals(Optional.of(RecordFieldType.TIMESTAMP.getDataType()), recordSchema.getDataType("C"));
        assertEquals(Optional.of(RecordFieldType.FLOAT.getDataType()), recordSchema.getDataType("D"));

        assertEquals("Hello World!", record.getAsString("A"));
        assertEquals(BigDecimal.valueOf(1.1d), record.getAsArray("B")[0]);
        assertEquals(Timestamp.valueOf("2020-01-01 01:02:03.123456"), record.getValue("C"));
        assertEquals(0.1f, record.getAsFloat("D"), 0.0f);
    }

    @Test
    public void testCreateSchemaWithGivenReaderSchema() throws Exception {
        List<BigDecimal> array = new ArrayList<>();
        array.add(BigDecimal.valueOf(1.1d));
        array.add(BigDecimal.valueOf(2.2d));

        MockResultSet resultSet = new MockResultSet("TestRecord");

        resultSet.addColumn("A");
        resultSet.addColumn("B");
        resultSet.addColumn("C");
        resultSet.addColumn("D");

        HashMap<String, Object> row = new HashMap<>();

        row.put("A", "Hello World!");
        row.put("B", array.toArray());
        row.put("C", Timestamp.valueOf("2020-01-01 01:02:03.123456"));
        row.put("D", 0.1f);

        resultSet.addRow(row);

        MockResultSetMetaData metadata = new MockResultSetMetaData();

        metadata.setColumnCount(4);

        metadata.setColumnType(1, Types.VARCHAR);
        metadata.setColumnName(1, "A");
        metadata.setNullable(1, 1);

        metadata.setColumnType(2, Types.ARRAY);
        metadata.setColumnName(2, "B");
        metadata.setNullable(2, 1);

        metadata.setColumnType(3, Types.TIMESTAMP);
        metadata.setColumnName(3, "C");
        metadata.setNullable(3, 1);

        metadata.setColumnType(4, Types.FLOAT);
        metadata.setColumnName(4, "D");
        metadata.setNullable(4, 1);

        resultSet.setResultSetMetaData(metadata);


        List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("A", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("B", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.DOUBLE.getDataType())));
        fields.add(new RecordField("C", RecordFieldType.TIMESTAMP.getDataType()));
        fields.add(new RecordField("D", RecordFieldType.FLOAT.getDataType()));

        RecordSchema schema = new SimpleRecordSchema(fields);


        ResultSetRecordSet resultSetRecordSet = new ResultSetRecordSet(resultSet, schema);

        Record record = resultSetRecordSet.next();

        RecordSchema recordSchema = record.getSchema();

        assertEquals(schema.getDataType("A"), recordSchema.getDataType("A"));
        assertEquals(schema.getDataType("B"), recordSchema.getDataType("B"));
        assertEquals(schema.getDataType("C"), recordSchema.getDataType("C"));
        assertEquals(schema.getDataType("D"), recordSchema.getDataType("D"));

        assertEquals("Hello World!", record.getAsString("A"));
        assertEquals(BigDecimal.valueOf(1.1d), record.getAsArray("B")[0]);
        assertEquals(Timestamp.valueOf("2020-01-01 01:02:03.123456"), record.getValue("C"));
        assertEquals(0.1f, record.getAsFloat("D"), 0.0f);
    }
}
