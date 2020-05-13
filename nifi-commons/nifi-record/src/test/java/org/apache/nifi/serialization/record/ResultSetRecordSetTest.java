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
package org.apache.nifi.serialization.record;

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class ResultSetRecordSetTest {
    private static final Object[][] COLUMNS = new Object[][] {
            // column number; column label / name / schema field; column type; schema data type;
            {1, "varchar", Types.VARCHAR, RecordFieldType.STRING.getDataType()},
            {2, "bigint", Types.BIGINT, RecordFieldType.LONG.getDataType()},
            {3, "rowid", Types.ROWID, RecordFieldType.LONG.getDataType()},
            {4, "bit", Types.BIT, RecordFieldType.BOOLEAN.getDataType()},
            {5, "boolean", Types.BOOLEAN, RecordFieldType.BOOLEAN.getDataType()},
            {6, "char", Types.CHAR, RecordFieldType.CHAR.getDataType()},
            {7, "date", Types.DATE, RecordFieldType.DATE.getDataType()},
            {8, "integer", Types.INTEGER, RecordFieldType.INT.getDataType()},
            {9, "double", Types.DOUBLE, RecordFieldType.DOUBLE.getDataType()},
            {10, "real", Types.REAL, RecordFieldType.DOUBLE.getDataType()},
            {11, "float", Types.FLOAT, RecordFieldType.FLOAT.getDataType()},
            {12, "smallint", Types.SMALLINT, RecordFieldType.SHORT.getDataType()},
            {13, "tinyint", Types.TINYINT, RecordFieldType.BYTE.getDataType()},
            {14, "bigDecimal1", Types.DECIMAL,RecordFieldType.DECIMAL.getDecimalDataType(7, 3)},
            {15, "bigDecimal2", Types.NUMERIC, RecordFieldType.DECIMAL.getDecimalDataType(4, 0)},
            {16, "bigDecimal3", Types.JAVA_OBJECT, RecordFieldType.DECIMAL.getDecimalDataType(501, 1)},
    };

    @Mock
    private ResultSet resultSet;

    @Mock
    private ResultSetMetaData resultSetMetaData;

    @Before
    public void setUp() throws SQLException {
        Mockito.when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        Mockito.when(resultSetMetaData.getColumnCount()).thenReturn(COLUMNS.length);

        for (final Object[] column : COLUMNS) {
            Mockito.when(resultSetMetaData.getColumnLabel((Integer) column[0])).thenReturn((column[1]) + "Col");
            Mockito.when(resultSetMetaData.getColumnName((Integer) column[0])).thenReturn((String) column[1]);
            Mockito.when(resultSetMetaData.getColumnType((Integer) column[0])).thenReturn((Integer) column[2]);
        }

        // Big decimal values are necessary in order to determine precision and scale
        Mockito.when(resultSet.getBigDecimal(14)).thenReturn(BigDecimal.valueOf(1234.567D));
        Mockito.when(resultSet.getBigDecimal(15)).thenReturn(BigDecimal.valueOf(1234L));
        Mockito.when(resultSet.getBigDecimal(16)).thenReturn(new BigDecimal(String.join("", Collections.nCopies(500, "1")) + ".1"));

        // This will be handled by a dedicated branch for Java Objects, needs some further details
        Mockito.when(resultSetMetaData.getColumnClassName(16)).thenReturn(BigDecimal.class.getName());
    }

    @Test
    public void testCreateSchema() throws SQLException {
        // given
        final RecordSchema recordSchema = givenRecordSchema();

        // when
        final ResultSetRecordSet testSubject = new ResultSetRecordSet(resultSet, recordSchema);
        final RecordSchema resultSchema = testSubject.getSchema();

        // then
        thenAllColumnDataTypesAreCorrect(resultSchema);
    }

    @Test
    public void testCreateSchemaWhenNoRecordSchema() throws SQLException {
        // when
        final ResultSetRecordSet testSubject = new ResultSetRecordSet(resultSet, null);
        final RecordSchema resultSchema = testSubject.getSchema();

        // then
        thenAllColumnDataTypesAreCorrect(resultSchema);
    }

    @Test
    public void testCreateSchemaWhenOtherType() throws SQLException {
        // given
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("column", RecordFieldType.DECIMAL.getDecimalDataType(30, 10)));
        final RecordSchema recordSchema = new SimpleRecordSchema(fields);
        final ResultSet resultSet = givenResultSetForOther();

        // when
        final ResultSetRecordSet testSubject = new ResultSetRecordSet(resultSet, recordSchema);
        final RecordSchema resultSchema = testSubject.getSchema();

        // then
        Assert.assertEquals(RecordFieldType.DECIMAL.getDecimalDataType(30, 10), resultSchema.getField(0).getDataType());
    }

    @Test
    public void testCreateSchemaWhenOtherTypeWithoutSchema() throws SQLException {
        // given
        final ResultSet resultSet = givenResultSetForOther();

        // when
        final ResultSetRecordSet testSubject = new ResultSetRecordSet(resultSet, null);
        final RecordSchema resultSchema = testSubject.getSchema();

        // then
        Assert.assertEquals(RecordFieldType.CHOICE, resultSchema.getField(0).getDataType().getFieldType());
    }

    private ResultSet givenResultSetForOther() throws SQLException {
        final ResultSet resultSet = Mockito.mock(ResultSet.class);
        final ResultSetMetaData resultSetMetaData = Mockito.mock(ResultSetMetaData.class);
        Mockito.when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        Mockito.when(resultSetMetaData.getColumnCount()).thenReturn(1);
        Mockito.when(resultSetMetaData.getColumnLabel(1)).thenReturn("column");
        Mockito.when(resultSetMetaData.getColumnName(1)).thenReturn("column");
        Mockito.when(resultSetMetaData.getColumnType(1)).thenReturn(Types.OTHER);
        return resultSet;
    }

    private RecordSchema givenRecordSchema() {
        final List<RecordField> fields = new ArrayList<>();

        for (final Object[] column : COLUMNS) {
            fields.add(new RecordField((String) column[1], (DataType) column[3]));
        }

        return new SimpleRecordSchema(fields);
    }

    private void thenAllColumnDataTypesAreCorrect(final RecordSchema resultSchema) {
        Assert.assertNotNull(resultSchema);

        for (final Object[] column : COLUMNS) {
            Assert.assertEquals("For column " + column[0] + " the converted type is not matching", column[3], resultSchema.getField((Integer) column[0] - 1).getDataType());
        }
    }
}