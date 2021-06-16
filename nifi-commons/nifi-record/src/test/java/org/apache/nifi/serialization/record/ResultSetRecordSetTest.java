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
import org.apache.nifi.serialization.record.type.DecimalDataType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ResultSetRecordSetTest {

    private static final String COLUMN_NAME_VARCHAR = "varchar";
    private static final String COLUMN_NAME_BIGINT = "bigint";
    private static final String COLUMN_NAME_ROWID = "rowid";
    private static final String COLUMN_NAME_BIT = "bit";
    private static final String COLUMN_NAME_BOOLEAN = "boolean";
    private static final String COLUMN_NAME_CHAR = "char";
    private static final String COLUMN_NAME_DATE = "date";
    private static final String COLUMN_NAME_INTEGER = "integer";
    private static final String COLUMN_NAME_DOUBLE = "double";
    private static final String COLUMN_NAME_REAL = "real";
    private static final String COLUMN_NAME_FLOAT = "float";
    private static final String COLUMN_NAME_SMALLINT = "smallint";
    private static final String COLUMN_NAME_TINYINT = "tinyint";
    private static final String COLUMN_NAME_BIG_DECIMAL_1 = "bigDecimal1";
    private static final String COLUMN_NAME_BIG_DECIMAL_2 = "bigDecimal2";
    private static final String COLUMN_NAME_BIG_DECIMAL_3 = "bigDecimal3";
    private static final String COLUMN_NAME_BIG_DECIMAL_4 = "bigDecimal4";
    private static final String COLUMN_NAME_BIG_DECIMAL_5 = "bigDecimal5";

    private static final Object[][] COLUMNS = new Object[][] {
            // column number; column label / name / schema field; column type; schema data type;
            {1, COLUMN_NAME_VARCHAR, Types.VARCHAR, RecordFieldType.STRING.getDataType()},
            {2, COLUMN_NAME_BIGINT, Types.BIGINT, RecordFieldType.LONG.getDataType()},
            {3, COLUMN_NAME_ROWID, Types.ROWID, RecordFieldType.LONG.getDataType()},
            {4, COLUMN_NAME_BIT, Types.BIT, RecordFieldType.BOOLEAN.getDataType()},
            {5, COLUMN_NAME_BOOLEAN, Types.BOOLEAN, RecordFieldType.BOOLEAN.getDataType()},
            {6, COLUMN_NAME_CHAR, Types.CHAR, RecordFieldType.CHAR.getDataType()},
            {7, COLUMN_NAME_DATE, Types.DATE, RecordFieldType.DATE.getDataType()},
            {8, COLUMN_NAME_INTEGER, Types.INTEGER, RecordFieldType.INT.getDataType()},
            {9, COLUMN_NAME_DOUBLE, Types.DOUBLE, RecordFieldType.DOUBLE.getDataType()},
            {10, COLUMN_NAME_REAL, Types.REAL, RecordFieldType.DOUBLE.getDataType()},
            {11, COLUMN_NAME_FLOAT, Types.FLOAT, RecordFieldType.FLOAT.getDataType()},
            {12, COLUMN_NAME_SMALLINT, Types.SMALLINT, RecordFieldType.SHORT.getDataType()},
            {13, COLUMN_NAME_TINYINT, Types.TINYINT, RecordFieldType.BYTE.getDataType()},
            {14, COLUMN_NAME_BIG_DECIMAL_1, Types.DECIMAL,RecordFieldType.DECIMAL.getDecimalDataType(7, 3)},
            {15, COLUMN_NAME_BIG_DECIMAL_2, Types.NUMERIC, RecordFieldType.DECIMAL.getDecimalDataType(4, 0)},
            {16, COLUMN_NAME_BIG_DECIMAL_3, Types.JAVA_OBJECT, RecordFieldType.DECIMAL.getDecimalDataType(501, 1)},
            {17, COLUMN_NAME_BIG_DECIMAL_4, Types.DECIMAL, RecordFieldType.DECIMAL.getDecimalDataType(10, 3)},
            {18, COLUMN_NAME_BIG_DECIMAL_5, Types.DECIMAL, RecordFieldType.DECIMAL.getDecimalDataType(3, 10)},
    };

    @Mock
    private ResultSet resultSet;

    @Mock
    private ResultSetMetaData resultSetMetaData;

    @Before
    public void setUp() throws SQLException {
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        when(resultSetMetaData.getColumnCount()).thenReturn(COLUMNS.length);

        for (final Object[] column : COLUMNS) {
            when(resultSetMetaData.getColumnLabel((Integer) column[0])).thenReturn((String) (column[1]));
            when(resultSetMetaData.getColumnName((Integer) column[0])).thenReturn((String) column[1]);
            when(resultSetMetaData.getColumnType((Integer) column[0])).thenReturn((Integer) column[2]);

            if(column[3] instanceof DecimalDataType) {
                DecimalDataType ddt = (DecimalDataType)column[3];
                when(resultSetMetaData.getPrecision((Integer) column[0])).thenReturn(ddt.getPrecision());
                when(resultSetMetaData.getScale((Integer) column[0])).thenReturn(ddt.getScale());
            }
        }

        // Big decimal values are necessary in order to determine precision and scale
        when(resultSet.getBigDecimal(16)).thenReturn(new BigDecimal(String.join("", Collections.nCopies(500, "1")) + ".1"));

        // This will be handled by a dedicated branch for Java Objects, needs some further details
        when(resultSetMetaData.getColumnClassName(16)).thenReturn(BigDecimal.class.getName());
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
        assertEquals(RecordFieldType.DECIMAL.getDecimalDataType(30, 10), resultSchema.getField(0).getDataType());
    }

    @Test
    public void testCreateSchemaWhenOtherTypeWithoutSchema() throws SQLException {
        // given
        final ResultSet resultSet = givenResultSetForOther();

        // when
        final ResultSetRecordSet testSubject = new ResultSetRecordSet(resultSet, null);
        final RecordSchema resultSchema = testSubject.getSchema();

        // then
        assertEquals(RecordFieldType.CHOICE, resultSchema.getField(0).getDataType().getFieldType());
    }

    @Test
    public void testCreateRecord() throws SQLException {
        // given
        final RecordSchema recordSchema = givenRecordSchema();

        LocalDate testDate = LocalDate.of(2021, 1, 26);

        final String varcharValue = "varchar";
        final Long bigintValue = 1234567890123456789L;
        final Long rowidValue = 11111111L;
        final Boolean bitValue = Boolean.FALSE;
        final Boolean booleanValue = Boolean.TRUE;
        final Character charValue = 'c';
        final Date dateValue = Date.valueOf(testDate);
        final Integer integerValue = 1234567890;
        final Double doubleValue = 0.12;
        final Double realValue = 3.45;
        final Float floatValue = 6.78F;
        final Short smallintValue = 12345;
        final Byte tinyintValue = 123;
        final BigDecimal bigDecimal1Value = new BigDecimal("1234.567");
        final BigDecimal bigDecimal2Value = new BigDecimal("1234");
        final BigDecimal bigDecimal3Value = new BigDecimal("1234567890.1");
        final BigDecimal bigDecimal4Value = new BigDecimal("1234567.089");
        final BigDecimal bigDecimal5Value = new BigDecimal("0.1234567");

        when(resultSet.getObject(COLUMN_NAME_VARCHAR)).thenReturn(varcharValue);
        when(resultSet.getObject(COLUMN_NAME_BIGINT)).thenReturn(bigintValue);
        when(resultSet.getObject(COLUMN_NAME_ROWID)).thenReturn(rowidValue);
        when(resultSet.getObject(COLUMN_NAME_BIT)).thenReturn(bitValue);
        when(resultSet.getObject(COLUMN_NAME_BOOLEAN)).thenReturn(booleanValue);
        when(resultSet.getObject(COLUMN_NAME_CHAR)).thenReturn(charValue);
        when(resultSet.getObject(COLUMN_NAME_DATE)).thenReturn(dateValue);
        when(resultSet.getObject(COLUMN_NAME_INTEGER)).thenReturn(integerValue);
        when(resultSet.getObject(COLUMN_NAME_DOUBLE)).thenReturn(doubleValue);
        when(resultSet.getObject(COLUMN_NAME_REAL)).thenReturn(realValue);
        when(resultSet.getObject(COLUMN_NAME_FLOAT)).thenReturn(floatValue);
        when(resultSet.getObject(COLUMN_NAME_SMALLINT)).thenReturn(smallintValue);
        when(resultSet.getObject(COLUMN_NAME_TINYINT)).thenReturn(tinyintValue);
        when(resultSet.getObject(COLUMN_NAME_BIG_DECIMAL_1)).thenReturn(bigDecimal1Value);
        when(resultSet.getObject(COLUMN_NAME_BIG_DECIMAL_2)).thenReturn(bigDecimal2Value);
        when(resultSet.getObject(COLUMN_NAME_BIG_DECIMAL_3)).thenReturn(bigDecimal3Value);
        when(resultSet.getObject(COLUMN_NAME_BIG_DECIMAL_4)).thenReturn(bigDecimal4Value);
        when(resultSet.getObject(COLUMN_NAME_BIG_DECIMAL_5)).thenReturn(bigDecimal5Value);

        // when
        ResultSetRecordSet testSubject = new ResultSetRecordSet(resultSet, recordSchema);
        Record record = testSubject.createRecord(resultSet);

        // then
        assertEquals(varcharValue, record.getAsString(COLUMN_NAME_VARCHAR));
        assertEquals(bigintValue, record.getAsLong(COLUMN_NAME_BIGINT));
        assertEquals(rowidValue, record.getAsLong(COLUMN_NAME_ROWID));
        assertEquals(bitValue, record.getAsBoolean(COLUMN_NAME_BIT));
        assertEquals(booleanValue, record.getAsBoolean(COLUMN_NAME_BOOLEAN));
        assertEquals(charValue, record.getValue(COLUMN_NAME_CHAR));

        // Date is expected in UTC normalized form
        Date expectedDate = new Date(testDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());
        assertEquals(expectedDate, record.getAsDate(COLUMN_NAME_DATE, null));

        assertEquals(integerValue, record.getAsInt(COLUMN_NAME_INTEGER));
        assertEquals(doubleValue, record.getAsDouble(COLUMN_NAME_DOUBLE));
        assertEquals(realValue, record.getAsDouble(COLUMN_NAME_REAL));
        assertEquals(floatValue, record.getAsFloat(COLUMN_NAME_FLOAT));
        assertEquals(smallintValue.shortValue(), record.getAsInt(COLUMN_NAME_SMALLINT).shortValue());
        assertEquals(tinyintValue.byteValue(), record.getAsInt(COLUMN_NAME_TINYINT).byteValue());
        assertEquals(bigDecimal1Value, record.getValue(COLUMN_NAME_BIG_DECIMAL_1));
        assertEquals(bigDecimal2Value, record.getValue(COLUMN_NAME_BIG_DECIMAL_2));
        assertEquals(bigDecimal3Value, record.getValue(COLUMN_NAME_BIG_DECIMAL_3));
        assertEquals(bigDecimal4Value, record.getValue(COLUMN_NAME_BIG_DECIMAL_4));
        assertEquals(bigDecimal5Value, record.getValue(COLUMN_NAME_BIG_DECIMAL_5));
    }

    private ResultSet givenResultSetForOther() throws SQLException {
        final ResultSet resultSet = Mockito.mock(ResultSet.class);
        final ResultSetMetaData resultSetMetaData = Mockito.mock(ResultSetMetaData.class);
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        when(resultSetMetaData.getColumnCount()).thenReturn(1);
        when(resultSetMetaData.getColumnLabel(1)).thenReturn("column");
        when(resultSetMetaData.getColumnName(1)).thenReturn("column");
        when(resultSetMetaData.getColumnType(1)).thenReturn(Types.OTHER);
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
        assertNotNull(resultSchema);

        for (final Object[] column : COLUMNS) {
            // The DECIMAL column with scale larger than precision will not match so verify that instead
            DataType actualDataType = resultSchema.getField((Integer) column[0] - 1).getDataType();
            DataType expectedDataType = (DataType) column[3];
            if (expectedDataType.equals(RecordFieldType.DECIMAL.getDecimalDataType(3, 10))) {
                DecimalDataType decimalDataType = (DecimalDataType) expectedDataType;
                if (decimalDataType.getScale() > decimalDataType.getPrecision()) {
                    expectedDataType = RecordFieldType.DECIMAL.getDecimalDataType(decimalDataType.getScale(), decimalDataType.getScale());
                }
            }
            assertEquals("For column " + column[0] + " the converted type is not matching", expectedDataType, actualDataType);
        }
    }
}