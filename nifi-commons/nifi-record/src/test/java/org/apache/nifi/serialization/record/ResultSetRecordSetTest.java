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
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.DecimalDataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class ResultSetRecordSetTest {

    private static final String COLUMN_NAME_VARCHAR = "varchar";
    private static final String COLUMN_NAME_BIGINT = "bigint";
    private static final String COLUMN_NAME_ROWID = "rowid";
    private static final String COLUMN_NAME_BIT = "bit";
    private static final String COLUMN_NAME_BOOLEAN = "boolean";
    private static final String COLUMN_NAME_CHAR = "char";
    private static final String COLUMN_NAME_DATE = "date";
    private static final String COLUMN_NAME_TIMESTAMP = "timestamp";
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

    private static final long TIMESTAMP_IN_MILLIS = 1631809132516L;

    private static final TestColumn[] COLUMNS = new TestColumn[] {
            new TestColumn(1, COLUMN_NAME_VARCHAR, Types.VARCHAR, RecordFieldType.STRING.getDataType()),
            new TestColumn(2, COLUMN_NAME_BIGINT, Types.BIGINT, RecordFieldType.LONG.getDataType()),
            new TestColumn(3, COLUMN_NAME_ROWID, Types.ROWID, RecordFieldType.LONG.getDataType()),
            new TestColumn(4, COLUMN_NAME_BIT, Types.BIT, RecordFieldType.BOOLEAN.getDataType()),
            new TestColumn(5, COLUMN_NAME_BOOLEAN, Types.BOOLEAN, RecordFieldType.BOOLEAN.getDataType()),
            new TestColumn(6, COLUMN_NAME_CHAR, Types.CHAR, RecordFieldType.STRING.getDataType()),
            new TestColumn(7, COLUMN_NAME_DATE, Types.DATE, RecordFieldType.DATE.getDataType()),
            new TestColumn(8, COLUMN_NAME_INTEGER, Types.INTEGER, RecordFieldType.INT.getDataType()),
            new TestColumn(9, COLUMN_NAME_DOUBLE, Types.DOUBLE, RecordFieldType.DOUBLE.getDataType()),
            new TestColumn(10, COLUMN_NAME_REAL, Types.REAL, RecordFieldType.DOUBLE.getDataType()),
            new TestColumn(11, COLUMN_NAME_FLOAT, Types.FLOAT, RecordFieldType.FLOAT.getDataType()),
            new TestColumn(12, COLUMN_NAME_SMALLINT, Types.SMALLINT, RecordFieldType.SHORT.getDataType()),
            new TestColumn(13, COLUMN_NAME_TINYINT, Types.TINYINT, RecordFieldType.BYTE.getDataType()),
            new TestColumn(14, COLUMN_NAME_BIG_DECIMAL_1, Types.DECIMAL,RecordFieldType.DECIMAL.getDecimalDataType(7, 3)),
            new TestColumn(15, COLUMN_NAME_BIG_DECIMAL_2, Types.NUMERIC, RecordFieldType.DECIMAL.getDecimalDataType(4, 0)),
            new TestColumn(16, COLUMN_NAME_BIG_DECIMAL_3, Types.JAVA_OBJECT, RecordFieldType.DECIMAL.getDecimalDataType(501, 1)),
            new TestColumn(17, COLUMN_NAME_BIG_DECIMAL_4, Types.DECIMAL, RecordFieldType.DECIMAL.getDecimalDataType(10, 3)),
            new TestColumn(18, COLUMN_NAME_BIG_DECIMAL_5, Types.DECIMAL, RecordFieldType.DECIMAL.getDecimalDataType(3, 10)),
            new TestColumn(19, COLUMN_NAME_TIMESTAMP, Types.TIMESTAMP, RecordFieldType.TIMESTAMP.getDataType())
    };

    @Mock
    private ResultSet resultSet;

    @Mock
    private ResultSetMetaData resultSetMetaData;

    @BeforeEach
    public void setUp() throws SQLException {
        setUpMocks(COLUMNS, resultSetMetaData, resultSet);
    }

    @Test
    public void testCreateSchema() throws SQLException {
        // given
        final RecordSchema recordSchema = givenRecordSchema(COLUMNS);
        final RecordSchema expectedSchema = givenRecordSchema(COLUMNS);

        // when
        final ResultSetRecordSet testSubject = new ResultSetRecordSet(resultSet, recordSchema);
        final RecordSchema actualSchema = testSubject.getSchema();

        // then
        thenAllColumnDataTypesAreCorrect(COLUMNS, expectedSchema, actualSchema);
    }

    @Test
    public void testCreateSchemaWhenScaleIsNonDefault() throws SQLException {
        // given
        final RecordSchema recordSchema = givenRecordSchema(COLUMNS);
        final RecordSchema expectedSchema = givenRecordSchema(COLUMNS);

        // when
        final ResultSetRecordSet testSubject = new ResultSetRecordSet(resultSet, recordSchema, 10, 2);
        final RecordSchema actualSchema = testSubject.getSchema();

        // then
        thenAllColumnDataTypesAreCorrect(COLUMNS, expectedSchema, actualSchema);
    }

    @Test
    public void testCreateSchemaWhenNoRecordSchema() throws SQLException {
        // given
        final RecordSchema expectedSchema = givenRecordSchema(COLUMNS);

        // when
        final ResultSetRecordSet testSubject = new ResultSetRecordSet(resultSet, null);
        final RecordSchema actualSchema = testSubject.getSchema();

        // then
        thenAllColumnDataTypesAreCorrect(COLUMNS, expectedSchema, actualSchema);
    }

    @Test
    public void testCreateSchemaWhenOtherType() throws SQLException {
        // given
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("column", RecordFieldType.DECIMAL.getDecimalDataType(30, 10)));
        final RecordSchema recordSchema = new SimpleRecordSchema(fields);
        final ResultSet resultSet = givenResultSetForOther(fields);

        // when
        final ResultSetRecordSet testSubject = new ResultSetRecordSet(resultSet, recordSchema);
        final RecordSchema resultSchema = testSubject.getSchema();

        // then
        assertEquals(RecordFieldType.DECIMAL.getDecimalDataType(30, 10), resultSchema.getField(0).getDataType());
    }

    @Test
    public void testCreateSchemaWhenOtherTypeUsingLogicalTypes() throws SQLException {
        // given
        final List<RecordField> fields = givenFieldsThatRequireLogicalTypes();
        final RecordSchema recordSchema = new SimpleRecordSchema(fields);
        final ResultSet resultSet = givenResultSetForOther(fields);

        // when
        final ResultSetRecordSet testSubject = new ResultSetRecordSet(resultSet, recordSchema, 10, 0, true);
        final RecordSchema resultSchema = testSubject.getSchema();

        // then
        thenAllDataTypesMatchInputFieldType(fields, resultSchema);
    }

    @Test
    public void testCreateSchemaWhenOtherTypeAndNoLogicalTypes() throws SQLException {
        // given
        final List<RecordField> fields = givenFieldsThatRequireLogicalTypes();
        final RecordSchema recordSchema = new SimpleRecordSchema(fields);
        final ResultSet resultSet = givenResultSetForOther(fields);

        // when
        final ResultSetRecordSet testSubject = new ResultSetRecordSet(resultSet, recordSchema, 10, 0, false);
        final RecordSchema resultSchema = testSubject.getSchema();

        // then
        thenAllDataTypesAreString(resultSchema);
    }

    @Test
    public void testCreateSchemaWhenOtherTypeUsingLogicalTypesNoSchema() throws SQLException {
        // given
        final List<RecordField> fields = givenFieldsThatRequireLogicalTypes();
        final ResultSet resultSet = givenResultSetForOther(fields);

        // when
        final ResultSetRecordSet testSubject = new ResultSetRecordSet(resultSet, null, 10, 0, true);
        final RecordSchema resultSchema = testSubject.getSchema();

        // then
        thenAllDataTypesAreChoice(fields, resultSchema);
    }

    @Test
    public void testCreateSchemaWhenOtherTypeAndNoLogicalTypesNoSchema() throws SQLException {
        // given
        final List<RecordField> fields = givenFieldsThatRequireLogicalTypes();
        final ResultSet resultSet = givenResultSetForOther(fields);

        // when
        final ResultSetRecordSet testSubject = new ResultSetRecordSet(resultSet, null, 10, 0, false);
        final RecordSchema resultSchema = testSubject.getSchema();

        // then
        thenAllDataTypesAreString(resultSchema);
    }

    @Test
    public void testCreateSchemaWhenOtherTypeUsingLogicalTypesWithRecord() throws SQLException {
        // given
        final Record inputRecord = givenInputRecord(); // The field's type is going to be RECORD (there is a record within a record)
        final List<RecordField> fields = givenFieldsThatAreOfTypeRecord(Arrays.asList(inputRecord));
        final ResultSet resultSet = givenResultSetForOther(fields);

        when(resultSet.getObject(1)).thenReturn(inputRecord);

        // when
        final ResultSetRecordSet testSubject = new ResultSetRecordSet(resultSet, null, 10, 0, true);
        final RecordSchema resultSchema = testSubject.getSchema();

        // then
        thenAllDataTypesMatchInputFieldType(fields, resultSchema);
    }

    @Test
    public void testCreateSchemaWhenOtherTypeWithoutSchema() throws SQLException {
        // given
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("column", RecordFieldType.STRING.getDataType()));
        final ResultSet resultSet = givenResultSetForOther(fields);

        // when
        final ResultSetRecordSet testSubject = new ResultSetRecordSet(resultSet, null);
        final RecordSchema resultSchema = testSubject.getSchema();

        // then
        assertEquals(RecordFieldType.CHOICE, resultSchema.getField(0).getDataType().getFieldType());
    }

    @Test
    public void testCreateRecord() throws SQLException {
        final RecordSchema recordSchema = givenRecordSchema(COLUMNS);

        LocalDate testDate = LocalDate.of(2021, 1, 26);
        LocalDateTime testDateTime = LocalDateTime.of(2021, 9, 10, 11, 11, 11);

        final String varcharValue = "varchar";
        final Long bigintValue = 1234567890123456789L;
        final Long rowidValue = 11111111L;
        final Boolean bitValue = Boolean.FALSE;
        final Boolean booleanValue = Boolean.TRUE;
        final Character charValue = 'c';
        final Timestamp timestampValue = Timestamp.valueOf(testDateTime);
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
        when(resultSet.getObject(COLUMN_NAME_DATE)).thenReturn(testDate);
        when(resultSet.getTimestamp(COLUMN_NAME_TIMESTAMP)).thenReturn(timestampValue);
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

        ResultSetRecordSet testSubject = new ResultSetRecordSet(resultSet, recordSchema);
        Record record = testSubject.createRecord(resultSet);

        assertEquals(varcharValue, record.getAsString(COLUMN_NAME_VARCHAR));
        assertEquals(bigintValue, record.getAsLong(COLUMN_NAME_BIGINT));
        assertEquals(rowidValue, record.getAsLong(COLUMN_NAME_ROWID));
        assertEquals(bitValue, record.getAsBoolean(COLUMN_NAME_BIT));
        assertEquals(booleanValue, record.getAsBoolean(COLUMN_NAME_BOOLEAN));
        assertEquals(charValue, record.getValue(COLUMN_NAME_CHAR));

        assertEquals(testDate, record.getAsLocalDate(COLUMN_NAME_DATE, null));
        final Object timestampObject = record.getValue(COLUMN_NAME_TIMESTAMP);
        assertEquals(timestampValue, timestampObject);

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

    @Test
    public void testCreateSchemaArrayThrowsException() throws SQLException {
        // given
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("column", RecordFieldType.DECIMAL.getDecimalDataType(30, 10)));
        final RecordSchema recordSchema = new SimpleRecordSchema(fields);
        final ResultSet resultSet = givenResultSetForArrayThrowsException(true);

        // when
        assertThrows(SQLException.class, () -> new ResultSetRecordSet(resultSet, recordSchema));
    }

    @Test
    public void testCreateSchemaThrowsExceptionSchemaCreationStillCalledConsideringLogicalTypeFlag() throws SQLException {
        // given
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("column", RecordFieldType.DECIMAL.getDecimalDataType(30, 10)));
        final RecordSchema recordSchema = new SimpleRecordSchema(fields);

        final ResultSet resultSet = Mockito.mock(ResultSet.class);
        final ResultSetMetaData resultSetMetaData = Mockito.mock(ResultSetMetaData.class);
        when(resultSet.getMetaData()).thenThrow(new SQLException("test exception")).thenReturn(resultSetMetaData);
        when(resultSetMetaData.getColumnCount()).thenReturn(1);
        when(resultSetMetaData.getColumnLabel(1)).thenReturn("column");
        when(resultSetMetaData.getColumnType(1)).thenReturn(Types.DECIMAL);

        // when
        ResultSetRecordSet testSubject = new ResultSetRecordSet(resultSet, recordSchema, 10,0, false);
        final RecordSchema resultSchema = testSubject.getSchema();

        // then
        thenAllDataTypesAreString(resultSchema);
    }

    @Test
    public void testCreateSchemaArrayThrowsNotSupportedException() throws SQLException {
        // given
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("column", RecordFieldType.DECIMAL.getDecimalDataType(30, 10)));
        final RecordSchema recordSchema = new SimpleRecordSchema(fields);
        final ResultSet resultSet = givenResultSetForArrayThrowsException(false);

        // when
        final ResultSetRecordSet testSubject = new ResultSetRecordSet(resultSet, recordSchema);
        final RecordSchema resultSchema = testSubject.getSchema();

        // then
        assertEquals(RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType()), resultSchema.getField(0).getDataType());
    }

    @Test
    public void testArrayTypeWithLogicalTypes() throws SQLException {
        testArrayType(true);
    }

    @Test
    public void testArrayTypeNoLogicalTypes() throws SQLException {
        testArrayType(false);
    }

    @Test
    public void testCreateSchemaWithLogicalTypes() throws SQLException {
        testCreateSchemaLogicalTypes(true, true);
    }

    @Test
    public void testCreateSchemaNoLogicalTypes() throws SQLException {
        testCreateSchemaLogicalTypes(false, true);
    }

    @Test
    public void testCreateSchemaWithLogicalTypesNoInputSchema() throws SQLException {
        testCreateSchemaLogicalTypes(true, false);
    }

    @Test
    public void testCreateSchemaNoLogicalTypesNoInputSchema() throws SQLException {
        testCreateSchemaLogicalTypes(false, false);
    }

    private void testArrayType(boolean useLogicalTypes) throws SQLException {
        // GIVEN
        List<ArrayTestData> testData = givenArrayTypesThatRequireLogicalTypes();
        Map<String, DataType> expectedTypes = givenExpectedTypesForArrayTypesThatRequireLogicalTypes(useLogicalTypes);

        // WHEN
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        ResultSetMetaData resultSetMetaData = Mockito.mock(ResultSetMetaData.class);
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        when(resultSetMetaData.getColumnCount()).thenReturn(testData.size());

        List<RecordField> fields = whenSchemaFieldsAreSetupForArrayType(testData, resultSet, resultSetMetaData);
        RecordSchema recordSchema = new SimpleRecordSchema(fields);

        ResultSetRecordSet testSubject = new ResultSetRecordSet(resultSet, recordSchema, 10,0, useLogicalTypes);
        RecordSchema actualSchema = testSubject.getSchema();

        // THEN
        thenActualArrayElementTypesMatchExpected(expectedTypes, actualSchema);
    }

    private void testCreateSchemaLogicalTypes(boolean useLogicalTypes, boolean provideInputSchema) throws SQLException {
        // GIVEN
        TestColumn[] columns = new TestColumn[]{
                new TestColumn(1, COLUMN_NAME_DATE, Types.DATE, RecordFieldType.DATE.getDataType()),
                new TestColumn(2, "time", Types.TIME, RecordFieldType.TIME.getDataType()),
                new TestColumn(3, "time_with_timezone", Types.TIME_WITH_TIMEZONE, RecordFieldType.TIME.getDataType()),
                new TestColumn(4, "timestamp", Types.TIMESTAMP, RecordFieldType.TIMESTAMP.getDataType()),
                new TestColumn(5, "timestamp_with_timezone", Types.TIMESTAMP_WITH_TIMEZONE, RecordFieldType.TIMESTAMP.getDataType()),
                new TestColumn(6, COLUMN_NAME_BIG_DECIMAL_1, Types.DECIMAL,RecordFieldType.DECIMAL.getDecimalDataType(7, 3)),
                new TestColumn(7, COLUMN_NAME_BIG_DECIMAL_2, Types.NUMERIC, RecordFieldType.DECIMAL.getDecimalDataType(4, 0)),
                new TestColumn(8, COLUMN_NAME_BIG_DECIMAL_3, Types.JAVA_OBJECT, RecordFieldType.DECIMAL.getDecimalDataType(501, 1)),
                new TestColumn(9, COLUMN_NAME_BIG_DECIMAL_4, Types.DECIMAL, RecordFieldType.DECIMAL.getDecimalDataType(10, 3)),
                new TestColumn(10, COLUMN_NAME_BIG_DECIMAL_5, Types.DECIMAL, RecordFieldType.DECIMAL.getDecimalDataType(3, 10)),
        };
        final RecordSchema recordSchema = provideInputSchema ? givenRecordSchema(columns) : null;

        ResultSetMetaData resultSetMetaData = Mockito.mock(ResultSetMetaData.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);

        RecordSchema expectedSchema = useLogicalTypes ? givenRecordSchema(columns) : givenRecordSchemaWithOnlyStringType(columns);

        // WHEN
        setUpMocks(columns, resultSetMetaData, resultSet);

        ResultSetRecordSet testSubject = new ResultSetRecordSet(resultSet, recordSchema, 10,0, useLogicalTypes);
        RecordSchema actualSchema = testSubject.getSchema();

        // THEN
        thenAllColumnDataTypesAreCorrect(columns, expectedSchema, actualSchema);
    }

    private void setUpMocks(TestColumn[] columns, ResultSetMetaData resultSetMetaData, ResultSet resultSet) throws SQLException {
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        when(resultSetMetaData.getColumnCount()).thenReturn(columns.length);

        int indexOfBigDecimal = -1;
        int index = 0;
        for (final TestColumn column : columns) {
            when(resultSetMetaData.getColumnLabel(column.getIndex())).thenReturn(column.getColumnName());
            when(resultSetMetaData.getColumnName(column.getIndex())).thenReturn(column.getColumnName());
            when(resultSetMetaData.getColumnType(column.getIndex())).thenReturn(column.getSqlType());

            if (column.getRecordFieldType() instanceof DecimalDataType) {
                DecimalDataType ddt = (DecimalDataType) column.getRecordFieldType();
                when(resultSetMetaData.getPrecision(column.getIndex())).thenReturn(ddt.getPrecision());
                when(resultSetMetaData.getScale(column.getIndex())).thenReturn(ddt.getScale());
            }
            if (column.getSqlType() == Types.JAVA_OBJECT) {
                indexOfBigDecimal = index + 1;
            }
            ++index;
        }

        // Big decimal values are necessary in order to determine precision and scale
        when(resultSet.getBigDecimal(indexOfBigDecimal)).thenReturn(new BigDecimal(String.join("", Collections.nCopies(500, "1")) + ".1"));

        // This will be handled by a dedicated branch for Java Objects, needs some further details
        when(resultSetMetaData.getColumnClassName(indexOfBigDecimal)).thenReturn(BigDecimal.class.getName());
    }

    private List<RecordField> givenFieldsThatRequireLogicalTypes() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("decimal", RecordFieldType.DECIMAL.getDecimalDataType(30, 10)));
        fields.add(new RecordField("date", RecordFieldType.DATE.getDataType()));
        fields.add(new RecordField("time", RecordFieldType.TIME.getDataType()));
        fields.add(new RecordField("timestamp", RecordFieldType.TIMESTAMP.getDataType()));
        return fields;
    }

    private RecordSchema givenRecordSchema(TestColumn[] columns) {
        final List<RecordField> fields = new ArrayList<>(columns.length);

        for (TestColumn column : columns) {
            fields.add(new RecordField(column.getColumnName(), column.getRecordFieldType()));
        }

        return new SimpleRecordSchema(fields);
    }

    private RecordSchema givenRecordSchemaWithOnlyStringType(TestColumn[] columns) {
        final List<RecordField> fields = new ArrayList<>(columns.length);

        for (TestColumn column : columns) {
            fields.add(new RecordField(column.getColumnName(), RecordFieldType.STRING.getDataType()));
        }

        return new SimpleRecordSchema(fields);
    }

    private List<ArrayTestData> givenArrayTypesThatRequireLogicalTypes() {
        List<ArrayTestData> testData = new ArrayList<>();
        testData.add(new ArrayTestData("arrayBigDecimal",
                new ResultBigDecimal[]{new ResultBigDecimal(), new ResultBigDecimal()}));
        testData.add(new ArrayTestData("arrayDate",
                new Date[]{new Date(TIMESTAMP_IN_MILLIS), new Date(TIMESTAMP_IN_MILLIS)}));
        testData.add(new ArrayTestData("arrayTime",
                new Time[]{new Time(TIMESTAMP_IN_MILLIS), new Time(TIMESTAMP_IN_MILLIS)}));
        testData.add(new ArrayTestData("arrayTimestamp",
                new Timestamp[]{new Timestamp(TIMESTAMP_IN_MILLIS), new Timestamp(TIMESTAMP_IN_MILLIS)}));
        return testData;
    }

    private Map<String, DataType> givenExpectedTypesForArrayTypesThatRequireLogicalTypes(final boolean useLogicalTypes) {
        Map<String, DataType> expectedTypes = new HashMap<>();
        if (useLogicalTypes) {
            expectedTypes.put("arrayBigDecimal", RecordFieldType.DECIMAL.getDecimalDataType(ResultBigDecimal.PRECISION, ResultBigDecimal.SCALE));
            expectedTypes.put("arrayDate", RecordFieldType.DATE.getDataType());
            expectedTypes.put("arrayTime", RecordFieldType.TIME.getDataType());
            expectedTypes.put("arrayTimestamp", RecordFieldType.TIMESTAMP.getDataType());
        } else {
            expectedTypes.put("arrayBigDecimal", RecordFieldType.STRING.getDataType());
            expectedTypes.put("arrayDate", RecordFieldType.STRING.getDataType());
            expectedTypes.put("arrayTime", RecordFieldType.STRING.getDataType());
            expectedTypes.put("arrayTimestamp", RecordFieldType.STRING.getDataType());
        }
        return expectedTypes;
    }

    private ResultSet givenResultSetForArrayThrowsException(boolean featureSupported) throws SQLException {
        final ResultSet resultSet = Mockito.mock(ResultSet.class);
        final ResultSetMetaData resultSetMetaData = Mockito.mock(ResultSetMetaData.class);
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        when(resultSet.getArray(ArgumentMatchers.anyInt())).thenThrow(featureSupported ? new SQLException("test exception") : new SQLFeatureNotSupportedException("not supported"));
        when(resultSetMetaData.getColumnCount()).thenReturn(1);
        when(resultSetMetaData.getColumnLabel(1)).thenReturn("column");
        when(resultSetMetaData.getColumnType(1)).thenReturn(Types.ARRAY);
        return resultSet;
    }

    private ResultSet givenResultSetForOther(List<RecordField> fields) throws SQLException {
        final ResultSet resultSet = Mockito.mock(ResultSet.class);
        final ResultSetMetaData resultSetMetaData = Mockito.mock(ResultSetMetaData.class);
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        when(resultSetMetaData.getColumnCount()).thenReturn(fields.size());
        for (int i = 0; i < fields.size(); ++i) {
            int columnIndex = i + 1;
            when(resultSetMetaData.getColumnLabel(columnIndex)).thenReturn(fields.get(i).getFieldName());
            when(resultSetMetaData.getColumnName(columnIndex)).thenReturn(fields.get(i).getFieldName());
            when(resultSetMetaData.getColumnType(columnIndex)).thenReturn(Types.OTHER);
        }
        return resultSet;
    }

    private Record givenInputRecord() {
        List<RecordField> inputRecordFields = new ArrayList<>(2);
        inputRecordFields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        inputRecordFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        RecordSchema inputRecordSchema = new SimpleRecordSchema(inputRecordFields);

        Map<String, Object> inputRecordData = new HashMap<>(2);
        inputRecordData.put("id", 1);
        inputRecordData.put("name", "John");

        return new MapRecord(inputRecordSchema, inputRecordData);
    }

    private List<RecordField> givenFieldsThatAreOfTypeRecord(List<Record> concreteRecords) {
        List<RecordField> fields = new ArrayList<>(concreteRecords.size());
        int i = 1;
        for (Record record : concreteRecords) {
            fields.add(new RecordField("record" + i, RecordFieldType.RECORD.getRecordDataType(record.getSchema())));
            ++i;
        }
        return fields;
    }

    private List<RecordField> whenSchemaFieldsAreSetupForArrayType(final List<ArrayTestData> testData,
                                                                   final ResultSet resultSet,
                                                                   final ResultSetMetaData resultSetMetaData)
            throws SQLException {
        List<RecordField> fields = new ArrayList<>();
        for (int i = 0; i < testData.size(); ++i) {
            ArrayTestData testDatum = testData.get(i);
            int columnIndex = i + 1;
            ResultSqlArray arrayDummy = Mockito.mock(ResultSqlArray.class);
            when(arrayDummy.getArray()).thenReturn(testDatum.getTestArray());
            when(resultSet.getArray(columnIndex)).thenReturn(arrayDummy);
            when(resultSetMetaData.getColumnLabel(columnIndex)).thenReturn(testDatum.getFieldName());
            when(resultSetMetaData.getColumnType(columnIndex)).thenReturn(Types.ARRAY);
            fields.add(new RecordField(testDatum.getFieldName(), RecordFieldType.ARRAY.getDataType()));
        }
        return fields;
    }

    private void thenAllDataTypesMatchInputFieldType(final List<RecordField> inputFields, final RecordSchema resultSchema) {
        assertEquals(inputFields.size(), resultSchema.getFieldCount(), "The number of input fields does not match the number of fields in the result schema.");
        for (int i = 0; i < inputFields.size(); ++i) {
            assertEquals(inputFields.get(i).getDataType(), resultSchema.getField(i).getDataType());
        }
    }

    private void thenAllDataTypesAreString(final RecordSchema resultSchema) {
        for (int i = 0; i < resultSchema.getFieldCount(); ++i) {
            assertEquals(RecordFieldType.STRING.getDataType(), resultSchema.getField(i).getDataType());
        }
    }

    private void thenAllColumnDataTypesAreCorrect(TestColumn[] columns, RecordSchema expectedSchema, RecordSchema actualSchema) {
        assertNotNull(actualSchema);

        for (TestColumn column : columns) {
            int fieldIndex = column.getIndex() - 1;
            // The DECIMAL column with scale larger than precision will not match so verify that instead
            DataType actualDataType = actualSchema.getField(fieldIndex).getDataType();
            DataType expectedDataType = expectedSchema.getField(fieldIndex).getDataType();
            if (expectedDataType.equals(RecordFieldType.DECIMAL.getDecimalDataType(3, 10))) {
                DecimalDataType decimalDataType = (DecimalDataType) expectedDataType;
                if (decimalDataType.getScale() > decimalDataType.getPrecision()) {
                    expectedDataType = RecordFieldType.DECIMAL.getDecimalDataType(decimalDataType.getScale(), decimalDataType.getScale());
                }
            }
            assertEquals(expectedDataType, actualDataType, "For column " + column.getIndex() + " the converted type is not matching");
        }
    }

    private void thenActualArrayElementTypesMatchExpected(Map<String, DataType> expectedTypes, RecordSchema actualSchema) {
        for (RecordField recordField : actualSchema.getFields()) {
            if (recordField.getDataType() instanceof ArrayDataType) {
                ArrayDataType arrayType = (ArrayDataType) recordField.getDataType();
                assertEquals(expectedTypes.get(recordField.getFieldName()), arrayType.getElementType(),
                        "Array element type for " + recordField.getFieldName()
                                + " is not of expected type " + expectedTypes.get(recordField.getFieldName()).toString());
            } else {
                fail("RecordField " + recordField.getFieldName() + " is not instance of ArrayDataType");
            }
        }
    }

    private void thenAllDataTypesAreChoice(final List<RecordField> inputFields, final RecordSchema resultSchema) {
        assertEquals(inputFields.size(), resultSchema.getFieldCount(), "The number of input fields does not match the number of fields in the result schema.");

        DataType expectedType = getBroadestChoiceDataType();
        for (int i = 0; i < inputFields.size(); ++i) {
            assertEquals(expectedType, resultSchema.getField(i).getDataType());
        }
    }

    private DataType getBroadestChoiceDataType() {
        List<DataType> dataTypes = Stream.of(RecordFieldType.BIGINT, RecordFieldType.BOOLEAN, RecordFieldType.BYTE, RecordFieldType.CHAR, RecordFieldType.DATE,
                RecordFieldType.DECIMAL, RecordFieldType.DOUBLE, RecordFieldType.FLOAT, RecordFieldType.INT, RecordFieldType.LONG, RecordFieldType.SHORT, RecordFieldType.STRING,
                RecordFieldType.TIME, RecordFieldType.TIMESTAMP)
                .map(RecordFieldType::getDataType)
                .collect(Collectors.toList());
        return RecordFieldType.CHOICE.getChoiceDataType(dataTypes);
    }

    private static class TestColumn {
        private final int index; // Column indexing starts from 1, not 0.
        private final String columnName;
        private final int sqlType;
        private final DataType recordFieldType;

        public TestColumn(final int index, final String columnName, final int sqlType, final DataType recordFieldType) {
            this.index = index;
            this.columnName = columnName;
            this.sqlType = sqlType;
            this.recordFieldType = recordFieldType;
        }

        public int getIndex() {
            return index;
        }

        public String getColumnName() {
            return columnName;
        }

        public int getSqlType() {
            return sqlType;
        }

        public DataType getRecordFieldType() {
            return recordFieldType;
        }
    }

    private static class ResultSqlArray implements Array {

        @Override
        public String getBaseTypeName() throws SQLException {
            return null;
        }

        @Override
        public int getBaseType() throws SQLException {
            return 0;
        }

        @Override
        public Object getArray() throws SQLException {
            return null;
        }

        @Override
        public Object getArray(Map<String, Class<?>> map) throws SQLException {
            return null;
        }

        @Override
        public Object getArray(long index, int count) throws SQLException {
            return null;
        }

        @Override
        public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
            return null;
        }

        @Override
        public ResultSet getResultSet() throws SQLException {
            return null;
        }

        @Override
        public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
            return null;
        }

        @Override
        public ResultSet getResultSet(long index, int count) throws SQLException {
            return null;
        }

        @Override
        public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
            return null;
        }

        @Override
        public void free() throws SQLException {

        }
    }

    private static class ResultBigDecimal extends BigDecimal {
        public static int PRECISION = 3;
        public static int SCALE = 0;
        public ResultBigDecimal() {
            super("123");
        }
    }

    private static class ArrayTestData {
        final private String fieldName;
        final private Object[] testArray;

        public ArrayTestData(String fieldName, Object[] testArray) {
            this.fieldName = fieldName;
            this.testArray = testArray;
        }

        public String getFieldName() {
            return fieldName;
        }

        public Object[] getTestArray() {
            return testArray;
        }
    }
}