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
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ResultSetRecordSet implements RecordSet, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(ResultSetRecordSet.class);
    private static final int JDBC_DEFAULT_PRECISION_VALUE = 10;
    private static final int JDBC_DEFAULT_SCALE_VALUE = 0;
    private final ResultSet rs;
    private final RecordSchema schema;
    private final Set<String> rsColumnNames;
    private final int defaultPrecision;
    private final int defaultScale;

    private boolean moreRows;

    private static final String STRING_CLASS_NAME = String.class.getName();
    private static final String INT_CLASS_NAME = Integer.class.getName();
    private static final String LONG_CLASS_NAME = Long.class.getName();
    private static final String DATE_CLASS_NAME = Date.class.getName();
    private static final String DOUBLE_CLASS_NAME = Double.class.getName();
    private static final String FLOAT_CLASS_NAME = Float.class.getName();
    private static final String BIGDECIMAL_CLASS_NAME = BigDecimal.class.getName();

    public ResultSetRecordSet(final ResultSet rs, final RecordSchema readerSchema) throws SQLException {
        this(rs, readerSchema, JDBC_DEFAULT_PRECISION_VALUE, JDBC_DEFAULT_SCALE_VALUE);
    }

    public ResultSetRecordSet(final ResultSet rs, final RecordSchema readerSchema, final int defaultPrecision, final int defaultScale) throws SQLException {
        this.defaultPrecision = defaultPrecision;
        this.defaultScale = defaultScale;
        this.rs = rs;
        this.rsColumnNames = new HashSet<>();
        RecordSchema tempSchema;
        try {
            tempSchema = createSchema(rs, readerSchema);
            moreRows = rs.next();
        } catch(SQLException se) {
            // Tried to create the schema with a ResultSet without calling next() first (probably for DB2), now try the other way around
            moreRows = rs.next();
            tempSchema = createSchema(rs, readerSchema);
        }
        this.schema = tempSchema;
    }

    @Override
    public RecordSchema getSchema() {
        return schema;
    }

    // Protected methods for subclasses to access private member variables
    protected ResultSet getResultSet() {
        return rs;
    }

    protected boolean hasMoreRows() {
        return moreRows;
    }

    protected void setMoreRows(boolean moreRows) {
        this.moreRows = moreRows;
    }

    @Override
    public Record next() throws IOException {
        try {
            if (moreRows) {
                final Record record = createRecord(rs);
                moreRows = !rs.isClosed() && rs.next();
                return record;
            } else {
                return null;
            }
        } catch (final SQLException e) {
            throw new IOException("Could not obtain next record from ResultSet", e);
        }
    }

    @Override
    public void close() {
        try {
            rs.close();
        } catch (final SQLException e) {
            logger.error("Failed to close ResultSet", e);
        }
    }

    protected Record createRecord(final ResultSet rs) throws SQLException {
        final Map<String, Object> values = new HashMap<>(schema.getFieldCount());

        for (final RecordField field : schema.getFields()) {
            final String fieldName = field.getFieldName();

            final Object value;
            if (rsColumnNames.contains(fieldName)) {
                value = normalizeValue(rs.getObject(fieldName));
            } else {
                value = null;
            }

            values.put(fieldName, value);
        }

        return new MapRecord(schema, values);
    }

    @SuppressWarnings("rawtypes")
    private Object normalizeValue(final Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        if (value instanceof java.sql.Date) {
            // Date objects should be stored in records as UTC normalized dates (UTC 00:00:00)
            // but they come from the driver in JVM's local time zone 00:00:00 and need to be converted.
            return DataTypeUtils.convertDateToUTC((java.sql.Date) value);
        }

        if (value instanceof List) {
            return ((List) value).toArray();
        }

        if (value instanceof Array) {
            return ((Array) value).getArray();
        }

        return value;
    }

    private RecordSchema createSchema(final ResultSet rs, final RecordSchema readerSchema) throws SQLException {
        final ResultSetMetaData metadata = rs.getMetaData();
        final int numCols = metadata.getColumnCount();
        final List<RecordField> fields = new ArrayList<>(numCols);

        for (int i = 0; i < numCols; i++) {
            final int column = i + 1;
            final int sqlType = metadata.getColumnType(column);

            final DataType dataType = getDataType(sqlType, rs, column, readerSchema);
            final String fieldName = metadata.getColumnLabel(column);

            final int nullableFlag = metadata.isNullable(column);
            final boolean nullable;
            if (nullableFlag == ResultSetMetaData.columnNoNulls) {
                nullable = false;
            } else {
                nullable = true;
            }

            final RecordField field = new RecordField(fieldName, dataType, nullable);
            fields.add(field);
            rsColumnNames.add(metadata.getColumnLabel(column));
        }

        return new SimpleRecordSchema(fields);
    }

    private DataType getDataType(final int sqlType, final ResultSet rs, final int columnIndex, final RecordSchema readerSchema) throws SQLException {
        switch (sqlType) {
            case Types.ARRAY:
                // The JDBC API does not allow us to know what the base type of an array is through the metadata.
                // As a result, we have to obtain the actual Array for this record. Once we have this, we can determine
                // the base type. However, if the base type is, itself, an array, we will simply return a base type of
                // String because otherwise, we need the ResultSet for the array itself, and many JDBC Drivers do not
                // support calling Array.getResultSet() and will throw an Exception if that is not supported.
                if (rs.isAfterLast()) {
                    return RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType());
                }

                final Array array = rs.getArray(columnIndex);
                if (array == null) {
                    return RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType());
                }

                final DataType baseType = getArrayBaseType(array);
                return RecordFieldType.ARRAY.getArrayDataType(baseType);
            case Types.BINARY:
            case Types.LONGVARBINARY:
            case Types.VARBINARY:
                return RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType());
            case Types.NUMERIC:
            case Types.DECIMAL:
                int decimalPrecision;
                final int decimalScale;
                final int resultSetPrecision = rs.getMetaData().getPrecision(columnIndex);
                final int resultSetScale = rs.getMetaData().getScale(columnIndex);
                if (rs.getMetaData().getPrecision(columnIndex) > 0) {
                    // When database returns a certain precision, we can rely on that.
                    decimalPrecision = resultSetPrecision;
                    //For the float data type Oracle return decimalScale < 0 which cause is not expected to org.apache.avro.LogicalTypes
                    //Hence falling back to default scale if decimalScale < 0
                    decimalScale = resultSetScale > 0 ? resultSetScale : defaultScale;
                } else {
                    // If not, use default precision.
                    decimalPrecision = defaultPrecision;
                    // Oracle returns precision=0, scale=-127 for variable scale value such as ROWNUM or function result.
                    // Specifying 'oracle.jdbc.J2EE13Compliant' SystemProperty makes it to return scale=0 instead.
                    // Queries for example, 'SELECT 1.23 as v from DUAL' can be problematic because it can't be mapped with decimal with scale=0.
                    // Default scale is used to preserve decimals in such case.
                    decimalScale = resultSetScale > 0 ? resultSetScale : defaultScale;
                }
                // Scale can be bigger than precision in some cases (Oracle, e.g.) If this is the case, assume precision refers to the number of
                // decimal digits and thus precision = scale
                if (decimalScale > decimalPrecision) {
                    decimalPrecision = decimalScale;
                }
                return RecordFieldType.DECIMAL.getDecimalDataType(decimalPrecision, decimalScale);
            case Types.OTHER: {
                // If we have no records to inspect, we can't really know its schema so we simply use the default data type.
                if (rs.isAfterLast()) {
                    return RecordFieldType.RECORD.getDataType();
                }

                final String columnName = rs.getMetaData().getColumnName(columnIndex);

                if (readerSchema != null) {
                    Optional<DataType> dataType = readerSchema.getDataType(columnName);
                    if (dataType.isPresent()) {
                        return dataType.get();
                    }
                }

                final Object obj = rs.getObject(columnIndex);
                if (!(obj instanceof Record)) {
                    final List<DataType> dataTypes = Stream.of(RecordFieldType.BIGINT, RecordFieldType.BOOLEAN, RecordFieldType.BYTE, RecordFieldType.CHAR, RecordFieldType.DATE,
                        RecordFieldType.DECIMAL, RecordFieldType.DOUBLE, RecordFieldType.FLOAT, RecordFieldType.INT, RecordFieldType.LONG, RecordFieldType.SHORT, RecordFieldType.STRING,
                            RecordFieldType.TIME, RecordFieldType.TIMESTAMP)
                    .map(RecordFieldType::getDataType)
                    .collect(Collectors.toList());

                    return RecordFieldType.CHOICE.getChoiceDataType(dataTypes);
                }

                final Record record = (Record) obj;
                final RecordSchema recordSchema = record.getSchema();
                return RecordFieldType.RECORD.getRecordDataType(recordSchema);
            }
            default: {
                final String columnName = rs.getMetaData().getColumnName(columnIndex);

                if (readerSchema != null) {
                    Optional<DataType> dataType = readerSchema.getDataType(columnName);
                    if (dataType.isPresent()) {
                        return dataType.get();
                    }
                }

                final RecordFieldType fieldType = getFieldType(sqlType, rs.getMetaData().getColumnClassName(columnIndex));

                if (RecordFieldType.DECIMAL.equals(fieldType)) {
                    final BigDecimal bigDecimalValue = rs.getBigDecimal(columnIndex);
                    return fieldType.getDecimalDataType(bigDecimalValue.precision(), bigDecimalValue.scale());
                } else {
                    return fieldType.getDataType();
                }
            }
        }
    }

    private static DataType getArrayBaseType(final Array array) throws SQLException {
        final Object arrayValue = array.getArray();
        if (arrayValue == null) {
            return RecordFieldType.STRING.getDataType();
        }

        if (arrayValue instanceof byte[]) {
            return RecordFieldType.BYTE.getDataType();
        }
        if (arrayValue instanceof int[]) {
            return RecordFieldType.INT.getDataType();
        }
        if (arrayValue instanceof long[]) {
            return RecordFieldType.LONG.getDataType();
        }
        if (arrayValue instanceof boolean[]) {
            return RecordFieldType.BOOLEAN.getDataType();
        }
        if (arrayValue instanceof short[]) {
            return RecordFieldType.SHORT.getDataType();
        }
        if (arrayValue instanceof byte[]) {
            return RecordFieldType.BYTE.getDataType();
        }
        if (arrayValue instanceof float[]) {
            return RecordFieldType.FLOAT.getDataType();
        }
        if (arrayValue instanceof double[]) {
            return RecordFieldType.DOUBLE.getDataType();
        }
        if (arrayValue instanceof char[]) {
            return RecordFieldType.CHAR.getDataType();
        }
        if (arrayValue instanceof Object[]) {
            final Object[] values = (Object[]) arrayValue;
            if (values.length == 0) {
                return RecordFieldType.STRING.getDataType();
            }

            Object valueToLookAt = null;
            for (int i = 0; i < values.length; i++) {
                valueToLookAt = values[i];
                if (valueToLookAt != null) {
                    break;
                }
            }
            if (valueToLookAt == null) {
                return RecordFieldType.STRING.getDataType();
            }

            if (valueToLookAt instanceof String) {
                return RecordFieldType.STRING.getDataType();
            }
            if (valueToLookAt instanceof Long) {
                return RecordFieldType.LONG.getDataType();
            }
            if (valueToLookAt instanceof Integer) {
                return RecordFieldType.INT.getDataType();
            }
            if (valueToLookAt instanceof Short) {
                return RecordFieldType.SHORT.getDataType();
            }
            if (valueToLookAt instanceof Byte) {
                return RecordFieldType.BYTE.getDataType();
            }
            if (valueToLookAt instanceof Float) {
                return RecordFieldType.FLOAT.getDataType();
            }
            if (valueToLookAt instanceof Double) {
                return RecordFieldType.DOUBLE.getDataType();
            }
            if (valueToLookAt instanceof BigDecimal) {
                final BigDecimal bigDecimal = (BigDecimal) valueToLookAt;
                return RecordFieldType.DECIMAL.getDecimalDataType(bigDecimal.precision(), bigDecimal.scale());
            }
            if (valueToLookAt instanceof Boolean) {
                return RecordFieldType.BOOLEAN.getDataType();
            }
            if (valueToLookAt instanceof Character) {
                return RecordFieldType.CHAR.getDataType();
            }
            if (valueToLookAt instanceof BigInteger) {
                return RecordFieldType.BIGINT.getDataType();
            }
            if (valueToLookAt instanceof Integer) {
                return RecordFieldType.INT.getDataType();
            }
            if (valueToLookAt instanceof java.sql.Time) {
                return RecordFieldType.TIME.getDataType();
            }
            if (valueToLookAt instanceof java.sql.Date) {
                return RecordFieldType.DATE.getDataType();
            }
            if (valueToLookAt instanceof java.sql.Timestamp) {
                return RecordFieldType.TIMESTAMP.getDataType();
            }
            if (valueToLookAt instanceof Record) {
                final Record record = (Record) valueToLookAt;
                return RecordFieldType.RECORD.getRecordDataType(record.getSchema());
            }
        }

        return RecordFieldType.STRING.getDataType();
    }


    private static RecordFieldType getFieldType(final int sqlType, final String valueClassName) {
        switch (sqlType) {
            case Types.BIGINT:
            case Types.ROWID:
                return RecordFieldType.LONG;
            case Types.BIT:
            case Types.BOOLEAN:
                return RecordFieldType.BOOLEAN;
            case Types.CHAR:
                return RecordFieldType.CHAR;
            case Types.DATE:
                return RecordFieldType.DATE;
            case Types.NUMERIC:
            case Types.DECIMAL:
                return RecordFieldType.DECIMAL;
            case Types.DOUBLE:
            case Types.REAL:
                return RecordFieldType.DOUBLE;
            case Types.FLOAT:
                return RecordFieldType.FLOAT;
            case Types.INTEGER:
                return RecordFieldType.INT;
            case Types.SMALLINT:
                return RecordFieldType.SHORT;
            case Types.TINYINT:
                return RecordFieldType.BYTE;
            case Types.LONGNVARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NULL:
            case Types.NVARCHAR:
            case Types.VARCHAR:
                return RecordFieldType.STRING;
            case Types.OTHER:
            case Types.JAVA_OBJECT:
                if (STRING_CLASS_NAME.equals(valueClassName)) {
                    return RecordFieldType.STRING;
                }
                if (INT_CLASS_NAME.equals(valueClassName)) {
                    return RecordFieldType.INT;
                }
                if (LONG_CLASS_NAME.equals(valueClassName)) {
                    return RecordFieldType.LONG;
                }
                if (DATE_CLASS_NAME.equals(valueClassName)) {
                    return RecordFieldType.DATE;
                }
                if (FLOAT_CLASS_NAME.equals(valueClassName)) {
                    return RecordFieldType.FLOAT;
                }
                if (DOUBLE_CLASS_NAME.equals(valueClassName)) {
                    return RecordFieldType.DOUBLE;
                }
                if (BIGDECIMAL_CLASS_NAME.equals(valueClassName)) {
                    return RecordFieldType.DECIMAL;
                }

                return RecordFieldType.RECORD;
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                return RecordFieldType.TIME;
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
            case -101: // Oracle's TIMESTAMP WITH TIME ZONE
            case -102: // Oracle's TIMESTAMP WITH LOCAL TIME ZONE
                return RecordFieldType.TIMESTAMP;
        }

        return RecordFieldType.STRING;
    }
}
