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

import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class MapRecord implements Record {
    private final RecordSchema schema;
    private final Map<String, Object> values;

    public MapRecord(final RecordSchema schema, final Map<String, Object> values) {
        this.schema = Objects.requireNonNull(schema);
        this.values = Objects.requireNonNull(values);
    }

    @Override
    public RecordSchema getSchema() {
        return schema;
    }

    @Override
    public Object[] getValues() {
        final Object[] values = new Object[schema.getFieldCount()];
        int i = 0;
        for (final String fieldName : schema.getFieldNames()) {
            values[i++] = getValue(fieldName);
        }
        return values;
    }

    @Override
    public Object getValue(final String fieldName) {
        return values.get(fieldName);
    }

    @Override
    public String getAsString(final String fieldName) {
        final Optional<DataType> dataTypeOption = schema.getDataType(fieldName);
        if (!dataTypeOption.isPresent()) {
            return null;
        }

        return convertToString(getValue(fieldName), dataTypeOption.get().getFormat());
    }

    @Override
    public String getAsString(final String fieldName, final String format) {
        return convertToString(getValue(fieldName), format);
    }

    private String getFormat(final String optionalFormat, final RecordFieldType fieldType) {
        return (optionalFormat == null) ? fieldType.getDefaultFormat() : optionalFormat;
    }

    private String convertToString(final Object value, final String format) {
        if (value == null) {
            return null;
        }

        if (value instanceof java.sql.Date) {
            java.sql.Date date = (java.sql.Date) value;
            final long time = date.getTime();
            return new SimpleDateFormat(getFormat(format, RecordFieldType.DATE)).format(new java.util.Date(time));
        }
        if (value instanceof java.util.Date) {
            return new SimpleDateFormat(getFormat(format, RecordFieldType.DATE)).format((java.util.Date) value);
        }
        if (value instanceof Timestamp) {
            java.sql.Timestamp date = (java.sql.Timestamp) value;
            final long time = date.getTime();
            return new SimpleDateFormat(getFormat(format, RecordFieldType.TIMESTAMP)).format(new java.util.Date(time));
        }
        if (value instanceof Time) {
            java.sql.Time date = (java.sql.Time) value;
            final long time = date.getTime();
            return new SimpleDateFormat(getFormat(format, RecordFieldType.TIME)).format(new java.util.Date(time));
        }

        return value.toString();
    }

    @Override
    public Long getAsLong(final String fieldName) {
        return convertToLong(getValue(fieldName), fieldName);
    }

    private Long convertToLong(final Object value, final Object fieldDesc) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof String) {
            return Long.parseLong((String) value);
        }
        if (value instanceof Date) {
            return ((Date) value).getTime();
        }

        throw new TypeMismatchException("Cannot convert value of type " + value.getClass() + " to Long for field " + fieldDesc);
    }

    @Override
    public Integer getAsInt(final String fieldName) {
        return convertToInt(getValue(fieldName), fieldName);
    }

    private Integer convertToInt(final Object value, final Object fieldDesc) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        if (value instanceof String) {
            return Integer.parseInt((String) value);
        }

        throw new TypeMismatchException("Cannot convert value of type " + value.getClass() + " to Integer for field " + fieldDesc);
    }


    @Override
    public Double getAsDouble(final String fieldName) {
        return convertToDouble(getValue(fieldName), fieldName);
    }

    private Double convertToDouble(final Object value, final Object fieldDesc) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        if (value instanceof String) {
            return Double.parseDouble((String) value);
        }

        throw new TypeMismatchException("Cannot convert value of type " + value.getClass() + " to Double for field " + fieldDesc);
    }

    @Override
    public Float getAsFloat(final String fieldName) {
        return convertToFloat(getValue(fieldName), fieldName);
    }

    private Float convertToFloat(final Object value, final Object fieldDesc) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }
        if (value instanceof String) {
            return Float.parseFloat((String) value);
        }

        throw new TypeMismatchException("Cannot convert value of type " + value.getClass() + " to Float for field " + fieldDesc);
    }

    @Override
    public Record getAsRecord(String fieldName) {
        return convertToRecord(getValue(fieldName), fieldName);
    }

    private Record convertToRecord(final Object value, final Object fieldDesc) {
        if (value == null) {
            return null;
        }

        if (value instanceof Record) {
            return (Record) value;
        }

        throw new TypeMismatchException("Cannot convert value of type " + value.getClass() + " to Record for field " + fieldDesc);
    }


    @Override
    public Boolean getAsBoolean(final String fieldName) {
        return convertToBoolean(getValue(fieldName), fieldName);
    }

    private Boolean convertToBoolean(final Object value, final Object fieldDesc) {
        if (value == null) {
            return null;
        }

        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof String) {
            final String string = (String) value;
            if (string.equalsIgnoreCase("true") || string.equalsIgnoreCase("t")) {
                return Boolean.TRUE;
            }

            if (string.equalsIgnoreCase("false") || string.equals("f")) {
                return Boolean.FALSE;
            }

            throw new TypeMismatchException("Cannot convert String value to Boolean for field " + fieldDesc + " because it is not a valid boolean value");
        }

        throw new TypeMismatchException("Cannot convert value of type " + value.getClass() + " to Boolean for field " + fieldDesc);
    }

    @Override
    public Date getAsDate(final String fieldName) {
        final Optional<DataType> dataTypeOption = schema.getDataType(fieldName);
        if (!dataTypeOption.isPresent()) {
            return null;
        }

        return convertToDate(getValue(fieldName), fieldName, dataTypeOption.get().getFormat());
    }

    @Override
    public Date getAsDate(final String fieldName, final String format) {
        return convertToDate(getValue(fieldName), fieldName, format);
    }

    private Date convertToDate(final Object value, final Object fieldDesc, final String format) {
        if (value == null) {
            return null;
        }

        if (value instanceof Date) {
            return (Date) value;
        }
        if (value instanceof Number) {
            final Long time = ((Number) value).longValue();
            return new Date(time);
        }
        if (value instanceof java.sql.Date) {
            return new Date(((java.sql.Date) value).getTime());
        }
        if (value instanceof String) {
            try {
                return new SimpleDateFormat(getFormat(format, RecordFieldType.DATE)).parse((String) value);
            } catch (final ParseException e) {
                throw new TypeMismatchException("Cannot convert String value to date for field " + fieldDesc + " because it is not in the correct format of: " + format, e);
            }
        }

        throw new TypeMismatchException("Cannot convert value of type " + value.getClass() + " to Boolean for field " + fieldDesc);
    }

    @Override
    public Object[] getAsArray(final String fieldName) {
        return convertToArray(getValue(fieldName));
    }

    private Object[] convertToArray(final Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Object[]) {
            return (Object[]) value;
        }

        if (value instanceof List) {
            return ((List<?>) value).toArray();
        }

        return new Object[] {value};
    }

    @Override
    public int hashCode() {
        return 31 + 41 * values.hashCode() + 7 * schema.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof MapRecord)) {
            return false;
        }
        final MapRecord other = (MapRecord) obj;
        return schema.equals(other.schema) && values.equals(other.values);
    }

    @Override
    public String toString() {
        return "MapRecord[values=" + values + "]";
    }
}
