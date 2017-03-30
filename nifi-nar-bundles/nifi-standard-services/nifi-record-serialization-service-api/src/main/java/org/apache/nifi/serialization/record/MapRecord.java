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

import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.nifi.serialization.record.util.DataTypeUtils;

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

        final String dateFormat = getFormat(format, RecordFieldType.DATE);
        final String timestampFormat = getFormat(format, RecordFieldType.TIMESTAMP);
        final String timeFormat = getFormat(format, RecordFieldType.TIME);
        return DataTypeUtils.toString(value, dateFormat, timeFormat, timestampFormat);
    }

    @Override
    public Long getAsLong(final String fieldName) {
        return DataTypeUtils.toLong(getValue(fieldName));
    }

    @Override
    public Integer getAsInt(final String fieldName) {
        return DataTypeUtils.toInteger(getValue(fieldName));
    }

    @Override
    public Double getAsDouble(final String fieldName) {
        return DataTypeUtils.toDouble(getValue(fieldName));
    }

    @Override
    public Float getAsFloat(final String fieldName) {
        return DataTypeUtils.toFloat(getValue(fieldName));
    }

    @Override
    public Record getAsRecord(String fieldName, final RecordSchema schema) {
        return DataTypeUtils.toRecord(getValue(fieldName), schema);
    }

    @Override
    public Boolean getAsBoolean(final String fieldName) {
        return DataTypeUtils.toBoolean(getValue(fieldName));
    }

    @Override
    public Date getAsDate(final String fieldName, final String format) {
        return DataTypeUtils.toDate(getValue(fieldName), format);
    }

    @Override
    public Object[] getAsArray(final String fieldName) {
        return DataTypeUtils.toArray(getValue(fieldName));
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
