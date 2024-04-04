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

package org.apache.nifi.csv;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import java.util.Optional;

abstract public class AbstractCSVRecordReader implements RecordReader {

    protected final ComponentLog logger;
    protected final boolean hasHeader;
    protected final boolean ignoreHeader;
    private final boolean trimDoubleQuote;

    protected final String dateFormat;
    protected final String timeFormat;
    protected final String timestampFormat;

    protected final RecordSchema schema;

    AbstractCSVRecordReader(final ComponentLog logger, final RecordSchema schema, final boolean hasHeader, final boolean ignoreHeader,
                            final String dateFormat, final String timeFormat, final String timestampFormat, final boolean trimDoubleQuote) {
        this.logger = logger;
        this.schema = schema;
        this.hasHeader = hasHeader;
        this.ignoreHeader = ignoreHeader;
        this.trimDoubleQuote = trimDoubleQuote;

        if (dateFormat == null || dateFormat.isEmpty()) {
            this.dateFormat = null;
        } else {
            this.dateFormat = dateFormat;
        }

        if (timeFormat == null || timeFormat.isEmpty()) {
            this.timeFormat = null;
        } else {
            this.timeFormat = timeFormat;
        }

        if (timestampFormat == null || timestampFormat.isEmpty()) {
            this.timestampFormat = null;
        } else {
            this.timestampFormat = timestampFormat;
        }
    }

    protected final Object convert(final String value, final DataType dataType, final String fieldName) {
        if (dataType == null || value == null) {
            return value;
        }

        final String trimmed;
        final RecordFieldType type = dataType.getFieldType();

        if (!trimDoubleQuote && (type.equals(RecordFieldType.STRING) || type.equals(RecordFieldType.CHOICE))) {
            trimmed = value;
        } else {
            trimmed = trim(value);
        }

        if (trimmed.isEmpty()) {
            return null;
        }

        return DataTypeUtils.convertType(trimmed, dataType, Optional.ofNullable(dateFormat), Optional.ofNullable(timeFormat), Optional.ofNullable(timestampFormat), fieldName);
    }

    protected final Object convertSimpleIfPossible(final String value, final DataType dataType, final String fieldName) {
        if (dataType == null || value == null) {
            return value;
        }

        final String trimmed;

        if (!trimDoubleQuote && dataType.getFieldType().equals(RecordFieldType.STRING)) {
            trimmed = value;
        } else {
            trimmed = trim(value);
        }

        if (trimmed.isEmpty()) {
            return null;
        }

        switch (dataType.getFieldType()) {
            case STRING:
                return value;
            case BOOLEAN:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case BYTE:
            case CHAR:
            case SHORT:
                if (DataTypeUtils.isCompatibleDataType(trimmed, dataType)) {
                    return DataTypeUtils.convertType(trimmed, dataType, Optional.ofNullable(dateFormat), Optional.ofNullable(timeFormat), Optional.ofNullable(timestampFormat), fieldName);
                }
                break;
            case DATE:
                if (DataTypeUtils.isDateTypeCompatible(trimmed, dateFormat)) {
                    return DataTypeUtils.convertType(trimmed, dataType, Optional.ofNullable(dateFormat), Optional.ofNullable(timeFormat), Optional.ofNullable(timestampFormat), fieldName);
                }
                break;
            case TIME:
                if (DataTypeUtils.isTimeTypeCompatible(trimmed, timeFormat)) {
                    return DataTypeUtils.convertType(trimmed, dataType, Optional.ofNullable(dateFormat), Optional.ofNullable(timeFormat), Optional.ofNullable(timestampFormat), fieldName);
                }
                break;
            case TIMESTAMP:
                if (DataTypeUtils.isTimestampTypeCompatible(trimmed, timestampFormat)) {
                    return DataTypeUtils.convertType(trimmed, dataType, Optional.ofNullable(dateFormat), Optional.ofNullable(timeFormat), Optional.ofNullable(timestampFormat), fieldName);
                }
                break;
        }

        return value;
    }

    protected String trim(String value) {
        return (value.length() > 1) && value.startsWith("\"") && value.endsWith("\"") ? value.substring(1, value.length() - 1) : value;
    }

    @Override
    public RecordSchema getSchema() {
        return schema;
    }
}
