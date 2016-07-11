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

package org.apache.nifi.json;

import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.DataTypeUtils;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.codehaus.jackson.JsonNode;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;

public class JsonPathRowRecordReader extends AbstractJsonRowRecordReader {
    private static final Configuration STRICT_PROVIDER_CONFIGURATION = Configuration.builder().jsonProvider(new JacksonJsonProvider()).build();

    private static final String TIME_FORMAT_DATE = "yyyy-MM-dd";
    private static final String TIME_FORMAT_TIME = "HH:mm:ss";
    private static final String TIME_FORMAT_TIMESTAMP = "yyyy-MM-dd HH:mm:ss";
    private static final TimeZone gmt = TimeZone.getTimeZone("GMT");

    private final LinkedHashMap<String, JsonPath> jsonPaths;
    private final Map<String, DataType> fieldTypeOverrides;
    private final InputStream in;
    private RecordSchema schema;

    public JsonPathRowRecordReader(final LinkedHashMap<String, JsonPath> jsonPaths, final Map<String, DataType> fieldTypeOverrides, final InputStream in, final ComponentLog logger)
        throws MalformedRecordException, IOException {
        super(in, logger);

        this.jsonPaths = jsonPaths;
        this.fieldTypeOverrides = fieldTypeOverrides;
        this.in = in;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public RecordSchema getSchema() {
        if (schema != null) {
            return schema;
        }

        final Optional<JsonNode> firstNodeOption = getFirstJsonNode();

        final List<RecordField> recordFields = new ArrayList<>();
        if (firstNodeOption.isPresent()) {
            final DocumentContext ctx = JsonPath.using(STRICT_PROVIDER_CONFIGURATION).parse(firstNodeOption.get().toString());
            for (final Map.Entry<String, JsonPath> entry : jsonPaths.entrySet()) {
                final String fieldName = PropertyNameUtil.getFieldName(entry.getKey());
                final JsonPath jsonPath = entry.getValue();

                final DataType dataType;
                final DataType dataTypeOverride = fieldTypeOverrides.get(fieldName);
                if (dataTypeOverride == null) {
                    Object value;
                    try {
                        value = ctx.read(jsonPath);
                    } catch (final PathNotFoundException pnfe) {
                        value = null;
                    }

                    if (value == null) {
                        dataType = RecordFieldType.STRING.getDataType();
                    } else {
                        dataType = DataTypeUtils.inferDataType(value);
                    }
                } else {
                    dataType = dataTypeOverride;
                }

                recordFields.add(new RecordField(fieldName, dataType));
            }
        }

        // If there are any overridden field types that we didn't find, add as the last fields.
        final Set<String> knownFieldNames = recordFields.stream()
            .map(f -> f.getFieldName())
            .collect(Collectors.toSet());

        for (final Map.Entry<String, DataType> entry : fieldTypeOverrides.entrySet()) {
            if (!knownFieldNames.contains(entry.getKey())) {
                recordFields.add(new RecordField(entry.getKey(), entry.getValue()));
            }
        }

        schema = new SimpleRecordSchema(recordFields);
        return schema;
    }


    @Override
    @SuppressWarnings("unchecked")
    protected Record convertJsonNodeToRecord(final JsonNode jsonNode, final RecordSchema schema) throws IOException {
        if (jsonNode == null) {
            return null;
        }

        final DocumentContext ctx = JsonPath.using(STRICT_PROVIDER_CONFIGURATION).parse(jsonNode.toString());
        final Map<String, Object> values = new HashMap<>(schema.getFieldCount());

        for (final Map.Entry<String, JsonPath> entry : jsonPaths.entrySet()) {
            final JsonPath jsonPath = entry.getValue();

            Object value;
            try {
                value = ctx.read(jsonPath);
            } catch (final PathNotFoundException pnfe) {
                value = null;
            }

            final String fieldName = entry.getKey();
            if (value != null) {
                final DataType determinedType = DataTypeUtils.inferDataType(value);
                final DataType desiredType = schema.getDataType(fieldName).orElse(null);

                if (value instanceof List) {
                    value = ((List<Object>) value).toArray();
                } else if (value instanceof Map && desiredType.getFieldType() == RecordFieldType.RECORD) {
                    value = convert(desiredType, value);
                } else if (desiredType != null && !determinedType.equals(desiredType) && shouldConvert(value, determinedType.getFieldType())) {
                    value = convert(desiredType, value);
                }
            }

            values.put(fieldName, value);
        }

        return new MapRecord(schema, values);
    }

    private boolean shouldConvert(final Object value, final RecordFieldType determinedType) {
        return determinedType != null
            && determinedType != RecordFieldType.ARRAY;
    }


    protected Object convert(final DataType dataType, final Object value) {
        if (dataType.getFieldType() == RecordFieldType.RECORD && dataType.getChildRecordSchema().isPresent() && value instanceof Map) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> map = (Map<String, Object>) value;
            return new MapRecord(dataType.getChildRecordSchema().get(), map);
        } else {
            return convertString(dataType, value.toString());
        }
    }

    /**
     * Coerces the given string into the provided data type, if possible
     *
     * @param dataType the desired type
     * @param string the string representation of the value
     * @return an Object representing the same value as the given string but in the requested data type
     */
    protected Object convertString(final DataType dataType, final String string) {
        if (dataType == null) {
            return string;
        }

        switch (dataType.getFieldType()) {
            case BOOLEAN:
                if (string.length() == 0) {
                    return null;
                }
                return Boolean.parseBoolean(string);
            case BYTE:
                if (string.length() == 0) {
                    return null;
                }
                return Byte.parseByte(string);
            case SHORT:
                if (string.length() == 0) {
                    return null;
                }
                return Short.parseShort(string);
            case INT:
                if (string.length() == 0) {
                    return null;
                }
                return Integer.parseInt(string);
            case LONG:
                if (string.length() == 0) {
                    return null;
                }
                return Long.parseLong(string);
            case FLOAT:
                if (string.length() == 0) {
                    return null;
                }
                return Float.parseFloat(string);
            case DOUBLE:
                if (string.length() == 0) {
                    return null;
                }
                return Double.parseDouble(string);
            case DATE:
                if (string.length() == 0) {
                    return null;
                }
                try {
                    final DateFormat format = new SimpleDateFormat(TIME_FORMAT_DATE);
                    format.setTimeZone(gmt);
                    Date date = format.parse(string);
                    return new java.sql.Date(date.getTime());
                } catch (ParseException e) {
                    return null;
                }
            case TIME:
                if (string.length() == 0) {
                    return null;
                }
                try {
                    final DateFormat format = new SimpleDateFormat(TIME_FORMAT_TIME);
                    format.setTimeZone(gmt);
                    Date date = format.parse(string);
                    return new java.sql.Time(date.getTime());
                } catch (ParseException e) {
                    return null;
                }
            case TIMESTAMP:
                if (string.length() == 0) {
                    return null;
                }
                try {
                    final DateFormat format = new SimpleDateFormat(TIME_FORMAT_TIMESTAMP);
                    format.setTimeZone(gmt);
                    Date date = format.parse(string);
                    return new java.sql.Timestamp(date.getTime());
                } catch (ParseException e) {
                    return null;
                }
            case STRING:
            default:
                return string;
        }
    }
}