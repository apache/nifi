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
package org.apache.nifi.cef;

import com.fluenda.parcefone.event.CEFHandlingException;
import com.fluenda.parcefone.event.CommonEvent;
import com.fluenda.parcefone.parser.CEFParser;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * CEF (Common Event Format) based implementation for {@code org.apache.nifi.serialization.RecordReader}. The implementation
 * builds on top of the {@code com.fluenda.parcefone.parser.CEFParser} which is responsible to deal with the textual representation
 * of the events. This implementation intends to fill the gap between the CEF parser's {@code com.fluenda.parcefone.event.CommonEvent}
 * data objects and the NiFi's internal Record representation.
 */
final class CEFRecordReader implements RecordReader {
    /**
     * Currently these are not used but set the way it follows the expected CEF format.
     */
    private static final String DATE_FORMAT = "MMM dd yyyy";
    private static final String TIME_FORMAT = "HH:mm:ss";
    private static final String DATETIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    private final RecordSchema schema;
    private final BufferedReader reader;
    private final CEFParser parser;
    private final ComponentLog logger;
    private final Locale locale;
    private final String rawMessageField;
    private final String invalidField;

    /**
     * It would not cause any functional drawback to acquire the custom extensions all the time, but there are some cases
     * (inferred schema when custom extension fields are not included) where we can be sure about that these fields are not
     * required. For better performance, in these cases we do not work with these fields.
     */
    private final boolean includeCustomExtensions;
    private final boolean acceptEmptyExtensions;

    CEFRecordReader(
        final InputStream inputStream,
        final RecordSchema recordSchema,
        final CEFParser parser,
        final ComponentLog logger,
        final Locale locale,
        final String rawMessageField,
        final String invalidField,
        final boolean includeCustomExtensions,
        final boolean acceptEmptyExtensions
    ) {
        this.reader = new BufferedReader(new InputStreamReader(inputStream));
        this.schema = recordSchema;
        this.parser = parser;
        this.logger = logger;
        this.locale = locale;
        this.rawMessageField = rawMessageField;
        this.invalidField = invalidField;
        this.includeCustomExtensions = includeCustomExtensions;
        this.acceptEmptyExtensions = acceptEmptyExtensions;
    }

    @Override
    public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws IOException, MalformedRecordException {
        final String line = nextLine();

        if (line == null) {
            return null;
        }

        final CommonEvent event = parser.parse(line, false, acceptEmptyExtensions, locale);

        if (event == null) {
            logger.debug("Event parsing resulted no event");

            if (invalidField != null && !invalidField.isEmpty()) {
                return new MapRecord(schema, Collections.singletonMap(invalidField, line));
            } else {
                throw new MalformedRecordException("The following line could not be parsed by the CEF parser: " + line);
            }
        }

        final Map<String, Object> values = new HashMap<>();

        try {
            event.getHeader().entrySet().forEach(field -> values.put(field.getKey(), convertValue(field.getKey(), field.getValue(), coerceTypes)));
            event.getExtension(true, includeCustomExtensions).entrySet().forEach(field -> values.put(field.getKey(), convertValue(field.getKey() ,field.getValue(), coerceTypes)));

            for (final String fieldName : schema.getFieldNames()) {
                if (!values.containsKey(fieldName)) {
                    values.put(fieldName, null);
                }
            }

        } catch (final CEFHandlingException e) {
            throw new MalformedRecordException("Error during extracting information from CEF event", e);
        }

        if (rawMessageField != null) {
            values.put(rawMessageField, line);
        }

        return new MapRecord(schema, values, true, dropUnknownFields);
    }

    private String nextLine() throws IOException {
        String line;

        while((line = reader.readLine()) != null) {
            if (!line.isEmpty()) {
                break;
            }
        }

        return line;
    }

    private Object convertValue(final String fieldName, final Object fieldValue, final boolean coerceType) {
        final DataType dataType = schema.getDataType(fieldName).orElse(RecordFieldType.STRING.getDataType());

        return coerceType
            ? convert(fieldValue, dataType, fieldName)
            : convertSimpleIfPossible(fieldValue, dataType, fieldName);
    }

    private Object convert(final Object value, final DataType dataType, final String fieldName) {
        return DataTypeUtils.convertType(prepareValue(value), dataType, Optional.of(DATE_FORMAT), Optional.of(TIME_FORMAT), Optional.of(DATETIME_FORMAT), fieldName);
    }

    private Object convertSimpleIfPossible(final Object value, final DataType dataType, final String fieldName) {
        if (dataType == null || value == null) {
            return value;
        }

        final Object preparedValue = prepareValue(value);

        switch (dataType.getFieldType()) {
            case STRING:
                return preparedValue;
            case BOOLEAN:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                if (DataTypeUtils.isCompatibleDataType(preparedValue, dataType)) {
                    return DataTypeUtils.convertType(preparedValue, dataType, Optional.of(DATE_FORMAT), Optional.of(TIME_FORMAT), Optional.of(DATETIME_FORMAT), fieldName);
                }
                break;
            case TIMESTAMP:
                if (DataTypeUtils.isTimestampTypeCompatible(preparedValue, DATETIME_FORMAT)) {
                    return DataTypeUtils.convertType(preparedValue, dataType, Optional.of(DATE_FORMAT), Optional.of(TIME_FORMAT), Optional.of(DATETIME_FORMAT), fieldName);
                }
                break;
        }

        return value;
    }

    private Object prepareValue(final Object value) {
        // InetAddress event fields are mapped as string values internally. Inet4Address and Inet6Address fields are handled by this too.
        if (value instanceof InetAddress) {
            return ((InetAddress) value).getHostAddress();
        }

        return value;
    }

    @Override
    public RecordSchema getSchema() throws MalformedRecordException {
        return schema;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
