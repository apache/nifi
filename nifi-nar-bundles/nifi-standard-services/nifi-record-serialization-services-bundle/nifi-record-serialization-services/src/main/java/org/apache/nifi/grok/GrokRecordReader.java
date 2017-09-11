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

package org.apache.nifi.grok;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import io.thekraken.grok.api.Grok;
import io.thekraken.grok.api.Match;

public class GrokRecordReader implements RecordReader {
    private final BufferedReader reader;
    private final Grok grok;
    private final boolean append;
    private final RecordSchema schemaFromGrok;
    private RecordSchema schema;

    private String nextLine;

    static final String STACK_TRACE_COLUMN_NAME = "stackTrace";
    static final String RAW_MESSAGE_NAME = "_raw";

    private static final Pattern STACK_TRACE_PATTERN = Pattern.compile(
        "^\\s*(?:(?:    |\\t)+at )|"
            + "(?:(?:    |\\t)+\\[CIRCULAR REFERENCE\\:)|"
            + "(?:Caused by\\: )|"
            + "(?:Suppressed\\: )|"
            + "(?:\\s+... \\d+ (?:more|common frames? omitted)$)");

    public GrokRecordReader(final InputStream in, final Grok grok, final RecordSchema schema, final RecordSchema schemaFromGrok, final boolean append) {
        this.reader = new BufferedReader(new InputStreamReader(in));
        this.grok = grok;
        this.schema = schema;
        this.append = append;
        this.schemaFromGrok = schemaFromGrok;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws IOException, MalformedRecordException {
        Map<String, Object> valueMap = null;
        StringBuilder raw = new StringBuilder();

        while (valueMap == null || valueMap.isEmpty()) {
            final String line = nextLine == null ? reader.readLine() : nextLine;
            raw.append(line);
            nextLine = null; // ensure that we don't process nextLine again
            if (line == null) {
                return null;
            }

            final Match match = grok.match(line);
            match.captures();
            valueMap = match.toMap();
        }

        // Read the next line to see if it matches the pattern (in which case we will simply leave it for
        // the next call to nextRecord()) or we will attach it to the previously read record.
        String stackTrace = null;
        final StringBuilder trailingText = new StringBuilder();
        while ((nextLine = reader.readLine()) != null) {
            final Match nextLineMatch = grok.match(nextLine);
            nextLineMatch.captures();
            final Map<String, Object> nextValueMap = nextLineMatch.toMap();
            if (nextValueMap.isEmpty()) {
                // next line did not match. Check if it indicates a Stack Trace. If so, read until
                // the stack trace ends. Otherwise, append the next line to the last field in the record.
                if (isStartOfStackTrace(nextLine)) {
                    stackTrace = readStackTrace(nextLine);
                    raw.append("\n").append(stackTrace);
                    break;
                } else if (append) {
                    trailingText.append("\n").append(nextLine);
                    raw.append("\n").append(nextLine);
                }
            } else {
                // The next line matched our pattern.
                break;
            }
        }

        final Record record = createRecord(valueMap, trailingText, stackTrace, raw.toString(), coerceTypes, dropUnknownFields);
        return record;
    }

    private Record createRecord(final Map<String, Object> valueMap, final StringBuilder trailingText, final String stackTrace, final String raw, final boolean coerceTypes, final boolean dropUnknown) {
        final Map<String, Object> converted = new HashMap<>();
        for (final Map.Entry<String, Object> entry : valueMap.entrySet()) {
            final String fieldName = entry.getKey();
            final Object rawValue = entry.getValue();

            final Object normalizedValue;
            if (rawValue instanceof List) {
                final List<?> list = (List<?>) rawValue;
                final String[] array = new String[list.size()];
                for (int i = 0; i < list.size(); i++) {
                    final Object rawObject = list.get(i);
                    array[i] = rawObject == null ? null : rawObject.toString();
                }
                normalizedValue = array;
            } else {
                normalizedValue = rawValue == null ? null : rawValue.toString();
            }

            final Optional<RecordField> optionalRecordField = schema.getField(fieldName);

            final Object coercedValue;
            if (coerceTypes && optionalRecordField.isPresent()) {
                final RecordField field = optionalRecordField.get();
                final DataType fieldType = field.getDataType();
                coercedValue = convert(fieldType, normalizedValue, fieldName);
            } else {
                coercedValue = normalizedValue;
            }

            converted.put(fieldName, coercedValue);
        }

        // If there is any trailing text, determine the last column from the grok schema
        // and then append the trailing text to it.
        if (append && trailingText.length() > 0) {
            String lastPopulatedFieldName = null;
            final List<RecordField> schemaFields = schemaFromGrok.getFields();
            for (int i = schemaFields.size() - 1; i >= 0; i--) {
                final RecordField field = schemaFields.get(i);

                Object value = converted.get(field.getFieldName());
                if (value != null) {
                    lastPopulatedFieldName = field.getFieldName();
                    break;
                }

                for (final String alias : field.getAliases()) {
                    value = converted.get(alias);
                    if (value != null) {
                        lastPopulatedFieldName = alias;
                        break;
                    }
                }
            }

            if (lastPopulatedFieldName != null) {
                final Object value = converted.get(lastPopulatedFieldName);
                if (value == null) {
                    converted.put(lastPopulatedFieldName, trailingText.toString());
                } else if (value instanceof String) { // if not a String it is a List and we will just drop the trailing text
                    converted.put(lastPopulatedFieldName, (String) value + trailingText.toString());
                }
            }
        }

        converted.put(STACK_TRACE_COLUMN_NAME, stackTrace);
        converted.put(RAW_MESSAGE_NAME, raw);

        return new MapRecord(schema, converted);
    }


    private boolean isStartOfStackTrace(final String line) {
        if (line == null) {
            return false;
        }

        // Stack Traces are generally of the form:
        // java.lang.IllegalArgumentException: My message
        //   at org.apache.nifi.MyClass.myMethod(MyClass.java:48)
        //   at java.lang.Thread.run(Thread.java:745) [na:1.8.0_60]
        // Caused by: java.net.SocketTimeoutException: null
        //   ... 13 common frames omitted

        int index = line.indexOf("Exception: ");
        if (index < 0) {
            index = line.indexOf("Error: ");
        }

        if (index < 0) {
            return false;
        }

        if (line.indexOf(" ") < index) {
            return false;
        }

        return true;
    }

    private String readStackTrace(final String firstLine) throws IOException {
        final StringBuilder sb = new StringBuilder(firstLine);

        String line;
        while ((line = reader.readLine()) != null) {
            if (isLineInStackTrace(line)) {
                sb.append("\n").append(line);
            } else {
                nextLine = line;
                break;
            }
        }

        return sb.toString();
    }

    private boolean isLineInStackTrace(final String line) {
        return STACK_TRACE_PATTERN.matcher(line).find();
    }


    protected Object convert(final DataType fieldType, final Object rawValue, final String fieldName) {
        if (fieldType == null) {
            return rawValue;
        }

        if (rawValue == null) {
            return null;
        }

        // If string is empty then return an empty string if field type is STRING. If field type is
        // anything else, we can't really convert it so return null
        final boolean fieldEmpty = rawValue instanceof String && ((String) rawValue).isEmpty();
        if (fieldEmpty && fieldType.getFieldType() != RecordFieldType.STRING) {
            return null;
        }

        return DataTypeUtils.convertType(rawValue, fieldType, fieldName);
    }


    @Override
    public RecordSchema getSchema() {
        return schema;
    }

}