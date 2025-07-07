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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import io.krakens.grok.api.Grok;

public class GrokRecordReader implements RecordReader {
    private final BufferedReader reader;
    private final List<Grok> groks;
    private final NoMatchStrategy noMatchStrategy;
    private final RecordSchema schemaFromGrok;
    private final RecordSchema schema;

    private String nextLine;
    Map<String, Object> nextMap = null;

    static final String STACK_TRACE_COLUMN_NAME = "stackTrace";
    static final String RAW_MESSAGE_NAME = "_raw";

    private static final Pattern STACK_TRACE_PATTERN = Pattern.compile(
        "^\\s*(?:(?:\\s{4}|\\t)+at )|"
            + "(?:(?:\\s{4}|\\t)+\\[CIRCULAR REFERENCE:)|"
            + "(?:Caused by: )|"
            + "(?:Suppressed: )|"
            + "(?:\\s+... \\d+ (?:more|common frames? omitted)$)");

    public GrokRecordReader(final InputStream in, final Grok grok, final RecordSchema schema, final RecordSchema schemaFromGrok, final NoMatchStrategy noMatchStrategy) {
        this(in, Collections.singletonList(grok), schema, schemaFromGrok, noMatchStrategy);
    }

    public GrokRecordReader(final InputStream in, final List<Grok> groks, final RecordSchema schema, final RecordSchema schemaFromGrok, final NoMatchStrategy noMatchStrategy) {
        this.reader = new BufferedReader(new InputStreamReader(in));
        this.groks = groks;
        this.schema = schema;
        this.noMatchStrategy = noMatchStrategy;
        this.schemaFromGrok = schemaFromGrok;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws IOException, MalformedRecordException {
        Map<String, Object> valueMap = nextMap;
        nextMap = null;
        StringBuilder raw = new StringBuilder();

        int iterations = 0;
        while (valueMap == null || valueMap.isEmpty()) {
            iterations++;
            final String line = nextLine == null ? reader.readLine() : nextLine;
            raw.append(line);
            nextLine = null; // ensure that we don't process nextLine again
            if (line == null) {
                return null;
            }

            valueMap = capture(line);
            if ((valueMap == null || valueMap.isEmpty()) && noMatchStrategy.equals(NoMatchStrategy.RAW)) {
                break;
            }
        }

        if (iterations == 0 && nextLine != null) {
            raw.append(nextLine);
        }

        // Read the next line to see if it matches the pattern (in which case we will simply leave it for
        // the next call to nextRecord()) or we will attach it to the previously read record.
        String stackTrace = null;
        final StringBuilder trailingText = new StringBuilder();
        while ((nextLine = reader.readLine()) != null) {
            final Map<String, Object> nextValueMap = capture(nextLine);
            if (nextValueMap.isEmpty() && !noMatchStrategy.equals(NoMatchStrategy.RAW)) {
                // next line did not match. Check if it indicates a Stack Trace. If so, read until
                // the stack trace ends. Otherwise, append the next line to the last field in the record.
                if (isStartOfStackTrace(nextLine)) {
                    stackTrace = readStackTrace(nextLine);
                    raw.append("\n").append(stackTrace);
                    break;
                } else if (noMatchStrategy.equals(NoMatchStrategy.APPEND)) {
                    trailingText.append("\n").append(nextLine);
                    raw.append("\n").append(nextLine);
                }
            } else {
                // The next line matched our pattern.
                nextMap = nextValueMap;
                break;
            }
        }

        return createRecord(valueMap, trailingText, stackTrace, raw.toString(), coerceTypes);
    }

    private Record createRecord(final Map<String, Object> valueMap, final StringBuilder trailingText, final String stackTrace, final String raw, final boolean coerceTypes) {
        final Map<String, Object> converted = new HashMap<>();

        if (valueMap != null && !valueMap.isEmpty()) {

            for (final Map.Entry<String, Object> entry : valueMap.entrySet()) {
                final String fieldName = entry.getKey();
                final Object rawValue = entry.getValue();

                final Object normalizedValue;
                if (rawValue instanceof List) {
                    final List<?> list = (List<?>) rawValue;
                    List<?> nonNullElements = list.stream().filter(Objects::nonNull).collect(Collectors.toList());
                    if (nonNullElements.isEmpty()) {
                        normalizedValue = null;
                    } else if (nonNullElements.size() == 1) {
                        normalizedValue = nonNullElements.get(0).toString();
                    } else {
                        final String[] array = new String[list.size()];
                        for (int i = 0; i < list.size(); i++) {
                            final Object rawObject = list.get(i);
                            array[i] = rawObject == null ? null : rawObject.toString();
                        }
                        normalizedValue = array;
                    }
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
            if (noMatchStrategy.equals(NoMatchStrategy.APPEND) && trailingText.length() > 0) {
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
                        converted.put(lastPopulatedFieldName, value + trailingText.toString());
                    }
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

        return line.indexOf(" ") >= index;
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

    private Map<String, Object> capture(final String log) {
        Map<String, Object> capture = Collections.emptyMap();

        for (final Grok grok : groks) {
            capture = grok.capture(log);
            if (!capture.isEmpty()) {
                break;
            }
        }

        return capture;
    }
}