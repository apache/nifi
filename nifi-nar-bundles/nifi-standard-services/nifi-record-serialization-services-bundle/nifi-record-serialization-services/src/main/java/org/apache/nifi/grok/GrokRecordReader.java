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
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import io.thekraken.grok.api.Grok;
import io.thekraken.grok.api.GrokUtils;
import io.thekraken.grok.api.Match;

public class GrokRecordReader implements RecordReader {
    private final BufferedReader reader;
    private final Grok grok;
    private final Map<String, DataType> fieldTypeOverrides;

    private String nextLine;
    private RecordSchema schema;

    static final String STACK_TRACE_COLUMN_NAME = "STACK_TRACE";
    private static final Pattern STACK_TRACE_PATTERN = Pattern.compile(
        "^\\s*(?:(?:    |\\t)+at )|"
            + "(?:(?:    |\\t)+\\[CIRCULAR REFERENCE\\:)|"
            + "(?:Caused by\\: )|"
            + "(?:Suppressed\\: )|"
            + "(?:\\s+... \\d+ (?:more|common frames? omitted)$)");

    private static final FastDateFormat TIME_FORMAT_DATE;
    private static final FastDateFormat TIME_FORMAT_TIME;
    private static final FastDateFormat TIME_FORMAT_TIMESTAMP;

    static {
        final TimeZone gmt = TimeZone.getTimeZone("GMT");
        TIME_FORMAT_DATE = FastDateFormat.getInstance("yyyy-MM-dd", gmt);
        TIME_FORMAT_TIME = FastDateFormat.getInstance("HH:mm:ss", gmt);
        TIME_FORMAT_TIMESTAMP = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", gmt);
    }

    public GrokRecordReader(final InputStream in, final Grok grok, final Map<String, DataType> fieldTypeOverrides) {
        this.reader = new BufferedReader(new InputStreamReader(in));
        this.grok = grok;
        this.fieldTypeOverrides = fieldTypeOverrides;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public Record nextRecord() throws IOException, MalformedRecordException {
        final String line = nextLine == null ? reader.readLine() : nextLine;
        nextLine = null; // ensure that we don't process nextLine again
        if (line == null) {
            return null;
        }

        final RecordSchema schema = getSchema();

        final Match match = grok.match(line);
        match.captures();
        final Map<String, Object> valueMap = match.toMap();
        if (valueMap.isEmpty()) {   // We were unable to match the pattern so return an empty Object array.
            return new MapRecord(schema, Collections.emptyMap());
        }

        // Read the next line to see if it matches the pattern (in which case we will simply leave it for
        // the next call to nextRecord()) or we will attach it to the previously read record.
        String stackTrace = null;
        final StringBuilder toAppend = new StringBuilder();
        while ((nextLine = reader.readLine()) != null) {
            final Match nextLineMatch = grok.match(nextLine);
            nextLineMatch.captures();
            final Map<String, Object> nextValueMap = nextLineMatch.toMap();
            if (nextValueMap.isEmpty()) {
                // next line did not match. Check if it indicates a Stack Trace. If so, read until
                // the stack trace ends. Otherwise, append the next line to the last field in the record.
                if (isStartOfStackTrace(nextLine)) {
                    stackTrace = readStackTrace(nextLine);
                    break;
                } else {
                    toAppend.append("\n").append(nextLine);
                }
            } else {
                // The next line matched our pattern.
                break;
            }
        }

        try {
            final List<DataType> fieldTypes = schema.getDataTypes();
            final Map<String, Object> values = new HashMap<>(fieldTypes.size());

            for (final String fieldName : schema.getFieldNames()) {
                final Object value = valueMap.get(fieldName);
                if (value == null) {
                    values.put(fieldName, null);
                    continue;
                }

                final DataType fieldType = schema.getDataType(fieldName).orElse(null);
                final Object converted = convert(fieldType, value.toString());
                values.put(fieldName, converted);
            }

            final String lastFieldBeforeStackTrace = schema.getFieldNames().get(schema.getFieldCount() - 2);
            if (toAppend.length() > 0) {
                final Object existingValue = values.get(lastFieldBeforeStackTrace);
                final String updatedValue = existingValue == null ? toAppend.toString() : existingValue + toAppend.toString();
                values.put(lastFieldBeforeStackTrace, updatedValue);
            }

            values.put(STACK_TRACE_COLUMN_NAME, stackTrace);

            return new MapRecord(schema, values);
        } catch (final Exception e) {
            throw new MalformedRecordException("Found invalid log record and will skip it. Record: " + line, e);
        }
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


    protected Object convert(final DataType fieldType, final String string) {
        if (fieldType == null) {
            return string;
        }
        switch (fieldType.getFieldType()) {
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
                    Date date = TIME_FORMAT_DATE.parse(string);
                    return new java.sql.Date(date.getTime());
                } catch (ParseException e) {
                    return null;
                }
            case TIME:
                if (string.length() == 0) {
                    return null;
                }
                try {
                    Date date = TIME_FORMAT_TIME.parse(string);
                    return new java.sql.Time(date.getTime());
                } catch (ParseException e) {
                    return null;
                }
            case TIMESTAMP:
                if (string.length() == 0) {
                    return null;
                }
                try {
                    Date date = TIME_FORMAT_TIMESTAMP.parse(string);
                    return new java.sql.Timestamp(date.getTime());
                } catch (ParseException e) {
                    return null;
                }
            case STRING:
            default:
                return string;
        }
    }


    @Override
    public RecordSchema getSchema() {
        if (schema != null) {
            return schema;
        }

        final List<RecordField> fields = new ArrayList<>();

        String grokExpression = grok.getOriginalGrokPattern();
        while (grokExpression.length() > 0) {
            final Matcher matcher = GrokUtils.GROK_PATTERN.matcher(grokExpression);
            if (matcher.find()) {
                final Map<String, String> namedGroups = GrokUtils.namedGroups(matcher, grokExpression);
                final String fieldName = namedGroups.get("subname");

                DataType dataType = fieldTypeOverrides.get(fieldName);
                if (dataType == null) {
                    dataType = RecordFieldType.STRING.getDataType();
                }

                final RecordField recordField = new RecordField(fieldName, dataType);
                fields.add(recordField);

                if (grokExpression.length() > matcher.end() + 1) {
                    grokExpression = grokExpression.substring(matcher.end() + 1);
                } else {
                    break;
                }
            }
        }

        fields.add(new RecordField(STACK_TRACE_COLUMN_NAME, RecordFieldType.STRING.getDataType()));

        schema = new SimpleRecordSchema(fields);
        return schema;
    }

}
