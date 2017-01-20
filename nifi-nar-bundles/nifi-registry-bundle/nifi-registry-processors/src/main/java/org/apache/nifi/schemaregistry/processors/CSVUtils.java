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
package org.apache.nifi.schemaregistry.processors;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;

/**
 * Various CSV related utility operations relevant to transforming contents of
 * the {@link FlowFile} from/to CSV/AVRO format.
 */
class CSVUtils {

    /**
     *
     */
    public static final Validator CHAR_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            // Allows special, escaped characters as input, which is then un-escaped and converted to a single character.
            // Examples for special characters: \t (or \u0009), \f.
            if (input.length() > 1) {
                input = StringEscapeUtils.unescapeJava(input);
            }

            return new ValidationResult.Builder()
                .subject(subject)
                .input(input)
                .explanation("Only non-null single characters are supported")
                .valid(input.length() == 1 && input.charAt(0) != 0)
                .build();
        }
    };

    /**
     *
     */
    public static GenericRecord read(InputStream record, char delimiter, Schema schema, char quoteChar) {
        return toAvroRecord(convertInputStreamToString(record), delimiter, schema, quoteChar);
    }

    /**
     * Converts CSV {@link String} into Avro record using provided delimiter and
     * schema. The 'quoteChar' is used to ensure that if a delimiter char is in
     * quotes it will not be parsed into a separate filed.
     */
    public static Record toAvroRecord(String record, char delimiter, Schema schema, char quoteChar) {
        Record avroRecord = new GenericData.Record(schema);
        String[] parsedRecord = parseFields(record, delimiter, quoteChar);
        List<Field> fields = schema.getFields();
        if (parsedRecord.length != fields.size()) {
            throw new IllegalStateException("Incompatible schema. Parsed fields count does not match the count of fields from schema. "
                    + "Schema: " + schema.toString(true) + "\n Record: " + record);
        }

        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);

            Type type = field.schema().getType();
            if (Type.INT == type) {
                avroRecord.put(field.name(), Integer.parseInt(parsedRecord[i]));
            } else if (Type.BOOLEAN == type) {
                avroRecord.put(field.name(), Boolean.getBoolean(parsedRecord[i]));
            } else if (Type.DOUBLE == type) {
                avroRecord.put(field.name(), Double.parseDouble(parsedRecord[i]));
            } else if (Type.FLOAT == type) {
                avroRecord.put(field.name(), Float.parseFloat(parsedRecord[i]));
            } else if (Type.LONG == type) {
                avroRecord.put(field.name(), Long.parseLong(parsedRecord[i]));
            } else if (Type.BYTES == type || Type.ARRAY == type || Type.ENUM == type || Type.FIXED == type
                    || Type.MAP == type || Type.NULL == type || Type.RECORD == type || Type.UNION == type) {
                throw new IllegalArgumentException("The field type '" + type + "' is not supported at the moment");
            } else {
                avroRecord.put(field.name(), parsedRecord[i]);
            }

        }
        return avroRecord;
    }

    /**
     * Parses provided record into fields using provided delimiter. The
     * 'quoteChar' is used to ensure that if a delimiter char is in quotes it
     * will not be parsed into a separate filed.
     */
    public static String[] parseFields(InputStream record, char delimiter, char quoteChar) {
        return parseFields(convertInputStreamToString(record), delimiter, quoteChar);
    }

    /**
     * Parses provided record into fields using provided delimiter. The
     * 'quoteChar' is used to ensure that if a delimiter char is in quotes it
     * will not be parsed into a separate filed.
     */
    public static String[] parseFields(String record, char delimiter, char quoteChar) {
        List<String> result = new ArrayList<String>();
        int start = 0;
        boolean inQuotes = false;
        for (int i = 0; i < record.length(); i++) {
            if (record.charAt(i) == quoteChar) {
                inQuotes = !inQuotes;
            }
            boolean atLastChar = (i == record.length() - 1);
            if (atLastChar) {
                result.add(record.substring(start));
            } else if (record.charAt(i) == delimiter && !inQuotes) {
                result.add(record.substring(start, i));
                start = i + 1;
            }
        }
        return result.toArray(new String[] {});
    }

    /**
     * Converts Avro {@link Record} to CSV string using provided delimiter
     */
    public static byte[] fromAvroRecord(GenericRecord record, char delimiter) {
        List<Field> fields = record.getSchema().getFields();
        StringBuilder builder = new StringBuilder();

        String delimiterToUse = "";
        for (Field field : fields) {
            builder.append(delimiterToUse);
            builder.append(record.get(field.name()));
            if (delimiterToUse.length() == 0) {
                delimiterToUse = String.valueOf(delimiter);
            }
        }
        return builder.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     *
     */
    private static String convertInputStreamToString(InputStream record) {
        StringWriter writer = new StringWriter();
        try {
            IOUtils.copy(record, writer, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read InputStream into String", e);
        }
        return writer.toString();
    }

    public static void write(GenericRecord record, char delimiter, OutputStream out) {
        List<Field> fields = record.getSchema().getFields();

        String delimiterToUse = "";
        try {
            for (Field field : fields) {
                out.write(delimiterToUse.getBytes(StandardCharsets.UTF_8));
                out.write(record.get(field.name()).toString().getBytes(StandardCharsets.UTF_8));
                if (delimiterToUse.length() == 0) {
                    delimiterToUse = String.valueOf(delimiter);
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to parse AVRO Record", e);
        }
    }
}
