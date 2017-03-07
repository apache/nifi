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
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
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
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;
import org.codehaus.jackson.node.DoubleNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.LongNode;
import org.codehaus.jackson.node.TextNode;

/**
 * Various CSV related utility operations relevant to transforming contents of
 * the {@link FlowFile} between CSV and AVRO formats.
 */
class CSVUtils {
    /**
     * Provides a {@link Validator} to ensure that provided value is a valid
     * character.
     */
    public static final Validator CHAR_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            // Allows special, escaped characters as input, which is then un-escaped and converted to a single character.
            // Examples for special characters: \t (or \u0009), \f.
            if (input.length() > 1) {
                input = StringEscapeUtils.unescapeJava(input);
            }
            return new ValidationResult.Builder().subject(subject).input(input)
                    .explanation("Only non-null single characters are supported")
                    .valid(input.length() == 1 && input.charAt(0) != 0).build();
        }
    };

    public static GenericRecord read(InputStream record, char delimiter, Schema schema, char quoteChar) {
        Record avroRecord = new GenericData.Record(schema);
        String[] parsedRecord = parseFields(convertInputStreamToString(record), delimiter, quoteChar);
        List<Field> fields = schema.getFields();
        if (parsedRecord.length != fields.size()) {
            throw new IllegalStateException("Incompatible schema. Parsed fields count does not match the count of fields from schema. "
                    + "Schema: " + schema.toString(true) + "\n Record: " + record);
        }

        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            Type type = field.schema().getType();
            updateRecord(field, type, parsedRecord[i], avroRecord);
        }
        return avroRecord;
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
                if (record.charAt(i) == delimiter) {
                    //missing last column value, add NULL
                    result.add(record.substring(start,i));
                    result.add(null);
                } else {
                    result.add(record.substring(start));
                }
            } else if (record.charAt(i) == delimiter && !inQuotes) {
                if (start == i) {
                    //There is no value, so add NULL to indicated the absence of a value for this field.
                    result.add(null);
                } else {
                    result.add(record.substring(start, i));
                }
                start = i + 1;
            }
        }
        return result.toArray(new String[]{});
    }

    /**
     * Writes {@link GenericRecord} as CSV (delimited) record to the
     * {@link OutputStream} using provided delimiter.
     */
    public static void write(GenericRecord record, char delimiter, OutputStream out) {
        List<Field> fields = record.getSchema().getFields();

        String delimiterToUse = "";
        try {
            for (Field field : fields) {
                out.write(delimiterToUse.getBytes(StandardCharsets.UTF_8));
                Object fieldValue = record.get(field.name());
                if (null == fieldValue) {
                    out.write(new byte[0]);
                } else {
                    if (Type.BYTES == field.schema().getType()) {
                        // need to create it from the ByteBuffer it is serialized as.
                        // need to ensure the type is one of the logical ones we support and if so convert it.
                        if(!"decimal".contentEquals(field.getProp("logicalType"))){
                            throw new IllegalArgumentException("The field '" + field.name() + "' has a logical type of '" +
                                    field.getProp("logicalType") + "' that is currently not supported.");
                        }

                        JsonNode rawPrecision = field.getJsonProp("precision");
                        if(null == rawPrecision){
                            throw new IllegalArgumentException("The field '" + field.name() + "' is missing the required precision property");
                        }
                        int precision = rawPrecision.asInt();
                        JsonNode rawScale = field.getJsonProp("scale");
                        int scale = null == rawScale ? 0 : rawScale.asInt();

                        // write out the decimal with the precision and scale.
                        NumberFormat numberFormat = DecimalFormat.getInstance();
                        numberFormat.setGroupingUsed(false);
                        normalizeNumberFormat(numberFormat, scale, precision);
                        String rawValue = new String(((ByteBuffer)fieldValue).array());
                        // raw value needs to be parsed to ensure that BigDecimal will not throw an exception for specific locale
                        rawValue = numberFormat.parse(rawValue).toString();
                        out.write(numberFormat.format(new BigDecimal(rawValue)).getBytes(StandardCharsets.UTF_8));
                    } else {
                        out.write(fieldValue.toString().getBytes(StandardCharsets.UTF_8));
                    }
                }
                if (delimiterToUse.length() == 0) {
                    delimiterToUse = String.valueOf(delimiter);
                }
            }
        } catch (IOException | ParseException e) {
            throw new IllegalStateException("Failed to parse AVRO Record", e);
        }
    }

    /**
     * According to the 1.7.7 spec If a logical type is invalid, for example a
     * decimal with scale greater than its precision,then implementations should
     * ignore the logical type and use the underlying Avro type.
     */
    private static void normalizeNumberFormat(NumberFormat numberFormat, int scale, int precision) {
        if (scale < precision) {
            // write out with the specified precision and scale.
            numberFormat.setMaximumIntegerDigits(precision);
            numberFormat.setMaximumFractionDigits(scale);
            numberFormat.setMinimumFractionDigits(scale);
        }
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

    /**
     *
     */
    private static ByteBuffer encodeLogicalType(final Field field, final String fieldValue) {
        String logicalType = field.getProp("logicalType");
        if (!"decimal".contentEquals(logicalType)) {
            throw new IllegalArgumentException("The field '" + field.name() + "' has a logical type of '" + logicalType
                    + "' that is currently not supported.");
        }

        JsonNode rawPrecision = field.getJsonProp("precision");
        if (null == rawPrecision) {
            throw new IllegalArgumentException("The field '" + field.name() + "' is missing the required precision property");
        }
        int precision = rawPrecision.asInt();
        JsonNode rawScale = field.getJsonProp("scale");
        int scale = null == rawScale ? 0 : rawScale.asInt();

        NumberFormat numberFormat = DecimalFormat.getInstance();
        numberFormat.setGroupingUsed(false);
        normalizeNumberFormat(numberFormat, scale, precision);
        BigDecimal decimal = null == fieldValue ? new BigDecimal(retrieveDefaultFieldValue(field).asText()) : new BigDecimal(fieldValue);
        return ByteBuffer.wrap(numberFormat.format(decimal).getBytes(StandardCharsets.UTF_8));
    }

    /**
     *
     */
    private static JsonNode retrieveDefaultFieldValue(Field field) {
        JsonNode jsonNode = field.defaultValue();
        if (null == jsonNode) {
            throw new IllegalArgumentException("The field '" + field.name() + "' is NULL and there is no default value supplied in the Avro Schema");
        }
        return jsonNode;
    }

    /**
     *
     */
    private static void updateRecord(Field field, Type type, String providedValue, Record avroRecord) {
        if (Type.NULL != type) {
            Object value;
            if (Type.INT == type) {
                value = null == providedValue ? possiblyGetDefaultValue(field, IntNode.class).getIntValue()
                        : Integer.parseInt(providedValue);
                avroRecord.put(field.name(), value);
            } else if (Type.BOOLEAN == type) {
                value = null == providedValue
                        ? possiblyGetDefaultValue(field, BooleanNode.class).getBooleanValue()
                        : Boolean.parseBoolean(providedValue);
                avroRecord.put(field.name(), value);
            } else if (Type.DOUBLE == type) {
                value = null == providedValue ? possiblyGetDefaultValue(field, DoubleNode.class).getDoubleValue()
                        : Double.parseDouble(providedValue);
                avroRecord.put(field.name(), value);
            } else if (Type.FLOAT == type) {
                value = null == providedValue ? possiblyGetDefaultValue(field, DoubleNode.class).getDoubleValue()
                        : Float.parseFloat(providedValue);
                avroRecord.put(field.name(), value);
            } else if (Type.LONG == type) {
                value = null == providedValue ? possiblyGetDefaultValue(field, LongNode.class).getLongValue()
                        : Long.parseLong(providedValue);
                avroRecord.put(field.name(), value);
            } else if (Type.STRING == type) {
                value = null == providedValue ? possiblyGetDefaultValue(field, TextNode.class).getTextValue()
                        : providedValue;
                avroRecord.put(field.name(), value);
            } else if (Type.BYTES == type) {
                value = encodeLogicalType(field, providedValue);
                avroRecord.put(field.name(), value);
            } else if (Type.UNION == type) {
                field.schema().getTypes()
                        .forEach(schema -> updateRecord(field, schema.getType(), providedValue, avroRecord));
            } else if (Type.ARRAY == type || Type.ENUM == type || Type.FIXED == type || Type.MAP == type
                    || Type.NULL == type || Type.RECORD == type) {
                throw new IllegalArgumentException("The field type '" + type + "' is not supported at the moment");
            } else {
                avroRecord.put(field.name(), providedValue);
            }
        }
    }

    /**
     * Check to see if there is a default value to use, if not will throw
     * {@link IllegalArgumentException}
     */
    private static <T extends JsonNode> JsonNode possiblyGetDefaultValue(Field field, Class<T> expectedDefaultType) {
        JsonNode jsonNode = retrieveDefaultFieldValue(field);
        if (field.schema().getType() != Type.UNION && !expectedDefaultType.isAssignableFrom(jsonNode.getClass())) {
            // since we do not support schema evolution here we need to throw an
            // exception here as the data is in error.
            throw new IllegalArgumentException("The field '" + field.name() + "' has a default value that "
                    + "does not match the field type. Field Type is: '" + expectedDefaultType.getName() + "' and the "
                    + "default value type is: '" + field.defaultValue().toString());
        }
        return jsonNode;
    }
}
