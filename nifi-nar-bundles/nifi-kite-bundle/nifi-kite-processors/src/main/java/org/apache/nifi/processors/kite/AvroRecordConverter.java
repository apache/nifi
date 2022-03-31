/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.kite;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Scanner;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.IndexedRecord;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Responsible for converting records of one Avro type to another. Supports
 * syntax like "record.field" to unpack fields and will try to do simple type
 * conversion.
 */
public class AvroRecordConverter {
    private final Schema inputSchema;
    private final Schema outputSchema;
    // Store this from output field to input field so we can look up by output.
    private final Map<String, String> fieldMapping;
    private final Locale locale;
    private static final Locale DEFAULT_LOCALE = Locale.getDefault();

    /**
     * @param inputSchema
     *            Schema of input record objects
     * @param outputSchema
     *            Schema of output record objects
     * @param fieldMapping
     *            Map from field name in input record to field name in output
     *            record.
     */
    public AvroRecordConverter(Schema inputSchema, Schema outputSchema,
            Map<String, String> fieldMapping) {
        this(inputSchema, outputSchema, fieldMapping, DEFAULT_LOCALE);
    }

    /**
     * @param inputSchema
     *            Schema of input record objects
     * @param outputSchema
     *            Schema of output record objects
     * @param fieldMapping
     *            Map from field name in input record to field name in output
     *            record.
     * @param locale
     *            Locale to use
     */
    public AvroRecordConverter(Schema inputSchema, Schema outputSchema,
            Map<String, String> fieldMapping, Locale locale) {
        this.inputSchema = inputSchema;
        this.outputSchema = outputSchema;
        // Need to reverse this map.
        this.fieldMapping = Maps
                .newHashMapWithExpectedSize(fieldMapping.size());
        for (Map.Entry<String, String> entry : fieldMapping.entrySet()) {
            this.fieldMapping.put(entry.getValue(), entry.getKey());
        }
        this.locale = locale;
    }

    /**
     * @return Any fields in the output schema that are not mapped or are mapped
     *         by a non-existent input field.
     */
    public Collection<String> getUnmappedFields() {
        List<String> result = Lists.newArrayList();
        for (Field f : outputSchema.getFields()) {
            String fieldName = f.name();
            if (fieldMapping.containsKey(fieldName)) {
                fieldName = fieldMapping.get(fieldName);
            }

            Schema currentSchema = inputSchema;
            while (fieldName.contains(".")) {
                // Recurse down the schema to find the right field.
                int dotIndex = fieldName.indexOf('.');
                String entityName = fieldName.substring(0, dotIndex);
                // Get the schema. In case we had an optional record, choose
                // just the record.
                currentSchema = getNonNullSchema(currentSchema);
                if (currentSchema.getField(entityName) == null) {
                    // Tried to step into a schema that doesn't exist. Break out
                    // of the loop
                    break;
                }
                currentSchema = currentSchema.getField(entityName).schema();
                fieldName = fieldName.substring(dotIndex + 1);
            }
            if (currentSchema == null
                    || getNonNullSchema(currentSchema).getField(fieldName) == null) {
                result.add(f.name());
            }
        }
        return result;
    }

    /**
     * Converts one record to another given a input and output schema plus
     * explicit mappings for certain target fields.
     *
     * @param input
     *            Input record to convert conforming to the inputSchema this
     *            converter was created with.
     * @return Record converted to the outputSchema this converter was created
     *         with.
     * @throws AvroConversionException
     *             When schemas do not match or illegal conversions are
     *             attempted, such as when numeric data fails to parse.
     */
    public Record convert(Record input) throws AvroConversionException {
        Record result = new Record(outputSchema);
        for (Field outputField : outputSchema.getFields()) {
            // Default to matching by name
            String inputFieldName = outputField.name();
            if (fieldMapping.containsKey(outputField.name())) {
                inputFieldName = fieldMapping.get(outputField.name());
            }

            IndexedRecord currentRecord = input;
            Schema currentSchema = getNonNullSchema(inputSchema);
            while (inputFieldName.contains(".")) {
                // Recurse down the schema to find the right field.
                int dotIndex = inputFieldName.indexOf('.');
                String entityName = inputFieldName.substring(0, dotIndex);
                // Get the record object
                Object innerRecord = currentRecord.get(currentSchema.getField(
                        entityName).pos());
                if (innerRecord == null) {
                    // Probably hit a null record here. Just break out of the
                    // loop so that null object will be passed to convertData
                    // below.
                    currentRecord = null;
                    break;
                }
                if (innerRecord != null
                        && !(innerRecord instanceof IndexedRecord)) {
                    throw new AvroConversionException(inputFieldName
                            + " stepped through a non-record");
                }
                currentRecord = (IndexedRecord) innerRecord;

                // Get the schema. In case we had an optional record, choose
                // just the record.
                currentSchema = currentSchema.getField(entityName).schema();
                currentSchema = getNonNullSchema(currentSchema);
                inputFieldName = inputFieldName.substring(dotIndex + 1);
            }

            // Current should now be in the right place to read the record.
            Field f = currentSchema.getField(inputFieldName);
            if (currentRecord == null) {
                // We may have stepped into a null union type and gotten a null
                // result.
                Schema s = null;
                if (f != null) {
                    s = f.schema();
                }
                result.put(outputField.name(),
                        convertData(null, s, outputField.schema()));
            } else {
                result.put(
                        outputField.name(),
                        convertData(currentRecord.get(f.pos()), f.schema(),
                                outputField.schema()));
            }
        }
        return result;
    }

    public Schema getInputSchema() {
        return inputSchema;
    }

    public Schema getOutputSchema() {
        return outputSchema;
    }

    /**
     * Converts the data from one schema to another. If the types are the same,
     * no change will be made, but simple conversions will be attempted for
     * other types.
     *
     * @param content
     *            The data to convert, generally taken from a field in an input
     *            Record.
     * @param inputSchema
     *            The schema of the content object
     * @param outputSchema
     *            The schema to convert to.
     * @return The content object, converted to the output schema.
     * @throws AvroConversionException
     *             When conversion is impossible, either because the output type
     *             is not supported or because numeric data failed to parse.
     */
    private Object convertData(Object content, Schema inputSchema,
            Schema outputSchema) throws AvroConversionException {
        if (content == null) {
            // No conversion can happen here.
            if (supportsNull(outputSchema)) {
                return null;
            }
            throw new AvroConversionException("Output schema " + outputSchema
                    + " does not support null");
        }

        Schema nonNillInput = getNonNullSchema(inputSchema);
        Schema nonNillOutput = getNonNullSchema(outputSchema);
        if (nonNillInput.getType().equals(nonNillOutput.getType())) {
            return content;
        } else {
            if (nonNillOutput.getType() == Schema.Type.STRING) {
                return content.toString();
            }

            // For the non-string cases of these, we will try to convert through
            // string using Scanner to validate types. This means we could
            // return questionable results when a String starts with a number
            // but then contains other content
            Scanner scanner = new Scanner(content.toString());
            scanner.useLocale(locale);
            switch (nonNillOutput.getType()) {
            case LONG:
                if (scanner.hasNextLong()) {
                    return scanner.nextLong();
                } else {
                    throw new AvroConversionException("Cannot convert "
                            + content + " to long");
                }
            case INT:
                if (scanner.hasNextInt()) {
                    return scanner.nextInt();
                } else {
                    throw new AvroConversionException("Cannot convert "
                            + content + " to int");
                }
            case DOUBLE:
                if (scanner.hasNextDouble()) {
                    return scanner.nextDouble();
                } else {
                    throw new AvroConversionException("Cannot convert "
                            + content + " to double");
                }
            case FLOAT:
                if (scanner.hasNextFloat()) {
                    return scanner.nextFloat();
                } else {
                    throw new AvroConversionException("Cannot convert "
                            + content + " to float");
                }
            default:
                throw new AvroConversionException("Cannot convert to type "
                        + nonNillOutput.getType());
            }
        }
    }

    /**
     * If s is a union schema of some type with null, returns that type.
     * Otherwise just return schema itself.
     *
     * Does not handle unions of schemas with anything except null and one type.
     *
     * @param s
     *            Schema to remove nillable from.
     * @return The Schema of the non-null part of a the union, if the input was
     *         a union type. Otherwise returns the input schema.
     */
    protected static Schema getNonNullSchema(Schema s) {
        // Handle the case where s is a union type. Assert that this must be a
        // union that only includes one non-null type.
        if (s.getType() == Schema.Type.UNION) {
            List<Schema> types = s.getTypes();
            boolean foundOne = false;
            Schema result = s;
            for (Schema type : types) {
                if (!type.getType().equals(Schema.Type.NULL)) {
                    Preconditions.checkArgument(foundOne == false,
                            "Cannot handle union of two non-null types");
                    foundOne = true;
                    result = type;
                }
            }
            return result;
        } else {
            return s;
        }
    }

    protected static boolean supportsNull(Schema s) {
        if (s.getType() == Schema.Type.NULL) {
            return true;
        } else if (s.getType() == Schema.Type.UNION) {
            for (Schema type : s.getTypes()) {
                if (type.getType() == Schema.Type.NULL) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Exception thrown when Avro conversion fails.
     */
    public class AvroConversionException extends Exception {
        public AvroConversionException(String string, IOException e) {
            super(string, e);
        }

        public AvroConversionException(String string) {
            super(string);
        }
    }
}
