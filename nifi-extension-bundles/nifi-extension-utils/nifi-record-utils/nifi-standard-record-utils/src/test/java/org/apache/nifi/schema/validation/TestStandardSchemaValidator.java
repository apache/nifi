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

package org.apache.nifi.schema.validation;

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.EnumDataType;
import org.apache.nifi.serialization.record.validation.DefaultValidationError;
import org.apache.nifi.serialization.record.validation.FieldValidator;
import org.apache.nifi.serialization.record.validation.RecordValidator;
import org.apache.nifi.serialization.record.validation.SchemaValidationResult;
import org.apache.nifi.serialization.record.validation.ValidationError;
import org.apache.nifi.serialization.record.validation.ValidationErrorType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestStandardSchemaValidator {
    private static final int FLOAT_BITS_PRECISION = 24;
    private static final int DOUBLE_BITS_PRECISION = 53;

    private static final Long MAX_PRECISE_WHOLE_IN_FLOAT = Double.valueOf(Math.pow(2, FLOAT_BITS_PRECISION)).longValue();
    private static final Long MAX_PRECISE_WHOLE_IN_DOUBLE = Double.valueOf(Math.pow(2, DOUBLE_BITS_PRECISION)).longValue();

    private static final Set<RecordFieldType> NUMERIC_TYPES = new HashSet<>(Arrays.asList(
            RecordFieldType.BYTE,
            RecordFieldType.SHORT,
            RecordFieldType.INT,
            RecordFieldType.LONG,
            RecordFieldType.BIGINT,
            RecordFieldType.FLOAT,
            RecordFieldType.DOUBLE,
            RecordFieldType.DECIMAL
    ));

    @Test
    public void testValidateCorrectSimpleTypesStrictValidation() {
        final List<RecordField> fields = new ArrayList<>();
        for (final RecordFieldType fieldType : RecordFieldType.values()) {
            if (fieldType == RecordFieldType.CHOICE) {
                final List<DataType> possibleTypes = new ArrayList<>();
                possibleTypes.add(RecordFieldType.INT.getDataType());
                possibleTypes.add(RecordFieldType.LONG.getDataType());

                fields.add(new RecordField(fieldType.name().toLowerCase(), fieldType.getChoiceDataType(possibleTypes)));
            } else if (fieldType == RecordFieldType.MAP) {
                fields.add(new RecordField(fieldType.name().toLowerCase(), fieldType.getMapDataType(RecordFieldType.INT.getDataType())));
            } else {
                fields.add(new RecordField(fieldType.name().toLowerCase(), fieldType.getDataType()));
            }

            if (NUMERIC_TYPES.contains(fieldType)) {
                for (final RecordFieldType narrowType : fieldType.getNarrowDataTypes()) {
                    fields.add(new RecordField(narrowType.name().toLowerCase() + "_as_" + fieldType.name().toLowerCase(), fieldType.getDataType()));
                }
            }
        }

        final long time = 1483290000000L;

        final Map<String, Object> intMap = new LinkedHashMap<>();
        intMap.put("height", 48);
        intMap.put("width", 96);

        List<RecordField> mapRecordFields = new ArrayList<>();
        RecordField mapRecordField = new RecordField("mapRecord", RecordFieldType.MAP.getMapDataType(RecordFieldType.INT.getDataType()));
        mapRecordFields.add(mapRecordField);
        fields.add(mapRecordField);
        RecordSchema mapRecordSchema = new SimpleRecordSchema(mapRecordFields);
        MapRecord mapRecord = new MapRecord(mapRecordSchema, intMap);

        final RecordSchema schema = new SimpleRecordSchema(fields);
        final Map<String, Object> valueMap = new LinkedHashMap<>();
        valueMap.put("string", "string");
        valueMap.put("boolean", true);
        valueMap.put("byte", (byte) 1);
        valueMap.put("char", 'c');
        valueMap.put("short", (short) 8);
        valueMap.put("int", 9);
        valueMap.put("bigint", BigInteger.valueOf(8L));
        valueMap.put("decimal", BigDecimal.valueOf(8.1D));
        valueMap.put("long", 8L);
        valueMap.put("float", 8.0F);
        valueMap.put("double", 8.0D);
        valueMap.put("date", new Date(time));
        valueMap.put("time", new Time(time));
        valueMap.put("timestamp", new Timestamp(time));
        valueMap.put("record", null);
        valueMap.put("array", null);
        valueMap.put("choice", 48L);
        valueMap.put("map", intMap);
        valueMap.put("mapRecord", mapRecord);

        valueMap.put("byte_as_short", (byte) 8);

        valueMap.put("short_as_int", (short) 8);
        valueMap.put("byte_as_int", (byte) 8);

        valueMap.put("int_as_long", 9);
        valueMap.put("short_as_long", (short) 8);
        valueMap.put("byte_as_long", (byte) 1);

        valueMap.put("byte_as_bigint", (byte) 8);
        valueMap.put("short_as_bigint", (short) 8);
        valueMap.put("int_as_bigint", 8);
        valueMap.put("long_as_bigint", 8L);

        valueMap.put("float_as_double", 8.0F);

        valueMap.put("float_as_decimal", 8.0F);
        valueMap.put("double_as_decimal", 8.0D);

        final Record record = new MapRecord(schema, valueMap);

        final SchemaValidationContext validationContext = new SchemaValidationContext(schema, false, true);
        final StandardSchemaValidator validator = new StandardSchemaValidator(validationContext);

        final SchemaValidationResult result = validator.validate(record);
        assertTrue(result.isValid());
        assertNotNull(result.getValidationErrors());
        assertTrue(result.getValidationErrors().isEmpty());
    }

    @Test
    public void testStringDoesNotAllowNarrowTypesWhenStrictValidation() {
        whenValueIsNotAcceptedAsDataTypeThenConsideredAsInvalid(12345, RecordFieldType.STRING);
    }

    @Test
    public void testDoubleWithinFloatRangeIsConsideredAsValid() {
        whenValueIsAcceptedAsDataTypeThenConsideredAsValid(1.5191525220870972D, RecordFieldType.FLOAT);
    }

    @Test
    public void testByteIsConsideredToBeValidFloatingPoint() {
        whenValueIsAcceptedAsDataTypeThenConsideredAsValid((byte) 9, RecordFieldType.FLOAT);
        whenValueIsAcceptedAsDataTypeThenConsideredAsValid((byte) 9, RecordFieldType.DOUBLE);
        whenValueIsAcceptedAsDataTypeThenConsideredAsValid((byte) 9, RecordFieldType.DECIMAL);
    }

    @Test
    public void testShortIsConsideredToBeValidFloatingPoint() {
        whenValueIsAcceptedAsDataTypeThenConsideredAsValid((short) 9, RecordFieldType.FLOAT);
        whenValueIsAcceptedAsDataTypeThenConsideredAsValid((short) 9, RecordFieldType.DOUBLE);
        whenValueIsAcceptedAsDataTypeThenConsideredAsValid((short) 9, RecordFieldType.DECIMAL);
    }

    @Test
    public void testIntegerWithinRangeIsConsideredToBeValidFloatingPoint() {
        whenValueIsAcceptedAsDataTypeThenConsideredAsValid(MAX_PRECISE_WHOLE_IN_FLOAT.intValue(), RecordFieldType.FLOAT);
        whenValueIsAcceptedAsDataTypeThenConsideredAsValid(Integer.MAX_VALUE, RecordFieldType.DOUBLE);
        whenValueIsAcceptedAsDataTypeThenConsideredAsValid(Integer.MAX_VALUE, RecordFieldType.DECIMAL);
    }


    @Test
    public void testLongWithinRangeIsConsideredToBeValidFloatingPoint() {
        whenValueIsAcceptedAsDataTypeThenConsideredAsValid(MAX_PRECISE_WHOLE_IN_FLOAT, RecordFieldType.FLOAT);
        whenValueIsAcceptedAsDataTypeThenConsideredAsValid(MAX_PRECISE_WHOLE_IN_DOUBLE, RecordFieldType.DOUBLE);
        whenValueIsAcceptedAsDataTypeThenConsideredAsValid(Long.MAX_VALUE, RecordFieldType.DECIMAL);
    }

    @Test
    public void testBigintWithinRangeIsConsideredToBeValidFloatingPoint() {
        whenValueIsAcceptedAsDataTypeThenConsideredAsValid(BigInteger.valueOf(5L), RecordFieldType.FLOAT);
        whenValueIsAcceptedAsDataTypeThenConsideredAsValid(BigInteger.valueOf(5L), RecordFieldType.DOUBLE);
        whenValueIsAcceptedAsDataTypeThenConsideredAsValid(BigInteger.valueOf(Long.MAX_VALUE), RecordFieldType.DECIMAL);
    }

    @Test
    public void testBigintOutsideRangeIsConsideredAsInvalid() {
        whenValueIsNotAcceptedAsDataTypeThenConsideredAsInvalid(String.join("", Collections.nCopies(100, "1")), RecordFieldType.FLOAT);
        whenValueIsNotAcceptedAsDataTypeThenConsideredAsInvalid(String.join("", Collections.nCopies(100, "1")), RecordFieldType.DOUBLE);
    }

    @Test
    public void testDoubleAboveFloatRangeIsConsideredAsInvalid() {
        final double aboveFloatRange = Float.MAX_VALUE * 1.1;
        whenValueIsNotAcceptedAsDataTypeThenConsideredAsInvalid(aboveFloatRange, RecordFieldType.FLOAT);
    }

    @Test
    public void testDoubleBelowFloatRangeIsConsideredAsInvalid() {
        final double belowFloatRange = Float.MAX_VALUE * -1.1;
        whenValueIsNotAcceptedAsDataTypeThenConsideredAsInvalid(belowFloatRange, RecordFieldType.FLOAT);
    }

    @Test
    public void testValidateWrongButCoerceableType() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> valueMap = new LinkedHashMap<>();
        valueMap.put("id", 1);
        Record record = new MapRecord(schema, valueMap);

        final SchemaValidationContext strictValidationContext = new SchemaValidationContext(schema, false, true);
        final SchemaValidationContext lenientValidationContext = new SchemaValidationContext(schema, false, false);

        // Validate with correct type of int and a strict validation
        StandardSchemaValidator validator = new StandardSchemaValidator(strictValidationContext);
        SchemaValidationResult result = validator.validate(record);
        assertTrue(result.isValid());
        assertNotNull(result.getValidationErrors());
        assertTrue(result.getValidationErrors().isEmpty());

        // Validate with correct type of int and a lenient validation
        validator = new StandardSchemaValidator(lenientValidationContext);
        result = validator.validate(record);
        assertTrue(result.isValid());
        assertNotNull(result.getValidationErrors());
        assertTrue(result.getValidationErrors().isEmpty());


        // Update Map to set value to a String that is coerceable to an int
        valueMap.put("id", "1");
        record = new MapRecord(schema, valueMap);


        // Validate with incorrect type of string and a strict validation
        validator = new StandardSchemaValidator(strictValidationContext);
        result = validator.validate(record);
        assertFalse(result.isValid());
        final Collection<ValidationError> validationErrors = result.getValidationErrors();
        assertEquals(1, validationErrors.size());

        final ValidationError validationError = validationErrors.iterator().next();
        assertEquals("/id", validationError.getFieldName().get());

        // Validate with incorrect type of string and a lenient validation
        validator = new StandardSchemaValidator(lenientValidationContext);
        result = validator.validate(record);
        assertTrue(result.isValid());
        assertNotNull(result.getValidationErrors());
        assertTrue(result.getValidationErrors().isEmpty());
    }

    @Test
    public void testMissingRequiredField() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType(), false));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> valueMap = new LinkedHashMap<>();
        valueMap.put("id", 1);
        final Record record = new MapRecord(schema, valueMap, false, false);

        final SchemaValidationContext allowExtraFieldsContext = new SchemaValidationContext(schema, true, true);

        StandardSchemaValidator validator = new StandardSchemaValidator(allowExtraFieldsContext);
        SchemaValidationResult result = validator.validate(record);
        assertFalse(result.isValid());
        assertNotNull(result.getValidationErrors());

        final ValidationError error = result.getValidationErrors().iterator().next();
        assertEquals("/name", error.getFieldName().get());
    }

    @Test
    public void testMissingNullableField() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> valueMap = new LinkedHashMap<>();
        valueMap.put("id", 1);
        Record record = new MapRecord(schema, valueMap, false, false);

        final SchemaValidationContext allowExtraFieldsContext = new SchemaValidationContext(schema, true, true);

        StandardSchemaValidator validator = new StandardSchemaValidator(allowExtraFieldsContext);
        SchemaValidationResult result = validator.validate(record);
        assertTrue(result.isValid());
        assertNotNull(result.getValidationErrors());
        assertTrue(result.getValidationErrors().isEmpty());
    }

    @Test
    public void testExtraFields() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> valueMap = new LinkedHashMap<>();
        valueMap.put("id", 1);
        valueMap.put("name", "John Doe");
        Record record = new MapRecord(schema, valueMap, false, false);

        final SchemaValidationContext allowExtraFieldsContext = new SchemaValidationContext(schema, true, true);
        final SchemaValidationContext forbidExtraFieldsContext = new SchemaValidationContext(schema, false, false);

        StandardSchemaValidator validator = new StandardSchemaValidator(allowExtraFieldsContext);
        SchemaValidationResult result = validator.validate(record);
        assertTrue(result.isValid());
        assertNotNull(result.getValidationErrors());
        assertTrue(result.getValidationErrors().isEmpty());

        validator = new StandardSchemaValidator(forbidExtraFieldsContext);
        result = validator.validate(record);
        assertFalse(result.isValid());
        assertNotNull(result.getValidationErrors());
        final Collection<ValidationError> validationErrors = result.getValidationErrors();
        assertEquals(1, validationErrors.size());
        final ValidationError validationError = validationErrors.iterator().next();
        assertEquals("/name", validationError.getFieldName().get());
    }


    @Test
    public void testInvalidEmbeddedField() {
        final List<RecordField> accountFields = new ArrayList<>();
        accountFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        accountFields.add(new RecordField("balance", RecordFieldType.DOUBLE.getDataType()));
        final RecordSchema accountSchema = new SimpleRecordSchema(accountFields);

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("account", RecordFieldType.RECORD.getRecordDataType(accountSchema)));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> accountValues = new HashMap<>();
        accountValues.put("name", "account-1");
        accountValues.put("balance", "123.45");
        final Record accountRecord = new MapRecord(accountSchema, accountValues);

        final Map<String, Object> valueMap = new LinkedHashMap<>();
        valueMap.put("id", 1);
        valueMap.put("account", accountRecord);
        Record record = new MapRecord(schema, valueMap, false, false);

        final SchemaValidationContext strictValidationContext = new SchemaValidationContext(schema, false, true);
        final SchemaValidationContext lenientValidationContext = new SchemaValidationContext(schema, false, false);

        StandardSchemaValidator validator = new StandardSchemaValidator(strictValidationContext);
        SchemaValidationResult result = validator.validate(record);
        assertFalse(result.isValid());
        assertNotNull(result.getValidationErrors());
        assertEquals(1, result.getValidationErrors().size());
        final ValidationError validationError = result.getValidationErrors().iterator().next();
        assertEquals("/account/balance", validationError.getFieldName().get());


        validator = new StandardSchemaValidator(lenientValidationContext);
        result = validator.validate(record);
        assertTrue(result.isValid());
        assertNotNull(result.getValidationErrors());
        assertTrue(result.getValidationErrors().isEmpty());
    }

    @Test
    public void testEnumValidation() {
        List<String> enums = List.of("X", "Y", "Z");
        EnumDataType enumDataType = new EnumDataType(enums);
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("enum_field", enumDataType));
        final RecordSchema schema = new SimpleRecordSchema(fields);
        final SchemaValidationContext strictValidationContext = new SchemaValidationContext(schema, false, true);
        final StandardSchemaValidator validator = new StandardSchemaValidator(strictValidationContext);

        List<Record> records = enums.stream().map(e -> new MapRecord(schema, Map.of("enum_field", e)))
                .collect(Collectors.toList());

        records.forEach(record -> {
            SchemaValidationResult result = validator.validate(record);
            assertTrue(result.isValid());
            assertNotNull(result.getValidationErrors());
            assertTrue(result.getValidationErrors().isEmpty());
        });
    }

    @Test
    public void testInvalidArrayValue() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("numbers", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType())));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> valueMap = new LinkedHashMap<>();
        valueMap.put("id", 1);
        valueMap.put("numbers", new Object[] {1, "2", "3"});
        Record record = new MapRecord(schema, valueMap, false, false);

        final SchemaValidationContext strictValidationContext = new SchemaValidationContext(schema, false, true);
        final SchemaValidationContext lenientValidationContext = new SchemaValidationContext(schema, false, false);

        StandardSchemaValidator validator = new StandardSchemaValidator(strictValidationContext);
        SchemaValidationResult result = validator.validate(record);
        assertFalse(result.isValid());
        assertNotNull(result.getValidationErrors());
        assertEquals(1, result.getValidationErrors().size());
        final ValidationError validationError = result.getValidationErrors().iterator().next();
        assertEquals("/numbers", validationError.getFieldName().get());

        validator = new StandardSchemaValidator(lenientValidationContext);
        result = validator.validate(record);
        assertTrue(result.isValid());
        assertNotNull(result.getValidationErrors());
        assertTrue(result.getValidationErrors().isEmpty());
    }

    @Test
    public void testFieldValidatorIsApplied() {
        final AtomicBoolean invoked = new AtomicBoolean(false);
        final FieldValidator validator = new FieldValidator() {
            @Override
            public Collection<ValidationError> validate(final RecordField field, final String path, final Object value) {
                invoked.set(true);
                return List.of(DefaultValidationError.builder()
                        .fieldName(path)
                        .inputValue(value)
                        .type(ValidationErrorType.INVALID_FIELD)
                        .explanation("value must be 'pass'")
                        .build());
            }

            @Override
            public String getDescription() {
                return "validator for testFieldValidatorIsApplied";
            }
        };

        final RecordField recordField = new RecordField("value", RecordFieldType.STRING.getDataType(), null, Collections.emptySet(), false, List.of(validator));
        final RecordSchema schema = new SimpleRecordSchema(List.of(recordField));
        final MapRecord record = new MapRecord(schema, Map.of("value", "fail"));

        final StandardSchemaValidator validatorService = new StandardSchemaValidator(new SchemaValidationContext(schema, false, true));
        final SchemaValidationResult result = validatorService.validate(record);

        assertTrue(invoked.get());
        assertFalse(result.isValid());
        final ValidationError validationError = result.getValidationErrors().iterator().next();
        assertEquals(ValidationErrorType.INVALID_FIELD, validationError.getType());
        assertEquals("/value", validationError.getFieldName().orElse(""));
    }

    @Test
    public void testFieldValidatorNotInvokedWhenValueNull() {
        final AtomicBoolean invoked = new AtomicBoolean(false);
        final FieldValidator validator = new FieldValidator() {
            @Override
            public Collection<ValidationError> validate(final RecordField field, final String path, final Object value) {
                invoked.set(true);
                return List.of(DefaultValidationError.builder()
                        .fieldName(path)
                        .inputValue(value)
                        .type(ValidationErrorType.INVALID_FIELD)
                        .explanation("should not be invoked")
                        .build());
            }

            @Override
            public String getDescription() {
                return "validator for testFieldValidatorNotInvokedWhenValueNull";
            }
        };

        final RecordField recordField = new RecordField("value", RecordFieldType.STRING.getDataType(), null, Collections.emptySet(), true, List.of(validator));
        final RecordSchema schema = new SimpleRecordSchema(List.of(recordField));
        final Map<String, Object> values = new HashMap<>();
        values.put("value", null);
        final MapRecord record = new MapRecord(schema, values);

        final StandardSchemaValidator validatorService = new StandardSchemaValidator(new SchemaValidationContext(schema, false, true));
        final SchemaValidationResult result = validatorService.validate(record);

        assertFalse(invoked.get());
        assertTrue(result.isValid());
    }

    @Test
    public void testRecordValidatorIsApplied() {
        final RecordField recordField = new RecordField("value", RecordFieldType.STRING.getDataType());
        final SimpleRecordSchema schema = new SimpleRecordSchema(List.of(recordField));

        final RecordValidator recordValidator = new RecordValidator() {
            @Override
            public Collection<ValidationError> validate(final Record record, final RecordSchema recordSchema, final String fieldPath) {
                return List.of(DefaultValidationError.builder()
                        .fieldName(fieldPath + "/value")
                        .inputValue(record.getValue("value"))
                        .type(ValidationErrorType.INVALID_FIELD)
                        .explanation("value must equal 'expected'")
                        .build());
            }

            @Override
            public String getDescription() {
                return "record validator for testRecordValidatorIsApplied";
            }
        };
        schema.setRecordValidators(List.of(recordValidator));

        final MapRecord record = new MapRecord(schema, Map.of("value", "actual"));
        final StandardSchemaValidator validatorService = new StandardSchemaValidator(new SchemaValidationContext(schema, true, true));
        final SchemaValidationResult result = validatorService.validate(record);

        assertFalse(result.isValid());
        final ValidationError validationError = result.getValidationErrors().iterator().next();
        assertEquals("/value", validationError.getFieldName().orElse(""));
        assertEquals(ValidationErrorType.INVALID_FIELD, validationError.getType());
    }

    private void whenValueIsAcceptedAsDataTypeThenConsideredAsValid(final Object value, final RecordFieldType schemaDataType) {
        final SchemaValidationResult result = whenSingleValueIsTested(value, schemaDataType);
        thenSingleValueIsValid(result);
    }

    private void whenValueIsNotAcceptedAsDataTypeThenConsideredAsInvalid(final Object value, final RecordFieldType schemaDataType) {
        final SchemaValidationResult result = whenSingleValueIsTested(value, schemaDataType);
        thenSingleValueIsInvalid(result);
    }

    private SchemaValidationResult whenSingleValueIsTested(final Object value, final RecordFieldType schemaDataType) {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("test", schemaDataType.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);
        final Map<String, Object> valueMap = new LinkedHashMap<>();
        valueMap.put("test", value);

        final Record record = new MapRecord(schema, valueMap);
        final SchemaValidationContext validationContext = new SchemaValidationContext(schema, false, true);
        final StandardSchemaValidator validator = new StandardSchemaValidator(validationContext);

        return validator.validate(record);
    }

    private void thenSingleValueIsValid(SchemaValidationResult result) {
        assertTrue(result.isValid());
        assertNotNull(result.getValidationErrors());
        assertTrue(result.getValidationErrors().isEmpty());
    }

    private void thenSingleValueIsInvalid(SchemaValidationResult result) {
        assertFalse(result.isValid());

        final Collection<ValidationError> validationErrors = result.getValidationErrors();
        assertEquals(1, validationErrors.size());

        final ValidationError validationError = validationErrors.iterator().next();
        assertEquals("/test", validationError.getFieldName().get());
        assertEquals(ValidationErrorType.INVALID_FIELD, validationError.getType());
    }
}
