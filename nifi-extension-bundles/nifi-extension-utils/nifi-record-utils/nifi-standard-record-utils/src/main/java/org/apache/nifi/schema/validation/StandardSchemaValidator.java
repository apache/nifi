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

import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.EnumDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.serialization.record.validation.FieldValidator;
import org.apache.nifi.serialization.record.validation.RecordSchemaValidator;
import org.apache.nifi.serialization.record.validation.RecordValidator;
import org.apache.nifi.serialization.record.validation.SchemaValidationResult;
import org.apache.nifi.serialization.record.validation.ValidationError;
import org.apache.nifi.serialization.record.validation.ValidationErrorType;

import java.math.BigInteger;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class StandardSchemaValidator implements RecordSchemaValidator {
    private final SchemaValidationContext validationContext;

    public StandardSchemaValidator(final SchemaValidationContext validationContext) {
        this.validationContext = validationContext;
    }

    @Override
    public SchemaValidationResult validate(final Record record) {
        return validate(record, validationContext.getSchema(), "");
    }

    private SchemaValidationResult validate(final Record record, final RecordSchema schema, final String fieldPrefix) {
        // Ensure that for every field in the schema, the type is correct (if we care) and that
        // a value is present (unless it is nullable).
        final StandardSchemaValidationResult result = new StandardSchemaValidationResult();

        for (final RecordField field : schema.getFields()) {
            final Object rawValue = record.getValue(field);

            // If there is no value, then it is always valid unless the field is required.
            if (rawValue == null) {
                if (!field.isNullable() && field.getDefaultValue() == null) {
                    result.addValidationError(new StandardValidationError(concat(fieldPrefix, field), ValidationErrorType.MISSING_FIELD, "Field is required"));
                }

                continue;
            }

            // Check that the type is correct.
            final DataType dataType = field.getDataType();
            if (validationContext.isStrictTypeChecking()) {
                if (!isTypeCorrect(rawValue, dataType)) {
                    result.addValidationError(new StandardValidationError(concat(fieldPrefix, field), rawValue, ValidationErrorType.INVALID_FIELD,
                        "Value is of type " + classNameOrNull(rawValue) + " but was expected to be of type " + dataType));

                    continue;
                }
            } else {
                // Use a lenient type check. This will be true if, for instance, a value is the String "123" and should be an integer
                // but will be false if the value is "123" and should be an Array or Record.
                if (!DataTypeUtils.isCompatibleDataType(rawValue, dataType)) {
                    result.addValidationError(new StandardValidationError(concat(fieldPrefix, field), rawValue, ValidationErrorType.INVALID_FIELD,
                        "Value is of type " + classNameOrNull(rawValue) + " but was expected to be of type " + dataType));

                    continue;
                }
            }

            // If the field type is RECORD, or if the field type is a CHOICE that allows for a RECORD and the value is a RECORD, then we
            // need to dig into each of the sub-fields. To do this, we first need to determine the 'canonical data type'.
            final DataType canonicalDataType = getCanonicalDataType(dataType, rawValue, result, fieldPrefix, field);
            if (canonicalDataType == null) {
                continue;
            }

            final String fieldPath = concat(fieldPrefix, field);
            applyFieldValidators(field, fieldPath, rawValue, result);

            // Now that we have the 'canonical data type', we check if it is a Record. If so, we need to validate each sub-field.
            verifyComplexType(dataType, rawValue, result, fieldPrefix, field);
        }

        if (!validationContext.isExtraFieldAllowed()) {
            for (final String fieldName : record.getRawFieldNames()) {
                if (!schema.getDataType(fieldName).isPresent()) {
                    result.addValidationError(new StandardValidationError(fieldPrefix + "/" + fieldName, ValidationErrorType.EXTRA_FIELD, "Field is not present in the schema"));
                }
            }
        }

        applyRecordValidators(record, schema, fieldPrefix, result);

        return result;
    }

    private void applyFieldValidators(final RecordField field, final String fieldPath, final Object value, final StandardSchemaValidationResult result) {
        if (value == null) {
            return;
        }

        for (final FieldValidator validator : field.getFieldValidators()) {
            final Collection<ValidationError> errors = validator.validate(field, fieldPath, value);
            if (errors == null || errors.isEmpty()) {
                continue;
            }

            for (final ValidationError validationError : errors) {
                result.addValidationError(validationError);
            }
        }
    }

    private void applyRecordValidators(final Record record, final RecordSchema schema, final String fieldPath, final StandardSchemaValidationResult result) {
        final List<RecordValidator> recordValidators = schema.getRecordValidators();
        if (recordValidators.isEmpty()) {
            return;
        }

        for (final RecordValidator recordValidator : recordValidators) {
            final Collection<ValidationError> validationErrors = recordValidator.validate(record, schema, fieldPath);
            if (validationErrors == null || validationErrors.isEmpty()) {
                continue;
            }

            for (final ValidationError validationError : validationErrors) {
                result.addValidationError(validationError);
            }
        }
    }

    private void verifyComplexType(final DataType dataType, final Object rawValue, final StandardSchemaValidationResult result, final String fieldPrefix, final RecordField field) {
        // If the field type is RECORD, or if the field type is a CHOICE that allows for a RECORD and the value is a RECORD, then we
        // need to dig into each of the sub-fields. To do this, we first need to determine the 'canonical data type'.
        final DataType canonicalDataType = getCanonicalDataType(dataType, rawValue, result, fieldPrefix, field);
        if (canonicalDataType == null) {
            return;
        }

        // Now that we have the 'canonical data type', we check if it is a Record. If so, we need to validate each sub-field.
        if (canonicalDataType.getFieldType() == RecordFieldType.RECORD) {
            verifyChildRecord(canonicalDataType, rawValue, dataType, result, field, fieldPrefix);
        }

        if (canonicalDataType.getFieldType() == RecordFieldType.ARRAY) {
            final ArrayDataType arrayDataType = (ArrayDataType) canonicalDataType;
            final DataType elementType = arrayDataType.getElementType();
            final Object[] arrayObject = (Object[]) rawValue;

            int i = 0;
            for (final Object arrayValue : arrayObject) {
                verifyComplexType(elementType, arrayValue, result, fieldPrefix + "[" + i + "]", field);
                i++;
            }
        }
    }

    private DataType getCanonicalDataType(final DataType dataType, final Object rawValue, final StandardSchemaValidationResult result, final String fieldPrefix, final RecordField field) {
        final RecordFieldType fieldType = dataType.getFieldType();
        final DataType canonicalDataType;
        if (fieldType == RecordFieldType.CHOICE) {
            canonicalDataType = DataTypeUtils.chooseDataType(rawValue, (ChoiceDataType) dataType);

            if (canonicalDataType == null) {
                result.addValidationError(new StandardValidationError(concat(fieldPrefix, field), rawValue, ValidationErrorType.INVALID_FIELD,
                    "Value is of type " + classNameOrNull(rawValue) + " but was expected to be of type " + dataType));

                return null;
            }
        } else {
            canonicalDataType = dataType;
        }

        return canonicalDataType;
    }

    private void verifyChildRecord(final DataType canonicalDataType, final Object rawValue, final DataType expectedDataType, final StandardSchemaValidationResult result,
        final RecordField field, final String fieldPrefix) {
        // Now that we have the 'canonical data type', we check if it is a Record. If so, we need to validate each sub-field.
        if (canonicalDataType.getFieldType() == RecordFieldType.RECORD) {
            if (!(rawValue instanceof Record)) { // sanity check
                result.addValidationError(new StandardValidationError(concat(fieldPrefix, field), rawValue, ValidationErrorType.INVALID_FIELD,
                    "Value is of type " + classNameOrNull(rawValue) + " but was expected to be of type " + expectedDataType));

                return;
            }

            final RecordDataType recordDataType = (RecordDataType) canonicalDataType;
            final RecordSchema childSchema = recordDataType.getChildSchema();

            final String fullChildFieldName = concat(fieldPrefix, field);
            final SchemaValidationResult childValidationResult = validate((Record) rawValue, childSchema, fullChildFieldName);
            if (childValidationResult.isValid()) {
                return;
            }

            for (final ValidationError validationError : childValidationResult.getValidationErrors()) {
                result.addValidationError(validationError);
            }
        }
    }

    private boolean isTypeCorrect(final Object value, final DataType dataType) {
        switch (dataType.getFieldType()) {
            case ENUM:
                if (!(value instanceof String)) {
                    return false;
                }
                final EnumDataType enumDataType = (EnumDataType) dataType;
                final List<String> enumList = enumDataType.getEnums();
                return enumList.contains(value);
            case ARRAY:
                if (!(value instanceof Object[])) {
                    return false;
                }

                final ArrayDataType arrayDataType = (ArrayDataType) dataType;
                final DataType elementType = arrayDataType.getElementType();

                final Object[] array = (Object[]) value;
                for (final Object arrayVal : array) {
                    if (arrayVal == null && arrayDataType.isElementsNullable()) {
                        continue;
                    }
                    if (!isTypeCorrect(arrayVal, elementType)) {
                        return false;
                    }
                }

                return true;
            case MAP:
                if (value instanceof Map) {
                    final MapDataType mapDataType = (MapDataType) dataType;
                    final DataType valueDataType = mapDataType.getValueType();
                    final Map<?, ?> map = (Map<?, ?>) value;

                    for (final Object mapValue : map.values()) {
                        if (mapValue == null && mapDataType.isValuesNullable()) {
                            continue;
                        }
                        if (!isTypeCorrect(mapValue, valueDataType)) {
                            return false;
                        }
                    }
                    return true;
                } else if (value instanceof Record) {
                    Record record = (Record) value;
                    final MapDataType mapDataType = (MapDataType) dataType;
                    final DataType valueDataType = mapDataType.getValueType();

                    for (final String fieldName : record.getRawFieldNames()) {
                        final Object fieldValue = record.getValue(fieldName);
                        if (!isTypeCorrect(fieldValue, valueDataType)) {
                            return false;
                        }
                    }
                    return true;
                } else {
                    return false;
                }
            case RECORD:
                return value instanceof Record;
            case CHOICE:
                final ChoiceDataType choiceDataType = (ChoiceDataType) dataType;
                for (final DataType choice : choiceDataType.getPossibleSubTypes()) {
                    if (isTypeCorrect(value, choice)) {
                        return true;
                    }
                }

                return false;
            case BOOLEAN:
                return value instanceof Boolean;
            case CHAR:
                return value instanceof Character;
            case DATE:
                return value instanceof java.sql.Date;
            case STRING:
                return value instanceof String;
            case TIME:
                return value instanceof java.sql.Time;
            case TIMESTAMP:
                return value instanceof java.sql.Timestamp;

            // Numeric data types
            case BIGINT:
            case LONG:
            case INT:
            case SHORT:
            case BYTE:
                return DataTypeUtils.isFittingNumberType(value, dataType.getFieldType());
            case DOUBLE:
                return DataTypeUtils.isFittingNumberType(value, dataType.getFieldType())
                        || value instanceof Byte
                        || value instanceof Short
                        || value instanceof Integer
                        || DataTypeUtils.isLongFitsToDouble(value)
                        || DataTypeUtils.isBigIntFitsToDouble(value);
            case FLOAT:
                // Some readers do not provide float vs. double.
                // We should consider if it makes sense to allow either a Float or a Double here or have
                // a Reader indicate whether or not it supports higher precision, etc.
                // Same goes for Short/Integer
                return DataTypeUtils.isFittingNumberType(value, dataType.getFieldType())
                        || value instanceof Byte
                        || value instanceof Short
                        || DataTypeUtils.isDoubleWithinFloatInterval(value)
                        || DataTypeUtils.isIntegerFitsToFloat(value)
                        || DataTypeUtils.isLongFitsToFloat(value)
                        || DataTypeUtils.isBigIntFitsToFloat(value);
            case DECIMAL:
                return DataTypeUtils.isFittingNumberType(value, dataType.getFieldType())
                        || value instanceof Byte
                        || value instanceof Short
                        || value instanceof Integer
                        || value instanceof Long
                        || value instanceof BigInteger;
        }

        return false;
    }

    private String concat(final String fieldPrefix, final RecordField field) {
        return fieldPrefix + "/" + field.getFieldName();
    }

    private String classNameOrNull(Object value) {
        return value == null ? "null" : value.getClass().getName();
    }
}
