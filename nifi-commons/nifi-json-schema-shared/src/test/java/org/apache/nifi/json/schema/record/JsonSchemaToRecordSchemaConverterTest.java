/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.json.schema.record;

import org.apache.nifi.json.schema.validation.AdditionalPropertiesValidator;
import org.apache.nifi.json.schema.validation.ArrayLengthValidator;
import org.apache.nifi.json.schema.validation.ConstValidator;
import org.apache.nifi.json.schema.validation.EnumValidator;
import org.apache.nifi.json.schema.validation.NumericRangeValidator;
import org.apache.nifi.json.schema.validation.RequiredFieldPresenceValidator;
import org.apache.nifi.json.schema.validation.StringLengthValidator;
import org.apache.nifi.json.schema.validation.UniqueItemsValidator;
import org.apache.nifi.schema.validation.SchemaValidationContext;
import org.apache.nifi.schema.validation.StandardSchemaValidator;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.EnumDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.validation.SchemaValidationResult;
import org.apache.nifi.serialization.record.validation.ValidationError;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class JsonSchemaToRecordSchemaConverterTest {
    private JsonSchemaToRecordSchemaConverter converter;
    private RecordSchema recordSchema;
    private RecordSchema metadataSchema;

    @BeforeEach
    void setUp() {
        converter = new JsonSchemaToRecordSchemaConverter();
        final String schemaText = """
            {
              "$id": "urn:example:person",
              "title": "Person",
              "type": "object",
              "additionalProperties": false,
              "minProperties": 2,
              "maxProperties": 8,
              "required": ["id", "name", "metadata"],
              "properties": {
                "id": {
                  "type": ["string", "null"],
                  "minLength": 3
                },
                "name": {
                  "type": "string",
                  "enum": ["Alice", "Bob"]
                },
                "age": {
                  "type": "integer",
                  "minimum": 0,
                  "exclusiveMaximum": 120
                },
                "score": {
                  "type": "number",
                  "exclusiveMinimum": 0,
                  "exclusiveMaximum": 1
                },
                "tags": {
                  "type": "array",
                  "items": { "type": "string" },
                  "minItems": 1,
                  "uniqueItems": true
                },
                "metadata": {
                  "type": "object",
                  "required": ["active"],
                  "properties": {
                    "active": { "type": "boolean" },
                    "level": {
                      "type": ["integer", "null"],
                      "minimum": 1
                    }
                  }
                },
                "status": {
                  "const": "ACTIVE"
                }
              }
            }
            """;
        recordSchema = converter.convert(schemaText);
        final RecordField metadataField = recordSchema.getField("metadata")
                .orElseThrow(() -> new AssertionError("metadata field missing"));
        final DataType metadataDataType = metadataField.getDataType();
        metadataSchema = ((RecordDataType) metadataDataType).getChildSchema();
    }

    @Test
    void testSchemaMetadataCaptured() {
        assertEquals(Optional.of("Person"), recordSchema.getSchemaName());
        assertEquals(Optional.of("json-schema"), recordSchema.getSchemaFormat());
        assertEquals(Optional.of("urn:example:person"), recordSchema.getSchemaNamespace());

        final RecordField nameField = recordSchema.getField("name").orElseThrow();
        assertEquals(RecordFieldType.ENUM, nameField.getDataType().getFieldType());
        assertTrue(nameField.getDataType() instanceof EnumDataType);
        final EnumDataType enumDataType = (EnumDataType) nameField.getDataType();
        assertEquals(List.of("Alice", "Bob"), enumDataType.getEnums());
        assertTrue(nameField.getFieldValidators().stream().anyMatch(EnumValidator.class::isInstance));

        final RecordField idField = recordSchema.getField("id").orElseThrow();
        assertTrue(idField.isNullable());
        assertTrue(idField.getFieldValidators().stream().anyMatch(StringLengthValidator.class::isInstance));

        final RecordField ageField = recordSchema.getField("age").orElseThrow();
        assertTrue(ageField.getFieldValidators().stream().anyMatch(NumericRangeValidator.class::isInstance));

        final RecordField scoreField = recordSchema.getField("score").orElseThrow();
        assertTrue(scoreField.getFieldValidators().stream().anyMatch(NumericRangeValidator.class::isInstance));

        final RecordField tagsField = recordSchema.getField("tags").orElseThrow();
        assertEquals(RecordFieldType.ARRAY, tagsField.getDataType().getFieldType());
        final ArrayDataType arrayDataType = (ArrayDataType) tagsField.getDataType();
        assertEquals(RecordFieldType.STRING, arrayDataType.getElementType().getFieldType());
        assertTrue(tagsField.getFieldValidators().stream().anyMatch(ArrayLengthValidator.class::isInstance));
        assertTrue(tagsField.getFieldValidators().stream().anyMatch(UniqueItemsValidator.class::isInstance));

        final RecordField statusField = recordSchema.getField("status").orElseThrow();
        assertTrue(statusField.getFieldValidators().stream().anyMatch(ConstValidator.class::isInstance));

        assertEquals(2 + 1, recordSchema.getRecordValidators().size());
        assertTrue(recordSchema.getRecordValidators().stream().anyMatch(AdditionalPropertiesValidator.class::isInstance));
        assertTrue(recordSchema.getRecordValidators().stream().anyMatch(RequiredFieldPresenceValidator.class::isInstance));
    }

    @Test
    void testValidationPassesForConformingRecord() {
        final SchemaValidationContext context = new SchemaValidationContext(recordSchema, true, true);
        final StandardSchemaValidator validator = new StandardSchemaValidator(context);

        final MapRecord metadataRecord = new MapRecord(metadataSchema, Map.of(
                "active", true,
                "level", 2
        ));

        final Map<String, Object> values = new HashMap<>();
        values.put("id", "abc");
        values.put("name", "Alice");
        values.put("age", 35);
        values.put("score", 0.5D);
        values.put("tags", new Object[]{"alpha"});
        values.put("metadata", metadataRecord);
        values.put("status", "ACTIVE");

        final MapRecord record = new MapRecord(recordSchema, values);
        final SchemaValidationResult result = validator.validate(record);
        assertTrue(result.isValid(), () -> "Expected valid record, found errors: " + result.getValidationErrors());
    }

    @Test
    void testEnumViolationDetected() {
        final SchemaValidationContext context = new SchemaValidationContext(recordSchema, true, true);
        final StandardSchemaValidator validator = new StandardSchemaValidator(context);

        final MapRecord metadataRecord = new MapRecord(metadataSchema, Map.of("active", true));

        final Map<String, Object> values = new HashMap<>();
        values.put("id", "abc");
        values.put("name", "Charlie");
        values.put("metadata", metadataRecord);
        values.put("tags", new Object[]{"alpha"});
        values.put("status", "ACTIVE");

        final MapRecord record = new MapRecord(recordSchema, values);
        final SchemaValidationResult result = validator.validate(record);
        assertFalse(result.isValid());
        assertTrue(result.getValidationErrors().stream().anyMatch(error -> error.getFieldName().map(name -> name.endsWith("name")).orElse(false)));
    }

    @Test
    void testAdditionalPropertyViolationDetected() {
        final SchemaValidationContext context = new SchemaValidationContext(recordSchema, true, true);
        final StandardSchemaValidator validator = new StandardSchemaValidator(context);

        final MapRecord metadataRecord = new MapRecord(metadataSchema, Map.of("active", true));

        final Map<String, Object> values = new HashMap<>();
        values.put("id", "abc");
        values.put("name", "Alice");
        values.put("metadata", metadataRecord);
        values.put("tags", new Object[]{"alpha"});
        values.put("status", "ACTIVE");
        values.put("extra", "value");

        final MapRecord record = new MapRecord(recordSchema, values);
        final SchemaValidationResult result = validator.validate(record);
        assertFalse(result.isValid());
        assertTrue(result.getValidationErrors().stream().anyMatch(error -> error.getType() == org.apache.nifi.serialization.record.validation.ValidationErrorType.EXTRA_FIELD));
    }

    @Test
    void testUniqueItemsViolationDetected() {
        final SchemaValidationContext context = new SchemaValidationContext(recordSchema, true, true);
        final StandardSchemaValidator validator = new StandardSchemaValidator(context);

        final MapRecord metadataRecord = new MapRecord(metadataSchema, Map.of("active", true));

        final Map<String, Object> values = new HashMap<>();
        values.put("id", "abc");
        values.put("name", "Alice");
        values.put("metadata", metadataRecord);
        values.put("tags", new Object[]{"dup", "dup"});
        values.put("status", "ACTIVE");

        final MapRecord record = new MapRecord(recordSchema, values);
        final SchemaValidationResult result = validator.validate(record);
        assertFalse(result.isValid());
        assertTrue(result.getValidationErrors().stream().anyMatch(error -> error.getFieldName().map(name -> name.contains("tags")).orElse(false)));
    }

    @Test
    void testRequiredNullableFieldPresenceValidatorTriggersWhenMissing() {
        final SchemaValidationContext context = new SchemaValidationContext(recordSchema, true, true);
        final StandardSchemaValidator validator = new StandardSchemaValidator(context);

        final MapRecord metadataRecord = new MapRecord(metadataSchema, Map.of("active", true));

        final Map<String, Object> values = new HashMap<>();
        values.put("name", "Alice");
        values.put("metadata", metadataRecord);
        values.put("tags", new Object[]{"alpha"});
        values.put("status", "ACTIVE");

        final MapRecord record = new MapRecord(recordSchema, values);
        final SchemaValidationResult result = validator.validate(record);
        assertFalse(result.isValid());
        assertTrue(result.getValidationErrors().stream().anyMatch(error -> error.getType() == org.apache.nifi.serialization.record.validation.ValidationErrorType.MISSING_FIELD));
    }

    @Test
    void testDateAndTimeValuesValidated() {
        final String schemaText = """
            {
              "type": "object",
              "properties": {
                "releaseDate": {
                  "type": "string",
                  "format": "date"
                },
                "releaseTime": {
                  "type": "string",
                  "format": "time"
                },
                "lastUpdated": {
                  "type": "string",
                  "format": "date-time"
                }
              }
            }
            """;

        final RecordSchema schema = converter.convert(schemaText);
        final RecordField dateField = schema.getField("releaseDate").orElseThrow();
        assertEquals(RecordFieldType.DATE, dateField.getDataType().getFieldType());

        final RecordField timeField = schema.getField("releaseTime").orElseThrow();
        assertEquals(RecordFieldType.TIME, timeField.getDataType().getFieldType());

        final RecordField timestampField = schema.getField("lastUpdated").orElseThrow();
        assertEquals(RecordFieldType.TIMESTAMP, timestampField.getDataType().getFieldType());

        final SchemaValidationContext context = new SchemaValidationContext(schema, false, false);
        final StandardSchemaValidator validator = new StandardSchemaValidator(context);

        final MapRecord validRecord = new MapRecord(schema, Map.of(
                "releaseDate", "2024-01-15",
                "releaseTime", "13:45:00",
                "lastUpdated", "2024-01-15T13:45:00Z"
        ));
        assertTrue(validator.validate(validRecord).isValid());

        final MapRecord invalidRecord = new MapRecord(schema, Map.of(
                "releaseDate", "2024-13-01",
                "releaseTime", "25:61:00",
                "lastUpdated", "not-a-timestamp"
        ));
        assertFalse(validator.validate(invalidRecord).isValid());
    }

    @Test
    void testPatternPropertiesValidation() {
        final String schemaText = """
            {
              "type": "object",
              "additionalProperties": false,
              "properties": {
                "fixed": { "type": "string" }
              },
              "patternProperties": {
                "^x-": {
                  "type": "string",
                  "minLength": 2
                },
                "^meta-": {
                  "type": "object",
                  "required": ["enabled"],
                  "properties": {
                    "enabled": { "type": "boolean" }
                  }
                }
              }
            }
            """;

        final RecordSchema schema = converter.convert(schemaText);
        final SchemaValidationContext context = new SchemaValidationContext(schema, true, false);
        final StandardSchemaValidator validator = new StandardSchemaValidator(context);

        final Map<String, Object> validValues = new HashMap<>();
        validValues.put("fixed", "value");
        validValues.put("x-ok", "ab");
        validValues.put("meta-config", Map.of("enabled", true));

        final MapRecord validRecord = new MapRecord(schema, validValues);
        final SchemaValidationResult validResult = validator.validate(validRecord);
        final boolean valid = validResult.isValid();
        if (!valid) {
            fail("Unexpected property name validation errors: " + validResult.getValidationErrors());
        }

        final Map<String, Object> invalidValues = new HashMap<>();
        invalidValues.put("fixed", "value");
        invalidValues.put("x-bad", "a");
        invalidValues.put("meta-config", Map.of());
        invalidValues.put("other", "not allowed");

        final MapRecord invalidRecord = new MapRecord(schema, invalidValues);
        final SchemaValidationResult result = validator.validate(invalidRecord);
        assertFalse(result.isValid());
        assertEquals(3, result.getValidationErrors().size());

        final List<String> explanations = result.getValidationErrors().stream()
                .map(ValidationError::getExplanation)
                .toList();

        assertTrue(explanations.stream().anyMatch(msg -> msg.contains("String length")));
        assertTrue(explanations.stream().anyMatch(msg -> msg.contains("Field is required")));
        assertTrue(explanations.stream().anyMatch(msg -> msg.contains("not defined")));
    }

    @Test
    void testPropertyNamesValidation() {
        final JsonSchemaToRecordSchemaConverter localConverter = new JsonSchemaToRecordSchemaConverter();
        final String schemaText = """
            {
              "$defs": {
                "LowerAlpha": {
                  "type": "string",
                  "pattern": "^[a-z]+$",
                  "minLength": 3
                }
              },
              "type": "object",
              "propertyNames": { "$ref": "#/$defs/LowerAlpha" },
              "additionalProperties": true
            }
            """;

        final RecordSchema schema = localConverter.convert(schemaText);
        final SchemaValidationContext context = new SchemaValidationContext(schema, true, true);
        final StandardSchemaValidator validator = new StandardSchemaValidator(context);

        final MapRecord validRecord = new MapRecord(schema, Map.of(
                "alpha", 1,
                "beta", "value"
        ));
        final SchemaValidationResult validResult = validator.validate(validRecord);
        if (!validResult.isValid()) {
            fail("Unexpected property name validation errors: " + validResult.getValidationErrors());
        }

        final MapRecord invalidRecord = new MapRecord(schema, Map.of(
                "A", 1,
                "beta", "value"
        ));
        final SchemaValidationResult invalidResult = validator.validate(invalidRecord);
        assertFalse(invalidResult.isValid());
        assertTrue(invalidResult.getValidationErrors().stream()
                .anyMatch(error -> error.getExplanation().contains("pattern") || error.getExplanation().contains("length")));
    }

    @Test
    void testReferencedObjectSchemaConverted() {
        final JsonSchemaToRecordSchemaConverter localConverter = new JsonSchemaToRecordSchemaConverter();
        final String schemaText = """
            {
              "$defs": {
                "Address": {
                  "type": "object",
                  "required": ["city"],
                  "properties": {
                    "city": { "type": "string" },
                    "zip": { "type": "string", "minLength": 5 }
                  }
                }
              },
              "type": "object",
              "properties": {
                "address": { "$ref": "#/$defs/Address" }
              },
              "required": ["address"]
            }
            """;

        final RecordSchema schema = localConverter.convert(schemaText);
        final RecordField addressField = schema.getField("address").orElseThrow();
        assertEquals(RecordFieldType.RECORD, addressField.getDataType().getFieldType());

        final RecordSchema addressSchema = ((RecordDataType) addressField.getDataType()).getChildSchema();
        final SchemaValidationContext context = new SchemaValidationContext(schema, true, true);
        final StandardSchemaValidator validator = new StandardSchemaValidator(context);

        final MapRecord validAddress = new MapRecord(addressSchema, Map.of(
                "city", "Paris",
                "zip", "75000"
        ));
        final MapRecord validRecord = new MapRecord(schema, Map.of("address", validAddress));
        assertTrue(validator.validate(validRecord).isValid());

        final MapRecord invalidAddress = new MapRecord(addressSchema, Map.of("zip", "123"));
        final MapRecord invalidRecord = new MapRecord(schema, Map.of("address", invalidAddress));
        final SchemaValidationResult invalidResult = validator.validate(invalidRecord);
        assertFalse(invalidResult.isValid());
        assertTrue(invalidResult.getValidationErrors().stream().anyMatch(error -> error.getFieldName().map(name -> name.contains("city")).orElse(false)));
    }

    @Test
    void testPatternPropertiesReferenceValidation() {
        final JsonSchemaToRecordSchemaConverter localConverter = new JsonSchemaToRecordSchemaConverter();
        final String schemaText = """
            {
              "$defs": {
                "Header": {
                  "type": "string",
                  "minLength": 2
                }
              },
              "type": "object",
              "additionalProperties": false,
              "properties": {
                "fixed": { "type": "string" }
              },
              "patternProperties": {
                "^x-": { "$ref": "#/$defs/Header" }
              },
              "required": ["fixed"]
            }
            """;

        final RecordSchema schema = localConverter.convert(schemaText);
        final SchemaValidationContext context = new SchemaValidationContext(schema, true, true);
        final StandardSchemaValidator validator = new StandardSchemaValidator(context);

        final Map<String, Object> validValues = new HashMap<>();
        validValues.put("fixed", "value");
        validValues.put("x-trace", "ab");
        final SchemaValidationResult validResult = validator.validate(new MapRecord(schema, validValues));
        assertTrue(validResult.isValid(), () -> "Expected valid record, found errors: " + validResult.getValidationErrors());

        final Map<String, Object> invalidValues = new HashMap<>();
        invalidValues.put("fixed", "value");
        invalidValues.put("x-trace", "a");
        final SchemaValidationResult invalidResult = validator.validate(new MapRecord(schema, invalidValues));
        assertFalse(invalidResult.isValid());
        assertTrue(invalidResult.getValidationErrors().stream().anyMatch(error -> error.getFieldName().map("/x-trace"::equals).orElse(false)));
    }

    @Test
    void testExternalReferenceNotSupported() {
        final JsonSchemaToRecordSchemaConverter localConverter = new JsonSchemaToRecordSchemaConverter();
        final String schemaText = """
            {
              "type": "object",
              "properties": {
                "ref": { "$ref": "http://example.com/external.json#/Some" }
              }
            }
            """;

        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> localConverter.convert(schemaText));
        assertTrue(exception.getMessage().contains("External JSON Schema references"));
    }
}
