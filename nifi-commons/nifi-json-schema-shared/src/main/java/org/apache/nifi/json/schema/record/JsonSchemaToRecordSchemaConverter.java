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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nifi.json.schema.validation.AdditionalPropertiesValidator;
import org.apache.nifi.json.schema.validation.ArrayLengthValidator;
import org.apache.nifi.json.schema.validation.ConstValidator;
import org.apache.nifi.json.schema.validation.EnumValidator;
import org.apache.nifi.json.schema.validation.MultipleOfValidator;
import org.apache.nifi.json.schema.validation.NumericRangeValidator;
import org.apache.nifi.json.schema.validation.PatternPropertiesValidator;
import org.apache.nifi.json.schema.validation.PatternValidator;
import org.apache.nifi.json.schema.validation.PropertyCountValidator;
import org.apache.nifi.json.schema.validation.PropertyNameValidator;
import org.apache.nifi.json.schema.validation.RequiredFieldPresenceValidator;
import org.apache.nifi.json.schema.validation.StringLengthValidator;
import org.apache.nifi.json.schema.validation.UniqueItemsValidator;
import org.apache.nifi.json.schema.validation.ValidatorUtils;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.validation.FieldValidator;
import org.apache.nifi.serialization.record.validation.RecordValidator;

import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

/**
 * Converts JSON Schema definitions into NiFi {@link RecordSchema} instances while preserving validation rules.
 */
public class JsonSchemaToRecordSchemaConverter {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String JSON_SCHEMA_FORMAT = "json-schema";
    private static final String TYPE_NULL = "null";
    private static final String TYPE_OBJECT = "object";
    private static final String TYPE_ARRAY = "array";
    private static final String TYPE_STRING = "string";
    private static final String TYPE_INTEGER = "integer";
    private static final String TYPE_NUMBER = "number";
    private static final String TYPE_BOOLEAN = "boolean";

    public RecordSchema convert(final String schemaText) {
        try {
            final JsonNode rootNode = OBJECT_MAPPER.readTree(schemaText);
            return convert(rootNode, schemaText);
        } catch (final JsonProcessingException e) {
            throw new IllegalArgumentException("Unable to parse JSON Schema", e);
        }
    }

    public RecordSchema convert(final JsonNode schemaNode, final String schemaText) {
        if (!schemaNode.isObject()) {
            throw new IllegalArgumentException("Root JSON Schema must be an object");
        }

        final ConversionContext context = new ConversionContext(schemaNode);
        final ConversionResult conversionResult = convertObjectSchema(schemaNode, "", context);
        final SimpleRecordSchema recordSchema = new SimpleRecordSchema(conversionResult.fields(), schemaText, JSON_SCHEMA_FORMAT, SchemaIdentifier.EMPTY);
        recordSchema.setRecordValidators(conversionResult.recordValidators());

        final String schemaName = textOrNull(schemaNode.get("title"));
        if (schemaName != null) {
            recordSchema.setSchemaName(schemaName);
        }

        final String schemaNamespace = textOrNull(schemaNode.get("$id"));
        if (schemaNamespace != null) {
            recordSchema.setSchemaNamespace(schemaNamespace);
        }

        return recordSchema;
    }

    private ConversionResult convertObjectSchema(final JsonNode schemaNode, final String fieldPath, final ConversionContext context) {
        final ResolvedSchema resolved = resolveSchemaNode(schemaNode, context, fieldPath);
        final JsonNode effectiveSchema = resolved.schemaNode();
        final String pointer = resolved.pointer();

        if (pointer != null) {
            return context.resolveObjectReference(pointer, () -> convertObjectSchemaInternal(effectiveSchema, fieldPath, context));
        }

        return convertObjectSchemaInternal(effectiveSchema, fieldPath, context);
    }

    private ConversionResult convertObjectSchemaInternal(final JsonNode schemaNode, final String fieldPath, final ConversionContext context) {
        final List<RecordField> fields = new ArrayList<>();
        final List<RecordValidator> recordValidators = new ArrayList<>();

        final Set<String> requiredFields = parseRequiredFields(schemaNode);
        final Set<String> requiredNullableFields = new LinkedHashSet<>();

        // TODO: Merge combinator keywords (allOf/anyOf/oneOf) into the synthesized schema.
        final JsonNode propertiesNode = schemaNode.get("properties");
        if (propertiesNode instanceof ObjectNode objectNode) {
            final Iterator<Map.Entry<String, JsonNode>> fieldsIterator = objectNode.fields();
            while (fieldsIterator.hasNext()) {
                final Map.Entry<String, JsonNode> entry = fieldsIterator.next();
                final String fieldName = entry.getKey();
                final JsonNode fieldSchema = entry.getValue();
                final boolean required = requiredFields.contains(fieldName);
                final String childPath = ValidatorUtils.buildFieldPath(fieldPath, fieldName);
                final FieldCreationResult fieldCreationResult = createRecordField(fieldName, fieldSchema, required, childPath, context);
                fields.add(fieldCreationResult.recordField());
                if (fieldCreationResult.requiredButNullable()) {
                    requiredNullableFields.add(fieldName);
                }
            }
        }

        final List<PatternPropertiesValidator.PatternPropertyDefinition> patternPropertyDefinitions = new ArrayList<>();
        final JsonNode patternPropertiesNode = schemaNode.get("patternProperties");
        if (patternPropertiesNode instanceof ObjectNode patternPropertiesObject) {
            final Iterator<Map.Entry<String, JsonNode>> patternsIterator = patternPropertiesObject.fields();
            while (patternsIterator.hasNext()) {
                final Map.Entry<String, JsonNode> entry = patternsIterator.next();
                final String patternExpression = entry.getKey();
                final Pattern compiledPattern;
                try {
                    compiledPattern = Pattern.compile(patternExpression);
                } catch (final PatternSyntaxException e) {
                    throw new IllegalArgumentException("Invalid regex pattern in patternProperties: " + patternExpression, e);
                }

                final JsonNode patternSchema = entry.getValue();
                final String childPath = ValidatorUtils.buildFieldPath(fieldPath, patternExpression);
                final FieldCreationResult patternResult = createRecordField(patternExpression, patternSchema, false, childPath, context);
                patternPropertyDefinitions.add(new PatternPropertiesValidator.PatternPropertyDefinition(compiledPattern, patternResult.recordField()));
            }
        }

        if (schemaNode.has("additionalProperties") && schemaNode.get("additionalProperties").isBoolean()
                && !schemaNode.get("additionalProperties").booleanValue()) {
            final Set<String> allowedFields = fields.stream().map(RecordField::getFieldName).collect(Collectors.toUnmodifiableSet());
            final List<Pattern> allowedPatterns = patternPropertyDefinitions.stream()
                    .map(PatternPropertiesValidator.PatternPropertyDefinition::pattern)
                    .collect(Collectors.toUnmodifiableList());
            recordValidators.add(new AdditionalPropertiesValidator(allowedFields, allowedPatterns));
        }

        final Integer minProperties = schemaNode.has("minProperties") ? Integer.valueOf(schemaNode.get("minProperties").intValue()) : null;
        final Integer maxProperties = schemaNode.has("maxProperties") ? Integer.valueOf(schemaNode.get("maxProperties").intValue()) : null;
        if (minProperties != null || maxProperties != null) {
            recordValidators.add(new PropertyCountValidator(minProperties, maxProperties));
        }

        if (!patternPropertyDefinitions.isEmpty()) {
            recordValidators.add(new PatternPropertiesValidator(patternPropertyDefinitions));
        }

        final JsonNode propertyNamesNode = schemaNode.get("propertyNames");
        if (propertyNamesNode != null) {
            final PropertyNameValidator propertyNameValidator = createPropertyNameValidator(propertyNamesNode, fieldPath, context);
            if (propertyNameValidator != null) {
                recordValidators.add(propertyNameValidator);
            }
        }

        if (!requiredNullableFields.isEmpty()) {
            recordValidators.add(new RequiredFieldPresenceValidator(requiredNullableFields));
        }

        return new ConversionResult(List.copyOf(fields), List.copyOf(recordValidators));
    }

    private FieldCreationResult createRecordField(final String fieldName, final JsonNode fieldSchema, final boolean required, final String fieldPath, final ConversionContext context) {
        final List<FieldValidator> fieldValidators = new ArrayList<>();

        final ResolvedSchema resolvedFieldSchema = resolveSchemaNode(fieldSchema, context, fieldPath);
        final JsonNode effectiveFieldSchema = resolvedFieldSchema.schemaNode();

        final List<String> typeTokens = new ArrayList<>(extractTypes(effectiveFieldSchema));
        final boolean explicitNull = typeTokens.remove(TYPE_NULL);
        final boolean nullable = !required || explicitNull;

        DataType dataType = determineDataType(effectiveFieldSchema, fieldPath, typeTokens, fieldValidators, context, resolvedFieldSchema.pointer());
        if (dataType == null) {
            dataType = RecordFieldType.STRING.getDataType();
        }

        applyStringConstraints(effectiveFieldSchema, typeTokens, fieldValidators);
        applyNumericConstraints(effectiveFieldSchema, typeTokens, fieldValidators);
        applyArrayConstraints(effectiveFieldSchema, typeTokens, fieldValidators);
        dataType = applyEnumConstraints(effectiveFieldSchema, typeTokens, fieldValidators, dataType);

        if (effectiveFieldSchema.has("const")) {
            final Object constantValue = convertJsonNode(effectiveFieldSchema.get("const"));
            fieldValidators.add(new ConstValidator(constantValue));
        }

        final Object defaultValue = extractDefaultValue(effectiveFieldSchema, dataType);
        final RecordField recordField = new RecordField(fieldName, dataType, defaultValue, Collections.emptySet(), nullable, fieldValidators);
        final boolean requiredButNullable = required && nullable && explicitNull;
        return new FieldCreationResult(recordField, requiredButNullable);
    }

    private DataType determineDataType(final JsonNode fieldSchema, final String fieldPath, final List<String> typeTokens,
                                       final List<FieldValidator> fieldValidators, final ConversionContext context, final String schemaPointer) {
        if (typeTokens.isEmpty()) {
            inferTypeWhenMissing(fieldSchema, typeTokens);
        }

        if (typeTokens.size() > 1) {
            final List<DataType> childTypes = typeTokens.stream()
                    .map(typeToken -> createDataTypeForToken(typeToken, fieldSchema, fieldPath, fieldValidators, context, schemaPointer))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            return RecordFieldType.CHOICE.getChoiceDataType(childTypes);
        } else if (!typeTokens.isEmpty()) {
            return createDataTypeForToken(typeTokens.get(0), fieldSchema, fieldPath, fieldValidators, context, schemaPointer);
        }

        return null;
    }

    private void inferTypeWhenMissing(final JsonNode fieldSchema, final List<String> typeTokens) {
        if (fieldSchema.has("properties") || fieldSchema.has("required")) {
            typeTokens.add(TYPE_OBJECT);
        } else if (fieldSchema.has("items")) {
            typeTokens.add(TYPE_ARRAY);
        } else if (fieldSchema.has("enum")) {
            typeTokens.add(TYPE_STRING);
        }
    }

    private DataType createDataTypeForToken(final String typeToken, final JsonNode fieldSchema, final String fieldPath,
                                           final List<FieldValidator> fieldValidators, final ConversionContext context, final String schemaPointer) {
        return switch (typeToken) {
            case TYPE_OBJECT -> createRecordDataType(fieldSchema, fieldPath, context, schemaPointer);
            case TYPE_ARRAY -> createArrayDataType(fieldSchema, fieldPath, fieldValidators, context);
            case TYPE_STRING -> createStringDataType(fieldSchema);
            case TYPE_INTEGER -> RecordFieldType.LONG.getDataType();
            case TYPE_NUMBER -> RecordFieldType.DOUBLE.getDataType();
            case TYPE_BOOLEAN -> RecordFieldType.BOOLEAN.getDataType();
            default -> RecordFieldType.STRING.getDataType();
        };
    }

    private DataType createRecordDataType(final JsonNode fieldSchema, final String fieldPath, final ConversionContext context, final String schemaPointer) {
        final ConversionResult nestedResult;
        if (schemaPointer != null) {
            nestedResult = context.resolveObjectReference(schemaPointer, () -> convertObjectSchemaInternal(fieldSchema, fieldPath, context));
        } else {
            nestedResult = convertObjectSchemaInternal(fieldSchema, fieldPath, context);
        }
        final SimpleRecordSchema childSchema = new SimpleRecordSchema(nestedResult.fields());
        childSchema.setRecordValidators(nestedResult.recordValidators());
        return RecordFieldType.RECORD.getRecordDataType(childSchema);
    }

    private DataType createArrayDataType(final JsonNode fieldSchema, final String fieldPath, final List<FieldValidator> fieldValidators, final ConversionContext context) {
        final JsonNode itemsNode = fieldSchema.get("items");
        final DataType elementType;
        boolean elementsNullable = false;
        if (itemsNode != null && itemsNode.isObject()) {
            final String elementPath = fieldPath + "[]";
            final List<FieldValidator> elementValidators = new ArrayList<>();
            final ResolvedSchema resolvedItems = resolveSchemaNode(itemsNode, context, elementPath);
            final JsonNode effectiveItemsNode = resolvedItems.schemaNode();
            final List<String> elementTypes = new ArrayList<>(extractTypes(effectiveItemsNode));
            elementsNullable = elementTypes.remove(TYPE_NULL);
            DataType determinedElementType = determineDataType(effectiveItemsNode, elementPath, elementTypes, elementValidators, context, resolvedItems.pointer());
            if (determinedElementType == null) {
                determinedElementType = RecordFieldType.STRING.getDataType();
            }
            applyStringConstraints(effectiveItemsNode, elementTypes, elementValidators);
            applyNumericConstraints(effectiveItemsNode, elementTypes, elementValidators);
            determinedElementType = applyEnumConstraints(effectiveItemsNode, elementTypes, elementValidators, determinedElementType);

            // Element-level validators are currently not surfaced separately; future enhancement could expose them via complex data types.
            elementType = determinedElementType;
        } else {
            // TODO: Support tuple typing where items is an array of schemas with positional semantics.
            elementType = RecordFieldType.STRING.getDataType();
        }

        final ArrayDataType arrayDataType = (ArrayDataType) RecordFieldType.ARRAY.getArrayDataType(elementType, elementsNullable);

        if (fieldSchema.has("minItems") || fieldSchema.has("maxItems")) {
            final Integer minItems = fieldSchema.has("minItems") ? fieldSchema.get("minItems").intValue() : null;
            final Integer maxItems = fieldSchema.has("maxItems") ? fieldSchema.get("maxItems").intValue() : null;
            fieldValidators.add(new ArrayLengthValidator(minItems, maxItems));
        }

        if (fieldSchema.path("uniqueItems").asBoolean(false)) {
            fieldValidators.add(new UniqueItemsValidator());
        }

        return arrayDataType;
    }

    private PropertyNameValidator createPropertyNameValidator(final JsonNode propertyNamesNode, final String fieldPath, final ConversionContext context) {
        final ResolvedSchema resolved = resolveSchemaNode(propertyNamesNode, context, fieldPath + "/propertyNames");
        final JsonNode effectiveSchema = resolved.schemaNode();
        if (effectiveSchema == null || effectiveSchema.isNull()) {
            return null;
        }

        final Set<String> supportedKeywords = Set.of(
                "type", "enum", "pattern", "minLength", "maxLength", "const",
                "description", "title", "default", "examples"
        );

        effectiveSchema.fieldNames().forEachRemaining(keyword -> {
            if (!supportedKeywords.contains(keyword)) {
                throw new IllegalArgumentException("Unsupported keyword '" + keyword + "' in propertyNames schema at " + fieldPath);
            }
        });

        final List<String> typeTokens = new ArrayList<>(extractTypes(effectiveSchema));
        if (!typeTokens.isEmpty()) {
            if (typeTokens.size() > 1 || !TYPE_STRING.equals(typeTokens.get(0))) {
                throw new IllegalArgumentException("propertyNames schema at " + fieldPath + " must target string type");
            }
        }

        Pattern pattern = null;
        if (effectiveSchema.has("pattern")) {
            pattern = Pattern.compile(effectiveSchema.get("pattern").textValue());
        }

        Integer minLength = null;
        if (effectiveSchema.has("minLength")) {
            minLength = effectiveSchema.get("minLength").intValue();
            if (minLength < 0) {
                throw new IllegalArgumentException("propertyNames minLength must be non-negative at " + fieldPath);
            }
        }

        Integer maxLength = null;
        if (effectiveSchema.has("maxLength")) {
            maxLength = effectiveSchema.get("maxLength").intValue();
            if (maxLength < 0) {
                throw new IllegalArgumentException("propertyNames maxLength must be non-negative at " + fieldPath);
            }
        }

        if (minLength != null && maxLength != null && minLength > maxLength) {
            throw new IllegalArgumentException("propertyNames minLength cannot exceed maxLength at " + fieldPath);
        }

        String constValue = null;
        if (effectiveSchema.has("const")) {
            final JsonNode constNode = effectiveSchema.get("const");
            if (!constNode.isTextual()) {
                throw new IllegalArgumentException("propertyNames const must be a string at " + fieldPath);
            }
            constValue = constNode.textValue();
        }

        Set<String> enumValues = Collections.emptySet();
        if (effectiveSchema.has("enum")) {
            final JsonNode enumNode = effectiveSchema.get("enum");
            if (!enumNode.isArray()) {
                throw new IllegalArgumentException("propertyNames enum must be an array at " + fieldPath);
            }

            final Set<String> values = new LinkedHashSet<>();
            enumNode.forEach(valueNode -> {
                if (!valueNode.isTextual()) {
                    throw new IllegalArgumentException("propertyNames enum values must be strings at " + fieldPath);
                }
                values.add(valueNode.textValue());
            });
            enumValues = values;
        }

        if (pattern == null && minLength == null && maxLength == null && enumValues.isEmpty() && constValue == null) {
            return null;
        }

        final List<String> descriptionParts = new ArrayList<>();
        if (pattern != null) {
            descriptionParts.add("pattern=" + pattern.pattern());
        }
        if (minLength != null) {
            descriptionParts.add("minLength=" + minLength);
        }
        if (maxLength != null) {
            descriptionParts.add("maxLength=" + maxLength);
        }
        if (!enumValues.isEmpty()) {
            descriptionParts.add("enum=" + enumValues);
        }
        if (constValue != null) {
            descriptionParts.add("const='" + constValue + "'");
        }

        final String description = descriptionParts.isEmpty()
                ? "propertyNames validator"
                : "propertyNames constraints: " + String.join(", ", descriptionParts);

        return new PropertyNameValidator(minLength, maxLength, pattern, enumValues, constValue, description);
    }
    private DataType createStringDataType(final JsonNode fieldSchema) {
        final String format = textOrNull(fieldSchema.get("format"));
        if (format == null) {
            return RecordFieldType.STRING.getDataType();
        }

        return switch (format) {
            case "date" -> RecordFieldType.DATE.getDataType(null);
            case "time" -> RecordFieldType.TIME.getDataType(null);
            case "date-time" -> RecordFieldType.TIMESTAMP.getDataType(null);
            case "uuid" -> RecordFieldType.UUID.getDataType();
            default -> RecordFieldType.STRING.getDataType();
        };
    }

    private void applyStringConstraints(final JsonNode fieldSchema, final List<String> typeTokens, final List<FieldValidator> fieldValidators) {
        if (!typeTokens.contains(TYPE_STRING)) {
            return;
        }

        final Integer minLength = fieldSchema.has("minLength") ? fieldSchema.get("minLength").intValue() : null;
        final Integer maxLength = fieldSchema.has("maxLength") ? fieldSchema.get("maxLength").intValue() : null;
        if (minLength != null || maxLength != null) {
            fieldValidators.add(new StringLengthValidator(minLength, maxLength));
        }

        if (fieldSchema.has("pattern")) {
            fieldValidators.add(new PatternValidator(fieldSchema.get("pattern").textValue()));
        }
    }

    private void applyNumericConstraints(final JsonNode fieldSchema, final List<String> typeTokens, final List<FieldValidator> fieldValidators) {
        if (!typeTokens.contains(TYPE_INTEGER) && !typeTokens.contains(TYPE_NUMBER)) {
            return;
        }

        BigDecimal minimum = fieldSchema.has("minimum") ? fieldSchema.get("minimum").decimalValue() : null;
        BigDecimal maximum = fieldSchema.has("maximum") ? fieldSchema.get("maximum").decimalValue() : null;
        boolean exclusiveMinimum = false;
        boolean exclusiveMaximum = false;

        final JsonNode exclusiveMinimumNode = fieldSchema.get("exclusiveMinimum");
        if (exclusiveMinimumNode != null) {
            if (exclusiveMinimumNode.isNumber()) {
                minimum = exclusiveMinimumNode.decimalValue();
                exclusiveMinimum = true;
            } else if (exclusiveMinimumNode.isBoolean()) {
                exclusiveMinimum = exclusiveMinimumNode.booleanValue();
            }
        }

        final JsonNode exclusiveMaximumNode = fieldSchema.get("exclusiveMaximum");
        if (exclusiveMaximumNode != null) {
            if (exclusiveMaximumNode.isNumber()) {
                maximum = exclusiveMaximumNode.decimalValue();
                exclusiveMaximum = true;
            } else if (exclusiveMaximumNode.isBoolean()) {
                exclusiveMaximum = exclusiveMaximumNode.booleanValue();
            }
        }

        if (minimum != null || maximum != null || exclusiveMinimum || exclusiveMaximum) {
            fieldValidators.add(new NumericRangeValidator(minimum, exclusiveMinimum, maximum, exclusiveMaximum));
        }

        if (fieldSchema.has("multipleOf")) {
            fieldValidators.add(new MultipleOfValidator(fieldSchema.get("multipleOf").decimalValue()));
        }

        // TODO: Enforce numeric format keywords (e.g., int32, int64, float, double) when provided.
    }

    private void applyArrayConstraints(final JsonNode fieldSchema, final List<String> typeTokens, final List<FieldValidator> fieldValidators) {
        if (!typeTokens.contains(TYPE_ARRAY)) {
            return;
        }
        // Array-specific validators are applied during array data type creation.
    }

    private DataType applyEnumConstraints(final JsonNode fieldSchema, final List<String> typeTokens, final List<FieldValidator> fieldValidators, final DataType currentDataType) {
        if (!fieldSchema.has("enum")) {
            return currentDataType;
        }

        final ArrayNode enumNode = (ArrayNode) fieldSchema.get("enum");
        if (enumNode.isEmpty()) {
            return currentDataType;
        }

        final List<Object> enumValues = new ArrayList<>();
        enumNode.forEach(enumEntry -> enumValues.add(convertJsonNode(enumEntry)));
        final boolean allStrings = enumValues.stream().allMatch(String.class::isInstance);
        if (allStrings && (typeTokens.isEmpty() || (typeTokens.size() == 1 && TYPE_STRING.equals(typeTokens.get(0))))) {
            final List<String> stringValues = enumValues.stream().map(Object::toString).collect(Collectors.toList());
            fieldValidators.add(new EnumValidator(stringValues));
            return RecordFieldType.ENUM.getEnumDataType(stringValues);
        }

        final Collection<String> canonicalValues = EnumValidator.canonicalizeAll(enumValues);
        fieldValidators.add(new EnumValidator(canonicalValues));
        return currentDataType;
    }

    private Object extractDefaultValue(final JsonNode fieldSchema, final DataType dataType) {
        final JsonNode defaultNode = fieldSchema.get("default");
        if (defaultNode == null || defaultNode.isNull()) {
            return null;
        }

        final Object defaultValue = convertJsonNode(defaultNode);
        if (defaultValue == null) {
            return null;
        }

        final RecordFieldType fieldType = dataType.getFieldType();
        if (fieldType == RecordFieldType.BOOLEAN && defaultValue instanceof Boolean) {
            return defaultValue;
        }
        if (fieldType == RecordFieldType.STRING && defaultValue instanceof String) {
            return defaultValue;
        }
        if ((fieldType == RecordFieldType.INT || fieldType == RecordFieldType.LONG || fieldType == RecordFieldType.BIGINT
                || fieldType == RecordFieldType.FLOAT || fieldType == RecordFieldType.DOUBLE || fieldType == RecordFieldType.DECIMAL)
                && defaultValue instanceof Number) {
            return defaultValue;
        }

        return null;
    }

    private Set<String> parseRequiredFields(final JsonNode schemaNode) {
        if (!schemaNode.has("required")) {
            return Set.of();
        }

        final ArrayNode requiredArray = (ArrayNode) schemaNode.get("required");
        final Set<String> requiredFields = new LinkedHashSet<>();
        requiredArray.forEach(node -> requiredFields.add(node.asText()));
        return requiredFields;
    }

    private List<String> extractTypes(final JsonNode schemaNode) {
        if (schemaNode == null || !schemaNode.has("type")) {
            return new ArrayList<>();
        }

        final JsonNode typeNode = schemaNode.get("type");
        if (typeNode.isTextual()) {
            return new ArrayList<>(List.of(typeNode.textValue()));
        }

        if (typeNode.isArray()) {
            final ArrayNode arrayNode = (ArrayNode) typeNode;
            final List<String> types = new ArrayList<>();
            arrayNode.forEach(node -> types.add(node.textValue()));
            return types;
        }

        return new ArrayList<>();
    }

    private ResolvedSchema resolveSchemaNode(final JsonNode schemaNode, final ConversionContext context, final String fieldPath) {
        if (schemaNode == null) {
            return new ResolvedSchema(null, null);
        }

        JsonNode currentNode = schemaNode;
        String pointerForCache = null;
        final Set<String> visitedPointers = new HashSet<>();

        while (currentNode.has("$ref")) {
            final JsonNode refNode = currentNode.get("$ref");
            if (!refNode.isTextual()) {
                throw new IllegalArgumentException("Schema $ref must be a string at " + fieldPath);
            }

            final String ref = refNode.textValue();
            if (ref == null || ref.isBlank()) {
                throw new IllegalArgumentException("Schema $ref must be a non-empty string at " + fieldPath);
            }

            if (!ref.startsWith("#")) {
                throw new IllegalArgumentException("External JSON Schema references are not supported: " + ref);
            }

            if (currentNode.size() > 1) {
                throw new IllegalArgumentException("Schema at " + fieldPath + " combines $ref with other keywords which is not supported");
            }

            final String pointer = ref.equals("#") ? "" : ref.substring(1);
            if (!pointer.isEmpty() && !pointer.startsWith("/")) {
                throw new IllegalArgumentException("Unsupported JSON Pointer syntax in reference '" + ref + "' at " + fieldPath);
            }

            final String cacheKey = pointer.isEmpty() ? "#" : pointer;
            if (!visitedPointers.add(cacheKey)) {
                throw new IllegalArgumentException("Detected circular schema reference involving pointer '" + cacheKey + "' at " + fieldPath);
            }

            final JsonNode target = pointer.isEmpty() ? context.rootNode : context.rootNode.at(pointer);
            if (target.isMissingNode()) {
                throw new IllegalArgumentException("Unable to resolve JSON Schema reference '" + ref + "' at " + fieldPath);
            }

            pointerForCache = cacheKey;
            currentNode = target;
        }

        return new ResolvedSchema(currentNode, pointerForCache);
    }

    private Object convertJsonNode(final JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isTextual()) {
            return node.textValue();
        }
        if (node.isBoolean()) {
            return node.booleanValue();
        }
        if (node.isInt() || node.isLong()) {
            return node.longValue();
        }
        if (node.isFloatingPointNumber()) {
            return node.doubleValue();
        }
        if (node.isArray()) {
            final ArrayNode arrayNode = (ArrayNode) node;
            final List<Object> values = new ArrayList<>(arrayNode.size());
            arrayNode.forEach(element -> values.add(convertJsonNode(element)));
            return values.toArray();
        }
        if (node.isObject()) {
            return OBJECT_MAPPER.convertValue(node, Map.class);
        }
        return null;
    }

    private String textOrNull(final JsonNode node) {
        return node != null && node.isTextual() ? node.textValue() : null;
    }

    private record ConversionResult(List<RecordField> fields, List<RecordValidator> recordValidators) {
    }

    private record FieldCreationResult(RecordField recordField, boolean requiredButNullable) {
    }

    private static final class ConversionContext {
        private final JsonNode rootNode;
        private final Map<String, ConversionResult> objectSchemaCache = new HashMap<>();
        private final Deque<String> resolutionStack = new ArrayDeque<>();

        private ConversionContext(final JsonNode rootNode) {
            this.rootNode = Objects.requireNonNull(rootNode);
        }

        private ConversionResult resolveObjectReference(final String pointer, final Supplier<ConversionResult> converter) {
            final ConversionResult cached = objectSchemaCache.get(pointer);
            if (cached != null) {
                return cached;
            }

            if (resolutionStack.contains(pointer)) {
                throw new IllegalArgumentException("Detected circular schema reference for pointer '" + pointer + "'");
            }

            resolutionStack.push(pointer);
            try {
                final ConversionResult result = converter.get();
                objectSchemaCache.put(pointer, result);
                return result;
            } finally {
                resolutionStack.pop();
            }
        }
    }

    private record ResolvedSchema(JsonNode schemaNode, String pointer) {
    }
}
