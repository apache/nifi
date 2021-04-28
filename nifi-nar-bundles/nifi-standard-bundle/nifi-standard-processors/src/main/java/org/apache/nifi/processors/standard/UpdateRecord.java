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

package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.record.path.validation.RecordPathPropertyNameValidator;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"update", "record", "generic", "schema", "json", "csv", "avro", "log", "logs", "freeform", "text"})
@CapabilityDescription("Updates the contents of a FlowFile that contains Record-oriented data (i.e., data that can be read via a RecordReader and written by a RecordWriter). "
    + "This Processor requires that at least one user-defined Property be added. The name of the Property should indicate a RecordPath that determines the field that should "
    + "be updated. The value of the Property is either a replacement value (optionally making use of the Expression Language) or is itself a RecordPath that extracts a value from "
    + "the Record. Whether the Property value is determined to be a RecordPath or a literal value depends on the configuration of the <Replacement Value Strategy> Property.")
@WritesAttributes({
    @WritesAttribute(attribute = "record.index", description = "This attribute provides the current row index and is only available inside the literal value expression."),
    @WritesAttribute(attribute = "record.error.message", description = "This attribute provides on failure the error message encountered by the Reader or Writer.")
})
@SeeAlso({ConvertRecord.class})
public class UpdateRecord extends AbstractRecordProcessor {
    private static final String FIELD_NAME = "field.name";
    private static final String FIELD_VALUE = "field.value";
    private static final String FIELD_TYPE = "field.type";

    private static final String RECORD_INDEX = "record.index";

    private volatile RecordPathCache recordPathCache;
    private volatile List<String> recordPaths;

    static final AllowableValue LITERAL_VALUES = new AllowableValue("literal-value", "Literal Value",
        "The value entered for a Property (after Expression Language has been evaluated) is the desired value to update the Record Fields with. Expression Language "
            + "may reference variables 'field.name', 'field.type', and 'field.value' to access information about the field and the value of the field being evaluated.");
    static final AllowableValue RECORD_PATH_VALUES = new AllowableValue("record-path-value", "Record Path Value",
        "The value entered for a Property (after Expression Language has been evaluated) is not the literal value to use but rather is a Record Path "
            + "that should be evaluated against the Record, and the result of the RecordPath will be used to update the Record. Note that if this option is selected, "
            + "and the Record Path results in multiple values for a given Record, the input FlowFile will be routed to the 'failure' Relationship.");

    static final PropertyDescriptor REPLACEMENT_VALUE_STRATEGY = new PropertyDescriptor.Builder()
        .name("replacement-value-strategy")
        .displayName("Replacement Value Strategy")
        .description("Specifies how to interpret the configured replacement values")
        .allowableValues(LITERAL_VALUES, RECORD_PATH_VALUES)
        .defaultValue(LITERAL_VALUES.getValue())
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(true)
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(REPLACEMENT_VALUE_STRATEGY);
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .description("Specifies the value to use to replace fields in the record that match the RecordPath: " + propertyDescriptorName)
            .required(false)
            .dynamic(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(new RecordPathPropertyNameValidator())
            .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final boolean containsDynamic = validationContext.getProperties().keySet().stream().anyMatch(PropertyDescriptor::isDynamic);

        if (containsDynamic) {
            return Collections.emptyList();
        }

        return Collections.singleton(new ValidationResult.Builder()
            .subject("User-defined Properties")
            .valid(false)
            .explanation("At least one RecordPath must be specified")
            .build());
    }

    @OnScheduled
    public void createRecordPaths(final ProcessContext context) {
        recordPathCache = new RecordPathCache(context.getProperties().size() * 2);

        final List<String> recordPaths = new ArrayList<>(context.getProperties().size() - 2);
        for (final PropertyDescriptor property : context.getProperties().keySet()) {
            if (property.isDynamic()) {
                recordPaths.add(property.getName());
            }
        }

        this.recordPaths = recordPaths;
    }

    private static class RecordWrapper {
        private Record originalRecord;
        private Record currentRecord;
        private Path path;
        private Map<RecordField, FieldValue> actualFields = new HashMap<>(); // The record might have fewer fields than its schema

        public RecordWrapper(Record record, String path) {
            this.originalRecord = record;
            this.path = new Path(path);

            if (this.path.isEmpty()) {
                this.currentRecord = originalRecord;
            } else {
                this.currentRecord = getRecordByPath(this.path.toString() + "/*");
            }

            if (this.currentRecord != null) {
                List<FieldValue> listOfActualFields = getSelectedFields(this.path + "/*");
                listOfActualFields.forEach(field -> actualFields.put(field.getField(), field));
            }
        }

        public boolean recordExists() {
            return currentRecord != null;
        }

        public Record getRecord() {
            return currentRecord;
        }

        public boolean actuallyContainsField(RecordField field) {
            return actualFields.containsKey(field);
        }

        public FieldValue getActualFieldValue(RecordField field) {
            return actualFields.get(field);
        }

        public String getPath() {
            return path.toString();
        }

        public Record getOriginalRecord() {
            return originalRecord;
        }

        private Record getRecordByPath(String recordPath) {
            Record resultRecord;
            Optional<FieldValue> fieldValueOption = RecordPath.compile(recordPath).evaluate(originalRecord).getSelectedFields().findAny();
            if (fieldValueOption.isPresent()) {
                resultRecord = getParentRecordOfField(fieldValueOption.get());
            } else {
                Path newPath = new Path(recordPath); // Cutting the "/*" from the end.
                if (newPath.isEmpty()) {
                    resultRecord = originalRecord;
                } else {
                    resultRecord = getTargetAsRecord(newPath.toString());
                }
            }
            return resultRecord;
        }

        private Record getParentRecordOfField(FieldValue fieldValue) {
            Optional<Record> parentRecordOption = fieldValue.getParentRecord();
            if (parentRecordOption.isPresent()) {
                return parentRecordOption.get();
            } else {
                return originalRecord;
            }
        }

        private Record getTargetAsRecord(String path) {
            Optional<FieldValue> targetFieldOption = RecordPath.compile(path).evaluate(originalRecord).getSelectedFields().findAny();
            //TODO: is it possible that there are two elements in a record with the same path ?
            if (targetFieldOption.isPresent()) {
                FieldValue targetField = targetFieldOption.get();
                return getFieldValueAsRecord(targetField);
            } else {
                return null;
            }
        }

        // Returns null if fieldValue is not of type Record
        private Record getFieldValueAsRecord(FieldValue fieldValue) {
            Object targetObject = fieldValue.getValue();
            if (targetObject instanceof Record) {
                return ((Record) targetObject);
            } else {
                return null;
            }
        }

        private List<FieldValue> getSelectedFields(String path) {
            RecordPath recordPath = RecordPath.compile(path);
            RecordPathResult recordPathResult = recordPath.evaluate(originalRecord);
            return recordPathResult.getSelectedFields().collect(Collectors.toList());
        }
    }

    private static class Path {
        private final String path;

        public Path(String path) {
            if (path.length() == 0 || path.equals("/") || path.equals("/*")) {
                this.path = "";
            }  else if (path.endsWith("/")) {
                this.path = path.substring(0, path.length() - 1);
            } else if (path.endsWith("/*")) {
                this.path = path.substring(0, path.length() - 2);
            } else {
                this.path = path;
            }
        }

        public boolean isEmpty() {
            return "".equals(path);
        }

        @Override
        public String toString() {
            return path;
        }
    }


    private Record remove(Record record, List<FieldValue> fieldsToRemove) {
        if (fieldsToRemove.isEmpty()) {
            return record;
        } else {
            RecordWrapper wrappedRecord = new RecordWrapper(record, "/");
            List<String> fieldsToRemoveWithPath = mapFieldValueToPath(fieldsToRemove);
            return remove(wrappedRecord, fieldsToRemove, fieldsToRemoveWithPath);
        }
    }

    private List<String> mapFieldValueToPath(List<FieldValue> fieldValues) {
        List<String> paths = new ArrayList<>();
        fieldValues.forEach(field -> paths.add(getAbsolutePath(field)));
        return paths;
    }

    private String getAbsolutePath(FieldValue fieldValue) {
        if (!fieldValue.getParent().isPresent()) {
            return "";
        }
        if ("root".equals(fieldValue.getParent().get().getField().getFieldName())) {
            return "/" + fieldValue.getField().getFieldName();
        }
        return getAbsolutePath(fieldValue.getParent().get()) + "/" + fieldValue.getField().getFieldName();
    }

    private Record remove(RecordWrapper wrappedRecord, List<FieldValue> fieldsToRemove, List<String> fieldsToRemoveWithPath) {
        List<RecordField> newSchemaFieldList = new ArrayList<>();
        Map<String, Object> newRecordMap = new LinkedHashMap<>();

        for (RecordField schemaField : wrappedRecord.getRecord().getSchema().getFields()) { // Iterate the fields of the record's schema
            if (wrappedRecord.actuallyContainsField(schemaField)) { // The record actually has data or null for that field
                handleFieldInSchemaAndData(schemaField, wrappedRecord, fieldsToRemove, fieldsToRemoveWithPath, newSchemaFieldList, newRecordMap);
            } else { // The record does not even contain the field (no data for it). We still need to check if it needs to be deleted from the schema.
                handleFieldInSchema(schemaField, wrappedRecord, fieldsToRemove, newSchemaFieldList, fieldsToRemoveWithPath);
            }
        }

        RecordSchema newSchema = new SimpleRecordSchema(newSchemaFieldList);
        Record newRecord = new MapRecord(newSchema, newRecordMap);
        return newRecord;
    }

    private void handleFieldInSchemaAndData(RecordField schemaField, RecordWrapper wrappedRecord, List<FieldValue> fieldsToRemove, List<String> fieldsToRemoveWithPath,
                                            List<RecordField> schemaFieldList, Map<String, Object> recordMap) {
        String fieldName = schemaField.getFieldName();
        FieldValue fieldValue = wrappedRecord.getActualFieldValue(schemaField);
        if (!fieldsToRemove.contains(fieldValue)) { // The field needs to be kept
            if (fieldValue.getValue() instanceof Record) {
                String newPath = wrappedRecord.getPath() + "/" + fieldName + "/*";
                Record newSubRecord = remove(new RecordWrapper(wrappedRecord.getOriginalRecord(), newPath), fieldsToRemove, fieldsToRemoveWithPath);
                schemaFieldList.add(new RecordField(fieldName, RecordFieldType.RECORD.getRecordDataType(newSubRecord.getSchema()), schemaField.isNullable()));
                recordMap.put(fieldName, newSubRecord);
            } else {
                schemaFieldList.add(schemaField);
                if (fieldValue.getValue() != null) {
                    recordMap.put(fieldName, fieldValue.getValue());
                }
            }
        }
    }

    private void handleFieldInSchema(RecordField schemaField, RecordWrapper wrappedRecord, List<FieldValue> fieldsToRemove,
                                     List<RecordField> schemaFieldList, List<String> fieldsToRemoveWithPath) {
        String fieldName = schemaField.getFieldName();
        if (!fieldNeedsToBeRemoved(fieldName, fieldsToRemove, wrappedRecord.getRecord())) {
            if (schemaField.getDataType() instanceof RecordDataType) {
                RecordSchema childSchema = ((RecordDataType) schemaField.getDataType()).getChildSchema();
                String path = wrappedRecord.getPath().equals("") ? ("/" + fieldName) : (wrappedRecord.getPath() + "/" + fieldName);
                RecordSchema newSchema = processSubRecordInSchema(childSchema, path, fieldsToRemoveWithPath);
                RecordField newSchemaField = new RecordField(fieldName, RecordFieldType.RECORD.getRecordDataType(newSchema));
                schemaFieldList.add(newSchemaField);
            } else {
                schemaFieldList.add(schemaField);
            }
        }
    }

    private boolean fieldNeedsToBeRemoved(String fieldName, List<FieldValue> fieldsToRemove, Record record) {
        for (FieldValue fieldToRemove : fieldsToRemove) {
            if (fieldToRemove.getField().getFieldName().equals(fieldName)
                    && fieldToRemove.getParentRecord().isPresent()
                    && fieldToRemove.getParentRecord().get().equals(record)) { // Have to make sure that only the current record's fields are examined.
                // If multiple fields with the same name are present, all will be removed regardless of their types.
                return true;
            }
        }
        return false;
    }

    private RecordSchema processSubRecordInSchema(RecordSchema schema, String path, List<String> fieldsToRemoveWithPath) {
        List<RecordField> newSchemaFieldList = new ArrayList<>();

        for (RecordField field : schema.getFields()) {
            String pathOfCurrentField = path + "/" + field.getFieldName();
            if (!fieldsToRemoveWithPath.contains(pathOfCurrentField)) {
                if (field.getDataType() instanceof RecordDataType) {
                    RecordSchema childSchema = ((RecordDataType) field.getDataType()).getChildSchema();
                    RecordSchema newSchema = processSubRecordInSchema(childSchema, pathOfCurrentField, fieldsToRemoveWithPath);
                    RecordField newSchemaField = new RecordField(field.getFieldName(), RecordFieldType.RECORD.getRecordDataType(newSchema));
                    newSchemaFieldList.add(newSchemaField);
                } else {
                    newSchemaFieldList.add(field);
                }
            }
        }

        return new SimpleRecordSchema(newSchemaFieldList);
    }

    @Override
    protected Record process(Record record, final FlowFile flowFile, final ProcessContext context, final long count) {
        final boolean evaluateValueAsRecordPath = context.getProperty(REPLACEMENT_VALUE_STRATEGY).getValue().equals(RECORD_PATH_VALUES.getValue());

        for (final String recordPathText : recordPaths) {
            final RecordPath recordPath = recordPathCache.getCompiled(recordPathText);
            final RecordPathResult result = recordPath.evaluate(record);

            if (evaluateValueAsRecordPath) {
                final String replacementValue = context.getProperty(recordPathText).evaluateAttributeExpressions(flowFile).getValue();
                if ("".equals(replacementValue)) {
                    List<FieldValue> selectedFields = result.getSelectedFields().collect(Collectors.toList());
                    record = remove(record, selectedFields);
                } else {
                    final RecordPath replacementRecordPath = recordPathCache.getCompiled(replacementValue);

                    // If we have an Absolute RecordPath, we need to evaluate the RecordPath only once against the Record.
                    // If the RecordPath is a Relative Path, then we have to evaluate it against each FieldValue.
                    if (replacementRecordPath.isAbsolute()) {
                        record = processAbsolutePath(replacementRecordPath, result.getSelectedFields(), record);
                    } else {
                        record = processRelativePath(replacementRecordPath, result.getSelectedFields(), record);
                    }
                }
            } else {
                final PropertyValue replacementValue = context.getProperty(recordPathText);

                if (replacementValue.isExpressionLanguagePresent()) {
                    final Map<String, String> fieldVariables = new HashMap<>();

                    result.getSelectedFields().forEach(fieldVal -> {
                        fieldVariables.clear();
                        fieldVariables.put(FIELD_NAME, fieldVal.getField().getFieldName());
                        fieldVariables.put(FIELD_VALUE, DataTypeUtils.toString(fieldVal.getValue(), (String) null));
                        fieldVariables.put(FIELD_TYPE, fieldVal.getField().getDataType().getFieldType().name());
                        fieldVariables.put(RECORD_INDEX, String.valueOf(count));

                        final String evaluatedReplacementVal = replacementValue.evaluateAttributeExpressions(flowFile, fieldVariables).getValue();
                        fieldVal.updateValue(evaluatedReplacementVal, RecordFieldType.STRING.getDataType());
                    });
                } else {
                    final String evaluatedReplacementVal = replacementValue.evaluateAttributeExpressions(flowFile).getValue();
                    result.getSelectedFields().forEach(fieldVal -> fieldVal.updateValue(evaluatedReplacementVal, RecordFieldType.STRING.getDataType()));
                }
            }
        }

        record.incorporateInactiveFields();

        return record;
    }

    private Record processAbsolutePath(final RecordPath replacementRecordPath, final Stream<FieldValue> destinationFields, final Record record) {
        final RecordPathResult replacementResult = replacementRecordPath.evaluate(record);
        final List<FieldValue> selectedFields = replacementResult.getSelectedFields().collect(Collectors.toList());
        final List<FieldValue> destinationFieldValues = destinationFields.collect(Collectors.toList());

        return updateRecord(destinationFieldValues, selectedFields, record);
    }

    private Record processRelativePath(final RecordPath replacementRecordPath, final Stream<FieldValue> destinationFields, Record record) {
        final List<FieldValue> destinationFieldValues = destinationFields.collect(Collectors.toList());

        for (final FieldValue fieldVal : destinationFieldValues) {
            final RecordPathResult replacementResult = replacementRecordPath.evaluate(record, fieldVal);
            final List<FieldValue> selectedFields = replacementResult.getSelectedFields().collect(Collectors.toList());
            final Object replacementObject = getReplacementObject(selectedFields);
            updateFieldValue(fieldVal, replacementObject);
        }

        return record;
    }

    private Record updateRecord(final List<FieldValue> destinationFields, final List<FieldValue> selectedFields, final Record record) {
        if (destinationFields.size() == 1 && !destinationFields.get(0).getParentRecord().isPresent()) {
            final Object replacement = getReplacementObject(selectedFields);
            if (replacement == null) {
                return record;
            }
            if (replacement instanceof Record) {
                return (Record) replacement;
            }

            final FieldValue replacementFieldValue = (FieldValue) replacement;
            if (replacementFieldValue.getValue() instanceof Record) {
                return (Record) replacementFieldValue.getValue();
            }

            final List<RecordField> fields = selectedFields.stream().map(FieldValue::getField).collect(Collectors.toList());
            final RecordSchema schema = new SimpleRecordSchema(fields);
            final Record mapRecord = new MapRecord(schema, new HashMap<>());
            for (final FieldValue selectedField : selectedFields) {
                mapRecord.setValue(selectedField.getField(), selectedField.getValue());
            }

            return mapRecord;
        } else {
            for (final FieldValue fieldVal : destinationFields) {
                final Object replacementObject = getReplacementObject(selectedFields);
                updateFieldValue(fieldVal, replacementObject);
            }
            return record;
        }
    }

    private void updateFieldValue(final FieldValue fieldValue, final Object replacement) {
        if (replacement instanceof FieldValue) {
            final FieldValue replacementFieldValue = (FieldValue) replacement;
            fieldValue.updateValue(replacementFieldValue.getValue(), replacementFieldValue.getField().getDataType());
        } else {
            fieldValue.updateValue(replacement);
        }
    }

    private Object getReplacementObject(final List<FieldValue> selectedFields) {
        if (selectedFields.size() > 1) {
            final List<RecordField> fields = selectedFields.stream().map(FieldValue::getField).collect(Collectors.toList());
            final RecordSchema schema = new SimpleRecordSchema(fields);
            final Record record = new MapRecord(schema, new HashMap<>());
            for (final FieldValue fieldVal : selectedFields) {
                record.setValue(fieldVal.getField(), fieldVal.getValue());
            }

            return record;
        }

        if (selectedFields.isEmpty()) {
            return null;
        } else {
            return selectedFields.get(0);
        }
    }
}
