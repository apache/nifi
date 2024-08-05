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

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.migration.PropertyConfiguration;
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
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
@DynamicProperty(name = "A RecordPath.", value = "The value to use to replace fields in the record that match the RecordPath",
    description = "Allows users to specify values to use to replace fields in the record that match the RecordPath.",
    expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
@SeeAlso({ConvertRecord.class})
@UseCase(
    description = "Combine multiple fields into a single field.",
    keywords = {"combine", "concatenate", "recordpath"},
    configuration = """
        "Replacement Value Strategy" = "Record Path Value"

        A single additional property is added to the Processor. The name of the property is a RecordPath identifying the field to place the result in.
        The value of the property uses the CONCAT Record Path function to concatenate multiple values together, potentially using other string literal values.
        For example, to combine the `title`, `firstName` and `lastName` fields into a single field named `fullName`, we add a property with the name `/fullName` \
        and a value of `CONCAT(/title, ' ', /firstName, ' ', /lastName)`
        """
)
@UseCase(
    description = "Change the value of a record field to an explicit value.",
    keywords = {"change", "update", "replace", "transform"},
    configuration = """
        "Replacement Value Strategy" = "Literal Value"

        A single additional property is added to the Processor. The name of the property is a RecordPath identifying the field to place the result in.
        The value of the property is the explicit value to set the field to. For example, we can set any field with a name of `txId`, regardless of its level in the data's hierarchy, \
        to `1111-1111` by adding a property with a name of `//txId` and a value of `1111-1111`
    """
)
@UseCase(
    description = "Copy the value of one record field to another record field.",
    keywords = {"change", "update", "copy", "recordpath", "hierarchy", "transform"},
    configuration = """
            "Replacement Value Strategy" = "Record Path Value"

            A single additional property is added to the Processor. The name of the property is a RecordPath identifying the field to update.
            The value of the property is a RecordPath identifying the field to copy the value from.
            For example, we can copy the value of `/identifiers/all/imei` to the `identifier` field at the root level, by adding a property named \
            `/identifier` with a value of `/identifiers/all/imei`.
        """
)
@UseCase(
    description = "Enrich data by injecting the value of an attribute into each Record.",
    keywords = {"enrich", "attribute", "change", "update", "replace", "insert", "transform"},
    configuration = """
        "Replacement Value Strategy" = "Literal Value"

        A single additional property is added to the Processor. The name of the property is a RecordPath identifying the field to place the result in.
        The value of the property is an Expression Language expression that references the attribute of interest. We can, for example, insert a new field name \
        `filename` into each record by adding a property named `/filename` with a value of `${filename}`
        """
)
@UseCase(
    description = "Change the format of a record field's value.",
    keywords = {"change", "update", "replace", "insert", "transform", "format", "date/time", "timezone", "expression language"},
    notes = "Use the RenameRecordField Processor in order to change a field's name.",
    configuration = """
        "Replacement Value Strategy" = "Literal Value"

        A single additional property is added to the Processor. The name of the property is a RecordPath identifying the field to update.
        The value is an Expression Language expression that references the `field.value` variable. For example, to change the date/time format of \
        a field named `txDate` from `year-month-day` format to `month/day/year` format, we add a property named `/txDate` with a value of \
        `${field.value:toDate('yyyy-MM-dd'):format('MM/dd/yyyy')}`. We could also change the timezone of a timestamp field (and insert the timezone for clarity) by using a value of \
        `${field.value:toDate('yyyy-MM-dd HH:mm:ss', 'UTC-0400'):format('yyyy-MM-dd HH:mm:ss Z', 'UTC')}`.
        """
)
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
        .name("Replacement Value Strategy")
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
    public void migrateProperties(final PropertyConfiguration config) {
        super.migrateProperties(config);
        config.renameProperty("replacement-value-strategy", REPLACEMENT_VALUE_STRATEGY.getName());
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

        return Set.of(new ValidationResult.Builder()
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

    @Override
    protected Record process(Record record, final FlowFile flowFile, final ProcessContext context, final long count) {
        final boolean evaluateValueAsRecordPath = context.getProperty(REPLACEMENT_VALUE_STRATEGY).getValue().equals(RECORD_PATH_VALUES.getValue());

        for (final String recordPathText : recordPaths) {
            final RecordPath recordPath = recordPathCache.getCompiled(recordPathText);
            final RecordPathResult result = recordPath.evaluate(record);

            if (evaluateValueAsRecordPath) {
                final String replacementValue = context.getProperty(recordPathText).evaluateAttributeExpressions(flowFile).getValue();
                final RecordPath replacementRecordPath = recordPathCache.getCompiled(replacementValue);

                // If we have an Absolute RecordPath, we need to evaluate the RecordPath only once against the Record.
                // If the RecordPath is a Relative Path, then we have to evaluate it against each FieldValue.
                if (replacementRecordPath.isAbsolute()) {
                    record = processAbsolutePath(replacementRecordPath, result.getSelectedFields(), record);
                } else {
                    record = processRelativePath(replacementRecordPath, result.getSelectedFields(), record);
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
        final List<FieldValue> selectedFields = getSelectedFields(replacementRecordPath, null, record);
        final List<FieldValue> destinationFieldValues = destinationFields.collect(Collectors.toList());

        return updateRecord(destinationFieldValues, selectedFields, record);
    }

    private boolean isReplacingRoot(final List<FieldValue> destinationFields) {
        return destinationFields.size() == 1 && destinationFields.getFirst().getParentRecord().isEmpty();
    }

    private Record processRelativePath(final RecordPath replacementRecordPath, final Stream<FieldValue> destinationFields, Record record) {
        final List<FieldValue> destinationFieldValues = destinationFields.collect(Collectors.toList());

        if (isReplacingRoot(destinationFieldValues)) {
            final List<FieldValue> selectedFields = getSelectedFields(replacementRecordPath, destinationFieldValues.getFirst(), record);
            record = updateRecord(destinationFieldValues, selectedFields, record);
        } else {
            for (final FieldValue fieldVal : destinationFieldValues) {
                final List<FieldValue> selectedFields = getSelectedFields(replacementRecordPath, fieldVal, record);
                final Object replacementObject = getReplacementObject(selectedFields);
                updateFieldValue(fieldVal, replacementObject);
            }
        }

        return record;
    }

    private Record updateRecord(final List<FieldValue> destinationFields, final List<FieldValue> selectedFields, final Record record) {
        if (isReplacingRoot(destinationFields)) {
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
        if (replacement instanceof FieldValue replacementFieldValue) {
            fieldValue.updateValue(replacementFieldValue.getValue(), replacementFieldValue.getField().getDataType());
        } else {
            fieldValue.updateValue(replacement);
        }
    }

    private List<FieldValue> getSelectedFields(final RecordPath replacementRecordPath, final FieldValue fieldValue, final Record record) {
        final RecordPathResult replacementResult = replacementRecordPath.evaluate(record, fieldValue);
        return replacementResult.getSelectedFields().collect(Collectors.toList());
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
            return selectedFields.getFirst();
        }
    }
}

