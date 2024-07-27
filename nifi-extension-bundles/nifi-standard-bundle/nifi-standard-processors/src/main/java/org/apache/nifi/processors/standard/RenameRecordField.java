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
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.record.path.validation.RecordPathPropertyNameValidator;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SideEffectFree
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"update", "record", "rename", "field", "generic", "schema", "json", "csv", "avro", "log", "logs"})
@CapabilityDescription("Renames one or more fields in each Record of a FlowFile. "
    + "This Processor requires that at least one user-defined Property be added. The name of the Property should indicate a RecordPath that determines the field that should "
    + "be updated. The value of the Property is the new name to assign to the Record Field that matches the RecordPath. The property value may use Expression Language to reference "
    + "FlowFile attributes as well as the variables `field.name`, `field.value`, `field.type`, and `record.index`")
@WritesAttributes({
    @WritesAttribute(attribute = "record.index", description = "This attribute provides the current row index and is only available inside the literal value expression.")
})
@DynamicProperty(name = "A RecordPath that identifies which field(s) to update",
    value = "The new name to assign to the Record field",
    description = "Allows users to specify a new name for each field that matches the RecordPath.",
    expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
@SeeAlso({UpdateRecord.class, RemoveRecordField.class})
@UseCase(
    description = "Rename a field in each Record to a specific, known name.",
    keywords = {"rename", "field", "static", "specific", "name"},
    configuration = """
        Configure the 'Record Reader' according to the input format.
        Configure the 'Record Writer' according to the desired output format.

        Add a property to the Processor such that the name of the property is a RecordPath to identifies the field to rename. \
        The value of the property is the new name of the property.

        For example, to rename the `name` field to `full_name`, add a property with a name of `/name` and a value of `full_name`.

        Many properties can be added following this pattern in order to rename multiple fields.
        """
)
@UseCase(
    description = "Rename a field in each Record to a name that is derived from a FlowFile attribute.",
    keywords = {"rename", "field", "expression language", "EL", "flowfile", "attribute"},
    configuration = """
        Configure the 'Record Reader' according to the input format.
        Configure the 'Record Writer' according to the desired output format.

        Add a property to the Processor such that the name of the property is a RecordPath to identifies the field to rename. \
        The value of the property is an Expression Language expression that can be used to determine the new name of the field.

        For example, to rename the `addr` field to whatever value is stored in the `preferred_address_name` attribute, \
        add a property with a name of `/name` and a value of `${preferred_address_name}`.

        Many properties can be added following this pattern in order to rename multiple fields.
        """
)
@UseCase(
    description = "Rename a field in each Record to a new name that is derived from the current field name.",
    notes = "This might be used, for example, to add a prefix or a suffix to some fields, or to transform the name of the field by making it uppercase.",
    keywords = {"rename", "field", "expression language", "EL", "field.name"},
    configuration = """
        Configure the 'Record Reader' according to the input format.
        Configure the 'Record Writer' according to the desired output format.

        Add a property to the Processor such that the name of the property is a RecordPath to identifies the field to rename. \
        The value of the property is an Expression Language expression that references the `field.name` property.

        For example, to rename all fields with a prefix of `pre_`, we add a property named `/*` and a value of `pre_${field.name}`. \
        If we would like this to happen recursively, to nested fields as well, we use a property name of `//*` with the value of `pre_${field.name}`.

        To make all field names uppercase, we can add a property named `//*` with a value of `${field.name:toUpper()}`.

        Many properties can be added following this pattern in order to rename multiple fields.
        """
)
public class RenameRecordField extends AbstractRecordProcessor {

    private static final String FIELD_NAME = "field.name";
    private static final String FIELD_VALUE = "field.value";
    private static final String FIELD_TYPE = "field.type";

    private static final String RECORD_INDEX = "record.index";

    private volatile RecordPathCache recordPathCache;
    private volatile List<String> recordPaths;


    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .description("Specifies the new name to use for any record field that match the RecordPath: " + propertyDescriptorName)
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
        recordPathCache = new RecordPathCache(context.getProperties().size());

        final List<String> recordPaths = new ArrayList<>(context.getProperties().size() - 2);
        for (final PropertyDescriptor property : context.getProperties().keySet()) {
            if (property.isDynamic()) {
                recordPaths.add(property.getName());
            }
        }

        this.recordPaths = recordPaths;
    }


    @Override
    protected Record process(final Record record, final FlowFile flowFile, final ProcessContext context, final long count) {
        for (final String propertyName : recordPaths) {
            final RecordPath recordPath = recordPathCache.getCompiled(propertyName);
            final RecordPathResult result = recordPath.evaluate(record);

            final PropertyValue newFieldNamePropertyValue = context.getProperty(propertyName);

            if (newFieldNamePropertyValue.isExpressionLanguagePresent()) {
                final Map<String, String> fieldVariables = new HashMap<>();

                result.getSelectedFields().forEach(fieldVal -> {
                    fieldVariables.clear();
                    fieldVariables.put(FIELD_NAME, fieldVal.getField().getFieldName());
                    fieldVariables.put(FIELD_VALUE, DataTypeUtils.toString(fieldVal.getValue(), (String) null));
                    fieldVariables.put(FIELD_TYPE, fieldVal.getField().getDataType().getFieldType().name());
                    fieldVariables.put(RECORD_INDEX, String.valueOf(count));

                    final String newFieldName = newFieldNamePropertyValue.evaluateAttributeExpressions(flowFile, fieldVariables).getValue();
                    fieldVal.getParentRecord().ifPresent(parentRecord -> {
                        parentRecord.rename(fieldVal.getField(), newFieldName);
                    });
                });
            } else {
                final String newFieldName = newFieldNamePropertyValue.evaluateAttributeExpressions(flowFile).getValue();

                result.getSelectedFields().forEach(fieldVal -> {
                    fieldVal.getParentRecord().ifPresent(parentRecord -> {
                        parentRecord.rename(fieldVal.getField(), newFieldName);
                    });
                });
            }
        }

        return record;
    }
}
