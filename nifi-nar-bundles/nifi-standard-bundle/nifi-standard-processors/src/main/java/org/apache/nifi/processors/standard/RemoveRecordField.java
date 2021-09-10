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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.Filters;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;


@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"update", "record", "generic", "schema", "json", "csv", "avro", "freeform", "text", "remove", "delete"})
@CapabilityDescription("Modifies the contents of a FlowFile that contains Record-oriented data (i.e., data that can be read via a RecordReader and written by a RecordWriter) "
        + "by removing selected fields. "
        + "This Processor requires that at least one user-defined Property be added. The name of the Property should indicate a RecordPath that determines the field that should "
        + "be removed.")
@WritesAttributes({
        @WritesAttribute(attribute = "record.index", description = "This attribute provides the current row index and is only available inside the literal value expression."),
        @WritesAttribute(attribute = "record.error.message", description = "This attribute provides on failure the error message encountered by the Reader or Writer.")
})
@SeeAlso({UpdateRecord.class, ConvertRecord.class})
public class RemoveRecordField extends AbstractRecordProcessor {

    private volatile List<String> recordPaths;

    public static final PropertyDescriptor FIELD_TO_REMOVE_1 = new PropertyDescriptor.Builder()
            .name("field-to-remove-1")
            .displayName("Field To Remove 1")
            .description("The path to the field that needs to be removed.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(FIELD_TO_REMOVE_1);
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .displayName(propertyDescriptorName)
                .description("The path to the field that needs to be removed.")
                .required(false)
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                .addValidator(StandardValidators.NON_BLANK_VALIDATOR) //TODO: add validator
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final boolean containsDynamic = validationContext.getProperties().keySet().stream().anyMatch(PropertyDescriptor::isDynamic);

        if (containsDynamic) {
            return Collections.emptyList();
        } else {
            return Collections.emptyList(); //TODO: not ready
        }
    }

    @OnScheduled
    public void collectRecordPaths(final ProcessContext context) {
        final List<String> recordPaths = new ArrayList<>(context.getProperties().size() - 2);
        for (final PropertyDescriptor property : context.getProperties().keySet()) {
            if (property.getName().equals("field-to-remove-1") || property.isDynamic()) {
                String path = context.getProperty(property).evaluateAttributeExpressions().getValue();
                recordPaths.add(path);
            }
        }

        this.recordPaths = recordPaths;
    }

    @Override
    protected Record process(Record record, FlowFile flowFile, ProcessContext context, long count) {
        for (String recordPath : recordPaths) {
            recordPath = preprocessRecordPath(recordPath);
            RecordPathResult recordPathResult = RecordPath.compile(recordPath).evaluate(record);
            List<FieldValue> selectedFields = recordPathResult.getSelectedFields().collect(Collectors.toList());

            boolean modifySchema = allSquareBracketsContainAsteriskOnly(recordPath);

            if (recordPath.endsWith("[*]") || recordPath.endsWith("[0..-1]")) {
                if (!selectedFields.isEmpty()) {
                    Optional<FieldValue> parentOptional = selectedFields.get(0).getParent();
                    if (parentOptional.isPresent()) {
                        FieldValue parent = parentOptional.get();
                        if (Filters.isArray(parent)) {
                            parent.updateValue(new Object[0]);
                        } else if (Filters.isMap(parent)) {
                            parent.updateValue(Collections.emptyMap());
                        }
                    }
                }
            } else {
                for (FieldValue field : selectedFields) {
                    field.remove(modifySchema);
                }
            }

            if (modifySchema) {
                List<List<String>> concretePaths = getConcretePaths(selectedFields);
                removePathsFromSchema(record, concretePaths);
            }
        }

        return record;
    }

    private void removePathsFromSchema(Record record, List<List<String>> paths) {
        for (List<String> path : paths) {
            RecordSchema schema = record.getSchema();
            removePathFromSchema(schema, path);
        }
    }

    private void removePathFromSchema(RecordSchema schema, List<String> path) {
        if (path.size() == 0) {
            return;
        } else if (path.size() == 1) {
            schema.removeField(path.get(0));
        } else {
            Optional<RecordField> fieldOptional = schema.getField(path.get(0));
            if (fieldOptional.isPresent()) {
                RecordField field = fieldOptional.get();
                if (path.size() == 2) {
                    DataType dataType = field.getDataType();
                    if (dataType instanceof RecordDataType) {
                        RecordSchema childSchema = ((RecordDataType) dataType).getChildSchema();
                        childSchema.removeField(path.get(1));
                    } else if (dataType instanceof ChoiceDataType) {
                        removePathFromChoiceDataType((ChoiceDataType) dataType, path.subList(1, path.size()));
                    }
                } else { // path.size() > 2
                    DataType dataType = field.getDataType();
                    if (dataType instanceof RecordDataType) {
                        RecordSchema childSchema = ((RecordDataType) dataType).getChildSchema();
                        removePathFromSchema(childSchema, path.subList(1,path.size()));
                    } else if (dataType instanceof ChoiceDataType) {
                        removePathFromChoiceDataType((ChoiceDataType) dataType, path.subList(1, path.size()));
                    }
                }
            }
        }
    }

    private void removePathFromChoiceDataType(ChoiceDataType choiceDataType, List<String> path) {
        if (path.size() == 0) {
            return;
        } else if (path.size() == 1) {
            for (DataType subType : choiceDataType.getPossibleSubTypes()) {
                if (subType instanceof RecordDataType) {
                    RecordSchema childSchema = ((RecordDataType) subType).getChildSchema();
                    removePathFromSchema(childSchema, path);
                }
            }
        } else {
            List<DataType> possibleSubTypes = choiceDataType.getPossibleSubTypes();
            for (DataType subType : possibleSubTypes) {
                if (subType instanceof RecordDataType) {
                    RecordSchema childSchema = ((RecordDataType) subType).getChildSchema();
                    removePathFromSchema(childSchema, path);
                } else if (subType instanceof ChoiceDataType) {
                    removePathFromChoiceDataType((ChoiceDataType) subType, path);
                }
            }
        }
    }

    private List<List<String>> getConcretePaths(List<FieldValue> selectedFields) {
        List<List<String>> paths = new ArrayList<>(selectedFields.size());
        for (FieldValue field : selectedFields) {
            List<String> path = new ArrayList<>();
            path.add(field.getField().getFieldName());

            Optional<FieldValue> parentOptional = field.getParent();
            while (parentOptional.isPresent()) {
                FieldValue parent = parentOptional.get();
                if (!parent.getField().getFieldName().equals("root")) {
                    path.add(parent.getField().getFieldName());
                }
                parentOptional = parent.getParent();
            }

            Collections.reverse(path);
            paths.add(path);
        }
        return paths;
    }

    private String preprocessRecordPath(String recordPath) {
        if (recordPath.endsWith("]")) {
            String lastSquareBracketsOperator = getLastSquareBracketsOperator(recordPath);
            if (lastSquareBracketsOperator.equals("[*]")) {
                return recordPath.substring(0, recordPath.lastIndexOf('[')) + "[*]";
            } else if (lastSquareBracketsOperator.equals("[0..-1]")) {
                return recordPath.substring(0, recordPath.lastIndexOf('[')) + "[0..-1]";
            }
        }
        return recordPath;
    }

    private String getLastSquareBracketsOperator(String recordPath) {
        int beginIndex = recordPath.lastIndexOf('[');
        return recordPath.substring(beginIndex).replaceAll("\\s","");
    }

    private boolean allSquareBracketsContainAsteriskOnly(String recordPath) {
        boolean allSquareBracketsContainAsteriskOnly = true;
        boolean inSquareBrackets = false;
        for (int i = 0; i < recordPath.length() && allSquareBracketsContainAsteriskOnly; ++i) {
            char character = recordPath.charAt(i);
            if (inSquareBrackets) {
                switch (character) {
                    case ' ':
                    case '*':
                        break;
                    case ']':
                        inSquareBrackets = false;
                        break;
                    default:
                        allSquareBracketsContainAsteriskOnly = false;
                }
            } else {
                if (character == '[') {
                    inSquareBrackets = true;
                }
            }
        }
        return allSquareBracketsContainAsteriskOnly;
    }
}
