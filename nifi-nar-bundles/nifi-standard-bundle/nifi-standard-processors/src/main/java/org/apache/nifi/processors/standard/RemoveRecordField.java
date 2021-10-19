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
import org.apache.nifi.record.path.RecordFieldRemover;
import org.apache.nifi.serialization.record.Record;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"update", "record", "generic", "schema", "json", "csv", "avro", "freeform", "text", "remove", "delete"})
@CapabilityDescription("Modifies the contents of a FlowFile that contains Record-oriented data (i.e., data that can be read via a RecordReader and written by a RecordWriter) "
        + "by removing selected fields. "
        + "This Processor requires that at least one user-defined Property be added. The value of the Property should indicate a RecordPath that determines the field that should "
        + "be removed.")
@WritesAttributes({
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
            if (property.getName().equals(FIELD_TO_REMOVE_1.getName()) || property.isDynamic()) {
                String path = context.getProperty(property).evaluateAttributeExpressions().getValue();
                recordPaths.add(path);
            }
        }

        this.recordPaths = recordPaths;
    }

    @Override
    protected Record process(Record record, FlowFile flowFile, ProcessContext context, long count) {
        for (String recordPath : recordPaths) {
            RecordFieldRemover recordFieldRemover = new RecordFieldRemover(record);
            recordFieldRemover.remove(recordPath);
            record = recordFieldRemover.getRecord();
        }
        return record;
    }
}
