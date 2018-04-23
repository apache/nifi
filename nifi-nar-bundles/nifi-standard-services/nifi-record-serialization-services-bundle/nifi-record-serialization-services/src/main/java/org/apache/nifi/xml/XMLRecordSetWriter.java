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

package org.apache.nifi.xml;

import org.apache.nifi.record.NullSuppression;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.DateTimeTextRecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Tags({"xml", "resultset", "writer", "serialize", "record", "recordset", "row"})
@CapabilityDescription("Writes a RecordSet to XML. The records are wrapped by a root tag.")
public class XMLRecordSetWriter extends DateTimeTextRecordSetWriter implements RecordSetWriterFactory {

    public static final AllowableValue ALWAYS_SUPPRESS = new AllowableValue("always-suppress", "Always Suppress",
            "Fields that are missing (present in the schema but not in the record), or that have a value of null, will not be written out");
    public static final AllowableValue NEVER_SUPPRESS = new AllowableValue("never-suppress", "Never Suppress",
            "Fields that are missing (present in the schema but not in the record), or that have a value of null, will be written out as a null value");
    public static final AllowableValue SUPPRESS_MISSING = new AllowableValue("suppress-missing", "Suppress Missing Values",
            "When a field has a value of null, it will be written out. However, if a field is defined in the schema and not present in the record, the field will not be written out.");

    public static final AllowableValue USE_PROPERTY_AS_WRAPPER = new AllowableValue("use-property-as-wrapper", "Use Property as Wrapper",
            "The value of the property \"Array Tag Name\" will be used as the tag name to wrap elements of an array. The field name of the array field will be used for the tag name " +
                    "of the elements.");
    public static final AllowableValue USE_PROPERTY_FOR_ELEMENTS = new AllowableValue("use-property-for-elements", "Use Property for Elements",
            "The value of the property \"Array Tag Name\" will be used for the tag name of the elements of an array. The field name of the array field will be used as the tag name " +
                    "to wrap elements.");
    public static final AllowableValue NO_WRAPPING = new AllowableValue("no-wrapping", "No Wrapping",
            "The elements of an array will not be wrapped");

    public static final PropertyDescriptor SUPPRESS_NULLS = new PropertyDescriptor.Builder()
            .name("suppress_nulls")
            .displayName("Suppress Null Values")
            .description("Specifies how the writer should handle a null field")
            .allowableValues(NEVER_SUPPRESS, ALWAYS_SUPPRESS, SUPPRESS_MISSING)
            .defaultValue(NEVER_SUPPRESS.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor PRETTY_PRINT_XML = new PropertyDescriptor.Builder()
            .name("pretty_print_xml")
            .displayName("Pretty Print XML")
            .description("Specifies whether or not the XML should be pretty printed")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor ROOT_TAG_NAME = new PropertyDescriptor.Builder()
            .name("root_tag_name")
            .displayName("Name of Root Tag")
            .description("Specifies the name of the XML root tag wrapping the record set. This property has to be defined if " +
                    "the writer is supposed to write multiple records in a single FlowFile.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .build();

    public static final PropertyDescriptor RECORD_TAG_NAME = new PropertyDescriptor.Builder()
            .name("record_tag_name")
            .displayName("Name of Record Tag")
            .description("Specifies the name of the XML record tag wrapping the record fields. If this is not set, the writer " +
                    "will use the record name in the schema.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .build();

    public static final PropertyDescriptor ARRAY_WRAPPING = new PropertyDescriptor.Builder()
            .name("array_wrapping")
            .displayName("Wrap Elements of Arrays")
            .description("Specifies how the writer wraps elements of fields of type array")
            .allowableValues(USE_PROPERTY_AS_WRAPPER, USE_PROPERTY_FOR_ELEMENTS, NO_WRAPPING)
            .defaultValue(NO_WRAPPING.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor ARRAY_TAG_NAME = new PropertyDescriptor.Builder()
            .name("array_tag_name")
            .displayName("Array Tag Name")
            .description("Name of the tag used by property \"Wrap Elements of Arrays\" to write arrays")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .build();

    public static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("The Character set to use when writing the data to the FlowFile")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(SUPPRESS_NULLS);
        properties.add(PRETTY_PRINT_XML);
        properties.add(ROOT_TAG_NAME);
        properties.add(RECORD_TAG_NAME);
        properties.add(ARRAY_WRAPPING);
        properties.add(ARRAY_TAG_NAME);
        properties.add(CHARACTER_SET);
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        if (!validationContext.getProperty(ARRAY_WRAPPING).getValue().equals(NO_WRAPPING.getValue())) {
            if (!validationContext.getProperty(ARRAY_TAG_NAME).isSet()) {
                StringBuilder explanation = new StringBuilder()
                        .append("if property \'")
                        .append(ARRAY_WRAPPING.getName())
                        .append("\' is defined as \'")
                        .append(USE_PROPERTY_AS_WRAPPER.getDisplayName())
                        .append("\' or \'")
                        .append(USE_PROPERTY_FOR_ELEMENTS.getDisplayName())
                        .append("\' the property \'")
                        .append(ARRAY_TAG_NAME.getDisplayName())
                        .append("\' has to be set.");

                return Collections.singleton(new ValidationResult.Builder()
                        .subject(ARRAY_TAG_NAME.getName())
                        .valid(false)
                        .explanation(explanation.toString())
                        .build());
            }
        }
        return Collections.emptyList();
    }

    @Override
    public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema schema, final OutputStream out) throws SchemaNotFoundException, IOException {

        final String nullSuppression = getConfigurationContext().getProperty(SUPPRESS_NULLS).getValue();
        final NullSuppression nullSuppressionEnum;
        if (nullSuppression.equals(ALWAYS_SUPPRESS.getValue())) {
            nullSuppressionEnum = NullSuppression.ALWAYS_SUPPRESS;
        } else if (nullSuppression.equals(NEVER_SUPPRESS.getValue())) {
            nullSuppressionEnum = NullSuppression.NEVER_SUPPRESS;
        } else {
            nullSuppressionEnum = NullSuppression.SUPPRESS_MISSING;
        }

        final boolean prettyPrint = getConfigurationContext().getProperty(PRETTY_PRINT_XML).getValue().equals("true");

        final String rootTagName = getConfigurationContext().getProperty(ROOT_TAG_NAME).isSet()
                ? getConfigurationContext().getProperty(ROOT_TAG_NAME).getValue() : null;
        final String recordTagName = getConfigurationContext().getProperty(RECORD_TAG_NAME).isSet()
                ? getConfigurationContext().getProperty(RECORD_TAG_NAME).getValue() : null;

        final String arrayWrapping = getConfigurationContext().getProperty(ARRAY_WRAPPING).getValue();
        final ArrayWrapping arrayWrappingEnum;
        if (arrayWrapping.equals(NO_WRAPPING.getValue())) {
            arrayWrappingEnum = ArrayWrapping.NO_WRAPPING;
        } else if (arrayWrapping.equals(USE_PROPERTY_AS_WRAPPER.getValue())) {
            arrayWrappingEnum = ArrayWrapping.USE_PROPERTY_AS_WRAPPER;
        } else {
            arrayWrappingEnum = ArrayWrapping.USE_PROPERTY_FOR_ELEMENTS;
        }

        final String arrayTagName;
        if (getConfigurationContext().getProperty(ARRAY_TAG_NAME).isSet()) {
            arrayTagName = getConfigurationContext().getProperty(ARRAY_TAG_NAME).getValue();
        } else {
            arrayTagName = null;
        }

        final String charSet = getConfigurationContext().getProperty(CHARACTER_SET).getValue();

        return new WriteXMLResult(logger, schema, getSchemaAccessWriter(schema),
                out, prettyPrint, nullSuppressionEnum, arrayWrappingEnum, arrayTagName, rootTagName, recordTagName, charSet,
                getDateFormat().orElse(null), getTimeFormat().orElse(null), getTimestampFormat().orElse(null));
    }
}
